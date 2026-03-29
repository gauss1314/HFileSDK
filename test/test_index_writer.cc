#include <gtest/gtest.h>
#include "index/block_index_writer.h"
#include <hfile/types.h>
#include <vector>
#include <cstring>

using namespace hfile;
using namespace hfile::index;

// Helper: call finish() and return only the root payload (ignores intermed)
static IndexWriteResult finish_simple(BlockIndexWriter& w,
                                       std::vector<uint8_t>& root_out) {
    std::vector<uint8_t> intermed;
    return w.finish(0, intermed, root_out);
}

struct ParsedRootEntry {
    int64_t offset;
    uint32_t data_size;
    std::string key;
    size_t bytes;
};

static ParsedRootEntry parse_root_entry(const std::vector<uint8_t>& root, size_t off) {
    ParsedRootEntry out;
    out.offset = static_cast<int64_t>(read_be64(root.data() + off));
    out.data_size = read_be32(root.data() + off + 8);
    int64_t key_len = 0;
    int vint_size = decode_writable_vint(root.data() + off + 12, key_len);
    out.key.assign(reinterpret_cast<const char*>(root.data() + off + 12 + vint_size),
                   static_cast<size_t>(key_len));
    out.bytes = 12 + static_cast<size_t>(vint_size) + static_cast<size_t>(key_len);
    return out;
}

// ─── 1-level (inline root) ────────────────────────────────────────────────────

TEST(BlockIndexWriter, EmptyOneLevel) {
    BlockIndexWriter w(128);
    std::vector<uint8_t> root;
    auto r = finish_simple(w, root);
    EXPECT_EQ(r.num_root_entries, 0u);
    EXPECT_EQ(r.num_levels, 1);
    EXPECT_TRUE(root.empty());
}

TEST(BlockIndexWriter, SingleEntryOneLevel) {
    BlockIndexWriter w(128);
    const uint8_t key[] = {'r','o','w','1'};
    w.add_entry({key, 4}, /*offset=*/33, /*data_size=*/65536);

    std::vector<uint8_t> root;
    auto r = finish_simple(w, root);
    EXPECT_EQ(r.num_root_entries, 1u);
    EXPECT_EQ(r.num_levels, 1);

    auto entry = parse_root_entry(root, 0);
    EXPECT_EQ(entry.offset, 33);
    EXPECT_EQ(entry.data_size, 65536u);
    EXPECT_EQ(entry.key, "row1");
    EXPECT_EQ(root.size(), entry.bytes);
}

TEST(BlockIndexWriter, BelowThresholdIsOneLevel) {
    BlockIndexWriter w(128);  // threshold = 128
    for (int i = 0; i < 100; ++i) {
        std::string k = "key" + std::to_string(i);
        std::vector<uint8_t> kb(k.begin(), k.end());
        w.add_entry(kb, i * 65536, 65536);
    }
    std::vector<uint8_t> root;
    auto r = finish_simple(w, root);
    EXPECT_EQ(r.num_levels, 1);
    EXPECT_EQ(r.num_root_entries, 100u);
    size_t off = 0;
    for (int i = 0; i < 100; ++i) {
        auto entry = parse_root_entry(root, off);
        EXPECT_EQ(entry.offset, static_cast<int64_t>(i * 65536));
        EXPECT_EQ(entry.data_size, 65536u);
        off += entry.bytes;
    }
    EXPECT_EQ(off, root.size());
}

// ─── 2-level (intermediate + root) ───────────────────────────────────────────

TEST(BlockIndexWriter, ExceedsThresholdIsTwoLevel) {
    const size_t threshold = 10;
    BlockIndexWriter w(threshold);

    // 25 entries → ceil(25/10) = 3 intermediate blocks → 3 root entries
    for (int i = 0; i < 25; ++i) {
        std::string k = "row" + std::to_string(i);
        std::vector<uint8_t> kb(k.begin(), k.end());
        w.add_entry(kb, static_cast<int64_t>(i) * 65536, 65536);
    }

    std::vector<uint8_t> intermed;
    std::vector<uint8_t> root;
    auto r = w.finish(0, intermed, root);

    EXPECT_EQ(r.num_levels, 2);
    EXPECT_EQ(r.num_root_entries, 3u);          // 3 intermediate blocks

    // Intermediate buffer must be non-empty
    EXPECT_GT(intermed.size(), 0u);

    auto entry0 = parse_root_entry(root, 0);
    auto entry1 = parse_root_entry(root, entry0.bytes);
    auto entry2 = parse_root_entry(root, entry0.bytes + entry1.bytes);
    EXPECT_EQ(entry0.key, "row0");
    EXPECT_EQ(entry1.key, "row10");
    EXPECT_EQ(entry2.key, "row20");

    // First intermediate block should start with IDXINTE2 magic
    EXPECT_EQ(std::memcmp(intermed.data(),
                           kIntermedIdxMagic.data(), 8), 0);
}

TEST(BlockIndexWriter, TwoLevelIntermediateOffsets) {
    const size_t threshold = 4;
    BlockIndexWriter w(threshold);

    for (int i = 0; i < 8; ++i) {
        std::string k = std::string(4, static_cast<char>('a' + i));
        std::vector<uint8_t> kb(k.begin(), k.end());
        w.add_entry(kb, static_cast<int64_t>(i * 1000), 512);
    }

    // Place intermediate blocks at a known file position
    const int64_t intermed_start = 999999;
    std::vector<uint8_t> intermed;
    std::vector<uint8_t> root;
    auto r = w.finish(intermed_start, intermed, root);

    EXPECT_EQ(r.num_levels, 2);
    EXPECT_EQ(r.num_root_entries, 2u);  // ceil(8/4) = 2 blocks

    auto entry0 = parse_root_entry(root, 0);
    auto entry1 = parse_root_entry(root, entry0.bytes);
    EXPECT_EQ(entry0.offset, intermed_start);

    uint32_t first_block_size = entry0.data_size;
    EXPECT_EQ(entry1.offset, intermed_start + static_cast<int64_t>(first_block_size));

    const uint8_t* payload = intermed.data() + kBlockHeaderSize;
    EXPECT_EQ(read_be32(payload), 4u);
    EXPECT_EQ(read_be32(payload + 4), 0u);
    EXPECT_EQ(read_be32(payload + 8), 16u);
    EXPECT_EQ(read_be32(payload + 12), 32u);
    EXPECT_EQ(read_be32(payload + 16), 48u);
    EXPECT_EQ(read_be32(payload + 20), 64u);
}

TEST(BlockIndexWriter, TwoLevelFirstKeysCorrect) {
    const size_t threshold = 3;
    BlockIndexWriter w(threshold);

    // Keys: "aaa","bbb","ccc","ddd","eee","fff"  → 2 intermediate blocks
    const char* keys[] = {"aaa","bbb","ccc","ddd","eee","fff"};
    for (int i = 0; i < 6; ++i) {
        std::vector<uint8_t> kb(keys[i], keys[i] + 3);
        w.add_entry(kb, i * 100, 100);
    }

    std::vector<uint8_t> intermed, root;
    auto r = w.finish(0, intermed, root);
    EXPECT_EQ(r.num_levels, 2);
    EXPECT_EQ(r.num_root_entries, 2u);

    auto entry0 = parse_root_entry(root, 0);
    auto entry1 = parse_root_entry(root, entry0.bytes);
    EXPECT_EQ(entry0.key, "aaa");
    EXPECT_EQ(entry1.key, "ddd");
}
