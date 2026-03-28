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

// ─── 1-level (inline root) ────────────────────────────────────────────────────

TEST(BlockIndexWriter, EmptyOneLevel) {
    BlockIndexWriter w(128);
    std::vector<uint8_t> root;
    auto r = finish_simple(w, root);
    EXPECT_EQ(r.num_root_entries, 0u);
    EXPECT_EQ(r.num_levels, 1);
    // root payload = count(4B) only
    EXPECT_EQ(root.size(), 4u);
    EXPECT_EQ(read_be32(root.data()), 0u);
}

TEST(BlockIndexWriter, SingleEntryOneLevel) {
    BlockIndexWriter w(128);
    const uint8_t key[] = {'r','o','w','1'};
    w.add_entry({key, 4}, /*offset=*/33, /*data_size=*/65536);

    std::vector<uint8_t> root;
    auto r = finish_simple(w, root);
    EXPECT_EQ(r.num_root_entries, 1u);
    EXPECT_EQ(r.num_levels, 1);

    // count(4) + offset(8) + dataSize(4) + keyLen(4) + key(4)
    EXPECT_EQ(root.size(), 4u + 8u + 4u + 4u + 4u);
    EXPECT_EQ(read_be32(root.data()), 1u);
    EXPECT_EQ(static_cast<int64_t>(read_be64(root.data() + 4)), 33);
    EXPECT_EQ(read_be32(root.data() + 12), 65536u);
    EXPECT_EQ(read_be32(root.data() + 16), 4u);
    EXPECT_EQ(std::memcmp(root.data() + 20, key, 4), 0);
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
    EXPECT_EQ(read_be32(root.data()), 100u);
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

    // Root has exactly 3 entries
    EXPECT_EQ(read_be32(root.data()), 3u);

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

    // First root entry should point to intermed_start
    // Root payload: count(4) + [offset(8)+dataSize(4)+keyLen(4)+key...] ...
    EXPECT_EQ(read_be32(root.data()), 2u);
    int64_t first_root_offset = static_cast<int64_t>(read_be64(root.data() + 4));
    EXPECT_EQ(first_root_offset, intermed_start);

    // Second root entry should point to intermed_start + size_of_first_block
    // First intermediate block: header(33) + count(4) + 4*(8+4+4+4) = 33+4+80 = 117
    // (key = 4 bytes each)
    uint32_t first_block_size = read_be32(root.data() + 4 + 8); // dataSize field
    int64_t second_offset = static_cast<int64_t>(
        read_be64(root.data() + 4 + (8 + 4 + 4 + 4)));          // second entry offset
    EXPECT_EQ(second_offset, intermed_start + static_cast<int64_t>(first_block_size));
    (void)first_block_size;
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

    // Root entry 0's first key must be "aaa" (first key of block 0)
    // Root layout: count(4) + entry0[offset(8)+dataSize(4)+keyLen(4)+key(3)] + entry1[...]
    uint32_t key0_len = read_be32(root.data() + 4 + 8 + 4);
    EXPECT_EQ(key0_len, 3u);
    EXPECT_EQ(std::memcmp(root.data() + 4 + 8 + 4 + 4, "aaa", 3), 0);

    // Root entry 1's first key must be "ddd" (first key of block 1)
    size_t e0_size = 8 + 4 + 4 + key0_len;
    uint32_t key1_len = read_be32(root.data() + 4 + e0_size + 8 + 4);
    EXPECT_EQ(key1_len, 3u);
    EXPECT_EQ(std::memcmp(root.data() + 4 + e0_size + 8 + 4 + 4, "ddd", 3), 0);
}
