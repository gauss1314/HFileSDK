#include <gtest/gtest.h>
#include "bloom/compound_bloom_filter_writer.h"
#include <vector>
#include <string>
#include <cstring>

using namespace hfile;
using namespace hfile::bloom;

static std::vector<uint8_t> make_key(const std::string& s) {
    return std::vector<uint8_t>(s.begin(), s.end());
}

TEST(BloomFilter, EmptyBloom) {
    CompoundBloomFilterWriter bf(BloomType::None);
    std::vector<uint8_t> meta;
    auto result = bf.finish(meta, 0);
    EXPECT_FALSE(result.enabled);
    EXPECT_TRUE(meta.empty());
}

TEST(BloomFilter, BasicAdd) {
    CompoundBloomFilterWriter bf(BloomType::Row, 0.01, 1000);
    for (int i = 0; i < 100; ++i) {
        auto k = make_key("row_" + std::to_string(i));
        bf.add(k);
    }
    EXPECT_EQ(bf.bloom_type(), BloomType::Row);
}

TEST(BloomFilter, FinishProducesBlocks) {
    CompoundBloomFilterWriter bf(BloomType::Row, 0.01, 100);
    for (int i = 0; i < 50; ++i) {
        auto k = make_key("row_" + std::to_string(i));
        bf.add(k);
    }
    std::vector<uint8_t> meta;
    auto result = bf.finish(meta, 0);
    EXPECT_TRUE(result.enabled);
    EXPECT_EQ(result.total_key_count, 50u);
    EXPECT_FALSE(meta.empty());
    // Meta block must start with BLMF magic
    EXPECT_EQ(meta[0], 'B');
}

TEST(BloomFilter, MurmurHash) {
    // Verify murmur3 is stable across calls
    const uint8_t data[] = {0x01, 0x02, 0x03};
    uint32_t h1 = murmur3_32(data, 3, 0);
    uint32_t h2 = murmur3_32(data, 3, 0);
    EXPECT_EQ(h1, h2);
    // Different seeds → different hashes
    uint32_t h3 = murmur3_32(data, 3, 1);
    EXPECT_NE(h1, h3);
}

TEST(BloomFilter, ChunkBoundary) {
    // Add many keys to force chunk rollover
    CompoundBloomFilterWriter bf(BloomType::Row, 0.01, 10);
    for (int i = 0; i < 200; ++i) {
        auto k = make_key("key_" + std::to_string(i));
        bf.add(k);
        if (i > 0 && i % 10 == 0) bf.finish_chunk();
    }
    std::vector<uint8_t> meta;
    auto result = bf.finish(meta, 0);
    EXPECT_TRUE(result.enabled);
    EXPECT_GT(meta.size(), 0u);
}

// ─── B-12 regression: finish_data_blocks / finish_meta_block API ─────────────

TEST(BloomFilter, FinishDataBlocksProducesOnlyChunkBlocks) {
    CompoundBloomFilterWriter bf(BloomType::Row, 0.01, 50);
    for (int i = 0; i < 30; ++i) {
        auto k = make_key("key_" + std::to_string(i));
        bf.add(k);
    }
    std::vector<uint8_t> chunk_buf;
    bool has_data = bf.finish_data_blocks(chunk_buf, /*file_offset=*/0);
    EXPECT_TRUE(has_data);
    EXPECT_FALSE(chunk_buf.empty());
    // chunk_buf must start with BLMFBLK2 magic
    ASSERT_GE(chunk_buf.size(), 8u);
    EXPECT_EQ(std::string(chunk_buf.begin(), chunk_buf.begin() + 8), "BLMFBLK2");
    // chunk_buf must NOT contain BLMFMET2 — that's written separately
    std::string content(chunk_buf.begin(), chunk_buf.end());
    EXPECT_EQ(content.find("BLMFMET2"), std::string::npos);
}

TEST(BloomFilter, FinishMetaBlockProducesOnlyMetaBlock) {
    CompoundBloomFilterWriter bf(BloomType::Row, 0.01, 50);
    for (int i = 0; i < 30; ++i) {
        auto k = make_key("key_" + std::to_string(i));
        bf.add(k);
    }
    std::vector<uint8_t> chunk_buf;
    bf.finish_data_blocks(chunk_buf, 0);

    std::vector<uint8_t> meta_buf;
    bf.finish_meta_block(meta_buf);
    EXPECT_FALSE(meta_buf.empty());
    // meta_buf must start with BLMFMET2 magic
    ASSERT_GE(meta_buf.size(), 8u);
    EXPECT_EQ(std::string(meta_buf.begin(), meta_buf.begin() + 8), "BLMFMET2");
}

TEST(BloomFilter, IsEnabledAndHasData) {
    CompoundBloomFilterWriter enabled(BloomType::Row, 0.01);
    CompoundBloomFilterWriter disabled(BloomType::None);
    EXPECT_TRUE(enabled.is_enabled());
    EXPECT_FALSE(disabled.is_enabled());
    EXPECT_FALSE(enabled.has_data());
    auto k = make_key("x");
    enabled.add(k);
    EXPECT_TRUE(enabled.has_data());
}

TEST(BloomFilter, DataBlocksAndMetaSeparated) {
    // Simulate the correct layout: chunk blocks written before load-on-open,
    // meta block written after FileInfo.
    CompoundBloomFilterWriter bf(BloomType::Row, 0.01, 50);
    for (int i = 0; i < 10; ++i) {
        auto k = make_key("r" + std::to_string(i));
        bf.add(k);
    }
    // Step 1: write chunk blocks at offset 1000
    std::vector<uint8_t> chunks;
    bool ok = bf.finish_data_blocks(chunks, 1000);
    EXPECT_TRUE(ok);
    // Step 2: write meta block at a later offset
    std::vector<uint8_t> meta;
    bf.finish_meta_block(meta);
    EXPECT_FALSE(meta.empty());
    // Chunks and meta are separate buffers — no interleaving
    std::string chunk_str(chunks.begin(), chunks.end());
    std::string meta_str(meta.begin(), meta.end());
    EXPECT_NE(chunk_str.find("BLMFBLK2"), std::string::npos);
    EXPECT_EQ(chunk_str.find("BLMFMET2"), std::string::npos);
    EXPECT_NE(meta_str.find("BLMFMET2"), std::string::npos);
    EXPECT_EQ(meta_str.find("BLMFBLK2"), std::string::npos);
}

TEST(BloomFilter, MetaPayloadMatchesHBaseCompoundBloomFormat) {
    CompoundBloomFilterWriter bf(BloomType::Row, 0.01, 50);
    for (int i = 0; i < 30; ++i) {
        auto k = make_key("row_" + std::to_string(i));
        bf.add(k);
    }
    std::vector<uint8_t> chunks;
    ASSERT_TRUE(bf.finish_data_blocks(chunks, 4096));

    std::vector<uint8_t> meta;
    bf.finish_meta_block(meta);
    ASSERT_GT(meta.size(), kBlockHeaderSize + 40);

    const uint8_t* p = meta.data() + kBlockHeaderSize;
    const uint8_t* end = meta.data() + meta.size();

    ASSERT_LE(p + 4, end);
    EXPECT_EQ(read_be32(p), 3u); p += 4; // version
    ASSERT_LE(p + 8, end);
    EXPECT_GT(read_be64(p), 0u); p += 8; // total byte size
    ASSERT_LE(p + 4, end);
    EXPECT_EQ(read_be32(p), 5u); p += 4; // hash count
    ASSERT_LE(p + 4, end);
    EXPECT_EQ(read_be32(p), 1u); p += 4; // hash type (murmur)
    ASSERT_LE(p + 8, end);
    EXPECT_EQ(read_be64(p), 30u); p += 8; // total key count
    ASSERT_LE(p + 8, end);
    EXPECT_GE(read_be64(p), 30u); p += 8; // total max keys
    ASSERT_LE(p + 4, end);
    const uint32_t num_chunks = read_be32(p); p += 4;
    EXPECT_GT(num_chunks, 0u);

    int64_t comparator_len = 0;
    const int len_bytes = decode_writable_vint(p, comparator_len);
    ASSERT_GT(len_bytes, 0);
    p += len_bytes;
    EXPECT_EQ(comparator_len, 0); // ROW bloom has null comparator

    for (uint32_t i = 0; i < num_chunks; ++i) {
        ASSERT_LE(p + 12, end);
        EXPECT_GE(read_be64(p), 4096u);
        p += 8;
        EXPECT_GT(read_be32(p), 0u);
        p += 4;

        int64_t key_len = 0;
        const int key_len_bytes = decode_writable_vint(p, key_len);
        ASSERT_GT(key_len_bytes, 0);
        p += key_len_bytes;
        ASSERT_GE(key_len, 0);
        ASSERT_LE(p + key_len, end);
        EXPECT_GT(key_len, 0);
        p += static_cast<size_t>(key_len);
    }
}
