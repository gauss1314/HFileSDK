#include <gtest/gtest.h>
#include "bloom/compound_bloom_filter_writer.h"
#include <vector>
#include <string>
#include <cstring>

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
