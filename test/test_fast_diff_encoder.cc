#include <gtest/gtest.h>
#include "block/fast_diff_encoder.h"
#include <hfile/types.h>
#include <vector>
#include <cstring>

using namespace hfile;
using namespace hfile::block;

static KeyValue make_kv(std::vector<uint8_t>& rk,
                         std::vector<uint8_t>& fam,
                         std::vector<uint8_t>& q,
                         std::vector<uint8_t>& v,
                         const char* row, const char* qual,
                         int64_t ts, const char* val,
                         KeyType ktype = KeyType::Put) {
    auto b = [](const char* s) {
        return std::vector<uint8_t>(
            reinterpret_cast<const uint8_t*>(s),
            reinterpret_cast<const uint8_t*>(s) + strlen(s));
    };
    rk  = b(row);
    fam = {'c','f'};
    q   = b(qual);
    v   = b(val);
    KeyValue kv;
    kv.row = rk; kv.family = fam; kv.qualifier = q;
    kv.timestamp = ts; kv.key_type = ktype; kv.value = v;
    return kv;
}

TEST(FastDiffEncoder, HandlesEmptyBlock) {
    FastDiffEncoder enc(64 * 1024);
    EXPECT_TRUE(enc.empty());
    EXPECT_EQ(enc.num_kvs(), 0u);
    auto data = enc.finish_block();
    EXPECT_TRUE(data.empty());
}

TEST(FastDiffEncoder, SingleKV) {
    FastDiffEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    auto kv = make_kv(rk, fam, q, v, "row1", "col", 1000, "value");
    EXPECT_TRUE(enc.append(kv));
    EXPECT_EQ(enc.num_kvs(), 1u);
    auto data = enc.finish_block();
    EXPECT_GT(data.size(), 0u);
}

TEST(FastDiffEncoder, MultipleKVsDecreasingTimestamp) {
    FastDiffEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    // Same row, same qualifier, descending timestamps (HBase ordering)
    for (int i = 100; i >= 90; --i) {
        auto kv = make_kv(rk, fam, q, v, "same_row", "col",
                          static_cast<int64_t>(i * 1000), "val");
        EXPECT_TRUE(enc.append(kv));
    }
    EXPECT_EQ(enc.num_kvs(), 11u);
}

TEST(FastDiffEncoder, ProducesCompactOutput) {
    FastDiffEncoder fenc(64 * 1024);
    NoneEncoder     nenc(64 * 1024);

    std::vector<uint8_t> rk, fam, q, v;
    for (int i = 0; i < 20; ++i) {
        std::string row = "row_prefix_" + std::to_string(i);
        auto kv = make_kv(rk, fam, q, v, row.c_str(), "col",
                          2000000LL - i * 1000, "value_data");
        EXPECT_TRUE(fenc.append(kv));
        EXPECT_TRUE(nenc.append(kv));
    }
    // FastDiff should be smaller due to prefix + timestamp delta encoding
    EXPECT_LT(fenc.finish_block().size(), nenc.finish_block().size());
}

TEST(FastDiffEncoder, FirstKey) {
    FastDiffEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    auto kv = make_kv(rk, fam, q, v, "alpha", "col", 999, "v");
    enc.append(kv);
    auto fk = enc.first_key();
    EXPECT_EQ(read_be16(fk.data()), 5u); // "alpha" length = 5
}

TEST(FastDiffEncoder, ResetClearsState) {
    FastDiffEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    enc.append(make_kv(rk, fam, q, v, "r1", "c", 1000, "v"));
    enc.reset();
    EXPECT_EQ(enc.num_kvs(), 0u);
    EXPECT_TRUE(enc.first_key().empty());
    // After reset, first KV of new block should work fine
    enc.append(make_kv(rk, fam, q, v, "r2", "c", 1000, "v"));
    EXPECT_EQ(enc.num_kvs(), 1u);
}

TEST(FastDiffEncoder, TagsAlwaysPresent) {
    // HFile v3: even with empty tags, tags_length field must be present
    FastDiffEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    auto kv = make_kv(rk, fam, q, v, "r1", "c", 1000, "v");
    EXPECT_TRUE(kv.tags.empty());
    EXPECT_TRUE(enc.append(kv));
    auto data = enc.finish_block();
    // The block should have at least enough bytes for one entry
    EXPECT_GT(data.size(), 10u);
}

TEST(FastDiffEncoder, LargeKeyFallsBackToHeap) {
    // A row key > 512 bytes triggers the heap-fallback path.
    // Before the fix this would have used a fixed 4096-byte stack buffer and
    // silently overflowed for row keys > 4083 bytes.
    FastDiffEncoder enc(512 * 1024);
    std::vector<uint8_t> rk(600, 'a');   // 600 bytes > kStackBuf (512)
    std::vector<uint8_t> fam = {'c','f'};
    std::vector<uint8_t> q   = {'q'};
    std::vector<uint8_t> v   = {'v'};
    KeyValue kv;
    kv.row = rk; kv.family = fam; kv.qualifier = q;
    kv.timestamp = 1000; kv.key_type = KeyType::Put; kv.value = v;
    EXPECT_TRUE(enc.append(kv));

    // Second KV with the same large prefix — exercises the shared-prefix path
    std::vector<uint8_t> rk2(600, 'a');
    rk2[599] = 'b';
    KeyValue kv2 = kv;
    kv2.row = rk2; kv2.timestamp = 999;
    EXPECT_TRUE(enc.append(kv2));

    auto data = enc.finish_block();
    EXPECT_GT(data.size(), 0u);
}

TEST(FastDiffEncoder, VeryLargeKeyExceeds4096Bytes) {
    // A row key > 4096 bytes would have caused a stack overflow with the old
    // fixed cur_key_buf[4096].  Verify the heap-fallback path handles it.
    FastDiffEncoder enc(512 * 1024);
    std::vector<uint8_t> rk(5000, 'x');   // 5000 bytes, well above old 4096 limit
    std::vector<uint8_t> fam = {'f'};
    std::vector<uint8_t> q   = {'q'};
    std::vector<uint8_t> v(100, 'v');
    KeyValue kv;
    kv.row = rk; kv.family = fam; kv.qualifier = q;
    kv.timestamp = 2000; kv.key_type = KeyType::Put; kv.value = v;
    EXPECT_TRUE(enc.append(kv));
    EXPECT_EQ(enc.num_kvs(), 1u);
    auto data = enc.finish_block();
    EXPECT_GT(data.size(), 0u);
}
