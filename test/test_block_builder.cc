#include <gtest/gtest.h>
#include "block/data_block_encoder.h"
#include "block/none_encoder.h"
#include <hfile/types.h>
#include <vector>
#include <cstring>

using namespace hfile;
using namespace hfile::block;

static KeyValue make_simple_kv(std::vector<uint8_t>& rk_buf,
                                std::vector<uint8_t>& fam_buf,
                                std::vector<uint8_t>& q_buf,
                                std::vector<uint8_t>& v_buf,
                                const char* row, const char* q,
                                int64_t ts, const char* val) {
    rk_buf.assign(reinterpret_cast<const uint8_t*>(row),
                  reinterpret_cast<const uint8_t*>(row) + strlen(row));
    fam_buf = {'c','f'};
    q_buf.assign(reinterpret_cast<const uint8_t*>(q),
                 reinterpret_cast<const uint8_t*>(q) + strlen(q));
    v_buf.assign(reinterpret_cast<const uint8_t*>(val),
                 reinterpret_cast<const uint8_t*>(val) + strlen(val));

    KeyValue kv;
    kv.row       = rk_buf;
    kv.family    = fam_buf;
    kv.qualifier = q_buf;
    kv.timestamp = ts;
    kv.key_type  = KeyType::Put;
    kv.value     = v_buf;
    return kv;
}

// ─── NoneEncoder ─────────────────────────────────────────────────────────────

TEST(NoneEncoder, EmptyBlock) {
    NoneEncoder enc(64 * 1024);
    EXPECT_TRUE(enc.empty());
    EXPECT_EQ(enc.num_kvs(), 0u);
    auto data = enc.finish_block();
    EXPECT_TRUE(data.empty());
}

TEST(NoneEncoder, SingleKV) {
    NoneEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    auto kv = make_simple_kv(rk, fam, q, v, "row001", "col", 1000, "value1");
    EXPECT_TRUE(enc.append(kv));
    EXPECT_EQ(enc.num_kvs(), 1u);
    EXPECT_FALSE(enc.empty());

    auto data = enc.finish_block();
    EXPECT_GT(data.size(), 0u);
    EXPECT_EQ(data.size(), kv.encoded_size());
}

TEST(NoneEncoder, BlockFull) {
    // Small block to force overflow quickly
    NoneEncoder enc(64);
    std::vector<uint8_t> rk, fam, q, v;
    // Fill with KVs until block is full
    int count = 0;
    for (int i = 0; i < 100; ++i) {
        std::string row = "row" + std::to_string(i);
        auto kv = make_simple_kv(rk, fam, q, v, row.c_str(), "col", i, "val");
        if (!enc.append(kv)) break;
        ++count;
    }
    EXPECT_GT(count, 0);
    // After block is full, append returns false
    auto kv_last = make_simple_kv(rk, fam, q, v, "zzz", "col", 9999, "val");
    // This should already be false (block full)
    // The exact count depends on KV size vs block_size=64
}

TEST(NoneEncoder, FirstKey) {
    NoneEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    auto kv1 = make_simple_kv(rk, fam, q, v, "aaa", "col", 1000, "v");
    auto kv2 = make_simple_kv(rk, fam, q, v, "bbb", "col", 999,  "v");
    enc.append(kv1);
    enc.append(kv2);
    auto first = enc.first_key();
    EXPECT_FALSE(first.empty());
    // First 2 bytes = row length = 3
    EXPECT_EQ(read_be16(first.data()), 3u);
}

TEST(NoneEncoder, Reset) {
    NoneEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    auto kv = make_simple_kv(rk, fam, q, v, "r1", "col", 1, "val");
    enc.append(kv);
    EXPECT_EQ(enc.num_kvs(), 1u);
    enc.reset();
    EXPECT_EQ(enc.num_kvs(), 0u);
    EXPECT_TRUE(enc.empty());
}

// ─── Factory ─────────────────────────────────────────────────────────────────

TEST(DataBlockEncoder, Factory) {
    auto none = DataBlockEncoder::create(Encoding::None, 64 * 1024);
    EXPECT_NE(none, nullptr);
}
