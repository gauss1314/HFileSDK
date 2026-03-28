#include <gtest/gtest.h>
#include "block/prefix_encoder.h"
#include <hfile/types.h>
#include <vector>
#include <cstring>

using namespace hfile;
using namespace hfile::block;

static KeyValue kv_from(std::vector<uint8_t>& rk, std::vector<uint8_t>& fam,
                         std::vector<uint8_t>& q,  std::vector<uint8_t>& v,
                         const char* row, const char* qual, int64_t ts,
                         const char* val) {
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
    kv.timestamp = ts; kv.key_type = KeyType::Put; kv.value = v;
    return kv;
}

TEST(PrefixEncoder, EmptyBlock) {
    PrefixEncoder enc(64 * 1024);
    EXPECT_TRUE(enc.empty());
    auto data = enc.finish_block();
    EXPECT_TRUE(data.empty());
}

TEST(PrefixEncoder, ShrinksBySharingPrefix) {
    // Two KVs with long common row prefix should produce smaller block
    // than NoneEncoder for the same data
    PrefixEncoder penc(64 * 1024);
    NoneEncoder   nenc(64 * 1024);

    std::vector<uint8_t> rk, fam, q, v;
    for (int i = 0; i < 10; ++i) {
        std::string row = "com.example.longprefix.row" + std::to_string(i);
        std::string val = "value_data_payload_" + std::to_string(i);
        auto kv = kv_from(rk, fam, q, v, row.c_str(), "col", 1000LL - i, val.c_str());
        EXPECT_TRUE(penc.append(kv));
        EXPECT_TRUE(nenc.append(kv));
    }
    EXPECT_LT(penc.finish_block().size(), nenc.finish_block().size());
}

TEST(PrefixEncoder, FirstKey) {
    PrefixEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    auto kv = kv_from(rk, fam, q, v, "first_row", "col", 999, "val");
    enc.append(kv);
    auto fk = enc.first_key();
    EXPECT_FALSE(fk.empty());
    // key = rowLen(2) + row + famLen(1) + fam + qualifier + ts(8) + type(1)
    EXPECT_EQ(read_be16(fk.data()), static_cast<uint16_t>(strlen("first_row")));
}

TEST(PrefixEncoder, Reset) {
    PrefixEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    enc.append(kv_from(rk, fam, q, v, "r1", "c", 1, "v"));
    enc.reset();
    EXPECT_EQ(enc.num_kvs(), 0u);
}
