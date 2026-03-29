#include <gtest/gtest.h>

#include "block/diff_encoder.h"
#include "block/none_encoder.h"

#include <hfile/types.h>

#include <cstring>
#include <vector>

using namespace hfile;
using namespace hfile::block;

static KeyValue make_kv(std::vector<uint8_t>& rk,
                        std::vector<uint8_t>& fam,
                        std::vector<uint8_t>& q,
                        std::vector<uint8_t>& v,
                        const char* row,
                        const char* qual,
                        int64_t ts,
                        const char* val,
                        KeyType type = KeyType::Put) {
    auto bytes = [](const char* s) {
        return std::vector<uint8_t>(
            reinterpret_cast<const uint8_t*>(s),
            reinterpret_cast<const uint8_t*>(s) + std::strlen(s));
    };
    rk = bytes(row);
    fam = {'c', 'f'};
    q = bytes(qual);
    v = bytes(val);
    KeyValue kv;
    kv.row = rk;
    kv.family = fam;
    kv.qualifier = q;
    kv.timestamp = ts;
    kv.key_type = type;
    kv.value = v;
    return kv;
}

TEST(DiffEncoder, HandlesEmptyBlock) {
    DiffEncoder enc(64 * 1024);
    EXPECT_TRUE(enc.empty());
    EXPECT_EQ(enc.num_kvs(), 0u);
    EXPECT_TRUE(enc.finish_block().empty());
}

TEST(DiffEncoder, SingleKV) {
    DiffEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    EXPECT_TRUE(enc.append(make_kv(rk, fam, q, v, "row1", "col", 1000, "value")));
    EXPECT_EQ(enc.num_kvs(), 1u);
    EXPECT_GT(enc.finish_block().size(), 0u);
}

TEST(DiffEncoder, ProducesCompactOutput) {
    DiffEncoder enc(64 * 1024);
    NoneEncoder raw(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    for (int i = 0; i < 20; ++i) {
        std::string row = "row_prefix_" + std::to_string(i);
        auto kv = make_kv(rk, fam, q, v, row.c_str(), "col", 2000000LL - i * 1000, "value_data");
        EXPECT_TRUE(enc.append(kv));
        EXPECT_TRUE(raw.append(kv));
    }
    EXPECT_LT(enc.finish_block().size(), raw.finish_block().size());
}

TEST(DiffEncoder, FirstKeyTracksFirstCell) {
    DiffEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    EXPECT_TRUE(enc.append(make_kv(rk, fam, q, v, "alpha", "col", 1000, "v")));
    auto fk = enc.first_key();
    ASSERT_FALSE(fk.empty());
    EXPECT_EQ(read_be16(fk.data()), 5u);
}

TEST(DiffEncoder, ResetClearsState) {
    DiffEncoder enc(64 * 1024);
    std::vector<uint8_t> rk, fam, q, v;
    EXPECT_TRUE(enc.append(make_kv(rk, fam, q, v, "r1", "c", 1000, "v")));
    enc.reset();
    EXPECT_EQ(enc.num_kvs(), 0u);
    EXPECT_TRUE(enc.first_key().empty());
    EXPECT_TRUE(enc.append(make_kv(rk, fam, q, v, "r2", "c", 999, "v")));
    EXPECT_EQ(enc.num_kvs(), 1u);
}

TEST(DiffEncoder, LargeKeyFallsBackToHeap) {
    DiffEncoder enc(512 * 1024);
    std::vector<uint8_t> rk(600, 'a');
    std::vector<uint8_t> fam = {'c', 'f'};
    std::vector<uint8_t> q = {'q'};
    std::vector<uint8_t> v = {'v'};
    KeyValue kv;
    kv.row = rk;
    kv.family = fam;
    kv.qualifier = q;
    kv.timestamp = 1000;
    kv.key_type = KeyType::Put;
    kv.value = v;
    EXPECT_TRUE(enc.append(kv));

    std::vector<uint8_t> rk2(600, 'a');
    rk2.back() = 'b';
    kv.row = rk2;
    kv.timestamp = 999;
    EXPECT_TRUE(enc.append(kv));
    EXPECT_GT(enc.finish_block().size(), 0u);
}
