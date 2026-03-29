#include <gtest/gtest.h>
#include "block/data_block_encoder.h"
#include <hfile/types.h>
#include <vector>
#include <cstring>

using namespace hfile;
using namespace hfile::block;

static KeyValue make_kv(const char* row, const char* family,
                         const char* qualifier, int64_t ts,
                         const char* value) {
    static std::vector<std::vector<uint8_t>> storage;
    auto to_bytes = [](const char* s) {
        return std::vector<uint8_t>(
            reinterpret_cast<const uint8_t*>(s),
            reinterpret_cast<const uint8_t*>(s) + strlen(s));
    };
    storage.push_back(to_bytes(row));
    storage.push_back(to_bytes(family));
    storage.push_back(to_bytes(qualifier));
    storage.push_back(to_bytes(value));

    KeyValue kv;
    kv.row       = storage[storage.size()-4];
    kv.family    = storage[storage.size()-3];
    kv.qualifier = storage[storage.size()-2];
    kv.timestamp = ts;
    kv.key_type  = KeyType::Put;
    kv.value     = storage[storage.size()-1];
    return kv;
}

TEST(KVSerialize, BasicRoundTrip) {
    const char row[] = "row001";
    const char fam[] = "cf";
    const char qua[] = "col1";
    const char val[] = "hello_world";
    int64_t ts = 1700000000000LL;

    KeyValue kv;
    kv.row       = {reinterpret_cast<const uint8_t*>(row), strlen(row)};
    kv.family    = {reinterpret_cast<const uint8_t*>(fam), strlen(fam)};
    kv.qualifier = {reinterpret_cast<const uint8_t*>(qua), strlen(qua)};
    kv.timestamp = ts;
    kv.key_type  = KeyType::Put;
    kv.value     = {reinterpret_cast<const uint8_t*>(val), strlen(val)};

    std::vector<uint8_t> buf(kv.encoded_size());
    size_t written = serialize_kv(kv, buf.data());
    EXPECT_EQ(written, buf.size());

    // Check key length field
    uint32_t key_len = read_be32(buf.data());
    EXPECT_EQ(key_len, kv.key_length());

    // Check value length field
    uint32_t val_len = read_be32(buf.data() + 4);
    EXPECT_EQ(val_len, static_cast<uint32_t>(strlen(val)));
}

TEST(KVSerialize, TagsLengthAlwaysPresent) {
    // HFile v3: tags_length (2B) must always be present, even when 0
    const char row[] = "r";
    const char fam[] = "f";
    const char qua[] = "q";
    const char val[] = "v";

    KeyValue kv;
    kv.row       = {reinterpret_cast<const uint8_t*>(row), 1};
    kv.family    = {reinterpret_cast<const uint8_t*>(fam), 1};
    kv.qualifier = {reinterpret_cast<const uint8_t*>(qua), 1};
    kv.timestamp = 0;
    kv.key_type  = KeyType::Put;
    kv.value     = {reinterpret_cast<const uint8_t*>(val), 1};
    // tags = empty

    std::vector<uint8_t> buf(kv.encoded_size());
    serialize_kv(kv, buf.data());

    // Tags length at offset: 4+4+key_len+1 = 4+4+(2+1+1+1+8+1)+1=... easier just check encoded_size
    // encoded_size = 4+4+key_len+value_len+2+tags_len+mvcc_len
    // For empty tags: +2 bytes for tags_len=0
    // Find the tags_len field: after value bytes
    uint32_t key_len = read_be32(buf.data());
    uint32_t val_len = read_be32(buf.data() + 4);
    size_t offset = 4 + 4 + key_len + val_len;
    ASSERT_LE(offset + 2, buf.size());
    uint16_t tags_len = read_be16(buf.data() + offset);
    EXPECT_EQ(tags_len, 0);  // No tags, but field must exist
}

TEST(KVSerialize, TimestampBigEndian) {
    const char row[] = "r";
    const char fam[] = "f";
    const char qua[] = "q";
    const char val[] = "v";
    int64_t ts = 0x1234567890ABCDEFll;

    KeyValue kv;
    kv.row       = {reinterpret_cast<const uint8_t*>(row), 1};
    kv.family    = {reinterpret_cast<const uint8_t*>(fam), 1};
    kv.qualifier = {reinterpret_cast<const uint8_t*>(qua), 1};
    kv.timestamp = ts;
    kv.key_type  = KeyType::Put;
    kv.value     = {reinterpret_cast<const uint8_t*>(val), 1};

    std::vector<uint8_t> buf(kv.encoded_size());
    serialize_kv(kv, buf.data());

    // Navigate to timestamp field in key:
    // offset: 4(kl)+4(vl)+2(rowLen)+row+1(famLen)+fam+qualifier
    size_t off = 4 + 4 + 2 + 1 + 1 + 1 + 1;  // row="r", fam="f", qualifier="q"
    uint64_t stored_ts = read_be64(buf.data() + off);
    EXPECT_EQ(stored_ts, static_cast<uint64_t>(ts));
}

TEST(KVCompare, OrderingRules) {
    // Row ASC
    KeyValue a, b;
    std::vector<uint8_t> r1 = {'a'}, r2 = {'b'};
    std::vector<uint8_t> fam = {'f'}, q = {'q'}, v = {'v'};
    a.row = r1; a.family = fam; a.qualifier = q; a.timestamp = 100; a.key_type = KeyType::Put; a.value = v;
    b.row = r2; b.family = fam; b.qualifier = q; b.timestamp = 100; b.key_type = KeyType::Put; b.value = v;
    EXPECT_LT(compare_keys(a, b), 0);

    // Timestamp DESC
    std::vector<uint8_t> r = {'r'};
    a.row = r; a.timestamp = 200;
    b.row = r; b.timestamp = 100;
    EXPECT_LT(compare_keys(a, b), 0);  // higher ts sorts first

    // Type DESC
    a.timestamp = b.timestamp = 100;
    a.key_type = KeyType::Put;    // 4
    b.key_type = KeyType::Delete; // 8
    EXPECT_GT(compare_keys(a, b), 0);  // higher type sorts first → b before a
}
