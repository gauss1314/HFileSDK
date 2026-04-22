#include <gtest/gtest.h>

#define private public
#include <hfile/status.h>
#undef private

#include <hfile/types.h>

#include <array>
#include <cstdint>
#include <vector>

using namespace hfile;

namespace {

Status passthrough_error(Status s) {
    HFILE_RETURN_IF_ERROR(s);
    return Status::OK();
}

KeyValue make_kv(std::vector<uint8_t>& row,
                 std::vector<uint8_t>& family,
                 std::vector<uint8_t>& qualifier,
                 std::vector<uint8_t>& value,
                 int64_t timestamp,
                 KeyType type) {
    KeyValue kv;
    kv.row = row;
    kv.family = family;
    kv.qualifier = qualifier;
    kv.timestamp = timestamp;
    kv.key_type = type;
    kv.value = value;
    return kv;
}

}  // namespace

TEST(StatusExtra, ToStringCoversAllNamedCodes) {
    EXPECT_EQ(Status().to_string(), "OK");
    EXPECT_EQ(Status(Status::Code::OutOfRange, "oor").to_string(), "OutOfRange: oor");
    EXPECT_EQ(Status(Status::Code::NotFound, "missing").to_string(), "NotFound: missing");
    EXPECT_EQ(Status(Status::Code::AlreadyExists, "dup").to_string(), "AlreadyExists: dup");
    EXPECT_EQ(Status::NotSupported("ns").to_string(), "NotSupported: ns");
    EXPECT_EQ(Status::Internal("boom").to_string(), "Internal: boom");
    EXPECT_EQ(Status::Corruption("bad").to_string(), "Corruption: bad");
}

TEST(StatusExtra, ReturnIfErrorMacroPropagatesFailures) {
    auto ok = passthrough_error(Status::OK());
    EXPECT_TRUE(ok.ok());

    auto err = passthrough_error(Status::InvalidArg("bad arg"));
    ASSERT_FALSE(err.ok());
    EXPECT_EQ(err.code(), Status::Code::InvalidArg);
    EXPECT_EQ(err.message(), "bad arg");
}

TEST(TypesExtra, VarintRoundTripAndMalformedDecode) {
    std::array<uint64_t, 5> values = {
        0u,
        1u,
        127u,
        128u,
        0xFEDCBA9876543210ULL,
    };

    for (uint64_t value : values) {
        uint8_t buf[16] = {};
        int n = encode_varint64(buf, value);
        ASSERT_GT(n, 0);
        uint64_t decoded = 0;
        EXPECT_EQ(decode_varint64(buf, decoded), n);
        EXPECT_EQ(decoded, value);
    }

    std::array<uint8_t, 10> malformed{};
    malformed.fill(0x80);
    uint64_t partial = 0;
    EXPECT_EQ(decode_varint64(malformed.data(), partial), -1);
}

TEST(TypesExtra, WritableVintRoundTripsSignedValues) {
    std::array<int64_t, 6> values = {
        -113,
        -1,
        0,
        1,
        127,
        0x102030405060708LL,
    };

    for (int64_t value : values) {
        uint8_t buf[16] = {};
        int n = encode_writable_vint(buf, value);
        ASSERT_EQ(n, writable_vint_size(value));
        int64_t decoded = 0;
        EXPECT_EQ(decode_writable_vint(buf, decoded), n);
        EXPECT_EQ(decoded, value);
    }
}

TEST(TypesExtra, KeyValueEncodingSizeAndOwnedView) {
    std::vector<uint8_t> row = {'r', '1'};
    std::vector<uint8_t> family = {'c', 'f'};
    std::vector<uint8_t> qualifier = {'q', '1'};
    std::vector<uint8_t> value = {'v', 'a', 'l'};
    std::vector<uint8_t> tags = {'t', 'g'};

    OwnedKeyValue owned;
    owned.row = row;
    owned.family = family;
    owned.qualifier = qualifier;
    owned.timestamp = 123;
    owned.key_type = KeyType::Delete;
    owned.value = value;
    owned.tags = tags;
    owned.memstore_ts = 300;
    owned.has_memstore_ts = true;

    KeyValue view = owned.as_view();
    EXPECT_EQ(view.row.size(), 2u);
    EXPECT_EQ(view.family.size(), 2u);
    EXPECT_EQ(view.qualifier.size(), 2u);
    EXPECT_EQ(view.value.size(), 3u);
    EXPECT_EQ(view.tags.size(), 2u);
    EXPECT_EQ(view.memstore_ts, 300u);
    EXPECT_TRUE(view.has_memstore_ts);
    EXPECT_EQ(view.key_length(), 2u + 2u + 1u + 2u + 2u + 8u + 1u);
    EXPECT_EQ(view.encoded_size(),
              4u + 4u + view.key_length() + 3u + 2u + 2u
                  + static_cast<size_t>(writable_vint_size(300)));
}

TEST(TypesExtra, CompareKeysCoversFamilyQualifierAndEqualityBranches) {
    std::vector<uint8_t> row = {'r'};
    std::vector<uint8_t> family_a = {'a'};
    std::vector<uint8_t> family_b = {'b'};
    std::vector<uint8_t> qualifier_a = {'a'};
    std::vector<uint8_t> qualifier_b = {'b'};
    std::vector<uint8_t> value = {'v'};

    KeyValue base = make_kv(row, family_a, qualifier_a, value, 10, KeyType::Put);
    KeyValue same = make_kv(row, family_a, qualifier_a, value, 10, KeyType::Put);
    KeyValue diff_family = make_kv(row, family_b, qualifier_a, value, 10, KeyType::Put);
    KeyValue diff_qualifier = make_kv(row, family_a, qualifier_b, value, 10, KeyType::Put);
    KeyValue newer = make_kv(row, family_a, qualifier_a, value, 20, KeyType::Put);
    KeyValue diff_type = make_kv(row, family_a, qualifier_a, value, 10, KeyType::Delete);

    EXPECT_EQ(compare_keys(base, same), 0);
    EXPECT_LT(compare_keys(base, diff_family), 0);
    EXPECT_LT(compare_keys(base, diff_qualifier), 0);
    EXPECT_GT(compare_keys(base, newer), 0);
    EXPECT_GT(compare_keys(base, diff_type), 0);
}
