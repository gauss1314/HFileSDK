#include <gtest/gtest.h>
#include "meta/trailer_builder.h"
#include <hfile/types.h>
#include <vector>

using namespace hfile;
using namespace hfile::meta;

TEST(TrailerBuilder, SerializesCorrectVersions) {
    TrailerBuilder tb;
    tb.set_file_info_offset(1024);
    tb.set_load_on_open_offset(512);
    tb.set_entry_count(1000);
    tb.set_data_index_count(10);
    tb.set_comparator_class_name(std::string(kCellComparator));

    std::vector<uint8_t> out;
    auto s = tb.finish(out);
    ASSERT_TRUE(s.ok()) << s.message();

    ASSERT_EQ(out.size(), kTrailerFixedSize);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(out.data()), 8), "TRABLK\"$");
    uint32_t version = read_be32(out.data() + out.size() - kTrailerVersionSize);
    EXPECT_EQ(version,
              (static_cast<uint32_t>(kHFileMinorVersion) << 24) |
              static_cast<uint32_t>(kHFileMajorVersion));
}

TEST(TrailerBuilder, ProtobufDeserializable) {
    TrailerBuilder tb;
    tb.set_file_info_offset(8192);
    tb.set_load_on_open_offset(4096);
    tb.set_entry_count(999999);
    tb.set_data_index_count(50);
    tb.set_num_data_index_levels(1);
    tb.set_first_data_block_offset(33);
    tb.set_last_data_block_offset(65536);
    tb.set_total_uncompressed_bytes(10 * 1024 * 1024);
    tb.set_comparator_class_name(std::string(kCellComparator));
    tb.set_compression_codec(static_cast<uint32_t>(Compression::GZip));

    std::vector<uint8_t> out;
    auto s = tb.finish(out);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(std::string(reinterpret_cast<const char*>(out.data()), 8), "TRABLK\"$");
    uint64_t pb_size = 0;
    int prefix_len = decode_varint64(out.data() + 8, pb_size);
    ASSERT_GT(prefix_len, 0);
    ASSERT_GE(out.size(), 8u + static_cast<size_t>(prefix_len) + pb_size + kTrailerVersionSize);

    hfile::pb::FileTrailerProto proto;
    bool ok = proto.ParseFromArray(out.data() + 8 + prefix_len, static_cast<int>(pb_size));
    EXPECT_TRUE(ok);
    EXPECT_EQ(proto.file_info_offset(), 8192u);
    EXPECT_EQ(proto.entry_count(), 999999u);
    EXPECT_EQ(proto.data_index_count(), 50u);
}
