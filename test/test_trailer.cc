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

    // Last 12 bytes: pb_offset(4) + major(4) + minor(4)
    ASSERT_GE(out.size(), 12u);
    const uint8_t* tail = out.data() + out.size() - 12;

    uint32_t pb_offset = read_be32(tail);
    uint32_t major     = read_be32(tail + 4);
    uint32_t minor     = read_be32(tail + 8);

    EXPECT_EQ(major, kHFileMajorVersion);
    EXPECT_EQ(minor, kHFileMinorVersion);
    // pb_offset = pb_size + 12 (tail)
    EXPECT_GE(pb_offset, 12u);
    // pb_offset should point to start of PB bytes from end of file
    EXPECT_EQ(pb_offset, static_cast<uint32_t>(out.size()));
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
    tb.set_compression_codec(static_cast<uint32_t>(Compression::LZ4));

    std::vector<uint8_t> out;
    auto s = tb.finish(out);
    ASSERT_TRUE(s.ok());

    // Parse PB bytes back
    uint32_t pb_offset = read_be32(out.data() + out.size() - 12);
    size_t pb_size     = pb_offset - 12;  // pb_offset includes the 12 tail bytes
    ASSERT_GE(out.size(), pb_size + 12);

    hfile::pb::FileTrailerProto proto;
    bool ok = proto.ParseFromArray(out.data(), static_cast<int>(pb_size));
    EXPECT_TRUE(ok);
    EXPECT_EQ(proto.file_info_offset(), 8192u);
    EXPECT_EQ(proto.entry_count(), 999999u);
    EXPECT_EQ(proto.data_index_count(), 50u);
}
