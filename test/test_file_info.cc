#include <gtest/gtest.h>
#include "meta/file_info_builder.h"
#include <hfile/types.h>
#include <vector>
#include <cstring>

using namespace hfile;
using namespace hfile::meta;

TEST(FileInfoBuilder, AllMandatoryFields) {
    FileInfoBuilder fib;

    const uint8_t last_key_data[] = {'r','o','w','1'};
    fib.set_last_key({last_key_data, 4});
    fib.set_avg_key_len(24);
    fib.set_avg_value_len(100);
    fib.set_max_tags_len(0);
    fib.set_key_value_version(1);
    fib.set_max_memstore_ts(0);
    fib.set_comparator(kCellComparator);
    fib.set_data_block_encoding(Encoding::FastDiff);
    fib.set_create_time(1700000000000LL);
    fib.set_len_of_biggest_cell(10240);

    std::vector<uint8_t> out;
    ASSERT_TRUE(fib.validate_required_fields().ok());
    fib.finish(out);

    ASSERT_GE(out.size(), 5u);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(out.data()), 4), "PBUF");
}

TEST(FileInfoBuilder, EncodingNames) {
    for (auto enc : {Encoding::None, Encoding::Prefix,
                     Encoding::Diff, Encoding::FastDiff}) {
        FileInfoBuilder fib;
        fib.set_last_key({});
        fib.set_avg_key_len(0);
        fib.set_avg_value_len(0);
        fib.set_max_tags_len(0);
        fib.set_key_value_version(1);
        fib.set_max_memstore_ts(0);
        fib.set_comparator(kCellComparator);
        fib.set_data_block_encoding(enc);
        fib.set_create_time();
        fib.set_len_of_biggest_cell(0);

        std::vector<uint8_t> out;
        ASSERT_TRUE(fib.validate_required_fields().ok());
        fib.finish(out);
        EXPECT_GE(out.size(), 5u);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(out.data()), 4), "PBUF");
    }
}

TEST(FileInfoBuilder, ComparatorString) {
    FileInfoBuilder fib;
    fib.set_comparator(kCellComparator);
    fib.set_last_key({});
    fib.set_avg_key_len(0);
    fib.set_avg_value_len(0);
    fib.set_max_tags_len(0);
    fib.set_key_value_version(1);
    fib.set_max_memstore_ts(0);
    fib.set_data_block_encoding(Encoding::None);
    fib.set_create_time();
    fib.set_len_of_biggest_cell(0);

    std::vector<uint8_t> out;
    ASSERT_TRUE(fib.validate_required_fields().ok());
    fib.finish(out);

    // The comparator string should appear somewhere in the serialized bytes
    std::string expected = std::string(kCellComparator);
    std::string haystack(out.begin(), out.end());
    EXPECT_NE(haystack.find("CellComparatorImpl"), std::string::npos);
}

TEST(FileInfoBuilder, RejectsMissingMandatoryFields) {
    FileInfoBuilder fib;
    fib.set_last_key({});
    fib.set_avg_key_len(0);
    fib.set_avg_value_len(0);
    auto s = fib.validate_required_fields();
    EXPECT_FALSE(s.ok());
    EXPECT_NE(s.message().find("mandatory field"), std::string::npos);
}
