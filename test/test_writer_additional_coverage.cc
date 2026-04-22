#include <gtest/gtest.h>

#include <hfile/writer.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using namespace hfile;

namespace {

fs::path temp_path(const std::string& name) {
    auto path = fs::temp_directory_path() / ("hfile_writer_extra_" + name + ".hfile");
    std::error_code ec;
    fs::remove(path, ec);
    return path;
}

struct ScopedEnvVar {
    explicit ScopedEnvVar(const char* value) {
        const char* current = std::getenv("HFILESDK_ENABLE_HOTPATH_PROFILING");
        if (current != nullptr) {
            had_old_ = true;
            old_ = current;
        }
        setenv("HFILESDK_ENABLE_HOTPATH_PROFILING", value, 1);
    }

    ~ScopedEnvVar() {
        if (had_old_) {
            setenv("HFILESDK_ENABLE_HOTPATH_PROFILING", old_.c_str(), 1);
        } else {
            unsetenv("HFILESDK_ENABLE_HOTPATH_PROFILING");
        }
    }

    bool had_old_{false};
    std::string old_;
};

KeyValue make_kv(std::vector<uint8_t>& row,
                 std::vector<uint8_t>& family,
                 std::vector<uint8_t>& qualifier,
                 std::vector<uint8_t>& value,
                 std::string_view row_text,
                 std::string_view qualifier_text,
                 int64_t ts,
                 std::string_view value_text) {
    row.assign(row_text.begin(), row_text.end());
    family = {'c', 'f'};
    qualifier.assign(qualifier_text.begin(), qualifier_text.end());
    value.assign(value_text.begin(), value_text.end());

    KeyValue kv;
    kv.row = row;
    kv.family = family;
    kv.qualifier = qualifier;
    kv.timestamp = ts;
    kv.key_type = KeyType::Put;
    kv.value = value;
    return kv;
}

}  // namespace

TEST(HFileWriterAdditionalCoverage, SkipPoliciesAndMaxErrorCountAreExercised) {
    auto path_skip_row = temp_path("skip_row");
    std::vector<std::string> callback_messages;
    auto [skip_row_writer, skip_row_status] = HFileWriter::builder()
        .set_path(path_skip_row.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_sort_mode(WriterOptions::SortMode::PreSortedVerified)
        .set_error_policy(ErrorPolicy::SkipRow)
        .set_max_error_count(1)
        .set_error_callback([&](const RowError& error) {
            callback_messages.push_back(error.message);
        })
        .build();
    ASSERT_TRUE(skip_row_status.ok()) << skip_row_status.message();

    KeyValue invalid{};
    invalid.timestamp = 1;
    invalid.key_type = KeyType::Put;
    EXPECT_TRUE(skip_row_writer->append(invalid).ok());
    auto second_error = skip_row_writer->append(invalid);
    EXPECT_FALSE(second_error.ok());
    EXPECT_NE(second_error.message().find("MAX_ERRORS_EXCEEDED"), std::string::npos);
    ASSERT_EQ(callback_messages.size(), 2u);
    EXPECT_TRUE(skip_row_writer->finish().ok());
    fs::remove(path_skip_row);

    auto path_skip_batch = temp_path("skip_batch");
    auto [skip_batch_writer, skip_batch_status] = HFileWriter::builder()
        .set_path(path_skip_batch.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_sort_mode(WriterOptions::SortMode::PreSortedVerified)
        .set_error_policy(ErrorPolicy::SkipBatch)
        .build();
    ASSERT_TRUE(skip_batch_status.ok()) << skip_batch_status.message();
    auto skip_batch = skip_batch_writer->append(invalid);
    EXPECT_FALSE(skip_batch.ok());
    EXPECT_NE(skip_batch.message().find("SKIP_BATCH"), std::string::npos);
    EXPECT_TRUE(skip_batch_writer->finish().ok());
    fs::remove(path_skip_batch);
}

TEST(HFileWriterAdditionalCoverage, HotPathPipelineRowColBloomAndTrustedVariants) {
    ScopedEnvVar profiling("1");
    auto path = temp_path("hotpath_rowcol");

    auto [writer, status] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::GZip)
        .set_compression_level(1)
        .set_block_size(128)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::RowCol)
        .set_sort_mode(WriterOptions::SortMode::PreSortedTrusted)
        .set_include_tags(false)
        .set_include_mvcc(false)
        .set_fsync_policy(FsyncPolicy::Paranoid)
        .set_compression_threads(1)
        .set_compression_queue_depth(0)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    std::vector<uint8_t> row, family, qualifier, value;
    auto kv1 = make_kv(row, family, qualifier, value, "row001", "q1", 10, "value1");
    auto kv2 = make_kv(row, family, qualifier, value, "row001", "q2", 9, "value2");
    auto kv3 = make_kv(row, family, qualifier, value, "row002", "q1", 8, "value3");

    ASSERT_TRUE(writer->append_trusted_new_row(kv1).ok());
    ASSERT_TRUE(writer->append_trusted_same_row(kv2).ok());
    ASSERT_TRUE(writer->append_trusted(kv3).ok());
    ASSERT_TRUE(writer->finish().ok());

    EXPECT_TRUE(fs::exists(path));
    WriterStats stats = writer->stats();
    EXPECT_GT(stats.data_block_count, 0u);
    EXPECT_GT(stats.load_on_open_block_count, 0u);
    EXPECT_GE(stats.bloom_chunk_flush_count, 0u);
    fs::remove(path);
}

TEST(HFileWriterAdditionalCoverage, MoveAndSpanTrustedOverloadsWork) {
    auto path = temp_path("move_and_span");

    auto [writer, status] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_sort_mode(WriterOptions::SortMode::PreSortedTrusted)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();
    EXPECT_EQ(writer->position(), 0);

    HFileWriter moved = std::move(*writer);
    std::vector<uint8_t> row1 = {'r', '1'};
    std::vector<uint8_t> row2 = {'r', '2'};
    std::vector<uint8_t> family = {'c', 'f'};
    std::vector<uint8_t> q1 = {'q', '1'};
    std::vector<uint8_t> q2 = {'q', '2'};
    std::vector<uint8_t> q3 = {'q', '3'};
    std::vector<uint8_t> v1 = {'v', '1'};
    std::vector<uint8_t> v2 = {'v', '2'};
    std::vector<uint8_t> v3 = {'v', '3'};

    ASSERT_TRUE(moved.append_trusted_new_row(row1, family, q1, 3, v1).ok());
    ASSERT_TRUE(moved.append_trusted_same_row(row1, family, q2, 2, v2).ok());
    ASSERT_TRUE(moved.append_trusted(row2, family, q3, 1, v3).ok());
    EXPECT_GE(moved.position(), 0);

    HFileWriter assigned = std::move(moved);
    EXPECT_TRUE(assigned.finish().ok());
    EXPECT_GE(assigned.position(), 0);

    EXPECT_TRUE(fs::exists(path));
    fs::remove(path);
}
