#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#define private public
#include <hfile/writer.h>
#undef private

using namespace hfile;
namespace fs = std::filesystem;

namespace {

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

fs::path temp_path(const std::string& name) {
    auto path = fs::temp_directory_path() / ("writer_edge_" + name + ".hfile");
    std::error_code ec;
    fs::remove(path, ec);
    return path;
}

OwnedKeyValue make_owned_kv(std::string_view row,
                            std::string_view qualifier,
                            int64_t timestamp,
                            std::string_view value) {
    OwnedKeyValue kv;
    kv.row.assign(row.begin(), row.end());
    kv.family = {'c', 'f'};
    kv.qualifier.assign(qualifier.begin(), qualifier.end());
    kv.timestamp = timestamp;
    kv.key_type = KeyType::Put;
    kv.value.assign(value.begin(), value.end());
    return kv;
}

}  // namespace

TEST(HFileWriterEdgeCases, HotpathEnabledParanoidFlushAndDiskChecks) {
    ScopedEnvVar profiling("1");
    auto path = temp_path("hotpath_paranoid");

    auto builder = HFileWriter::builder();
    builder
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::Row)
        .set_block_size(128)
        .set_sort_mode(WriterOptions::SortMode::PreSortedTrusted)
        .set_fsync_policy(FsyncPolicy::Paranoid)
        .set_min_free_disk(1)
        .set_disk_check_interval(1);
    builder.opts_.fsync_block_interval = 1;

    auto [writer, status] = builder.build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto kv = make_owned_kv("row001", "payload", 7, std::string(70 * 1024, 'x'));
    ASSERT_TRUE(writer->append_trusted_new_row(kv.as_view()).ok());
    ASSERT_TRUE(writer->finish().ok());

    WriterStats stats = writer->stats();
    EXPECT_GT(stats.data_block_count, 0u);
    EXPECT_GT(stats.load_on_open_block_count, 0u);
    EXPECT_TRUE(fs::exists(path));
    fs::remove(path);
}

TEST(HFileWriterEdgeCases, BuilderRejectsZeroBytesPerChecksum) {
    auto path = temp_path("zero_checksum_bytes");

    auto builder = HFileWriter::builder();
    builder
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None);
    builder.opts_.bytes_per_checksum = 0;

    auto [writer, status] = builder.build();
    EXPECT_EQ(writer, nullptr);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.message().find("bytes_per_checksum"), std::string::npos);
}

TEST(HFileWriterEdgeCases, BuilderRejectsInvalidCompressionAndEncodingValues) {
    auto path = temp_path("invalid_builder_values");

    auto compression_builder = HFileWriter::builder();
    compression_builder
        .set_path(path.string())
        .set_column_family("cf");
    compression_builder.opts_.compression = static_cast<Compression>(99);

    auto [compression_writer, compression_status] = compression_builder.build();
    EXPECT_EQ(compression_writer, nullptr);
    EXPECT_FALSE(compression_status.ok());
    EXPECT_NE(compression_status.message().find("compression=NONE or GZ"), std::string::npos);

    auto encoding_builder = HFileWriter::builder();
    encoding_builder
        .set_path(path.string())
        .set_column_family("cf");
    encoding_builder.opts_.data_block_encoding = static_cast<Encoding>(99);

    auto [encoding_writer, encoding_status] = encoding_builder.build();
    EXPECT_EQ(encoding_writer, nullptr);
    EXPECT_FALSE(encoding_status.ok());
    EXPECT_NE(encoding_status.message().find("data_block_encoding=NONE"), std::string::npos);
}

TEST(HFileWriterEdgeCases, AppendVariantsRejectAfterFinishAndAutoSortTrustedPathsWork) {
    auto path = temp_path("append_after_finish");
    auto [writer, status] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_sort_mode(WriterOptions::SortMode::PreSortedTrusted)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto kv = make_owned_kv("row", "q", 1, "value");
    ASSERT_TRUE(writer->append_trusted_new_row(kv.as_view()).ok());
    ASSERT_TRUE(writer->finish().ok());

    EXPECT_FALSE(writer->append(kv.as_view()).ok());
    EXPECT_FALSE(writer->append_trusted(kv.as_view()).ok());
    EXPECT_FALSE(writer->append_trusted_new_row(kv.as_view()).ok());
    EXPECT_FALSE(writer->append_trusted_same_row(kv.as_view()).ok());
    fs::remove(path);

    auto autosort_path = temp_path("autosort_trusted");
    auto [autosort_writer, autosort_status] = HFileWriter::builder()
        .set_path(autosort_path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_sort_mode(WriterOptions::SortMode::AutoSort)
        .build();
    ASSERT_TRUE(autosort_status.ok()) << autosort_status.message();

    auto kv_b = make_owned_kv("row-b", "q", 1, "vb");
    auto kv_a = make_owned_kv("row-a", "q1", 1, "va");
    auto kv_a_same_row = make_owned_kv("row-a", "q2", 1, "va2");
    ASSERT_TRUE(autosort_writer->append_trusted(kv_b.as_view()).ok());
    ASSERT_TRUE(autosort_writer->append_trusted_new_row(kv_a.as_view()).ok());
    ASSERT_TRUE(autosort_writer->append_trusted_same_row(kv_a_same_row.as_view()).ok());
    ASSERT_TRUE(autosort_writer->finish().ok());
    EXPECT_TRUE(fs::exists(autosort_path));
    fs::remove(autosort_path);
}

TEST(HFileWriterEdgeCases, AppendDropsTagsAndMvccWhenDisabled) {
    auto path = temp_path("append_flags");
    auto [writer, status] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_sort_mode(WriterOptions::SortMode::PreSortedVerified)
        .set_include_tags(false)
        .set_include_mvcc(false)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto kv = make_owned_kv("row", "q", 1, "value");
    kv.tags = {'t', 'a', 'g'};
    kv.memstore_ts = 9;
    kv.has_memstore_ts = true;
    ASSERT_TRUE(writer->append(kv.as_view()).ok());
    ASSERT_TRUE(writer->finish().ok());
    EXPECT_TRUE(fs::exists(path));
    fs::remove(path);
}

TEST(HFileWriterEdgeCases, TrustedOutOfOrderAcrossBlocksThrowsOnDescendingRow) {
    auto path = temp_path("descending_midpoint");
    auto [writer, status] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_block_size(32)
        .set_sort_mode(WriterOptions::SortMode::PreSortedTrusted)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto first = make_owned_kv("b", "q", 2, std::string(256, 'a'));
    auto second = make_owned_kv("a", "q", 1, std::string(256, 'b'));
    ASSERT_TRUE(writer->append_trusted_new_row(first.as_view()).ok());
    ASSERT_TRUE(writer->append_trusted_new_row(second.as_view()).ok());
    EXPECT_THROW((void)writer->finish(), std::invalid_argument);

    writer.reset();
    EXPECT_FALSE(fs::exists(path));
}

TEST(HFileWriterEdgeCases, TrustedOutOfOrderAcrossBlocksThrowsOnShorterPrefix) {
    auto path = temp_path("prefix_midpoint");
    auto [writer, status] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_block_size(32)
        .set_sort_mode(WriterOptions::SortMode::PreSortedTrusted)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto first = make_owned_kv("ab", "q", 2, std::string(256, 'a'));
    auto second = make_owned_kv("a", "q", 1, std::string(256, 'b'));
    ASSERT_TRUE(writer->append_trusted_new_row(first.as_view()).ok());
    ASSERT_TRUE(writer->append_trusted_new_row(second.as_view()).ok());
    EXPECT_THROW((void)writer->finish(), std::invalid_argument);

    writer.reset();
    EXPECT_FALSE(fs::exists(path));
}

TEST(HFileWriterEdgeCases, TrustedDuplicateKeysAcrossBlocksCanStillFinish) {
    auto path = temp_path("duplicate_key_fallback");
    auto [writer, status] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .set_block_size(32)
        .set_sort_mode(WriterOptions::SortMode::PreSortedTrusted)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto first = make_owned_kv("dup", "q", 2, std::string(256, 'a'));
    auto second = make_owned_kv("dup", "q", 2, std::string(256, 'b'));
    ASSERT_TRUE(writer->append_trusted_new_row(first.as_view()).ok());
    ASSERT_TRUE(writer->append_trusted_new_row(second.as_view()).ok());
    ASSERT_TRUE(writer->finish().ok());

    EXPECT_TRUE(fs::exists(path));
    fs::remove(path);
}

TEST(HFileWriterEdgeCases, UnfinishedPlainWriterRemovesPartialFile) {
    auto path = temp_path("plain_cleanup");
    {
        auto [writer, status] = HFileWriter::builder()
            .set_path(path.string())
            .set_column_family("cf")
            .set_compression(Compression::None)
            .set_data_block_encoding(Encoding::None)
            .set_bloom_type(BloomType::None)
            .set_fsync_policy(FsyncPolicy::Paranoid)
            .build();
        ASSERT_TRUE(status.ok()) << status.message();
        EXPECT_TRUE(fs::exists(path));
    }
    EXPECT_FALSE(fs::exists(path));
}
