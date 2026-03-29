#include <gtest/gtest.h>
#include <hfile/writer.h>
#include <hfile/types.h>
#include "meta/trailer_builder.h"

#include <filesystem>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>

namespace fs = std::filesystem;
using namespace hfile;

// ─── Test helpers ─────────────────────────────────────────────────────────────

static fs::path tmp_path(const std::string& name) {
    auto p = fs::temp_directory_path() / ("hfile_test_" + name + ".hfile");
    return p;
}

static std::vector<uint8_t> read_file(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    return std::vector<uint8_t>(
        std::istreambuf_iterator<char>(f),
        std::istreambuf_iterator<char>());
}

static KeyValue make_kv(std::vector<uint8_t>& rk,
                         std::vector<uint8_t>& fam,
                         std::vector<uint8_t>& q,
                         std::vector<uint8_t>& v,
                         const std::string& row,
                         const std::string& qualifier,
                         int64_t ts,
                         const std::string& value) {
    rk.assign(row.begin(), row.end());
    fam = {'c','f'};
    q.assign(qualifier.begin(), qualifier.end());
    v.assign(value.begin(), value.end());
    KeyValue kv;
    kv.row = rk; kv.family = fam; kv.qualifier = q;
    kv.timestamp = ts; kv.key_type = KeyType::Put; kv.value = v;
    return kv;
}

// ─── Tests ────────────────────────────────────────────────────────────────────

TEST(HFileWriter, CreateAndFinishEmpty) {
    auto path = tmp_path("empty");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .build();
    ASSERT_TRUE(s.ok()) << s.message();
    ASSERT_NE(w, nullptr);

    auto fs2 = w->finish();
    ASSERT_TRUE(fs2.ok()) << fs2.message();

    // File must exist and be non-empty (trailer alone is several bytes)
    EXPECT_TRUE(fs::exists(path));
    EXPECT_GT(fs::file_size(path), 0u);
    fs::remove(path);
}

TEST(HFileWriter, TrailerVersionsAtEndOfFile) {
    auto path = tmp_path("trailer_versions");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .build();
    ASSERT_TRUE(s.ok());

    auto fin = w->finish();
    ASSERT_TRUE(fin.ok()) << fin.message();

    auto data = read_file(path);
    ASSERT_GE(data.size(), 12u);

    // Last 12 bytes: pb_offset(4) + major(4) + minor(4)
    const uint8_t* tail = data.data() + data.size() - 12;
    uint32_t major = read_be32(tail + 4);
    uint32_t minor = read_be32(tail + 8);
    EXPECT_EQ(major, kHFileMajorVersion);   // 3
    EXPECT_EQ(minor, kHFileMinorVersion);   // 3

    fs::remove(path);
}

TEST(HFileWriter, SingleKV) {
    auto path = tmp_path("single_kv");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    auto kv = make_kv(rk, fam, q, v, "row001", "col1", 1000000LL, "hello");
    EXPECT_TRUE(w->append(kv).ok());
    EXPECT_EQ(w->entry_count(), 1u);
    EXPECT_TRUE(w->finish().ok());

    EXPECT_TRUE(fs::exists(path));
    EXPECT_GT(fs::file_size(path), 100u);  // header + data block + index + trailer
    fs::remove(path);
}

TEST(HFileWriter, MultipleKVsSorted) {
    auto path = tmp_path("multi_kv");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::FastDiff)
        .set_bloom_type(BloomType::Row)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    const int N = 1000;
    for (int i = 0; i < N; ++i) {
        char row_buf[32];
        std::snprintf(row_buf, sizeof(row_buf), "row%08d", i);
        auto kv = make_kv(rk, fam, q, v,
                           row_buf, "col",
                           static_cast<int64_t>(N - i) * 1000,
                           "value_" + std::to_string(i));
        auto st = w->append(kv);
        ASSERT_TRUE(st.ok()) << "row " << i << ": " << st.message();
    }
    EXPECT_EQ(w->entry_count(), static_cast<uint64_t>(N));
    EXPECT_TRUE(w->finish().ok());
    EXPECT_TRUE(fs::exists(path));
    fs::remove(path);
}

TEST(HFileWriter, OutOfOrderRejected) {
    auto path = tmp_path("out_of_order");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_sort_mode(WriterOptions::SortMode::PreSortedVerified)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    EXPECT_TRUE(w->append(make_kv(rk, fam, q, v, "b", "col", 100, "v")).ok());
    // "a" < "b" → out of order
    auto st = w->append(make_kv(rk, fam, q, v, "a", "col", 100, "v"));
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), Status::Code::InvalidArg);
    fs::remove(path);
}

TEST(HFileWriter, WrongFamilyRejected) {
    auto path = tmp_path("wrong_family");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf1")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    fam = {'c','f','2'};  // wrong family
    rk  = {'r'};
    q   = {'c'};
    v   = {'v'};
    KeyValue kv;
    kv.row = rk; kv.family = fam; kv.qualifier = q;
    kv.timestamp = 1; kv.key_type = KeyType::Put; kv.value = v;
    auto st = w->append(kv);
    EXPECT_FALSE(st.ok());
    fs::remove(path);
}

TEST(HFileWriter, WithLZ4Compression) {
    auto path = tmp_path("lz4");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::LZ4)
        .set_data_block_encoding(Encoding::FastDiff)
        .set_bloom_type(BloomType::Row)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    for (int i = 0; i < 500; ++i) {
        char row[32];
        std::snprintf(row, sizeof(row), "row%08d", i);
        ASSERT_TRUE(w->append(
            make_kv(rk, fam, q, v, row, "col", 1000 - i, "compressible_value_payload")).ok());
    }
    EXPECT_TRUE(w->finish().ok());
    fs::remove(path);
}

TEST(HFileWriter, WithZstdCompression) {
    auto path = tmp_path("zstd");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::Zstd)
        .set_data_block_encoding(Encoding::FastDiff)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    for (int i = 0; i < 200; ++i) {
        char row[32]; std::snprintf(row, sizeof(row), "key%06d", i);
        ASSERT_TRUE(w->append(
            make_kv(rk, fam, q, v, row, "c", 9999 - i, "value")).ok());
    }
    EXPECT_TRUE(w->finish().ok());
    fs::remove(path);
}

TEST(HFileWriter, MultipleDataBlocks) {
    // Force multiple data block flushes with tiny block size
    auto path = tmp_path("multi_block");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_block_size(256)   // tiny: forces many blocks
        .set_bloom_type(BloomType::Row)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    for (int i = 0; i < 100; ++i) {
        char row[32]; std::snprintf(row, sizeof(row), "row%05d", i);
        ASSERT_TRUE(w->append(
            make_kv(rk, fam, q, v, row, "col", 99999 - i,
                    "some_medium_length_value_payload")).ok());
    }
    EXPECT_EQ(w->entry_count(), 100u);
    EXPECT_TRUE(w->finish().ok());
    fs::remove(path);
}

TEST(HFileWriter, FileInfoContainsMandatoryFields) {
    // Write a file and check the raw bytes for required field keys
    auto path = tmp_path("fileinfo_check");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_bloom_type(BloomType::None)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    ASSERT_TRUE(w->append(
        make_kv(rk, fam, q, v, "r001", "col", 1000, "val")).ok());
    ASSERT_TRUE(w->finish().ok());

    auto data = read_file(path);
    std::string content(data.begin(), data.end());

    // These keys MUST appear in the serialized FileInfo
    EXPECT_NE(content.find("hfile.LASTKEY"),           std::string::npos);
    EXPECT_NE(content.find("hfile.AVG_KEY_LEN"),       std::string::npos);
    EXPECT_NE(content.find("hfile.AVG_VALUE_LEN"),     std::string::npos);
    EXPECT_NE(content.find("hfile.MAX_TAGS_LEN"),      std::string::npos);
    EXPECT_NE(content.find("hfile.COMPARATOR"),        std::string::npos);
    EXPECT_NE(content.find("hfile.DATA_BLOCK_ENCODING"), std::string::npos);

    fs::remove(path);
}

TEST(HFileWriter, TagsAndMVCCPresent) {
    auto path = tmp_path("tags_mvcc");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_include_tags(true)
        .set_include_mvcc(true)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk = {'r'}, fam = {'c','f'}, q = {'q'}, v = {'v'};
    KeyValue kv;
    kv.row = rk; kv.family = fam; kv.qualifier = q;
    kv.timestamp = 1000; kv.key_type = KeyType::Put; kv.value = v;
    kv.memstore_ts = 0;
    ASSERT_TRUE(w->append(kv).ok());
    EXPECT_TRUE(w->finish().ok());
    fs::remove(path);
}

TEST(HFileWriter, AutoSortSortsOutOfOrderInput) {
    auto path = tmp_path("autosort");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    EXPECT_TRUE(w->append(make_kv(rk, fam, q, v, "b", "col", 100, "v")).ok());
    EXPECT_TRUE(w->append(make_kv(rk, fam, q, v, "a", "col", 100, "v")).ok());
    EXPECT_TRUE(w->finish().ok());
    EXPECT_TRUE(fs::exists(path));
    fs::remove(path);
}

TEST(HFileWriter, AutoSortRespectsMemoryBudget) {
    auto path = tmp_path("autosort_memory");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_block_size(128)
        .set_max_memory(66000)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    std::string big_value(4096, 'x');
    auto st = w->append(make_kv(rk, fam, q, v, "row", "col", 100, big_value));
    EXPECT_FALSE(st.ok());
    fs::remove(path);
}

TEST(HFileWriter, DisableTagsAndMVCCShrinksOutput) {
    auto with_path = tmp_path("tags_mvcc_on");
    auto without_path = tmp_path("tags_mvcc_off");

    auto [with_writer, s1] = HFileWriter::builder()
        .set_path(with_path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_include_tags(true)
        .set_include_mvcc(true)
        .build();
    auto [without_writer, s2] = HFileWriter::builder()
        .set_path(without_path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_include_tags(false)
        .set_include_mvcc(false)
        .build();
    ASSERT_TRUE(s1.ok());
    ASSERT_TRUE(s2.ok());

    std::vector<uint8_t> rk = {'r'}, fam = {'c','f'}, q = {'q'}, v(64, 'v'), tags(64, 't');
    KeyValue kv;
    kv.row = rk; kv.family = fam; kv.qualifier = q;
    kv.timestamp = 1000; kv.key_type = KeyType::Put; kv.value = v;
    kv.tags = tags;
    kv.memstore_ts = 123456;

    ASSERT_TRUE(with_writer->append(kv).ok());
    ASSERT_TRUE(without_writer->append(kv).ok());
    ASSERT_TRUE(with_writer->finish().ok());
    ASSERT_TRUE(without_writer->finish().ok());

    EXPECT_LT(fs::file_size(without_path), fs::file_size(with_path));
    fs::remove(with_path);
    fs::remove(without_path);
}

TEST(HFileWriter, TrustedSortMode) {
    // PreSortedTrusted: no validation, so out-of-order is accepted
    auto path = tmp_path("trusted_sort");
    auto [w, s] = HFileWriter::builder()
        .set_path(path.string())
        .set_column_family("cf")
        .set_compression(Compression::None)
        .set_data_block_encoding(Encoding::None)
        .set_sort_mode(WriterOptions::SortMode::PreSortedTrusted)
        .build();
    ASSERT_TRUE(s.ok());

    std::vector<uint8_t> rk, fam, q, v;
    // Properly sorted — trusted mode accepts it
    EXPECT_TRUE(w->append(make_kv(rk, fam, q, v, "a", "col", 100, "v")).ok());
    EXPECT_TRUE(w->append(make_kv(rk, fam, q, v, "b", "col", 100, "v")).ok());
    EXPECT_TRUE(w->finish().ok());
    fs::remove(path);
}

TEST(HFileWriter, BuilderRequiresPathAndCF) {
    {
        auto [w, s] = HFileWriter::builder()
            .set_column_family("cf")
            .build();
        EXPECT_FALSE(s.ok());  // missing path
    }
    {
        auto [w, s] = HFileWriter::builder()
            .set_path("/tmp/x.hfile")
            .build();
        EXPECT_FALSE(s.ok());  // missing column_family
    }
}
