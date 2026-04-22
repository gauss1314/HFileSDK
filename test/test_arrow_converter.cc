#include <gtest/gtest.h>
#include "codec/compressor.h"
#include "convert/converter.h"
#include <hfile/types.h>

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>

#include <vector>
#include <string>
#include <filesystem>
#include <chrono>
#include <fstream>
#include <atomic>
#include <map>
#include <span>

using namespace hfile;
namespace fs = std::filesystem;

// ─── Helpers to build Arrow batches ──────────────────────────────────────────

static std::shared_ptr<arrow::RecordBatch> make_wide_batch(int n_rows = 3) {
    arrow::StringBuilder rk_builder;
    arrow::Int64Builder  int_builder;
    arrow::StringBuilder str_builder;

    for (int i = 0; i < n_rows; ++i) {
        ARROW_EXPECT_OK(rk_builder.Append("row_" + std::to_string(i)));
        ARROW_EXPECT_OK(int_builder.Append(i * 100));
        ARROW_EXPECT_OK(str_builder.Append("val_" + std::to_string(i)));
    }

    std::shared_ptr<arrow::Array> rk_arr, int_arr, str_arr;
    ARROW_EXPECT_OK(rk_builder.Finish(&rk_arr));
    ARROW_EXPECT_OK(int_builder.Finish(&int_arr));
    ARROW_EXPECT_OK(str_builder.Finish(&str_arr));

    auto schema = arrow::schema({
        arrow::field("__row_key__", arrow::utf8()),
        arrow::field("age",         arrow::int64()),
        arrow::field("name",        arrow::utf8()),
    });

    return arrow::RecordBatch::Make(schema, n_rows, {rk_arr, int_arr, str_arr});
}

static fs::path make_temp_dir() {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    fs::path dir = fs::temp_directory_path() / ("hfilesdk_test_" + std::to_string(now));
    fs::create_directories(dir);
    return dir;
}

static void write_ipc_stream(const arrow::RecordBatch& batch, const fs::path& path) {
    auto sink_result = arrow::io::FileOutputStream::Open(path.string());
    ASSERT_TRUE(sink_result.ok()) << sink_result.status().ToString();
    auto sink = *sink_result;

    auto writer_result = arrow::ipc::MakeStreamWriter(sink.get(), batch.schema());
    ASSERT_TRUE(writer_result.ok()) << writer_result.status().ToString();
    auto writer = *writer_result;
    ARROW_EXPECT_OK(writer->WriteRecordBatch(batch));
    ARROW_EXPECT_OK(writer->Close());
    ARROW_EXPECT_OK(sink->Close());
}

static void write_bytes(const fs::path& path, std::string_view bytes) {
    std::ofstream out(path, std::ios::binary);
    ASSERT_TRUE(out.is_open());
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.close();
}

struct BlockView {
    std::string magic;
    uint32_t uncompressed_size;
    std::span<const uint8_t> payload;
};

static std::vector<uint8_t> read_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<uint8_t>(
        std::istreambuf_iterator<char>(in),
        std::istreambuf_iterator<char>());
}

static std::vector<BlockView> scan_blocks(const std::vector<uint8_t>& data) {
    std::vector<BlockView> blocks;
    const size_t limit = data.size() - kTrailerFixedSize;
    size_t offset = 0;
    while (offset + kBlockHeaderSize <= limit) {
        const auto on_disk_size_without_header = read_be32(data.data() + offset + 8);
        const auto on_disk_data_with_header = read_be32(data.data() + offset + 29);
        const auto payload_size =
            static_cast<size_t>(on_disk_data_with_header) - kBlockHeaderSize;
        blocks.push_back(BlockView{
            std::string(reinterpret_cast<const char*>(data.data() + offset), 8),
            read_be32(data.data() + offset + 12),
            {data.data() + offset + kBlockHeaderSize, payload_size},
        });
        offset += kBlockHeaderSize + on_disk_size_without_header;
    }
    return blocks;
}

TEST(ArrowConverter, ConvertRejectsDuplicateRowsWithSameGeneratedRowKey) {
    arrow::StringBuilder rk_builder;
    arrow::Int64Builder age_builder;
    arrow::StringBuilder name_builder;

    ARROW_EXPECT_OK(rk_builder.Append("dup"));
    ARROW_EXPECT_OK(age_builder.Append(10));
    ARROW_EXPECT_OK(name_builder.AppendNull());

    ARROW_EXPECT_OK(rk_builder.Append("dup"));
    ARROW_EXPECT_OK(age_builder.AppendNull());
    ARROW_EXPECT_OK(name_builder.Append("alice"));

    std::shared_ptr<arrow::Array> rk_arr, age_arr, name_arr;
    ARROW_EXPECT_OK(rk_builder.Finish(&rk_arr));
    ARROW_EXPECT_OK(age_builder.Finish(&age_arr));
    ARROW_EXPECT_OK(name_builder.Finish(&name_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::utf8()),
        arrow::field("age", arrow::int64()),
        arrow::field("name", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 2, {rk_arr, age_arr, name_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,0";
    opts.column_family = "cf";
    opts.default_timestamp = 1234;
    opts.writer_opts.column_family = "cf";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_EQ(result.duplicate_key_count, 1);
    EXPECT_EQ(result.kv_written_count, 3);
    EXPECT_EQ(result.kv_skipped_count, 1);
    EXPECT_TRUE(fs::exists(hfile_path));

    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertRejectsDuplicateCellsWithinSameRowKey) {
    arrow::StringBuilder rk_builder;
    arrow::Int64Builder age_builder;
    arrow::StringBuilder name_builder;

    ARROW_EXPECT_OK(rk_builder.Append("dup"));
    ARROW_EXPECT_OK(age_builder.Append(10));
    ARROW_EXPECT_OK(name_builder.AppendNull());

    ARROW_EXPECT_OK(rk_builder.Append("dup"));
    ARROW_EXPECT_OK(age_builder.Append(20));
    ARROW_EXPECT_OK(name_builder.AppendNull());

    std::shared_ptr<arrow::Array> rk_arr, age_arr, name_arr;
    ARROW_EXPECT_OK(rk_builder.Finish(&rk_arr));
    ARROW_EXPECT_OK(age_builder.Finish(&age_arr));
    ARROW_EXPECT_OK(name_builder.Finish(&name_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::utf8()),
        arrow::field("age", arrow::int64()),
        arrow::field("name", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 2, {rk_arr, age_arr, name_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,0";
    opts.column_family = "cf";
    opts.default_timestamp = 1234;
    opts.writer_opts.column_family = "cf";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_EQ(result.duplicate_key_count, 1);
    EXPECT_EQ(result.kv_written_count, 2);
    EXPECT_EQ(result.kv_skipped_count, 2);

    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertSupportsLargeStringColumns) {
    arrow::StringBuilder rk_builder;
    arrow::LargeStringBuilder payload_builder;

    ARROW_EXPECT_OK(rk_builder.Append("row_1"));
    ARROW_EXPECT_OK(payload_builder.Append("large_payload"));

    std::shared_ptr<arrow::Array> rk_arr, payload_arr;
    ARROW_EXPECT_OK(rk_builder.Finish(&rk_arr));
    ARROW_EXPECT_OK(payload_builder.Finish(&payload_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::utf8()),
        arrow::field("payload", arrow::large_utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 1, {rk_arr, payload_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,0";
    opts.column_family = "cf";
    opts.default_timestamp = 1234;
    opts.writer_opts.column_family = "cf";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_EQ(result.kv_written_count, 2);

    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertRejectsEmptyArrowPath) {
    ConvertOptions opts;
    opts.hfile_path = "/tmp/out.hfile";
    opts.row_key_rule = "ID,0,false,0";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::INVALID_ARGUMENT);
    EXPECT_NE(result.error_message.find("arrow_path is empty"), std::string::npos);
}

TEST(ArrowConverter, ConvertRejectsMissingArrowFile) {
    auto dir = make_temp_dir();
    ConvertOptions opts;
    opts.arrow_path = (dir / "missing.arrow").string();
    opts.hfile_path = (dir / "out.hfile").string();
    opts.row_key_rule = "ID,0,false,0";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::ARROW_FILE_ERROR);
    EXPECT_NE(result.error_message.find("Arrow file not found"), std::string::npos);
    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertRejectsInvalidRowKeyRule) {
    auto batch = make_wide_batch(1);
    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.row_key_rule = "bad_rule";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::INVALID_ROW_KEY_RULE);
    EXPECT_FALSE(result.error_message.empty());
    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertRejectsSchemaMismatchWhenRowKeyColumnMissing) {
    auto batch = make_wide_batch(1);
    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.row_key_rule = "ID,9,false,0";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::SCHEMA_MISMATCH);
    EXPECT_NE(result.error_message.find("SCHEMA_MISMATCH"), std::string::npos);
    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertRejectsUnsupportedBinaryRowKeyField) {
    arrow::BinaryBuilder key_builder;
    arrow::StringBuilder payload_builder;
    ARROW_EXPECT_OK(key_builder.Append("row1", 4));
    ARROW_EXPECT_OK(payload_builder.Append("v1"));

    std::shared_ptr<arrow::Array> key_arr, payload_arr;
    ARROW_EXPECT_OK(key_builder.Finish(&key_arr));
    ARROW_EXPECT_OK(payload_builder.Finish(&payload_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::binary()),
        arrow::field("payload", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 1, {key_arr, payload_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.row_key_rule = "ID,0,false,0";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::SCHEMA_MISMATCH);
    EXPECT_NE(result.error_message.find("unsupported Arrow type"), std::string::npos);
    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertAcceptsLongHashRowKeyRule) {
    arrow::StringBuilder id_builder;
    arrow::StringBuilder value_builder;

    ARROW_EXPECT_OK(id_builder.Append("123456789"));
    ARROW_EXPECT_OK(value_builder.Append("v1"));
    ARROW_EXPECT_OK(id_builder.Append("223456789"));
    ARROW_EXPECT_OK(value_builder.Append("v2"));

    std::shared_ptr<arrow::Array> id_arr, value_arr;
    ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
    ARROW_EXPECT_OK(value_builder.Finish(&value_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::utf8()),
        arrow::field("payload", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 2, {id_arr, value_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "long(hash),0,false,0";
    opts.column_family = "cf";
    opts.default_timestamp = 1234;
    opts.writer_opts.column_family = "cf";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_EQ(result.kv_written_count, 4);
    EXPECT_TRUE(fs::exists(hfile_path));
    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertAllowsUnusedUnsupportedColumnsBeforeReferencedIndex) {
    arrow::BinaryBuilder c0_builder;
    arrow::StringBuilder c1_builder;
    arrow::StringBuilder c2_builder;
    arrow::StringBuilder c3_builder;
    arrow::StringBuilder c4_builder;
    arrow::StringBuilder c5_builder;

    ARROW_EXPECT_OK(c0_builder.Append("raw", 3));
    ARROW_EXPECT_OK(c1_builder.Append("a"));
    ARROW_EXPECT_OK(c2_builder.Append("b"));
    ARROW_EXPECT_OK(c3_builder.Append("c"));
    ARROW_EXPECT_OK(c4_builder.Append("d"));
    ARROW_EXPECT_OK(c5_builder.Append("row-key"));

    std::shared_ptr<arrow::Array> c0, c1, c2, c3, c4, c5;
    ARROW_EXPECT_OK(c0_builder.Finish(&c0));
    ARROW_EXPECT_OK(c1_builder.Finish(&c1));
    ARROW_EXPECT_OK(c2_builder.Finish(&c2));
    ARROW_EXPECT_OK(c3_builder.Finish(&c3));
    ARROW_EXPECT_OK(c4_builder.Finish(&c4));
    ARROW_EXPECT_OK(c5_builder.Finish(&c5));

    auto schema = arrow::schema({
        arrow::field("c0", arrow::binary()),
        arrow::field("c1", arrow::utf8()),
        arrow::field("c2", arrow::utf8()),
        arrow::field("c3", arrow::utf8()),
        arrow::field("c4", arrow::utf8()),
        arrow::field("c5", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 1, {c0, c1, c2, c3, c4, c5});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "KEY,5,false,0";
    opts.column_family = "cf";
    opts.default_timestamp = 1;
    opts.writer_opts.column_family = "cf";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_TRUE(fs::exists(hfile_path));
    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertSupportsExcludedColumnPrefixes) {
    arrow::StringBuilder meta_builder;
    arrow::StringBuilder id_builder;
    arrow::StringBuilder payload_builder;

    ARROW_EXPECT_OK(meta_builder.Append("meta-1"));
    ARROW_EXPECT_OK(meta_builder.Append("meta-2"));
    ARROW_EXPECT_OK(id_builder.Append("row-1"));
    ARROW_EXPECT_OK(id_builder.Append("row-2"));
    ARROW_EXPECT_OK(payload_builder.Append("value-1"));
    ARROW_EXPECT_OK(payload_builder.Append("value-2"));

    std::shared_ptr<arrow::Array> meta_arr, id_arr, payload_arr;
    ARROW_EXPECT_OK(meta_builder.Finish(&meta_arr));
    ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
    ARROW_EXPECT_OK(payload_builder.Finish(&payload_arr));

    auto schema = arrow::schema({
        arrow::field("_hoodie_commit_time", arrow::utf8()),
        arrow::field("id", arrow::utf8()),
        arrow::field("payload", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 2, {meta_arr, id_arr, payload_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,0";
    opts.column_family = "cf";
    opts.default_timestamp = 1;
    opts.writer_opts.column_family = "cf";
    opts.excluded_column_prefixes = {"_hoodie"};

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_EQ(result.arrow_rows_read, 2);
    EXPECT_TRUE(fs::exists(hfile_path));
    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertCombinesSortingExclusionAndGZipCompression) {
    arrow::StringBuilder meta_builder;
    arrow::StringBuilder id_builder;
    arrow::StringBuilder payload_builder;
    arrow::StringBuilder city_builder;

    ARROW_EXPECT_OK(meta_builder.Append("meta-2"));
    ARROW_EXPECT_OK(meta_builder.Append("meta-1"));
    ARROW_EXPECT_OK(id_builder.Append("row-2"));
    ARROW_EXPECT_OK(id_builder.Append("row-1"));
    ARROW_EXPECT_OK(payload_builder.Append("drop-2"));
    ARROW_EXPECT_OK(payload_builder.Append("drop-1"));
    ARROW_EXPECT_OK(city_builder.Append("hz"));
    ARROW_EXPECT_OK(city_builder.Append("sh"));

    std::shared_ptr<arrow::Array> meta_arr, id_arr, payload_arr, city_arr;
    ARROW_EXPECT_OK(meta_builder.Finish(&meta_arr));
    ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
    ARROW_EXPECT_OK(payload_builder.Finish(&payload_arr));
    ARROW_EXPECT_OK(city_builder.Finish(&city_arr));

    auto schema = arrow::schema({
        arrow::field("_hoodie_commit_time", arrow::utf8()),
        arrow::field("id", arrow::utf8()),
        arrow::field("payload", arrow::utf8()),
        arrow::field("city", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(
        schema, 2, {meta_arr, id_arr, payload_arr, city_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,0";
    opts.column_family = "cf";
    opts.default_timestamp = 1;
    opts.writer_opts.column_family = "cf";
    opts.writer_opts.compression = Compression::GZip;
    opts.excluded_columns = {"payload"};
    opts.excluded_column_prefixes = {"_hoodie"};

    auto result = convert(opts);
    ASSERT_EQ(result.error_code, ErrorCode::OK) << result.error_message;
    ASSERT_TRUE(fs::exists(hfile_path));

    auto file_bytes = read_file(hfile_path);
    auto blocks = scan_blocks(file_bytes);
    auto gzip = codec::Compressor::create(Compression::GZip, 6);
    ASSERT_NE(gzip, nullptr);

    std::map<std::string, int> counts;
    std::string decompressed_data_blocks;
    for (const auto& block : blocks) {
        if (block.magic != "DATABLK*" &&
            block.magic != "BLMFBLK2" &&
            block.magic != "BLMFMET2" &&
            block.magic != "FILEINF2" &&
            block.magic != "IDXROOT2") {
            continue;
        }

        std::vector<uint8_t> decompressed(block.uncompressed_size);
        auto status = gzip->decompress(
            block.payload, decompressed.data(), decompressed.size());
        ASSERT_TRUE(status.ok()) << block.magic << " " << status.message();
        counts[block.magic]++;
        if (block.magic == "DATABLK*") {
            decompressed_data_blocks.append(
                reinterpret_cast<const char*>(decompressed.data()), decompressed.size());
        }
    }

    EXPECT_GE(counts["DATABLK*"], 1);
    EXPECT_GE(counts["IDXROOT2"], 1);
    EXPECT_GE(counts["FILEINF2"], 1);
    EXPECT_GE(counts["BLMFBLK2"], 1);
    EXPECT_GE(counts["BLMFMET2"], 1);

    auto row1_pos = decompressed_data_blocks.find("row-1");
    auto row2_pos = decompressed_data_blocks.find("row-2");
    ASSERT_NE(row1_pos, std::string::npos);
    ASSERT_NE(row2_pos, std::string::npos);
    EXPECT_LT(row1_pos, row2_pos);
    EXPECT_EQ(decompressed_data_blocks.find("payload"), std::string::npos);
    EXPECT_EQ(decompressed_data_blocks.find("_hoodie_commit_time"), std::string::npos);
    EXPECT_NE(decompressed_data_blocks.find("city"), std::string::npos);

    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertSupportsZeroLeftPaddedNumericRowKey) {
    arrow::Int64Builder id_builder;
    arrow::StringBuilder city_builder;

    ARROW_EXPECT_OK(id_builder.Append(12));
    ARROW_EXPECT_OK(id_builder.Append(2));
    ARROW_EXPECT_OK(city_builder.Append("hz"));
    ARROW_EXPECT_OK(city_builder.Append("sh"));

    std::shared_ptr<arrow::Array> id_arr, city_arr;
    ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
    ARROW_EXPECT_OK(city_builder.Finish(&city_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("city", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 2, {id_arr, city_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,5";
    opts.column_family = "cf";
    opts.default_timestamp = 1;
    opts.writer_opts.column_family = "cf";
    opts.writer_opts.compression = Compression::None;
    opts.numeric_sort_fast_path = NumericSortFastPathMode::On;

    auto result = convert(opts);
    ASSERT_EQ(result.error_code, ErrorCode::OK) << result.error_message;
    EXPECT_EQ(result.numeric_sort_fast_path_mode, NumericSortFastPathMode::On);
    EXPECT_TRUE(result.numeric_sort_fast_path_used);
    ASSERT_TRUE(fs::exists(hfile_path));

    auto file_bytes = read_file(hfile_path);
    auto blocks = scan_blocks(file_bytes);

    std::string data_blocks;
    for (const auto& block : blocks) {
        if (block.magic != "DATABLK*") continue;
        data_blocks.append(
            reinterpret_cast<const char*>(block.payload.data()),
            block.payload.size());
    }

    auto row2_pos = data_blocks.find("00002");
    auto row12_pos = data_blocks.find("00012");
    ASSERT_NE(row2_pos, std::string::npos);
    ASSERT_NE(row12_pos, std::string::npos);
    EXPECT_LT(row2_pos, row12_pos);

    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertSupportsNumericPrefixFastPathForCompositeRowKey) {
    arrow::Int64Builder id_builder;
    arrow::StringBuilder city_builder;

    ARROW_EXPECT_OK(id_builder.Append(12));
    ARROW_EXPECT_OK(id_builder.Append(2));
    ARROW_EXPECT_OK(id_builder.Append(2));
    ARROW_EXPECT_OK(city_builder.Append("hz"));
    ARROW_EXPECT_OK(city_builder.Append("sh"));
    ARROW_EXPECT_OK(city_builder.Append("aa"));

    std::shared_ptr<arrow::Array> id_arr, city_arr;
    ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
    ARROW_EXPECT_OK(city_builder.Finish(&city_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("city", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 3, {id_arr, city_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,5#CITY,1,false,0";
    opts.column_family = "cf";
    opts.default_timestamp = 1;
    opts.writer_opts.column_family = "cf";
    opts.writer_opts.compression = Compression::None;
    opts.numeric_sort_fast_path = NumericSortFastPathMode::On;

    auto result = convert(opts);
    ASSERT_EQ(result.error_code, ErrorCode::OK) << result.error_message;
    EXPECT_EQ(result.numeric_sort_fast_path_mode, NumericSortFastPathMode::On);
    EXPECT_TRUE(result.numeric_sort_fast_path_used);
    ASSERT_TRUE(fs::exists(hfile_path));

    auto file_bytes = read_file(hfile_path);
    auto blocks = scan_blocks(file_bytes);

    std::string data_blocks;
    for (const auto& block : blocks) {
        if (block.magic != "DATABLK*") continue;
        data_blocks.append(
            reinterpret_cast<const char*>(block.payload.data()),
            block.payload.size());
    }

    auto row2aa_pos = data_blocks.find("00002aa");
    auto row2sh_pos = data_blocks.find("00002sh");
    auto row12hz_pos = data_blocks.find("00012hz");
    ASSERT_NE(row2aa_pos, std::string::npos);
    ASSERT_NE(row2sh_pos, std::string::npos);
    ASSERT_NE(row12hz_pos, std::string::npos);
    EXPECT_LT(row2aa_pos, row2sh_pos);
    EXPECT_LT(row2sh_pos, row12hz_pos);

    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertFallsBackForNegativeNumericRowKeyValues) {
    arrow::Int64Builder id_builder;
    arrow::StringBuilder city_builder;

    ARROW_EXPECT_OK(id_builder.Append(-2));
    ARROW_EXPECT_OK(id_builder.Append(2));
    ARROW_EXPECT_OK(id_builder.Append(-10));
    ARROW_EXPECT_OK(city_builder.Append("hz"));
    ARROW_EXPECT_OK(city_builder.Append("sh"));
    ARROW_EXPECT_OK(city_builder.Append("bj"));

    std::shared_ptr<arrow::Array> id_arr, city_arr;
    ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
    ARROW_EXPECT_OK(city_builder.Finish(&city_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("city", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 3, {id_arr, city_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,5";
    opts.column_family = "cf";
    opts.default_timestamp = 1;
    opts.writer_opts.column_family = "cf";
    opts.writer_opts.compression = Compression::None;

    auto result = convert(opts);
    ASSERT_EQ(result.error_code, ErrorCode::OK) << result.error_message;
    EXPECT_EQ(result.numeric_sort_fast_path_mode, NumericSortFastPathMode::Auto);
    EXPECT_FALSE(result.numeric_sort_fast_path_used);
    ASSERT_TRUE(fs::exists(hfile_path));

    auto file_bytes = read_file(hfile_path);
    auto blocks = scan_blocks(file_bytes);

    std::string data_blocks;
    for (const auto& block : blocks) {
        if (block.magic != "DATABLK*") continue;
        data_blocks.append(
            reinterpret_cast<const char*>(block.payload.data()),
            block.payload.size());
    }

    auto row_neg10_pos = data_blocks.find("00-10");
    auto row_neg2_pos = data_blocks.find("000-2");
    auto row_pos2_pos = data_blocks.find("00002");
    ASSERT_NE(row_neg10_pos, std::string::npos);
    ASSERT_NE(row_neg2_pos, std::string::npos);
    ASSERT_NE(row_pos2_pos, std::string::npos);
    EXPECT_LT(row_neg10_pos, row_neg2_pos);
    EXPECT_LT(row_neg2_pos, row_pos2_pos);

    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertFallsBackWhenNumericValueExceedsPadLength) {
    arrow::Int64Builder id_builder;
    arrow::StringBuilder city_builder;

    ARROW_EXPECT_OK(id_builder.Append(999));
    ARROW_EXPECT_OK(id_builder.Append(1000));
    ARROW_EXPECT_OK(id_builder.Append(2));
    ARROW_EXPECT_OK(city_builder.Append("hz"));
    ARROW_EXPECT_OK(city_builder.Append("sh"));
    ARROW_EXPECT_OK(city_builder.Append("bj"));

    std::shared_ptr<arrow::Array> id_arr, city_arr;
    ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
    ARROW_EXPECT_OK(city_builder.Finish(&city_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("city", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 3, {id_arr, city_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,3";
    opts.column_family = "cf";
    opts.default_timestamp = 1;
    opts.writer_opts.column_family = "cf";
    opts.writer_opts.compression = Compression::None;

    auto result = convert(opts);
    ASSERT_EQ(result.error_code, ErrorCode::OK) << result.error_message;
    EXPECT_EQ(result.numeric_sort_fast_path_mode, NumericSortFastPathMode::Auto);
    EXPECT_FALSE(result.numeric_sort_fast_path_used);
    ASSERT_TRUE(fs::exists(hfile_path));

    auto file_bytes = read_file(hfile_path);
    auto blocks = scan_blocks(file_bytes);

    std::string data_blocks;
    for (const auto& block : blocks) {
        if (block.magic != "DATABLK*") continue;
        data_blocks.append(
            reinterpret_cast<const char*>(block.payload.data()),
            block.payload.size());
    }

    auto row2_pos = data_blocks.find("002");
    auto row1000_pos = data_blocks.find("1000");
    auto row999_pos = data_blocks.find("999");
    ASSERT_NE(row2_pos, std::string::npos);
    ASSERT_NE(row1000_pos, std::string::npos);
    ASSERT_NE(row999_pos, std::string::npos);
    EXPECT_LT(row2_pos, row1000_pos);
    EXPECT_LT(row1000_pos, row999_pos);

    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertRejectsForcedNumericSortFastPathWhenValuesAreNegative) {
    arrow::Int64Builder id_builder;
    arrow::StringBuilder city_builder;

    ARROW_EXPECT_OK(id_builder.Append(-2));
    ARROW_EXPECT_OK(id_builder.Append(2));
    ARROW_EXPECT_OK(city_builder.Append("hz"));
    ARROW_EXPECT_OK(city_builder.Append("sh"));

    std::shared_ptr<arrow::Array> id_arr, city_arr;
    ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
    ARROW_EXPECT_OK(city_builder.Finish(&city_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("city", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 2, {id_arr, city_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,5";
    opts.column_family = "cf";
    opts.default_timestamp = 1;
    opts.writer_opts.column_family = "cf";
    opts.writer_opts.compression = Compression::None;
    opts.numeric_sort_fast_path = NumericSortFastPathMode::On;

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::INVALID_ARGUMENT);
    EXPECT_NE(result.error_message.find("numeric_sort_fast_path=on"), std::string::npos);

    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertRejectsForcedNumericSortFastPathWhenValueExceedsPadLength) {
    arrow::Int64Builder id_builder;
    arrow::StringBuilder city_builder;

    ARROW_EXPECT_OK(id_builder.Append(999));
    ARROW_EXPECT_OK(id_builder.Append(1000));
    ARROW_EXPECT_OK(city_builder.Append("hz"));
    ARROW_EXPECT_OK(city_builder.Append("sh"));

    std::shared_ptr<arrow::Array> id_arr, city_arr;
    ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
    ARROW_EXPECT_OK(city_builder.Finish(&city_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("city", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 2, {id_arr, city_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,3";
    opts.column_family = "cf";
    opts.default_timestamp = 1;
    opts.writer_opts.column_family = "cf";
    opts.writer_opts.compression = Compression::None;
    opts.numeric_sort_fast_path = NumericSortFastPathMode::On;

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::INVALID_ARGUMENT);
    EXPECT_NE(result.error_message.find("fit the configured padLen"), std::string::npos);

    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertRejectsCorruptedArrowStream) {
    auto dir = make_temp_dir();
    auto arrow_path = dir / "broken.arrow";
    auto hfile_path = dir / "out.hfile";
    write_bytes(arrow_path, "not-an-arrow-stream");

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.row_key_rule = "ID,0,false,0";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::ARROW_FILE_ERROR);
    EXPECT_FALSE(result.error_message.empty());
    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertReportsProgress) {
    auto batch = make_wide_batch(5);
    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    std::atomic<int> callback_count{0};
    std::atomic<int64_t> last_rows_done{0};
    std::atomic<int64_t> last_total_rows{0};

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,0";
    opts.column_family = "cf";
    opts.default_timestamp = 1234;
    opts.writer_opts.column_family = "cf";
    opts.progress_cb = [&](int64_t rows_done, int64_t total_rows) {
        ++callback_count;
        last_rows_done = rows_done;
        last_total_rows = total_rows;
    };

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_GT(callback_count.load(), 0);
    EXPECT_EQ(last_rows_done.load(), 5);
    EXPECT_EQ(last_total_rows.load(), 5);
    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertWritesEmptyHFileWhenAllRowKeysAreEmpty) {
    arrow::StringBuilder id_builder;
    arrow::StringBuilder value_builder;
    ARROW_EXPECT_OK(id_builder.Append(""));
    ARROW_EXPECT_OK(id_builder.Append(""));
    ARROW_EXPECT_OK(value_builder.Append("v1"));
    ARROW_EXPECT_OK(value_builder.Append("v2"));
    std::shared_ptr<arrow::Array> id_arr, value_arr;
    ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
    ARROW_EXPECT_OK(value_builder.Finish(&value_arr));

    auto schema = arrow::schema({
        arrow::field("id", arrow::utf8()),
        arrow::field("value", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 2, {id_arr, value_arr});

    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,0";
    opts.column_family = "cf";
    opts.writer_opts.column_family = "cf";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_EQ(result.kv_written_count, 0);
    EXPECT_EQ(result.kv_skipped_count, 2);
    EXPECT_TRUE(fs::exists(hfile_path));
    fs::remove_all(dir);
}

TEST(ArrowConverter, ConvertReturnsMemoryExhaustedWhenBudgetTooSmall) {
    auto batch = make_wide_batch(1);
    auto dir = make_temp_dir();
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    write_ipc_stream(*batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.table_name = "t";
    opts.row_key_rule = "ID,0,false,0";
    opts.column_family = "cf";
    opts.writer_opts.column_family = "cf";
    opts.writer_opts.max_memory_bytes = 1;

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::MEMORY_EXHAUSTED);
    EXPECT_FALSE(result.error_message.empty());
    EXPECT_EQ(result.memory_budget_bytes, 1);
    EXPECT_LE(result.tracked_memory_peak_bytes, result.memory_budget_bytes);
    fs::remove_all(dir);
}
