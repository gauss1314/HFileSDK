#include <gtest/gtest.h>
#include "arrow/arrow_to_kv_converter.h"
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

using namespace hfile;
using namespace hfile::arrow_convert;
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

static std::shared_ptr<arrow::RecordBatch> make_tall_batch(int n_rows = 3) {
    arrow::StringBuilder rk_b, cf_b, q_b, v_b;
    arrow::Int64Builder  ts_b;

    for (int i = 0; i < n_rows; ++i) {
        ARROW_EXPECT_OK(rk_b.Append("row_" + std::to_string(i)));
        ARROW_EXPECT_OK(cf_b.Append("cf"));
        ARROW_EXPECT_OK(q_b.Append("col" + std::to_string(i)));
        ARROW_EXPECT_OK(ts_b.Append(1000000LL + i));
        ARROW_EXPECT_OK(v_b.Append("value_" + std::to_string(i)));
    }

    std::shared_ptr<arrow::Array> rk, cf, q, ts, v;
    ARROW_EXPECT_OK(rk_b.Finish(&rk));
    ARROW_EXPECT_OK(cf_b.Finish(&cf));
    ARROW_EXPECT_OK(q_b.Finish(&q));
    ARROW_EXPECT_OK(ts_b.Finish(&ts));
    ARROW_EXPECT_OK(v_b.Finish(&v));

    auto schema = arrow::schema({
        arrow::field("row_key",   arrow::utf8()),
        arrow::field("cf",        arrow::utf8()),
        arrow::field("qualifier", arrow::utf8()),
        arrow::field("timestamp", arrow::int64()),
        arrow::field("value",     arrow::utf8()),
    });

    return arrow::RecordBatch::Make(schema, n_rows, {rk, cf, q, ts, v});
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

// ─── Wide Table ───────────────────────────────────────────────────────────────

TEST(ArrowConverter, WideTableBasic) {
    auto batch = make_wide_batch(4);
    WideTableConfig cfg;
    cfg.column_family   = "cf";
    cfg.default_timestamp = 9999999LL;

    std::vector<OwnedKeyValue> out_kvs;

    auto cb = [&](const KeyValue& kv) -> Status {
        OwnedKeyValue o;
        o.row.assign(kv.row.begin(), kv.row.end());
        o.family.assign(kv.family.begin(), kv.family.end());
        o.qualifier.assign(kv.qualifier.begin(), kv.qualifier.end());
        o.timestamp = kv.timestamp;
        o.key_type  = kv.key_type;
        o.value.assign(kv.value.begin(), kv.value.end());
        out_kvs.push_back(std::move(o));
        return Status::OK();
    };

    auto s = ArrowToKVConverter::convert_wide_table(*batch, cfg, cb);
    ASSERT_TRUE(s.ok()) << s.message();

    // 4 rows × 2 non-key columns = 8 KVs
    EXPECT_EQ(out_kvs.size(), 8u);

    // All KVs should have family = "cf"
    for (const auto& kv : out_kvs) {
        std::string fam(kv.family.begin(), kv.family.end());
        EXPECT_EQ(fam, "cf");
    }
}

TEST(ArrowConverter, WideTableMissingRowKeyColumn) {
    arrow::StringBuilder col_builder;
    ARROW_EXPECT_OK(col_builder.Append("v1"));
    std::shared_ptr<arrow::Array> arr;
    ARROW_EXPECT_OK(col_builder.Finish(&arr));

    auto schema = arrow::schema({arrow::field("other", arrow::utf8())});
    auto batch  = arrow::RecordBatch::Make(schema, 1, {arr});

    WideTableConfig cfg;
    auto s = ArrowToKVConverter::convert_wide_table(
        *batch, cfg, [](const KeyValue&) { return Status::OK(); });
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), Status::Code::InvalidArg);
}

// ─── Tall Table ───────────────────────────────────────────────────────────────

TEST(ArrowConverter, TallTableBasic) {
    auto batch = make_tall_batch(5);
    TallTableConfig cfg;

    std::vector<OwnedKeyValue> kvs;
    auto cb = [&](const KeyValue& kv) -> Status {
        OwnedKeyValue o;
        o.row.assign(kv.row.begin(), kv.row.end());
        o.family.assign(kv.family.begin(), kv.family.end());
        o.qualifier.assign(kv.qualifier.begin(), kv.qualifier.end());
        o.timestamp = kv.timestamp;
        o.key_type  = kv.key_type;
        o.value.assign(kv.value.begin(), kv.value.end());
        kvs.push_back(std::move(o));
        return Status::OK();
    };

    auto s = ArrowToKVConverter::convert_tall_table(*batch, cfg, cb);
    ASSERT_TRUE(s.ok()) << s.message();
    EXPECT_EQ(kvs.size(), 5u);

    // Verify first row
    std::string rk(kvs[0].row.begin(), kvs[0].row.end());
    EXPECT_EQ(rk, "row_0");

    std::string fam(kvs[0].family.begin(), kvs[0].family.end());
    EXPECT_EQ(fam, "cf");
}

TEST(ArrowConverter, TallTableMissingColumn) {
    arrow::StringBuilder rk_b;
    ARROW_EXPECT_OK(rk_b.Append("r1"));
    std::shared_ptr<arrow::Array> rk_arr;
    ARROW_EXPECT_OK(rk_b.Finish(&rk_arr));

    auto schema = arrow::schema({arrow::field("row_key", arrow::utf8())});
    auto batch  = arrow::RecordBatch::Make(schema, 1, {rk_arr});

    TallTableConfig cfg;
    auto s = ArrowToKVConverter::convert_tall_table(
        *batch, cfg, [](const KeyValue&) { return Status::OK(); });
    EXPECT_FALSE(s.ok());
}

TEST(ArrowConverter, WideTableNullRowKeyRejected) {
    arrow::StringBuilder rk_builder;
    arrow::StringBuilder value_builder;
    ARROW_EXPECT_OK(rk_builder.AppendNull());
    ARROW_EXPECT_OK(value_builder.Append("v1"));
    std::shared_ptr<arrow::Array> rk_arr, value_arr;
    ARROW_EXPECT_OK(rk_builder.Finish(&rk_arr));
    ARROW_EXPECT_OK(value_builder.Finish(&value_arr));

    auto schema = arrow::schema({
        arrow::field("__row_key__", arrow::utf8()),
        arrow::field("value", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 1, {rk_arr, value_arr});

    WideTableConfig cfg;
    auto s = ArrowToKVConverter::convert_wide_table(
        *batch, cfg, [](const KeyValue&) { return Status::OK(); });
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), Status::Code::InvalidArg);
}

TEST(ArrowConverter, RawKVRejectsCorruptedKey) {
    arrow::BinaryBuilder key_builder;
    arrow::BinaryBuilder value_builder;
    const uint8_t short_key[] = {0x00, 0x01};
    const uint8_t value[] = {'v'};
    ARROW_EXPECT_OK(key_builder.Append(short_key, 2));
    ARROW_EXPECT_OK(value_builder.Append(value, 1));
    std::shared_ptr<arrow::Array> key_arr, value_arr;
    ARROW_EXPECT_OK(key_builder.Finish(&key_arr));
    ARROW_EXPECT_OK(value_builder.Finish(&value_arr));

    auto schema = arrow::schema({
        arrow::field("key", arrow::binary()),
        arrow::field("value", arrow::binary()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 1, {key_arr, value_arr});

    auto s = ArrowToKVConverter::convert_raw_kv(
        *batch, "key", "value", [](const KeyValue&) { return Status::OK(); });
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), Status::Code::Corruption);
}

// ─── Type serialization ───────────────────────────────────────────────────────

TEST(ArrowConverter, Int64BigEndian) {
    arrow::Int64Builder b;
    ARROW_EXPECT_OK(b.Append(0x0102030405060708LL));
    std::shared_ptr<arrow::Array> arr;
    ARROW_EXPECT_OK(b.Finish(&arr));

    auto bytes = ArrowToKVConverter::serialize_scalar(*arr, 0);
    ASSERT_EQ(bytes.size(), 8u);
    EXPECT_EQ(bytes[0], 0x01);
    EXPECT_EQ(bytes[1], 0x02);
    EXPECT_EQ(bytes[7], 0x08);
}

TEST(ArrowConverter, BooleanByte) {
    arrow::BooleanBuilder b;
    ARROW_EXPECT_OK(b.Append(true));
    ARROW_EXPECT_OK(b.Append(false));
    std::shared_ptr<arrow::Array> arr;
    ARROW_EXPECT_OK(b.Finish(&arr));

    auto t = ArrowToKVConverter::serialize_scalar(*arr, 0);
    auto f = ArrowToKVConverter::serialize_scalar(*arr, 1);
    ASSERT_EQ(t.size(), 1u);
    ASSERT_EQ(f.size(), 1u);
    EXPECT_EQ(t[0], 1u);
    EXPECT_EQ(f[0], 0u);
}

TEST(ArrowConverter, StringPassThrough) {
    arrow::StringBuilder b;
    ARROW_EXPECT_OK(b.Append("hello_world"));
    std::shared_ptr<arrow::Array> arr;
    ARROW_EXPECT_OK(b.Finish(&arr));

    auto bytes = ArrowToKVConverter::serialize_scalar(*arr, 0);
    std::string result(bytes.begin(), bytes.end());
    EXPECT_EQ(result, "hello_world");
}

TEST(ArrowConverter, Float32BigEndian) {
    arrow::FloatBuilder b;
    ARROW_EXPECT_OK(b.Append(1.0f));
    std::shared_ptr<arrow::Array> arr;
    ARROW_EXPECT_OK(b.Finish(&arr));

    auto bytes = ArrowToKVConverter::serialize_scalar(*arr, 0);
    ASSERT_EQ(bytes.size(), 4u);
    // 1.0f in IEEE 754 BE = 0x3F800000
    EXPECT_EQ(bytes[0], 0x3F);
    EXPECT_EQ(bytes[1], 0x80);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x00);
}

TEST(ArrowConverter, TimestampSecondNormalizedToMilliseconds) {
    auto type = arrow::timestamp(arrow::TimeUnit::SECOND);
    arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
    ARROW_EXPECT_OK(builder.Append(123));
    std::shared_ptr<arrow::Array> arr;
    ARROW_EXPECT_OK(builder.Finish(&arr));

    auto bytes = ArrowToKVConverter::serialize_scalar(*arr, 0);
    ASSERT_EQ(bytes.size(), 8u);
    EXPECT_EQ(read_be64(bytes.data()), 123000u);
}

TEST(ArrowConverter, TimestampNanoNormalizedToMilliseconds) {
    auto type = arrow::timestamp(arrow::TimeUnit::NANO);
    arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
    ARROW_EXPECT_OK(builder.Append(1234567890LL));
    std::shared_ptr<arrow::Array> arr;
    ARROW_EXPECT_OK(builder.Finish(&arr));

    auto bytes = ArrowToKVConverter::serialize_scalar(*arr, 0);
    ASSERT_EQ(bytes.size(), 8u);
    EXPECT_EQ(read_be64(bytes.data()), 1234u);
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
    EXPECT_EQ(result.error_code, ErrorCode::SORT_VIOLATION);
    EXPECT_FALSE(result.error_message.empty());
    EXPECT_FALSE(fs::exists(hfile_path));

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
    EXPECT_EQ(result.error_code, ErrorCode::SORT_VIOLATION);
    EXPECT_FALSE(result.error_message.empty());

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
    fs::remove_all(dir);
}
