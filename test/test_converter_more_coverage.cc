#include <gtest/gtest.h>

#include "convert/converter.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>

#include <filesystem>
#include <limits>
#include <string>
#include <vector>

using namespace hfile;
namespace fs = std::filesystem;

namespace {

fs::path temp_dir(const std::string& name) {
    auto path = fs::temp_directory_path() / ("converter_more_" + name);
    std::error_code ec;
    fs::remove_all(path, ec);
    fs::create_directories(path, ec);
    return path;
}

void write_ipc_stream(const std::shared_ptr<arrow::RecordBatch>& batch,
                      const fs::path& path) {
    auto sink_result = arrow::io::FileOutputStream::Open(path.string());
    ASSERT_TRUE(sink_result.ok()) << sink_result.status().ToString();
    auto sink = *sink_result;

    auto writer_result = arrow::ipc::MakeStreamWriter(sink.get(), batch->schema());
    ASSERT_TRUE(writer_result.ok()) << writer_result.status().ToString();
    auto writer = *writer_result;
    ASSERT_TRUE(writer->WriteRecordBatch(*batch).ok());
    ASSERT_TRUE(writer->Close().ok());
    ASSERT_TRUE(sink->Close().ok());
}

std::shared_ptr<arrow::RecordBatch> make_string_payload_batch(
        const std::shared_ptr<arrow::Array>& id_array,
        const std::shared_ptr<arrow::DataType>& id_type,
        const std::vector<std::string>& payloads) {
    arrow::StringBuilder payload_builder;
    for (const auto& payload : payloads) {
        EXPECT_TRUE(payload_builder.Append(payload).ok());
    }

    std::shared_ptr<arrow::Array> payload_array;
    EXPECT_TRUE(payload_builder.Finish(&payload_array).ok());

    auto schema = arrow::schema({
        arrow::field("id", id_type),
        arrow::field("payload", arrow::utf8()),
    });
    return arrow::RecordBatch::Make(
        schema, id_array->length(), {id_array, payload_array});
}

std::shared_ptr<arrow::RecordBatch> make_wide_scalar_batch() {
    auto require_ok = [](const arrow::Status& status) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        return status.ok();
    };

    arrow::StringBuilder s_builder;
    arrow::LargeStringBuilder ls_builder;
    arrow::Int8Builder i8_builder;
    arrow::Int16Builder i16_builder;
    arrow::Int32Builder i32_builder;
    arrow::Int64Builder i64_builder;
    arrow::UInt8Builder u8_builder;
    arrow::UInt16Builder u16_builder;
    arrow::UInt32Builder u32_builder;
    arrow::UInt64Builder u64_builder;
    arrow::FloatBuilder f32_builder;
    arrow::DoubleBuilder f64_builder;
    arrow::BooleanBuilder bool_builder;
    arrow::TimestampBuilder ts_s_builder(
        arrow::timestamp(arrow::TimeUnit::SECOND), arrow::default_memory_pool());
    arrow::TimestampBuilder ts_ms_builder(
        arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
    arrow::TimestampBuilder ts_us_builder(
        arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    arrow::TimestampBuilder ts_ns_builder(
        arrow::timestamp(arrow::TimeUnit::NANO), arrow::default_memory_pool());
    arrow::BinaryBuilder bin_builder;
    arrow::LargeBinaryBuilder lbin_builder;
    arrow::Date32Builder date_builder;

    if (!require_ok(s_builder.Append("s"))) return nullptr;
    if (!require_ok(ls_builder.Append("large"))) return nullptr;
    if (!require_ok(i8_builder.Append(-8))) return nullptr;
    if (!require_ok(i16_builder.Append(-16))) return nullptr;
    if (!require_ok(i32_builder.Append(-32))) return nullptr;
    if (!require_ok(i64_builder.Append(64))) return nullptr;
    if (!require_ok(u8_builder.Append(8))) return nullptr;
    if (!require_ok(u16_builder.Append(16))) return nullptr;
    if (!require_ok(u32_builder.Append(32))) return nullptr;
    if (!require_ok(u64_builder.Append(64))) return nullptr;
    if (!require_ok(f32_builder.Append(1.25f))) return nullptr;
    if (!require_ok(f64_builder.Append(2.5))) return nullptr;
    if (!require_ok(bool_builder.Append(true))) return nullptr;
    if (!require_ok(ts_s_builder.Append(3))) return nullptr;
    if (!require_ok(ts_ms_builder.Append(4000))) return nullptr;
    if (!require_ok(ts_us_builder.Append(5000))) return nullptr;
    if (!require_ok(ts_ns_builder.Append(6000000))) return nullptr;
    if (!require_ok(bin_builder.Append(reinterpret_cast<const uint8_t*>("ab"), 2))) return nullptr;
    if (!require_ok(lbin_builder.Append(reinterpret_cast<const uint8_t*>("cde"), 3))) return nullptr;
    if (!require_ok(date_builder.Append(1))) return nullptr;

    std::shared_ptr<arrow::Array> s, ls, i8, i16, i32, i64, u8, u16, u32, u64;
    std::shared_ptr<arrow::Array> f32, f64, b, ts_s, ts_ms, ts_us, ts_ns, bin, lbin, date32;
    if (!require_ok(s_builder.Finish(&s))) return nullptr;
    if (!require_ok(ls_builder.Finish(&ls))) return nullptr;
    if (!require_ok(i8_builder.Finish(&i8))) return nullptr;
    if (!require_ok(i16_builder.Finish(&i16))) return nullptr;
    if (!require_ok(i32_builder.Finish(&i32))) return nullptr;
    if (!require_ok(i64_builder.Finish(&i64))) return nullptr;
    if (!require_ok(u8_builder.Finish(&u8))) return nullptr;
    if (!require_ok(u16_builder.Finish(&u16))) return nullptr;
    if (!require_ok(u32_builder.Finish(&u32))) return nullptr;
    if (!require_ok(u64_builder.Finish(&u64))) return nullptr;
    if (!require_ok(f32_builder.Finish(&f32))) return nullptr;
    if (!require_ok(f64_builder.Finish(&f64))) return nullptr;
    if (!require_ok(bool_builder.Finish(&b))) return nullptr;
    if (!require_ok(ts_s_builder.Finish(&ts_s))) return nullptr;
    if (!require_ok(ts_ms_builder.Finish(&ts_ms))) return nullptr;
    if (!require_ok(ts_us_builder.Finish(&ts_us))) return nullptr;
    if (!require_ok(ts_ns_builder.Finish(&ts_ns))) return nullptr;
    if (!require_ok(bin_builder.Finish(&bin))) return nullptr;
    if (!require_ok(lbin_builder.Finish(&lbin))) return nullptr;
    if (!require_ok(date_builder.Finish(&date32))) return nullptr;

    auto schema = arrow::schema({
        arrow::field("s", arrow::utf8()),
        arrow::field("ls", arrow::large_utf8()),
        arrow::field("i8", arrow::int8()),
        arrow::field("i16", arrow::int16()),
        arrow::field("i32", arrow::int32()),
        arrow::field("i64", arrow::int64()),
        arrow::field("u8", arrow::uint8()),
        arrow::field("u16", arrow::uint16()),
        arrow::field("u32", arrow::uint32()),
        arrow::field("u64", arrow::uint64()),
        arrow::field("f32", arrow::float32()),
        arrow::field("f64", arrow::float64()),
        arrow::field("b", arrow::boolean()),
        arrow::field("ts_s", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_ms", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_us", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_ns", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("bin", arrow::binary()),
        arrow::field("lbin", arrow::large_binary()),
        arrow::field("date32", arrow::date32()),
    });
    return arrow::RecordBatch::Make(
        schema,
        1,
        {s, ls, i8, i16, i32, i64, u8, u16, u32, u64,
         f32, f64, b, ts_s, ts_ms, ts_us, ts_ns, bin, lbin, date32});
}

ConvertOptions make_base_opts(const fs::path& arrow_path,
                              const fs::path& hfile_path,
                              const std::string& row_key_rule) {
    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.row_key_rule = row_key_rule;
    opts.column_family = "cf";
    opts.writer_opts.column_family = "cf";
    opts.writer_opts.compression = Compression::None;
    opts.writer_opts.data_block_encoding = Encoding::None;
    return opts;
}

}  // namespace

TEST(ConverterMoreCoverage, ConvertCoversWideScalarTypesAndFallbackValues) {
    auto dir = temp_dir("wide_types");
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";

    auto batch = make_wide_scalar_batch();
    ASSERT_NE(batch, nullptr);
    write_ipc_stream(batch, arrow_path);

    auto opts = make_base_opts(
        arrow_path,
        hfile_path,
        "C0,0,false,0#C1,1,false,0#C2,2,false,0#C3,3,false,0#C4,4,false,0"
        "#C5,5,false,0#C6,6,false,0#C7,7,false,0#C8,8,false,0#C9,9,false,0"
        "#C10,10,false,0#C11,11,false,0#C12,12,false,0#C13,13,false,0"
        "#C14,14,false,0#C15,15,false,0#C16,16,false,0");
    opts.default_timestamp = 0;

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_EQ(result.kv_written_count, 20);
    EXPECT_GT(result.hfile_size_bytes, 0);
    EXPECT_TRUE(fs::exists(hfile_path));

    fs::remove_all(dir);
}

TEST(ConverterMoreCoverage, ConvertDirectLargeStringFastPathSkipsNullAndEmptyRows) {
    auto dir = temp_dir("large_string_direct");
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";

    arrow::LargeStringBuilder id_builder;
    arrow::StringBuilder payload_builder;
    ASSERT_TRUE(id_builder.AppendNull().ok());
    ASSERT_TRUE(id_builder.Append("").ok());
    ASSERT_TRUE(id_builder.Append("row-ok").ok());
    ASSERT_TRUE(payload_builder.Append("p0").ok());
    ASSERT_TRUE(payload_builder.Append("p1").ok());
    ASSERT_TRUE(payload_builder.Append("p2").ok());

    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> payload_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(payload_builder.Finish(&payload_array).ok());

    auto batch = arrow::RecordBatch::Make(
        arrow::schema({
            arrow::field("id", arrow::large_utf8()),
            arrow::field("payload", arrow::utf8()),
        }),
        3,
        {id_array, payload_array});
    write_ipc_stream(batch, arrow_path);

    auto opts = make_base_opts(arrow_path, hfile_path, "ID,0,false,0");
    opts.default_timestamp = 7;

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_EQ(result.arrow_rows_read, 3);
    EXPECT_EQ(result.kv_written_count, 2);
    EXPECT_EQ(result.kv_skipped_count, 2);

    fs::remove_all(dir);
}

TEST(ConverterMoreCoverage, NumericFastPathOnRejectsIneligibleRule) {
    auto dir = temp_dir("numeric_on_reject");
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";

    arrow::StringBuilder id_builder;
    ASSERT_TRUE(id_builder.Append("abc").ok());
    std::shared_ptr<arrow::Array> id_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());

    auto batch = make_string_payload_batch(id_array, arrow::utf8(), {"payload"});
    write_ipc_stream(batch, arrow_path);

    auto opts = make_base_opts(arrow_path, hfile_path, "ID,0,false,4");
    opts.numeric_sort_fast_path = NumericSortFastPathMode::On;

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::INVALID_ARGUMENT);
    EXPECT_NE(result.error_message.find("numeric_sort_fast_path=on"), std::string::npos);

    fs::remove_all(dir);
}

TEST(ConverterMoreCoverage, NumericFastPathCoversBoolUint64AndTimestamp) {
    auto dir = temp_dir("numeric_extra_types");

    {
        arrow::BooleanBuilder builder;
        ASSERT_TRUE(builder.Append(true).ok());
        std::shared_ptr<arrow::Array> id_array;
        ASSERT_TRUE(builder.Finish(&id_array).ok());
        auto batch = make_string_payload_batch(id_array, arrow::boolean(), {"payload"});
        auto arrow_path = dir / "bool.arrow";
        auto off_hfile = dir / "bool_off.hfile";
        auto auto_hfile = dir / "bool_auto.hfile";
        write_ipc_stream(batch, arrow_path);

        auto off_opts = make_base_opts(arrow_path, off_hfile, "ID,0,false,1");
        off_opts.numeric_sort_fast_path = NumericSortFastPathMode::Off;
        EXPECT_EQ(convert(off_opts).error_code, ErrorCode::OK);

        auto auto_opts = make_base_opts(arrow_path, auto_hfile, "ID,0,false,1");
        auto auto_result = convert(auto_opts);
        EXPECT_EQ(auto_result.error_code, ErrorCode::OK);
        EXPECT_TRUE(auto_result.numeric_sort_fast_path_used);
    }

    {
        arrow::UInt64Builder builder;
        ASSERT_TRUE(builder.Append(1234567890123ULL).ok());
        std::shared_ptr<arrow::Array> id_array;
        ASSERT_TRUE(builder.Finish(&id_array).ok());
        auto batch = make_string_payload_batch(id_array, arrow::uint64(), {"payload"});
        auto arrow_path = dir / "u64.arrow";
        auto off_hfile = dir / "u64_off.hfile";
        auto auto_hfile = dir / "u64_auto.hfile";
        write_ipc_stream(batch, arrow_path);

        auto off_opts = make_base_opts(arrow_path, off_hfile, "ID,0,false,20");
        off_opts.numeric_sort_fast_path = NumericSortFastPathMode::Off;
        EXPECT_EQ(convert(off_opts).error_code, ErrorCode::OK);

        auto auto_opts = make_base_opts(arrow_path, auto_hfile, "ID,0,false,20");
        auto auto_result = convert(auto_opts);
        EXPECT_EQ(auto_result.error_code, ErrorCode::OK);
        EXPECT_TRUE(auto_result.numeric_sort_fast_path_used);
    }

    {
        arrow::TimestampBuilder builder(
            arrow::timestamp(arrow::TimeUnit::SECOND), arrow::default_memory_pool());
        ASSERT_TRUE(builder.Append(1234).ok());
        std::shared_ptr<arrow::Array> id_array;
        ASSERT_TRUE(builder.Finish(&id_array).ok());
        auto batch = make_string_payload_batch(
            id_array, arrow::timestamp(arrow::TimeUnit::SECOND), {"payload"});
        auto arrow_path = dir / "ts.arrow";
        auto off_hfile = dir / "ts_off.hfile";
        auto auto_hfile = dir / "ts_auto.hfile";
        write_ipc_stream(batch, arrow_path);

        auto off_opts = make_base_opts(arrow_path, off_hfile, "ID,0,false,13");
        off_opts.numeric_sort_fast_path = NumericSortFastPathMode::Off;
        EXPECT_EQ(convert(off_opts).error_code, ErrorCode::OK);

        auto auto_opts = make_base_opts(arrow_path, auto_hfile, "ID,0,false,13");
        auto auto_result = convert(auto_opts);
        EXPECT_EQ(auto_result.error_code, ErrorCode::OK);
        EXPECT_TRUE(auto_result.numeric_sort_fast_path_used);
    }

    fs::remove_all(dir);
}

TEST(ConverterMoreCoverage, EncodedSegmentFailureMapsToInvalidRowKeyRule) {
    auto dir = temp_dir("encoded_failure");
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";

    arrow::StringBuilder id_builder;
    ASSERT_TRUE(id_builder.Append("not-a-number").ok());
    std::shared_ptr<arrow::Array> id_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());

    auto batch = make_string_payload_batch(id_array, arrow::utf8(), {"payload"});
    write_ipc_stream(batch, arrow_path);

    auto opts = make_base_opts(arrow_path, hfile_path, "short(hash),0,false,0");
    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::INVALID_ROW_KEY_RULE);
    EXPECT_NE(result.error_message.find("requires numeric input"), std::string::npos);

    fs::remove_all(dir);
}

TEST(ConverterMoreCoverage, FinishDiskExhaustedMapsDedicatedErrorCode) {
    auto dir = temp_dir("disk_exhausted");
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";

    arrow::StringBuilder id_builder;
    ASSERT_TRUE(id_builder.Append("row-1").ok());
    std::shared_ptr<arrow::Array> id_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());

    auto batch = make_string_payload_batch(id_array, arrow::utf8(), {"payload"});
    write_ipc_stream(batch, arrow_path);

    auto opts = make_base_opts(arrow_path, hfile_path, "ID,0,false,0");
    opts.writer_opts.min_free_disk_bytes = std::numeric_limits<size_t>::max();
    opts.writer_opts.disk_check_interval_bytes = 1;

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::DISK_EXHAUSTED);
    EXPECT_NE(result.error_message.find("DISK_SPACE_EXHAUSTED"), std::string::npos);

    fs::remove_all(dir);
}
