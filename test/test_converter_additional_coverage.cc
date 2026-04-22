#include <gtest/gtest.h>

#include "arrow/arrow_to_kv_converter.h"
#include "convert/converter.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using namespace hfile;
using namespace hfile::arrow_convert;

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

fs::path temp_dir(const std::string& name) {
    auto path = fs::temp_directory_path() / ("converter_extra_" + name);
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

std::shared_ptr<arrow::RecordBatch> make_wide_types_batch() {
    arrow::StringBuilder row_builder;
    arrow::Int8Builder int8_builder;
    arrow::Int16Builder int16_builder;
    arrow::UInt8Builder uint8_builder;
    arrow::UInt16Builder uint16_builder;
    arrow::UInt32Builder uint32_builder;
    arrow::UInt64Builder uint64_builder;
    arrow::DoubleBuilder double_builder;
    arrow::BinaryBuilder binary_builder;
    arrow::LargeStringBuilder large_string_builder;
    arrow::TimestampBuilder ts_builder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());

    auto require_ok = [](const arrow::Status& status) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        return status.ok();
    };

    if (!require_ok(row_builder.Append("row01"))) return nullptr;
    if (!require_ok(int8_builder.Append(-7))) return nullptr;
    if (!require_ok(int16_builder.Append(-1024))) return nullptr;
    if (!require_ok(uint8_builder.Append(7))) return nullptr;
    if (!require_ok(uint16_builder.Append(1024))) return nullptr;
    if (!require_ok(uint32_builder.Append(65536))) return nullptr;
    if (!require_ok(uint64_builder.Append(123456789ULL))) return nullptr;
    if (!require_ok(double_builder.Append(3.5))) return nullptr;
    if (!require_ok(binary_builder.Append(reinterpret_cast<const uint8_t*>("bin"), 3))) return nullptr;
    if (!require_ok(large_string_builder.Append("large-string"))) return nullptr;
    if (!require_ok(ts_builder.Append(1234567))) return nullptr;

    std::shared_ptr<arrow::Array> row_arr, int8_arr, int16_arr, uint8_arr, uint16_arr;
    std::shared_ptr<arrow::Array> uint32_arr, uint64_arr, double_arr, binary_arr, large_string_arr, ts_arr;
    if (!require_ok(row_builder.Finish(&row_arr))) return nullptr;
    if (!require_ok(int8_builder.Finish(&int8_arr))) return nullptr;
    if (!require_ok(int16_builder.Finish(&int16_arr))) return nullptr;
    if (!require_ok(uint8_builder.Finish(&uint8_arr))) return nullptr;
    if (!require_ok(uint16_builder.Finish(&uint16_arr))) return nullptr;
    if (!require_ok(uint32_builder.Finish(&uint32_arr))) return nullptr;
    if (!require_ok(uint64_builder.Finish(&uint64_arr))) return nullptr;
    if (!require_ok(double_builder.Finish(&double_arr))) return nullptr;
    if (!require_ok(binary_builder.Finish(&binary_arr))) return nullptr;
    if (!require_ok(large_string_builder.Finish(&large_string_arr))) return nullptr;
    if (!require_ok(ts_builder.Finish(&ts_arr))) return nullptr;

    auto schema = arrow::schema({
        arrow::field("__row_key__", arrow::utf8()),
        arrow::field("i8", arrow::int8()),
        arrow::field("i16", arrow::int16()),
        arrow::field("u8", arrow::uint8()),
        arrow::field("u16", arrow::uint16()),
        arrow::field("u32", arrow::uint32()),
        arrow::field("u64", arrow::uint64()),
        arrow::field("d", arrow::float64()),
        arrow::field("bin", arrow::binary()),
        arrow::field("ls", arrow::large_utf8()),
        arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
    });
    return arrow::RecordBatch::Make(
        schema, 1,
        {row_arr, int8_arr, int16_arr, uint8_arr, uint16_arr,
         uint32_arr, uint64_arr, double_arr, binary_arr, large_string_arr, ts_arr});
}

template <typename BuilderT, typename ValueT>
fs::path write_single_numeric_arrow(const fs::path& dir,
                                    const std::string& name,
                                    const std::shared_ptr<arrow::DataType>& type,
                                    ValueT value) {
    arrow::StringBuilder payload_builder;
    BuilderT key_builder;
    EXPECT_TRUE(payload_builder.Append("payload").ok());
    EXPECT_TRUE(key_builder.Append(value).ok());

    std::shared_ptr<arrow::Array> payload_arr;
    std::shared_ptr<arrow::Array> key_arr;
    EXPECT_TRUE(payload_builder.Finish(&payload_arr).ok());
    EXPECT_TRUE(key_builder.Finish(&key_arr).ok());

    auto schema = arrow::schema({
        arrow::field("id", type),
        arrow::field("payload", arrow::utf8()),
    });
    auto batch = arrow::RecordBatch::Make(schema, 1, {key_arr, payload_arr});
    auto path = dir / (name + ".arrow");
    write_ipc_stream(batch, path);
    return path;
}

}  // namespace

TEST(ConverterAdditionalCoverage, WideTableSupportsRemainingScalarTypesAndCallbackErrors) {
    auto batch = make_wide_types_batch();
    WideTableConfig cfg;
    cfg.column_family = "cf";
    cfg.default_timestamp = 99;

    int callback_count = 0;
    auto ok = ArrowToKVConverter::convert_wide_table(
        *batch, cfg, [&](const KeyValue& kv) {
            ++callback_count;
            EXPECT_FALSE(kv.row.empty());
            EXPECT_FALSE(kv.family.empty());
            return Status::OK();
        });
    ASSERT_TRUE(ok.ok()) << ok.message();
    EXPECT_EQ(callback_count, 10);

    auto stopped = ArrowToKVConverter::convert_wide_table(
        *batch, cfg, [&](const KeyValue&) {
            return Status::Internal("stop");
        });
    EXPECT_FALSE(stopped.ok());
    EXPECT_EQ(stopped.code(), Status::Code::Internal);
}

TEST(ConverterAdditionalCoverage, ConvertRejectsEmptyHFilePathAndInvalidOutputParent) {
    ConvertOptions empty_path;
    empty_path.arrow_path = "/tmp/does-not-matter.arrow";
    empty_path.hfile_path = "";
    empty_path.row_key_rule = "id,0,false,4";
    auto empty_result = convert(empty_path);
    EXPECT_EQ(empty_result.error_code, ErrorCode::INVALID_ARGUMENT);
    EXPECT_NE(empty_result.error_message.find("hfile_path is empty"), std::string::npos);

    auto dir = temp_dir("invalid_parent");
    auto arrow_path = dir / "input.arrow";
    auto batch = make_wide_types_batch();
    write_ipc_stream(batch, arrow_path);

    auto parent_file = dir / "not_a_dir";
    std::ofstream out(parent_file);
    ASSERT_TRUE(out.is_open());
    out << "x";
    out.close();

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = (parent_file / "child.hfile").string();
    opts.row_key_rule = "__row_key__,0,false,8";
    opts.column_family = "cf";
    opts.writer_opts.column_family = "cf";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::IO_ERROR);
    EXPECT_FALSE(result.error_message.empty());

    fs::remove_all(dir);
}

TEST(ConverterAdditionalCoverage, ConvertWithHotPathAndCompressionPipelineSucceeds) {
    ScopedEnvVar profiling("1");
    auto dir = temp_dir("hotpath");
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";
    auto batch = make_wide_types_batch();
    write_ipc_stream(batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.row_key_rule = "__row_key__,0,false,8";
    opts.column_family = "cf";
    opts.default_timestamp = 7;
    opts.writer_opts.column_family = "cf";
    opts.writer_opts.compression = Compression::GZip;
    opts.writer_opts.compression_level = 1;
    opts.writer_opts.data_block_encoding = Encoding::None;
    opts.writer_opts.bloom_type = BloomType::Row;
    opts.writer_opts.sort_mode = WriterOptions::SortMode::PreSortedTrusted;
    opts.writer_opts.compression_threads = 1;
    opts.writer_opts.compression_queue_depth = 0;
    opts.numeric_sort_fast_path = NumericSortFastPathMode::Off;

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_EQ(result.numeric_sort_fast_path_mode, NumericSortFastPathMode::Off);
    EXPECT_FALSE(result.numeric_sort_fast_path_used);
    EXPECT_EQ(result.kv_written_count, 11);
    EXPECT_TRUE(fs::exists(hfile_path));

    fs::remove_all(dir);
}

TEST(ConverterAdditionalCoverage, NumericFastPathCoversMultipleIntegralTypes) {
    auto dir = temp_dir("numeric_fast_path_types");

    struct Case {
        std::string name;
        std::shared_ptr<arrow::DataType> type;
        std::function<fs::path()> write_input;
    };

    std::vector<Case> cases;
    cases.push_back({"int8", arrow::int8(), [&] {
        return write_single_numeric_arrow<arrow::Int8Builder>(dir, "int8", arrow::int8(), static_cast<int8_t>(7));
    }});
    cases.push_back({"int16", arrow::int16(), [&] {
        return write_single_numeric_arrow<arrow::Int16Builder>(dir, "int16", arrow::int16(), static_cast<int16_t>(42));
    }});
    cases.push_back({"int32", arrow::int32(), [&] {
        return write_single_numeric_arrow<arrow::Int32Builder>(dir, "int32", arrow::int32(), static_cast<int32_t>(321));
    }});
    cases.push_back({"uint8", arrow::uint8(), [&] {
        return write_single_numeric_arrow<arrow::UInt8Builder>(dir, "uint8", arrow::uint8(), static_cast<uint8_t>(9));
    }});
    cases.push_back({"uint16", arrow::uint16(), [&] {
        return write_single_numeric_arrow<arrow::UInt16Builder>(dir, "uint16", arrow::uint16(), static_cast<uint16_t>(77));
    }});
    cases.push_back({"uint32", arrow::uint32(), [&] {
        return write_single_numeric_arrow<arrow::UInt32Builder>(dir, "uint32", arrow::uint32(), static_cast<uint32_t>(999));
    }});

    for (const auto& test_case : cases) {
        auto arrow_path = test_case.write_input();
        auto hfile_path = dir / (test_case.name + ".hfile");

        ConvertOptions opts;
        opts.arrow_path = arrow_path.string();
        opts.hfile_path = hfile_path.string();
        opts.row_key_rule = "id,0,false,4";
        opts.column_family = "cf";
        opts.default_timestamp = 1;
        opts.writer_opts.column_family = "cf";
        opts.writer_opts.compression = Compression::None;
        opts.writer_opts.data_block_encoding = Encoding::None;
        opts.writer_opts.bloom_type = BloomType::None;
        opts.numeric_sort_fast_path = NumericSortFastPathMode::On;

        auto result = convert(opts);
        EXPECT_EQ(result.error_code, ErrorCode::OK) << test_case.name << ": " << result.error_message;
        EXPECT_EQ(result.numeric_sort_fast_path_mode, NumericSortFastPathMode::On);
        EXPECT_TRUE(result.numeric_sort_fast_path_used) << test_case.name;
        EXPECT_TRUE(fs::exists(hfile_path));
    }

    fs::remove_all(dir);
}
