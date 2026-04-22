#include <gtest/gtest.h>

#include "convert/converter.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>

#include <cstdlib>
#include <filesystem>
#include <string>

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

fs::path temp_dir(const std::string& name) {
    auto path = fs::temp_directory_path() / ("converter_hotpath_" + name);
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

}  // namespace

TEST(ConverterHotPathGrouped, DuplicateRowsUseGroupedWriterPathWithProfiling) {
    ScopedEnvVar profiling("1");

    auto dir = temp_dir("grouped_rows");
    auto arrow_path = dir / "input.arrow";
    auto hfile_path = dir / "output.hfile";

    arrow::StringBuilder id_builder;
    arrow::Int64Builder age_builder;
    arrow::StringBuilder name_builder;
    ASSERT_TRUE(id_builder.Append("dup").ok());
    ASSERT_TRUE(age_builder.Append(10).ok());
    ASSERT_TRUE(name_builder.AppendNull().ok());
    ASSERT_TRUE(id_builder.Append("dup").ok());
    ASSERT_TRUE(age_builder.Append(20).ok());
    ASSERT_TRUE(name_builder.Append("alice").ok());

    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> age_array;
    std::shared_ptr<arrow::Array> name_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(age_builder.Finish(&age_array).ok());
    ASSERT_TRUE(name_builder.Finish(&name_array).ok());

    auto batch = arrow::RecordBatch::Make(
        arrow::schema({
            arrow::field("id", arrow::utf8()),
            arrow::field("age", arrow::int64()),
            arrow::field("name", arrow::utf8()),
        }),
        2,
        {id_array, age_array, name_array});
    write_ipc_stream(batch, arrow_path);

    ConvertOptions opts;
    opts.arrow_path = arrow_path.string();
    opts.hfile_path = hfile_path.string();
    opts.row_key_rule = "ID,0,false,0";
    opts.column_family = "cf";
    opts.default_timestamp = 0;
    opts.writer_opts.column_family = "cf";

    auto result = convert(opts);
    EXPECT_EQ(result.error_code, ErrorCode::OK);
    EXPECT_EQ(result.duplicate_key_count, 1);
    EXPECT_EQ(result.kv_written_count, 3);
    EXPECT_EQ(result.kv_skipped_count, 2);
    EXPECT_TRUE(fs::exists(hfile_path));

    fs::remove_all(dir);
}
