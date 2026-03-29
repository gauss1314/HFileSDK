#include <gtest/gtest.h>

#include <hfile/bulk_load_writer.h>

#include <arrow/api.h>

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

using namespace hfile;
namespace fs = std::filesystem;

namespace {

class FirstBytePartitioner : public RegionPartitioner {
public:
    int region_for(std::span<const uint8_t> row_key) const noexcept override {
        if (row_key.empty()) return 0;
        return (row_key[0] % 2);
    }
    int num_regions() const noexcept override { return 2; }
    const std::vector<std::vector<uint8_t>>& split_points() const noexcept override {
        return split_points_;
    }
private:
    std::vector<std::vector<uint8_t>> split_points_{std::vector<std::vector<uint8_t>>{
        std::vector<uint8_t>{'b'}
    }};
};

fs::path temp_dir(const std::string& name) {
    auto dir = fs::temp_directory_path() / ("bulk_behavior_" + name);
    std::error_code ec;
    fs::remove_all(dir, ec);
    fs::create_directories(dir, ec);
    return dir;
}

std::shared_ptr<arrow::RecordBatch> make_wide_batch(
        const std::vector<std::optional<std::string>>& rows,
        const std::vector<std::string>& values) {
    arrow::StringBuilder rk;
    arrow::StringBuilder val;
    for (size_t i = 0; i < rows.size(); ++i) {
        if (rows[i].has_value()) {
            auto s = rk.Append(*rows[i]);
            if (!s.ok()) {
                ADD_FAILURE() << s.ToString();
                return {};
            }
        } else {
            auto s = rk.AppendNull();
            if (!s.ok()) {
                ADD_FAILURE() << s.ToString();
                return {};
            }
        }
        auto s = val.Append(values[i]);
        if (!s.ok()) {
            ADD_FAILURE() << s.ToString();
            return {};
        }
    }
    std::shared_ptr<arrow::Array> rk_arr, val_arr;
    auto s1 = rk.Finish(&rk_arr);
    if (!s1.ok()) {
        ADD_FAILURE() << s1.ToString();
        return {};
    }
    auto s2 = val.Finish(&val_arr);
    if (!s2.ok()) {
        ADD_FAILURE() << s2.ToString();
        return {};
    }
    auto schema = arrow::schema({
        arrow::field("__row_key__", arrow::utf8()),
        arrow::field("v", arrow::utf8()),
    });
    return arrow::RecordBatch::Make(schema, static_cast<int64_t>(rows.size()), {rk_arr, val_arr});
}

} // namespace

TEST(BulkLoadWriterBehavior, SkipBatchSkipsWholeBatch) {
    auto dir = temp_dir("skip_batch");
    auto [writer, status] = BulkLoadWriter::builder()
        .set_output_dir(dir.string())
        .set_column_families({"cf"})
        .set_partitioner(RegionPartitioner::none())
        .set_error_policy(ErrorPolicy::SkipBatch)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto batch = make_wide_batch({std::string("row1"), std::nullopt}, {"v1", "v2"});
    ASSERT_TRUE(writer->write_batch(*batch, MappingMode::WideTable).ok());

    auto [result, finish_status] = writer->finish();
    ASSERT_TRUE(finish_status.ok()) << finish_status.message();
    EXPECT_EQ(result.total_entries, 0u);
    EXPECT_EQ(result.skipped_rows, 2u);
    EXPECT_TRUE(result.files.empty());
    fs::remove_all(dir);
}

TEST(BulkLoadWriterBehavior, MaxOpenFilesRollsOutputFiles) {
    auto dir = temp_dir("max_open_files");
    auto [writer, status] = BulkLoadWriter::builder()
        .set_output_dir(dir.string())
        .set_column_families({"cf"})
        .set_partitioner(std::make_unique<FirstBytePartitioner>())
        .set_max_open_files(1)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto batch = make_wide_batch(
        {std::string("a0"), std::string("b0"), std::string("a1"), std::string("b1")},
        {"v0", "v1", "v2", "v3"});
    ASSERT_TRUE(writer->write_batch(*batch, MappingMode::WideTable).ok());

    auto [result, finish_status] = writer->finish();
    ASSERT_TRUE(finish_status.ok()) << finish_status.message();
    EXPECT_EQ(result.total_entries, 4u);
    EXPECT_GT(result.files.size(), 2u);
    EXPECT_GT(result.total_bytes, 0u);
    fs::remove_all(dir);
}
