#include <gtest/gtest.h>

#include <hfile/bulk_load_writer.h>

#include "block/data_block_encoder.h"

#include <arrow/api.h>

#include <algorithm>
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

std::shared_ptr<arrow::RecordBatch> make_tall_batch(
        const std::vector<std::string>& rows,
        const std::vector<std::string>& families,
        const std::vector<std::string>& qualifiers,
        const std::vector<int64_t>& timestamps,
        const std::vector<std::string>& values) {
    arrow::StringBuilder rk;
    arrow::StringBuilder cf;
    arrow::StringBuilder q;
    arrow::Int64Builder ts;
    arrow::StringBuilder val;
    for (size_t i = 0; i < rows.size(); ++i) {
        auto s1 = rk.Append(rows[i]);
        auto s2 = cf.Append(families[i]);
        auto s3 = q.Append(qualifiers[i]);
        auto s4 = ts.Append(timestamps[i]);
        auto s5 = val.Append(values[i]);
        if (!s1.ok() || !s2.ok() || !s3.ok() || !s4.ok() || !s5.ok()) {
            ADD_FAILURE() << "append tall batch failed";
            return {};
        }
    }
    std::shared_ptr<arrow::Array> rk_arr, cf_arr, q_arr, ts_arr, val_arr;
    if (!rk.Finish(&rk_arr).ok() || !cf.Finish(&cf_arr).ok() ||
        !q.Finish(&q_arr).ok() || !ts.Finish(&ts_arr).ok() ||
        !val.Finish(&val_arr).ok()) {
        ADD_FAILURE() << "finish tall batch failed";
        return {};
    }
    auto schema = arrow::schema({
        arrow::field("row_key", arrow::utf8()),
        arrow::field("cf", arrow::utf8()),
        arrow::field("qualifier", arrow::utf8()),
        arrow::field("timestamp", arrow::int64()),
        arrow::field("value", arrow::utf8()),
    });
    return arrow::RecordBatch::Make(
        schema, static_cast<int64_t>(rows.size()),
        {rk_arr, cf_arr, q_arr, ts_arr, val_arr});
}

std::vector<uint8_t> encode_key(const std::string& row,
                                const std::string& family,
                                const std::string& qualifier,
                                int64_t timestamp) {
    std::vector<uint8_t> rk(row.begin(), row.end());
    std::vector<uint8_t> fam(family.begin(), family.end());
    std::vector<uint8_t> q(qualifier.begin(), qualifier.end());
    KeyValue kv;
    kv.row = rk;
    kv.family = fam;
    kv.qualifier = q;
    kv.timestamp = timestamp;
    kv.key_type = KeyType::Put;
    std::vector<uint8_t> out(kv.key_length());
    block::serialize_key(kv, out.data());
    return out;
}

std::shared_ptr<arrow::RecordBatch> make_raw_kv_batch(
        const std::vector<std::vector<uint8_t>>& keys,
        const std::vector<std::vector<uint8_t>>& values) {
    arrow::BinaryBuilder key_builder;
    arrow::BinaryBuilder value_builder;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto s1 = key_builder.Append(keys[i].data(), static_cast<int32_t>(keys[i].size()));
        auto s2 = value_builder.Append(values[i].data(), static_cast<int32_t>(values[i].size()));
        if (!s1.ok() || !s2.ok()) {
            ADD_FAILURE() << "append raw batch failed";
            return {};
        }
    }
    std::shared_ptr<arrow::Array> key_arr, value_arr;
    if (!key_builder.Finish(&key_arr).ok() || !value_builder.Finish(&value_arr).ok()) {
        ADD_FAILURE() << "finish raw batch failed";
        return {};
    }
    auto schema = arrow::schema({
        arrow::field("key", arrow::binary()),
        arrow::field("value", arrow::binary()),
    });
    return arrow::RecordBatch::Make(
        schema, static_cast<int64_t>(keys.size()), {key_arr, value_arr});
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

TEST(BulkLoadWriterBehavior, TallTableRoutesMultipleFamiliesWithParallelFinish) {
    auto dir = temp_dir("tall_parallel");
    auto [writer, status] = BulkLoadWriter::builder()
        .set_output_dir(dir.string())
        .set_column_families({"cf1", "cf2"})
        .set_partitioner(RegionPartitioner::none())
        .set_parallelism(2)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto batch = make_tall_batch(
        {"row1", "row2", "row3", "row4"},
        {"cf1", "cf2", "cf1", "cf2"},
        {"q1", "q2", "q3", "q4"},
        {100, 101, 102, 103},
        {"v1", "v2", "v3", "v4"});
    ASSERT_NE(batch, nullptr);
    ASSERT_TRUE(writer->write_batch(*batch, MappingMode::TallTable).ok());

    auto [result, finish_status] = writer->finish();
    ASSERT_TRUE(finish_status.ok()) << finish_status.message();
    EXPECT_EQ(result.total_rows, 4u);
    EXPECT_EQ(result.total_entries, 4u);
    EXPECT_EQ(result.files.size(), 2u);
    EXPECT_GT(result.total_bytes, 0u);
    EXPECT_NE(std::find(result.files.begin(), result.files.end(), "cf1/hfile_region_0000.hfile"),
              result.files.end());
    EXPECT_NE(std::find(result.files.begin(), result.files.end(), "cf2/hfile_region_0000.hfile"),
              result.files.end());
    fs::remove_all(dir);
}

TEST(BulkLoadWriterBehavior, RawKVWritesPreencodedKeys) {
    auto dir = temp_dir("raw_kv");
    auto [writer, status] = BulkLoadWriter::builder()
        .set_output_dir(dir.string())
        .set_column_families({"cf"})
        .set_partitioner(RegionPartitioner::none())
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    std::vector<std::vector<uint8_t>> keys{
        encode_key("row1", "cf", "q1", 100),
        encode_key("row2", "cf", "q2", 99),
    };
    std::vector<std::vector<uint8_t>> values{
        std::vector<uint8_t>{'v', '1'},
        std::vector<uint8_t>{'v', '2'},
    };
    auto batch = make_raw_kv_batch(keys, values);
    ASSERT_NE(batch, nullptr);
    ASSERT_TRUE(writer->write_batch(*batch, MappingMode::RawKV).ok());

    auto [result, finish_status] = writer->finish();
    ASSERT_TRUE(finish_status.ok()) << finish_status.message();
    EXPECT_EQ(result.total_rows, 2u);
    EXPECT_EQ(result.total_entries, 2u);
    EXPECT_EQ(result.files.size(), 1u);
    EXPECT_EQ(result.files.front(), "cf/hfile_region_0000.hfile");
    fs::remove_all(dir);
}

TEST(BulkLoadWriterBehavior, MultipleBatchesAccumulateStatistics) {
    auto dir = temp_dir("multi_batches");
    auto [writer, status] = BulkLoadWriter::builder()
        .set_output_dir(dir.string())
        .set_column_families({"cf"})
        .set_partitioner(RegionPartitioner::none())
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto first = make_wide_batch({std::string("row1"), std::string("row2")}, {"v1", "v2"});
    auto second = make_wide_batch({std::string("row3")}, {"v3"});
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    ASSERT_TRUE(writer->write_batch(*first, MappingMode::WideTable).ok());
    ASSERT_TRUE(writer->write_batch(*second, MappingMode::WideTable).ok());

    auto [result, finish_status] = writer->finish();
    ASSERT_TRUE(finish_status.ok()) << finish_status.message();
    EXPECT_EQ(result.total_rows, 3u);
    EXPECT_EQ(result.total_entries, 3u);
    EXPECT_EQ(result.skipped_rows, 0u);
    EXPECT_EQ(result.files.size(), 1u);
    fs::remove_all(dir);
}

TEST(BulkLoadWriterBehavior, StrictModeRejectsUnknownFamily) {
    auto dir = temp_dir("strict_unknown_cf");
    auto [writer, status] = BulkLoadWriter::builder()
        .set_output_dir(dir.string())
        .set_column_families({"cf"})
        .set_partitioner(RegionPartitioner::none())
        .set_error_policy(ErrorPolicy::Strict)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto batch = make_tall_batch(
        {"row1"},
        {"bad_cf"},
        {"q1"},
        {100},
        {"v1"});
    ASSERT_NE(batch, nullptr);

    auto ws = writer->write_batch(*batch, MappingMode::TallTable);
    EXPECT_FALSE(ws.ok());
    EXPECT_EQ(ws.code(), Status::Code::InvalidArg);
    fs::remove_all(dir);
}

TEST(BulkLoadWriterBehavior, SkipBatchSkipsOutOfOrderTallBatch) {
    auto dir = temp_dir("skip_batch_sort_violation");
    auto [writer, status] = BulkLoadWriter::builder()
        .set_output_dir(dir.string())
        .set_column_families({"cf"})
        .set_partitioner(RegionPartitioner::none())
        .set_error_policy(ErrorPolicy::SkipBatch)
        .build();
    ASSERT_TRUE(status.ok()) << status.message();

    auto batch = make_tall_batch(
        {"row2", "row1"},
        {"cf", "cf"},
        {"q", "q"},
        {100, 100},
        {"v2", "v1"});
    ASSERT_NE(batch, nullptr);
    ASSERT_TRUE(writer->write_batch(*batch, MappingMode::TallTable).ok());

    auto [result, finish_status] = writer->finish();
    ASSERT_TRUE(finish_status.ok()) << finish_status.message();
    EXPECT_EQ(result.total_entries, 0u);
    EXPECT_EQ(result.skipped_rows, 2u);
    EXPECT_TRUE(result.files.empty());
    fs::remove_all(dir);
}

TEST(BulkLoadWriterBehavior, BuilderValidatesRequiredArguments) {
    {
        auto [writer, status] = BulkLoadWriter::builder()
            .set_column_families({"cf"})
            .build();
        EXPECT_EQ(writer, nullptr);
        EXPECT_FALSE(status.ok());
    }
    {
        auto [writer, status] = BulkLoadWriter::builder()
            .set_output_dir(temp_dir("builder_validation").string())
            .build();
        EXPECT_EQ(writer, nullptr);
        EXPECT_FALSE(status.ok());
    }
}
