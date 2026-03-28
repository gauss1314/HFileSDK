#include <benchmark/benchmark.h>
#include "arrow/arrow_to_kv_converter.h"
#include <hfile/types.h>

#include <arrow/api.h>
#include <arrow/builder.h>

#include <vector>
#include <string>

using namespace hfile;
using namespace hfile::arrow_convert;

static std::shared_ptr<arrow::RecordBatch> build_wide_batch(int n_rows, int n_cols) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    // Row key column
    {
        arrow::StringBuilder b;
        for (int i = 0; i < n_rows; ++i) {
            char buf[24]; std::snprintf(buf, sizeof(buf), "row%010d", i);
            ARROW_EXPECT_OK(b.Append(buf));
        }
        std::shared_ptr<arrow::Array> arr;
        ARROW_EXPECT_OK(b.Finish(&arr));
        arrays.push_back(arr);
        fields.push_back(arrow::field("__row_key__", arrow::utf8()));
    }

    // Payload int64 columns
    for (int c = 0; c < n_cols; ++c) {
        arrow::Int64Builder b;
        for (int i = 0; i < n_rows; ++i)
            ARROW_EXPECT_OK(b.Append(static_cast<int64_t>(i * c)));
        std::shared_ptr<arrow::Array> arr;
        ARROW_EXPECT_OK(b.Finish(&arr));
        arrays.push_back(arr);
        fields.push_back(arrow::field("col_" + std::to_string(c), arrow::int64()));
    }

    return arrow::RecordBatch::Make(arrow::schema(fields), n_rows, arrays);
}

static void BM_ArrowToKV_WideTable(benchmark::State& state) {
    int n_rows = static_cast<int>(state.range(0));
    int n_cols = static_cast<int>(state.range(1));
    auto batch = build_wide_batch(n_rows, n_cols);

    WideTableConfig cfg;
    cfg.column_family    = "cf";
    cfg.default_timestamp = 1000000LL;

    for (auto _ : state) {
        int64_t count = 0;
        auto s = ArrowToKVConverter::convert_wide_table(
            *batch, cfg, [&](const KeyValue&) -> Status { ++count; return Status::OK(); });
        benchmark::DoNotOptimize(count);
        if (!s.ok()) state.SkipWithError(s.message().c_str());
    }
    state.SetItemsProcessed(state.iterations() *
                             static_cast<int64_t>(n_rows) * n_cols);
}
BENCHMARK(BM_ArrowToKV_WideTable)
    ->Args({1000, 5})
    ->Args({1000, 20})
    ->Args({10000, 5})
    ->Args({10000, 20})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
