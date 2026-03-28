#include <benchmark/benchmark.h>
#include <hfile/writer.h>
#include <hfile/types.h>

#include <filesystem>
#include <vector>
#include <string>
#include <cstdio>

namespace fs = std::filesystem;
using namespace hfile;

// ─── Dataset generators ───────────────────────────────────────────────────────

struct Dataset {
    std::string name;
    std::vector<OwnedKeyValue> kvs;

    static Dataset small_kv(int n = 100000) {
        // DS-1: 16B random-looking row key, 100B value
        Dataset ds;
        ds.name = "SmallKV_100B";
        ds.kvs.reserve(n);
        for (int i = 0; i < n; ++i) {
            char row[20];
            std::snprintf(row, sizeof(row), "%016d", i);
            OwnedKeyValue kv;
            kv.row.assign(reinterpret_cast<const uint8_t*>(row),
                          reinterpret_cast<const uint8_t*>(row) + 16);
            kv.family    = {'c','f'};
            kv.qualifier = {'c','o','l'};
            kv.timestamp = static_cast<int64_t>(n - i);
            kv.key_type  = KeyType::Put;
            kv.value.assign(100, static_cast<uint8_t>(i & 0xFF));
            ds.kvs.push_back(std::move(kv));
        }
        return ds;
    }

    static Dataset wide_table(int n = 20000) {
        // DS-3: 8B row key, 20 columns of 50B each
        Dataset ds;
        ds.name = "WideTable";
        ds.kvs.reserve(n * 20);
        for (int i = 0; i < n; ++i) {
            uint8_t row[8]; write_be64(row, static_cast<uint64_t>(i));
            for (int c = 0; c < 20; ++c) {
                OwnedKeyValue kv;
                kv.row.assign(row, row + 8);
                kv.family    = {'c','f'};
                char col[8]; std::snprintf(col, sizeof(col), "col%02d", c);
                kv.qualifier.assign(reinterpret_cast<const uint8_t*>(col),
                                    reinterpret_cast<const uint8_t*>(col) + strlen(col));
                kv.timestamp = static_cast<int64_t>(n - i);
                kv.key_type  = KeyType::Put;
                kv.value.assign(50, static_cast<uint8_t>((i + c) & 0xFF));
                ds.kvs.push_back(std::move(kv));
            }
        }
        return ds;
    }
};

// ─── Benchmark fixture ───────────────────────────────────────────────────────

static void BM_E2E_Write(benchmark::State& state) {
    int64_t      n_kvs    = state.range(0);
    Compression  comp     = static_cast<Compression>(state.range(1));
    Encoding     enc_type = static_cast<Encoding>(state.range(2));

    auto ds = Dataset::small_kv(static_cast<int>(n_kvs));

    auto tmp = fs::temp_directory_path() / "bm_hfile_e2e.hfile";

    for (auto _ : state) {
        state.PauseTiming();
        fs::remove(tmp);
        state.ResumeTiming();

        auto [w, s] = HFileWriter::builder()
            .set_path(tmp.string())
            .set_column_family("cf")
            .set_compression(comp)
            .set_data_block_encoding(enc_type)
            .set_bloom_type(BloomType::Row)
            .set_sort_mode(WriterOptions::SortMode::PreSortedTrusted)
            .build();
        if (!s.ok()) { state.SkipWithError(s.message().c_str()); return; }

        for (const auto& okv : ds.kvs) {
            auto kv = okv.as_view();
            auto ws = w->append(kv);
            if (!ws.ok()) { state.SkipWithError(ws.message().c_str()); return; }
        }
        auto fs2 = w->finish();
        if (!fs2.ok()) { state.SkipWithError(fs2.message().c_str()); return; }

        state.PauseTiming();
        auto file_sz = fs::file_size(tmp);
        state.counters["file_MB"] = benchmark::Counter(
            static_cast<double>(file_sz) / (1024.0 * 1024.0));
        state.counters["compression_ratio"] = benchmark::Counter(
            static_cast<double>(n_kvs) * 120.0 / static_cast<double>(file_sz));
        state.ResumeTiming();
    }

    state.SetBytesProcessed(
        state.iterations() * static_cast<int64_t>(n_kvs) * 120);
    state.SetItemsProcessed(state.iterations() * n_kvs);
    fs::remove(tmp);
}

BENCHMARK(BM_E2E_Write)
    // (n_kvs, compression, encoding)
    ->Args({100000, static_cast<int64_t>(Compression::None),   static_cast<int64_t>(Encoding::None)})
    ->Args({100000, static_cast<int64_t>(Compression::LZ4),    static_cast<int64_t>(Encoding::FastDiff)})
    ->Args({100000, static_cast<int64_t>(Compression::Zstd),   static_cast<int64_t>(Encoding::FastDiff)})
    ->Args({100000, static_cast<int64_t>(Compression::Snappy), static_cast<int64_t>(Encoding::FastDiff)})
    ->Iterations(3)
    ->Unit(benchmark::kMillisecond);

static void BM_E2E_WideTable(benchmark::State& state) {
    auto ds = Dataset::wide_table(10000);  // 10K rows × 20 cols = 200K KVs
    auto tmp = fs::temp_directory_path() / "bm_hfile_wide.hfile";

    for (auto _ : state) {
        state.PauseTiming();
        fs::remove(tmp);
        state.ResumeTiming();

        auto [w, s] = HFileWriter::builder()
            .set_path(tmp.string())
            .set_column_family("cf")
            .set_compression(Compression::LZ4)
            .set_data_block_encoding(Encoding::FastDiff)
            .set_bloom_type(BloomType::RowCol)
            .set_sort_mode(WriterOptions::SortMode::PreSortedTrusted)
            .build();
        if (!s.ok()) { state.SkipWithError(s.message().c_str()); return; }

        for (const auto& okv : ds.kvs) {
            auto kv = okv.as_view();
            w->append(kv);
        }
        w->finish();
    }

    state.SetBytesProcessed(
        state.iterations() * static_cast<int64_t>(ds.kvs.size()) * 80);
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(ds.kvs.size()));
    fs::remove(tmp);
}
BENCHMARK(BM_E2E_WideTable)->Iterations(3)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
