#include <benchmark/benchmark.h>
#include "block/none_encoder.h"
#include "block/prefix_encoder.h"
#include "block/fast_diff_encoder.h"
#include "block/data_block_encoder.h"
#include <hfile/types.h>
#include <vector>
#include <cstring>
#include <cstdio>

using namespace hfile;
using namespace hfile::block;

// ─── Fixture helpers ─────────────────────────────────────────────────────────

static std::vector<OwnedKeyValue> make_kv_dataset(int n,
                                                    size_t value_size = 100) {
    std::vector<OwnedKeyValue> kvs;
    kvs.reserve(n);
    std::string fam = "cf";
    std::string col = "col1";
    std::string val(value_size, 'x');

    for (int i = 0; i < n; ++i) {
        char row[32];
        std::snprintf(row, sizeof(row), "row%010d", i);

        OwnedKeyValue kv;
        kv.row.assign(reinterpret_cast<const uint8_t*>(row),
                      reinterpret_cast<const uint8_t*>(row) + strlen(row));
        kv.family.assign(fam.begin(), fam.end());
        kv.qualifier.assign(col.begin(), col.end());
        kv.timestamp = static_cast<int64_t>(n - i) * 1000;
        kv.key_type  = KeyType::Put;
        kv.value.assign(val.begin(), val.end());
        kvs.push_back(std::move(kv));
    }
    return kvs;
}

// ─── None encoder ─────────────────────────────────────────────────────────────

static void BM_KVEncode_None_100B(benchmark::State& state) {
    auto kvs = make_kv_dataset(10000, 100);
    for (auto _ : state) {
        NoneEncoder enc(64 * 1024);
        for (const auto& okv : kvs) {
            auto kv = okv.as_view();
            if (!enc.append(kv)) {
                benchmark::DoNotOptimize(enc.finish_block());
                enc.reset();
                enc.append(kv);
            }
        }
        benchmark::DoNotOptimize(enc.finish_block());
    }
    state.SetBytesProcessed(
        state.iterations() * static_cast<int64_t>(kvs.size()) *
        (kvs[0].row.size() + kvs[0].value.size() + 20));
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kvs.size()));
}
BENCHMARK(BM_KVEncode_None_100B)->Unit(benchmark::kMillisecond);

static void BM_KVEncode_None_1KB(benchmark::State& state) {
    auto kvs = make_kv_dataset(5000, 1024);
    for (auto _ : state) {
        NoneEncoder enc(64 * 1024);
        for (const auto& okv : kvs) {
            auto kv = okv.as_view();
            if (!enc.append(kv)) {
                enc.finish_block(); enc.reset(); enc.append(kv);
            }
        }
        enc.finish_block();
    }
    state.SetBytesProcessed(
        state.iterations() * static_cast<int64_t>(kvs.size()) * 1024);
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kvs.size()));
}
BENCHMARK(BM_KVEncode_None_1KB)->Unit(benchmark::kMillisecond);

// ─── FastDiff encoder ─────────────────────────────────────────────────────────

static void BM_KVEncode_FastDiff_100B(benchmark::State& state) {
    auto kvs = make_kv_dataset(10000, 100);
    for (auto _ : state) {
        FastDiffEncoder enc(64 * 1024);
        for (const auto& okv : kvs) {
            auto kv = okv.as_view();
            if (!enc.append(kv)) {
                enc.finish_block(); enc.reset(); enc.append(kv);
            }
        }
        enc.finish_block();
    }
    state.SetBytesProcessed(
        state.iterations() * static_cast<int64_t>(kvs.size()) * 120);
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kvs.size()));
}
BENCHMARK(BM_KVEncode_FastDiff_100B)->Unit(benchmark::kMillisecond);

static void BM_KVEncode_FastDiff_10KB(benchmark::State& state) {
    auto kvs = make_kv_dataset(1000, 10240);
    for (auto _ : state) {
        FastDiffEncoder enc(64 * 1024);
        for (const auto& okv : kvs) {
            auto kv = okv.as_view();
            if (!enc.append(kv)) {
                enc.finish_block(); enc.reset(); enc.append(kv);
            }
        }
        enc.finish_block();
    }
    state.SetBytesProcessed(
        state.iterations() * static_cast<int64_t>(kvs.size()) * 10240);
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kvs.size()));
}
BENCHMARK(BM_KVEncode_FastDiff_10KB)->Unit(benchmark::kMillisecond);

// ─── All encoders comparison ───────────────────────────────────────────────────

static void BM_KVEncode_ByType(benchmark::State& state) {
    Encoding enc_type = static_cast<Encoding>(state.range(0));
    size_t   val_size = static_cast<size_t>(state.range(1));
    auto kvs = make_kv_dataset(5000, val_size);

    for (auto _ : state) {
        auto enc = DataBlockEncoder::create(enc_type, 64 * 1024);
        for (const auto& okv : kvs) {
            auto kv = okv.as_view();
            if (!enc->append(kv)) {
                enc->finish_block(); enc->reset(); enc->append(kv);
            }
        }
        enc->finish_block();
    }
    state.SetBytesProcessed(
        state.iterations() * static_cast<int64_t>(kvs.size()) *
        static_cast<int64_t>(val_size + 50));
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kvs.size()));
}
BENCHMARK(BM_KVEncode_ByType)
    ->Args({static_cast<int64_t>(Encoding::None),     100})
    ->Args({static_cast<int64_t>(Encoding::Prefix),   100})
    ->Args({static_cast<int64_t>(Encoding::Diff),     100})
    ->Args({static_cast<int64_t>(Encoding::FastDiff), 100})
    ->Args({static_cast<int64_t>(Encoding::FastDiff), 1024})
    ->Args({static_cast<int64_t>(Encoding::FastDiff), 10240})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
