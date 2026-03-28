#include <benchmark/benchmark.h>
#include "bloom/compound_bloom_filter_writer.h"
#include <hfile/types.h>
#include <vector>
#include <string>
#include <cstdio>

using namespace hfile;
using namespace hfile::bloom;

static void BM_BloomAdd(benchmark::State& state) {
    int64_t n_keys = state.range(0);
    CompoundBloomFilterWriter bf(BloomType::Row, 0.01,
                                  static_cast<size_t>(n_keys));

    for (auto _ : state) {
        for (int64_t i = 0; i < n_keys; ++i) {
            char buf[20];
            std::snprintf(buf, sizeof(buf), "row%012lld", static_cast<long long>(i));
            std::span<const uint8_t> k{
                reinterpret_cast<const uint8_t*>(buf), strlen(buf)};
            bf.add(k);
        }
    }
    state.SetItemsProcessed(state.iterations() * n_keys);
}
BENCHMARK(BM_BloomAdd)
    ->Arg(1000)->Arg(10000)->Arg(100000)
    ->Unit(benchmark::kMillisecond);

static void BM_MurmurHash(benchmark::State& state) {
    size_t sz = static_cast<size_t>(state.range(0));
    std::vector<uint8_t> key(sz, 0xAB);

    for (auto _ : state) {
        benchmark::DoNotOptimize(murmur3_32(key.data(), sz, 0));
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(sz));
}
BENCHMARK(BM_MurmurHash)
    ->Arg(8)->Arg(16)->Arg(32)->Arg(64)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_MAIN();
