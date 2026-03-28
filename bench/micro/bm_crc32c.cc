#include <benchmark/benchmark.h>
#include "checksum/crc32c.h"
#include <vector>
#include <numeric>

using namespace hfile::checksum;

static void BM_CRC32C(benchmark::State& state) {
    size_t sz = static_cast<size_t>(state.range(0));
    std::vector<uint8_t> data(sz);
    std::iota(data.begin(), data.end(), 0);

    for (auto _ : state) {
        benchmark::DoNotOptimize(crc32c(data.data(), sz));
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(sz));
}
BENCHMARK(BM_CRC32C)->RangeMultiplier(4)->Range(1024, 4 * 1024 * 1024)
    ->Unit(benchmark::kMicrosecond);

static void BM_CRC32C_Chunked(benchmark::State& state) {
    // Simulate HFile block checksum computation
    std::vector<uint8_t> data(64 * 1024, 0xAB);
    std::vector<uint8_t> out((data.size() / 512 + 1) * 4);

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            compute_hfile_checksums(data.data(), data.size(), 512, out.data()));
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(data.size()));
}
BENCHMARK(BM_CRC32C_Chunked)->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
