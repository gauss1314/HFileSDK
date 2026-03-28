#include <benchmark/benchmark.h>
#include "codec/compressor.h"
#include <hfile/types.h>
#include <vector>
#include <string>
#include <numeric>

using namespace hfile;
using namespace hfile::codec;

static std::vector<uint8_t> make_compressible(size_t sz) {
    std::vector<uint8_t> data(sz);
    // Repeating pattern — compressible
    for (size_t i = 0; i < sz; ++i)
        data[i] = static_cast<uint8_t>(i % 64);
    return data;
}

static std::vector<uint8_t> make_random(size_t sz) {
    std::vector<uint8_t> data(sz);
    std::iota(data.begin(), data.end(), 0);
    return data;
}

static void BM_Compress(benchmark::State& state) {
    Compression   codec_type = static_cast<Compression>(state.range(0));
    size_t        block_sz   = static_cast<size_t>(state.range(1));

    auto compressor = Compressor::create(codec_type);
    auto input      = make_compressible(block_sz);
    std::vector<uint8_t> output(compressor->max_compressed_size(block_sz));

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            compressor->compress(input, output.data(), output.size()));
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(block_sz));
}

BENCHMARK(BM_Compress)
    ->Args({static_cast<int64_t>(Compression::None),   32768})
    ->Args({static_cast<int64_t>(Compression::LZ4),    32768})
    ->Args({static_cast<int64_t>(Compression::LZ4),    65536})
    ->Args({static_cast<int64_t>(Compression::LZ4),   131072})
    ->Args({static_cast<int64_t>(Compression::Zstd),   65536})
    ->Args({static_cast<int64_t>(Compression::Snappy), 65536})
    ->Unit(benchmark::kMicrosecond);

static void BM_Decompress(benchmark::State& state) {
    Compression codec_type = static_cast<Compression>(state.range(0));
    size_t      block_sz   = 65536;

    auto compressor = Compressor::create(codec_type);
    auto input      = make_compressible(block_sz);
    std::vector<uint8_t> compressed(compressor->max_compressed_size(block_sz));
    size_t comp_sz = compressor->compress(input, compressed.data(), compressed.size());

    std::vector<uint8_t> output(block_sz);
    for (auto _ : state) {
        benchmark::DoNotOptimize(
            compressor->decompress({compressed.data(), comp_sz},
                                   output.data(), output.size()));
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(block_sz));
}

BENCHMARK(BM_Decompress)
    ->Arg(static_cast<int64_t>(Compression::None))
    ->Arg(static_cast<int64_t>(Compression::LZ4))
    ->Arg(static_cast<int64_t>(Compression::Zstd))
    ->Arg(static_cast<int64_t>(Compression::Snappy))
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
