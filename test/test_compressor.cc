#include <gtest/gtest.h>
#include "codec/compressor.h"
#include <hfile/types.h>
#include <vector>
#include <numeric>
#include <cstring>

using namespace hfile;
using namespace hfile::codec;

struct CompressTestParam {
    Compression type;
    const char* name;
};

class CompressorTest : public ::testing::TestWithParam<CompressTestParam> {};

TEST_P(CompressorTest, RoundTrip) {
    auto c = Compressor::create(GetParam().type);
    ASSERT_NE(c, nullptr);

    // Compressible data: repeating pattern
    std::vector<uint8_t> input(4096);
    for (size_t i = 0; i < input.size(); ++i)
        input[i] = static_cast<uint8_t>(i % 64);

    std::vector<uint8_t> compressed(c->max_compressed_size(input.size()));
    size_t comp_len = c->compress(input, compressed.data(), compressed.size());
    EXPECT_GT(comp_len, 0u);

    std::vector<uint8_t> decompressed(input.size());
    auto s = c->decompress(
        {compressed.data(), comp_len}, decompressed.data(), decompressed.size());
    EXPECT_TRUE(s.ok()) << s.message();
    EXPECT_EQ(decompressed, input);
}

TEST_P(CompressorTest, EmptyInput) {
    auto c = Compressor::create(GetParam().type);
    std::vector<uint8_t> output(c->max_compressed_size(0) + 4, 0);
    size_t n = c->compress({}, output.data(), output.size());
    // None codec: n=0 is fine; compressed codecs may produce empty output too
    (void)n;
}

TEST_P(CompressorTest, MaxCompressedSizeAdequate) {
    auto c = Compressor::create(GetParam().type);
    std::vector<uint8_t> input(65536);
    std::iota(input.begin(), input.end(), 0);  // incompressible

    size_t max_sz = c->max_compressed_size(input.size());
    EXPECT_GE(max_sz, input.size());  // must have enough space

    std::vector<uint8_t> out(max_sz);
    size_t n = c->compress(input, out.data(), out.size());
    EXPECT_GT(n, 0u);
    EXPECT_LE(n, max_sz);
}

INSTANTIATE_TEST_SUITE_P(
    AllCodecs, CompressorTest,
    ::testing::Values(
        CompressTestParam{Compression::None,   "None"},
        CompressTestParam{Compression::LZ4,    "LZ4"},
        CompressTestParam{Compression::Zstd,   "Zstd"},
        CompressTestParam{Compression::Snappy, "Snappy"}
    ),
    [](const ::testing::TestParamInfo<CompressTestParam>& info) {
        return info.param.name;
    });
