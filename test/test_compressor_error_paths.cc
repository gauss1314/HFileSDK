#include <gtest/gtest.h>

#include "codec/compressor.h"

#include <cstring>
#include <string>
#include <vector>

using namespace hfile;
using namespace hfile::codec;

namespace {

std::vector<uint8_t> to_bytes(std::string_view s) {
    return std::vector<uint8_t>(s.begin(), s.end());
}

}  // namespace

TEST(CompressorErrorPaths, NoneCodecRejectsSmallBuffers) {
    auto codec = Compressor::create(Compression::None);
    ASSERT_NE(codec, nullptr);

    auto input = to_bytes("hello");
    std::vector<uint8_t> out(4);
    EXPECT_EQ(codec->compress(input, out.data(), out.size()), 0u);

    std::vector<uint8_t> tiny(4);
    auto status = codec->decompress(input, tiny.data(), tiny.size());
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), Status::Code::InvalidArg);
}

TEST(CompressorErrorPaths, GzipCoversDefaultLevelAndErrorBranches) {
    auto codec = Compressor::create(Compression::GZip, 0);
    ASSERT_NE(codec, nullptr);

    std::vector<uint8_t> input(256, 'g');
    std::vector<uint8_t> tiny(12);
    EXPECT_EQ(codec->compress(input, tiny.data(), tiny.size()), 0u);

    std::vector<uint8_t> compressed(codec->max_compressed_size(input.size()));
    size_t n = codec->compress(input, compressed.data(), compressed.size());
    ASSERT_GT(n, 0u);

    std::vector<uint8_t> too_small(8);
    auto small = codec->decompress({compressed.data(), n}, too_small.data(), too_small.size());
    EXPECT_FALSE(small.ok());
    EXPECT_EQ(small.code(), Status::Code::InvalidArg);

    auto truncated = codec->decompress({compressed.data(), n - 1}, compressed.data(), input.size());
    EXPECT_FALSE(truncated.ok());

    std::vector<uint8_t> wrong_size(input.size() + 1);
    auto mismatch = codec->decompress({compressed.data(), n}, wrong_size.data(), wrong_size.size());
    EXPECT_FALSE(mismatch.ok());
    EXPECT_EQ(mismatch.code(), Status::Code::Corruption);

    auto garbage = to_bytes("not-a-gzip-stream");
    std::vector<uint8_t> out(64);
    auto corrupt = codec->decompress(garbage, out.data(), out.size());
    EXPECT_FALSE(corrupt.ok());
    EXPECT_EQ(corrupt.code(), Status::Code::Corruption);
}
