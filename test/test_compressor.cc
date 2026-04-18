#include <gtest/gtest.h>
#include "codec/compressor.h"
#include <hfile/types.h>
#include <vector>
#include <numeric>
#include <cstring>
#include <set>
#include <zlib.h>

using namespace hfile;
using namespace hfile::codec;

struct CompressTestParam {
    Compression type;
    const char* name;
};

class CompressorTest : public ::testing::TestWithParam<CompressTestParam> {};

static std::vector<uint8_t> gzip_reference_oneshot(std::span<const uint8_t> input, int level) {
    static constexpr uint8_t kGzipHeader[10] = {
        0x1f, 0x8b, 0x08, 0x00,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0xff
    };

    z_stream strm{};
    int rc = deflateInit2(&strm, level <= 0 ? Z_DEFAULT_COMPRESSION : level,
                          Z_DEFLATED, -MAX_WBITS, 8, Z_DEFAULT_STRATEGY);
    EXPECT_EQ(rc, Z_OK);

    std::vector<uint8_t> out(compressBound(static_cast<uLong>(input.size())) + 18);
    std::memcpy(out.data(), kGzipHeader, sizeof(kGzipHeader));

    strm.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input.data()));
    strm.avail_in = static_cast<uInt>(input.size());
    strm.next_out = reinterpret_cast<Bytef*>(out.data() + sizeof(kGzipHeader));
    strm.avail_out = static_cast<uInt>(out.size() - sizeof(kGzipHeader) - 8);

    while (strm.avail_in > 0) {
        rc = deflate(&strm, Z_NO_FLUSH);
        EXPECT_EQ(rc, Z_OK);
    }
    do {
        rc = deflate(&strm, Z_FINISH);
        EXPECT_TRUE(rc == Z_OK || rc == Z_STREAM_END);
    } while (rc != Z_STREAM_END);

    size_t deflated_size = strm.total_out;
    deflateEnd(&strm);

    const uint32_t crc = static_cast<uint32_t>(
        ::crc32(0L, reinterpret_cast<const Bytef*>(input.data()),
                static_cast<uInt>(input.size())));
    const uint32_t input_size = static_cast<uint32_t>(input.size());
    uint8_t* trailer = out.data() + sizeof(kGzipHeader) + deflated_size;
    trailer[0] = static_cast<uint8_t>(crc & 0xff);
    trailer[1] = static_cast<uint8_t>((crc >> 8) & 0xff);
    trailer[2] = static_cast<uint8_t>((crc >> 16) & 0xff);
    trailer[3] = static_cast<uint8_t>((crc >> 24) & 0xff);
    trailer[4] = static_cast<uint8_t>(input_size & 0xff);
    trailer[5] = static_cast<uint8_t>((input_size >> 8) & 0xff);
    trailer[6] = static_cast<uint8_t>((input_size >> 16) & 0xff);
    trailer[7] = static_cast<uint8_t>((input_size >> 24) & 0xff);
    out.resize(sizeof(kGzipHeader) + deflated_size + 8);
    return out;
}

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

TEST(CompressorStandalone, GZipCompressionLevelsRoundTrip) {
    std::vector<uint8_t> input(256 * 1024);
    for (size_t i = 0; i < input.size(); ++i)
        input[i] = static_cast<uint8_t>('a' + (i % 4));

    std::vector<size_t> compressed_sizes;
    compressed_sizes.reserve(9);
    std::set<size_t> unique_sizes;

    for (int level = 1; level <= 9; ++level) {
        auto c = Compressor::create(Compression::GZip, level);
        ASSERT_NE(c, nullptr);

        std::vector<uint8_t> compressed(c->max_compressed_size(input.size()));
        size_t comp_len = c->compress(input, compressed.data(), compressed.size());
        ASSERT_GT(comp_len, 0u) << "level=" << level;
        ASSERT_GE(comp_len, 2u);
        EXPECT_EQ(compressed[0], 0x1f);
        EXPECT_EQ(compressed[1], 0x8b);

        std::vector<uint8_t> decompressed(input.size());
        auto s = c->decompress(
            {compressed.data(), comp_len}, decompressed.data(), decompressed.size());
        EXPECT_TRUE(s.ok()) << "level=" << level << " " << s.message();
        EXPECT_EQ(decompressed, input);

        compressed_sizes.push_back(comp_len);
        unique_sizes.insert(comp_len);
    }

    EXPECT_LE(compressed_sizes.back(), compressed_sizes.front());
    EXPECT_GT(unique_sizes.size(), 1u);
}

TEST(CompressorStandalone, GZipSingleInstanceCanCompressMultipleBlocks) {
    auto c = Compressor::create(Compression::GZip, 1);
    ASSERT_NE(c, nullptr);

    std::vector<uint8_t> input_a(64 * 1024, 'a');
    std::vector<uint8_t> input_b(64 * 1024, 'b');
    for (int round = 0; round < 4; ++round) {
        for (const auto* input : {&input_a, &input_b}) {
            std::vector<uint8_t> compressed(c->max_compressed_size(input->size()));
            size_t comp_len = c->compress(*input, compressed.data(), compressed.size());
            ASSERT_GT(comp_len, 0u) << "round=" << round;

            std::vector<uint8_t> decompressed(input->size());
            auto s = c->decompress(
                {compressed.data(), comp_len}, decompressed.data(), decompressed.size());
            ASSERT_TRUE(s.ok()) << s.message();
            EXPECT_EQ(decompressed, *input);
        }
    }
}

TEST(CompressorStandalone, GZipSingleInstanceMatchesFreshInstanceOutput) {
    auto reused = Compressor::create(Compression::GZip, 1);
    ASSERT_NE(reused, nullptr);

    std::vector<uint8_t> input(96 * 1024);
    for (size_t i = 0; i < input.size(); ++i)
        input[i] = static_cast<uint8_t>('a' + (i % 7));

    std::vector<uint8_t> fresh_a(Compressor::create(Compression::GZip, 1)->max_compressed_size(input.size()));
    std::vector<uint8_t> fresh_b(Compressor::create(Compression::GZip, 1)->max_compressed_size(input.size()));
    std::vector<uint8_t> reused_a(reused->max_compressed_size(input.size()));
    std::vector<uint8_t> reused_b(reused->max_compressed_size(input.size()));

    auto fresh1 = Compressor::create(Compression::GZip, 1);
    auto fresh2 = Compressor::create(Compression::GZip, 1);
    ASSERT_NE(fresh1, nullptr);
    ASSERT_NE(fresh2, nullptr);

    size_t fresh_len_a = fresh1->compress(input, fresh_a.data(), fresh_a.size());
    size_t fresh_len_b = fresh2->compress(input, fresh_b.data(), fresh_b.size());
    size_t reused_len_a = reused->compress(input, reused_a.data(), reused_a.size());
    size_t reused_len_b = reused->compress(input, reused_b.data(), reused_b.size());

    ASSERT_GT(fresh_len_a, 0u);
    ASSERT_GT(fresh_len_b, 0u);
    ASSERT_GT(reused_len_a, 0u);
    ASSERT_GT(reused_len_b, 0u);

    fresh_a.resize(fresh_len_a);
    fresh_b.resize(fresh_len_b);
    reused_a.resize(reused_len_a);
    reused_b.resize(reused_len_b);

    EXPECT_EQ(fresh_a, fresh_b);
    EXPECT_EQ(fresh_a, reused_a);
    EXPECT_EQ(fresh_a, reused_b);
}

TEST(CompressorStandalone, GZipMatchesReferenceOneShotZlib) {
    auto c = Compressor::create(Compression::GZip, 1);
    ASSERT_NE(c, nullptr);

    std::vector<uint8_t> input(96 * 1024);
    for (size_t i = 0; i < input.size(); ++i)
        input[i] = static_cast<uint8_t>('0' + (i % 10));

    std::vector<uint8_t> actual(c->max_compressed_size(input.size()));
    size_t actual_len = c->compress(input, actual.data(), actual.size());
    ASSERT_GT(actual_len, 0u);
    actual.resize(actual_len);

    auto expected = gzip_reference_oneshot(input, 1);
    EXPECT_EQ(expected, actual);
}

INSTANTIATE_TEST_SUITE_P(
    AllCodecs, CompressorTest,
    ::testing::Values(
        CompressTestParam{Compression::None,   "None"},
        CompressTestParam{Compression::GZip,   "GZip"},
        CompressTestParam{Compression::LZ4,    "LZ4"},
        CompressTestParam{Compression::Zstd,   "Zstd"},
        CompressTestParam{Compression::Snappy, "Snappy"}
    ),
    [](const ::testing::TestParamInfo<CompressTestParam>& info) {
        return info.param.name;
    });
