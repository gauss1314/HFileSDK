#include <gtest/gtest.h>
#include "checksum/crc32c.h"
#include <hfile/types.h>
#include <vector>
#include <numeric>

using namespace hfile::checksum;

TEST(CRC32C, KnownValues) {
    // CRC32C of empty string is 0x00000000
    EXPECT_EQ(crc32c(nullptr, 0), 0x00000000u);

    // Known value: CRC32C("123456789") = 0xE3069283
    const uint8_t* data = reinterpret_cast<const uint8_t*>("123456789");
    EXPECT_EQ(crc32c(data, 9), 0xE3069283u);
}

TEST(CRC32C, Incremental) {
    const uint8_t data[] = {0x01, 0x02, 0x03, 0x04, 0x05};
    uint32_t full = crc32c(data, 5);

    // Compute incrementally using internal crc with carry
    uint32_t c = 0xFFFFFFFF;
    c = crc32c(c, data,     3);
    c = crc32c(c, data + 3, 2);
    c ^= 0xFFFFFFFF;
    EXPECT_EQ(full, c);
}

TEST(CRC32C, LargeBuffer) {
    std::vector<uint8_t> buf(1024 * 1024);
    std::iota(buf.begin(), buf.end(), 0);
    uint32_t crc = crc32c(buf.data(), buf.size());
    // Just verify it's stable across two calls
    EXPECT_EQ(crc, crc32c(buf.data(), buf.size()));
}

TEST(CRC32C, SpanOverload) {
    std::vector<uint8_t> v = {0xDE, 0xAD, 0xBE, 0xEF};
    uint32_t a = crc32c(v.data(), v.size());
    uint32_t b = crc32c(std::span<const uint8_t>{v});
    EXPECT_EQ(a, b);
}

TEST(CRC32C, ChunkChecksums) {
    std::vector<uint8_t> data(1024, 0xAB);
    // 512-byte chunks → 2 checksums → 8 bytes
    std::vector<uint8_t> out(8);
    size_t written = compute_hfile_checksums(
        data.data(), data.size(), 512, out.data());
    EXPECT_EQ(written, 8u);

    // Verify each chunk checksum independently
    uint32_t c0 = crc32c(data.data(), 512);
    uint32_t c1 = crc32c(data.data() + 512, 512);
    uint32_t stored0 = hfile::read_be32(out.data());
    uint32_t stored1 = hfile::read_be32(out.data() + 4);
    EXPECT_EQ(c0, stored0);
    EXPECT_EQ(c1, stored1);
}
