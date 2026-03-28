#include "crc32c.h"
#include <cstring>

#ifdef __SSE4_2__
#  include <nmmintrin.h>
#endif

namespace hfile {
namespace checksum {

// ─── Scalar fallback table ─────────────────────────────────────────────────

static uint32_t kCRC32CTable[256];
static bool     kTableInitialized = false;

static void init_table() noexcept {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            if (crc & 1)
                crc = 0x82F63B78u ^ (crc >> 1);
            else
                crc >>= 1;
        }
        kCRC32CTable[i] = crc;
    }
    kTableInitialized = true;
}

static uint32_t crc32c_scalar(uint32_t crc, const uint8_t* data, size_t len) noexcept {
    if (!kTableInitialized) init_table();
    for (size_t i = 0; i < len; ++i)
        crc = kCRC32CTable[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return crc;
}

// ─── SSE4.2 hardware path ─────────────────────────────────────────────────
//
// NOTE: A "3-way parallel" CRC requires merging independent lane results via
// CRC folding using PCLMULQDQ (carryless multiplication).  The naive pattern
//   crc = crc32(crc32(c0, c1), c2)
// is mathematically wrong — it feeds a CRC value as plain data into another
// CRC, which does not preserve the polynomial identity CRC(A||B||C) and
// produces incorrect checksums.
//
// The correct approach (feeding back the carries with CLMUL) adds significant
// complexity and a compile-time dependency on PCLMULQDQ.  For 64 KB blocks,
// the serial 8-byte loop below already saturates memory bandwidth and is
// correct.  A proper 3-way implementation can be added later if profiling
// shows CRC32C is a bottleneck (it typically is not).

#ifdef __SSE4_2__

static uint32_t crc32c_hw(uint32_t crc, const uint8_t* data, size_t len) noexcept {
    // Serial 8-byte lane: one _mm_crc32_u64 per iteration.
    // Latency = 3 cycles, throughput = 1/cycle; this is correct and fast.
    while (len >= sizeof(uint64_t)) {
        uint64_t v;
        std::memcpy(&v, data, sizeof(uint64_t));
        crc  = static_cast<uint32_t>(_mm_crc32_u64(crc, v));
        data += sizeof(uint64_t);
        len  -= sizeof(uint64_t);
    }
    // Drain 4-byte tail
    if (len >= sizeof(uint32_t)) {
        uint32_t v;
        std::memcpy(&v, data, sizeof(uint32_t));
        crc  = _mm_crc32_u32(crc, v);
        data += sizeof(uint32_t);
        len  -= sizeof(uint32_t);
    }
    // Drain byte tail
    while (len-- > 0)
        crc = _mm_crc32_u8(crc, *data++);

    return crc;
}

#endif // __SSE4_2__

// ─── Public API ───────────────────────────────────────────────────────────

uint32_t crc32c(uint32_t crc, const uint8_t* data, size_t len) noexcept {
#ifdef __SSE4_2__
    return crc32c_hw(crc, data, len);
#else
    return crc32c_scalar(crc, data, len);
#endif
}

size_t compute_hfile_checksums(
    const uint8_t* data,
    size_t         data_len,
    uint32_t       bytes_per_chunk,
    uint8_t*       out_checksums) noexcept
{
    size_t out_off = 0;
    size_t offset  = 0;
    while (offset < data_len) {
        size_t chunk_len = std::min<size_t>(bytes_per_chunk, data_len - offset);
        uint32_t c = crc32c(data + offset, chunk_len);
        // Store Big-Endian
        out_checksums[out_off + 0] = static_cast<uint8_t>(c >> 24);
        out_checksums[out_off + 1] = static_cast<uint8_t>(c >> 16);
        out_checksums[out_off + 2] = static_cast<uint8_t>(c >>  8);
        out_checksums[out_off + 3] = static_cast<uint8_t>(c);
        out_off += 4;
        offset  += chunk_len;
    }
    return out_off;
}

} // namespace checksum
} // namespace hfile
