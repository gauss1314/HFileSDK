#pragma once

#include <cstdint>
#include <cstddef>
#include <span>

namespace hfile {
namespace checksum {

/// Compute CRC32C over a buffer.
/// Uses SSE4.2 hardware instruction if available, otherwise scalar fallback.
uint32_t crc32c(uint32_t crc, const uint8_t* data, size_t len) noexcept;

inline uint32_t crc32c(const uint8_t* data, size_t len) noexcept {
    return crc32c(0xFFFFFFFF, data, len) ^ 0xFFFFFFFF;
}

inline uint32_t crc32c(std::span<const uint8_t> buf) noexcept {
    return crc32c(buf.data(), buf.size());
}

/// Compute per-chunk checksums matching HBase HFile format.
/// Returns number of bytes written into out_checksums.
/// Each 512-byte chunk (or partial) gets one 4-byte CRC32C.
size_t compute_hfile_checksums(
    const uint8_t* data,
    size_t data_len,
    uint32_t bytes_per_chunk,
    uint8_t* out_checksums) noexcept;

} // namespace checksum
} // namespace hfile
