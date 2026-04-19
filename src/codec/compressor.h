#pragma once

#include <hfile/types.h>
#include <hfile/status.h>
#include <span>
#include <vector>
#include <memory>
#include <cstdint>

namespace hfile {
namespace codec {

/// Abstract compressor interface.
class Compressor {
public:
    virtual ~Compressor() = default;

    /// Maximum compressed size for given input size.
    virtual size_t max_compressed_size(size_t input_size) const noexcept = 0;

    /// Compress `input` into `output`.
    /// `output` must be at least `max_compressed_size(input.size())` bytes.
    /// Returns number of bytes written, or 0 on failure.
    virtual size_t compress(
        std::span<const uint8_t> input,
        uint8_t*                 output,
        size_t                   output_capacity) const noexcept = 0;

    /// Compress `input` into `output` using a caller-provided CRC32 for the
    /// uncompressed payload when the codec can use it (currently GZip trailer).
    /// The default implementation ignores `precomputed_crc32` and falls back to
    /// `compress()`.
    virtual size_t compress_with_crc32(
        std::span<const uint8_t> input,
        uint8_t*                 output,
        size_t                   output_capacity,
        uint32_t                 precomputed_crc32) const noexcept {
        (void)precomputed_crc32;
        return compress(input, output, output_capacity);
    }

    /// Decompress (for validation / testing).
    virtual Status decompress(
        std::span<const uint8_t> compressed,
        uint8_t*                 output,
        size_t                   output_size) const noexcept = 0;

    Compression type() const noexcept { return type_; }

    /// Create a compressor for the given algorithm.
    /// @param level Compression level (1=fastest, 9=best ratio).
    ///              Only meaningful for GZip and Zstd; ignored for LZ4/Snappy/None.
    ///              Default (0) uses the algorithm's default level.
    static std::unique_ptr<Compressor> create(Compression c, int level = 0);

protected:
    explicit Compressor(Compression t) : type_{t} {}

private:
    Compression type_;
};

} // namespace codec
} // namespace hfile
