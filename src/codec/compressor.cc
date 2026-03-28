#include "compressor.h"
#include <cstring>
#include <lz4.h>
#include <zstd.h>
#include <snappy.h>
#include <zlib.h>

namespace hfile {
namespace codec {

// ─── None (passthrough) ───────────────────────────────────────────────────────

class NoneCompressor final : public Compressor {
public:
    NoneCompressor() : Compressor(Compression::None) {}

    size_t max_compressed_size(size_t n) const noexcept override { return n; }

    size_t compress(std::span<const uint8_t> input,
                    uint8_t* output, size_t capacity) const noexcept override {
        if (capacity < input.size()) return 0;
        std::memcpy(output, input.data(), input.size());
        return input.size();
    }

    Status decompress(std::span<const uint8_t> compressed,
                      uint8_t* output, size_t output_size) const noexcept override {
        if (output_size < compressed.size()) return Status::InvalidArg("buffer too small");
        std::memcpy(output, compressed.data(), compressed.size());
        return Status::OK();
    }
};

// ─── LZ4 ──────────────────────────────────────────────────────────────────────

class LZ4Compressor final : public Compressor {
public:
    LZ4Compressor() : Compressor(Compression::LZ4) {}

    size_t max_compressed_size(size_t n) const noexcept override {
        return static_cast<size_t>(LZ4_compressBound(static_cast<int>(n)));
    }

    size_t compress(std::span<const uint8_t> input,
                    uint8_t* output, size_t capacity) const noexcept override {
        int result = LZ4_compress_default(
            reinterpret_cast<const char*>(input.data()),
            reinterpret_cast<char*>(output),
            static_cast<int>(input.size()),
            static_cast<int>(capacity));
        return result > 0 ? static_cast<size_t>(result) : 0;
    }

    Status decompress(std::span<const uint8_t> compressed,
                      uint8_t* output, size_t output_size) const noexcept override {
        int r = LZ4_decompress_safe(
            reinterpret_cast<const char*>(compressed.data()),
            reinterpret_cast<char*>(output),
            static_cast<int>(compressed.size()),
            static_cast<int>(output_size));
        if (r < 0) return Status::Corruption("LZ4 decompress failed");
        return Status::OK();
    }
};

// ─── ZSTD ─────────────────────────────────────────────────────────────────────

class ZstdCompressor final : public Compressor {
public:
    ZstdCompressor() : Compressor(Compression::Zstd) {}

    size_t max_compressed_size(size_t n) const noexcept override {
        return ZSTD_compressBound(n);
    }

    size_t compress(std::span<const uint8_t> input,
                    uint8_t* output, size_t capacity) const noexcept override {
        size_t r = ZSTD_compress(output, capacity,
                                 input.data(), input.size(),
                                 /* level */ 3);
        return ZSTD_isError(r) ? 0 : r;
    }

    Status decompress(std::span<const uint8_t> compressed,
                      uint8_t* output, size_t output_size) const noexcept override {
        size_t r = ZSTD_decompress(output, output_size,
                                   compressed.data(), compressed.size());
        if (ZSTD_isError(r)) return Status::Corruption(ZSTD_getErrorName(r));
        return Status::OK();
    }
};

// ─── Snappy ───────────────────────────────────────────────────────────────────

class SnappyCompressor final : public Compressor {
public:
    SnappyCompressor() : Compressor(Compression::Snappy) {}

    size_t max_compressed_size(size_t n) const noexcept override {
        return snappy::MaxCompressedLength(n);
    }

    size_t compress(std::span<const uint8_t> input,
                    uint8_t* output, size_t /*capacity*/) const noexcept override {
        size_t out_len = 0;
        snappy::RawCompress(
            reinterpret_cast<const char*>(input.data()), input.size(),
            reinterpret_cast<char*>(output), &out_len);
        return out_len;
    }

    Status decompress(std::span<const uint8_t> compressed,
                      uint8_t* output, size_t output_size) const noexcept override {
        // RawUncompress writes exactly GetUncompressedLength() bytes into output
        // without bounds-checking — verify the buffer is large enough first.
        size_t needed = 0;
        if (!snappy::GetUncompressedLength(
                reinterpret_cast<const char*>(compressed.data()),
                compressed.size(), &needed)) {
            return Status::Corruption("Snappy: cannot read uncompressed length");
        }
        if (needed > output_size)
            return Status::InvalidArg(
                "Snappy decompress: output buffer too small ("
                + std::to_string(output_size) + " < " + std::to_string(needed) + ")");

        bool ok = snappy::RawUncompress(
            reinterpret_cast<const char*>(compressed.data()), compressed.size(),
            reinterpret_cast<char*>(output));
        if (!ok) return Status::Corruption("Snappy decompress failed");
        return Status::OK();
    }
};

// ─── GZip ─────────────────────────────────────────────────────────────────────

class GZipCompressor final : public Compressor {
public:
    GZipCompressor() : Compressor(Compression::GZip) {}

    size_t max_compressed_size(size_t n) const noexcept override {
        return n + n / 1000 + 32;
    }

    size_t compress(std::span<const uint8_t> input,
                    uint8_t* output, size_t capacity) const noexcept override {
        uLongf dest_len = static_cast<uLongf>(capacity);
        int r = compress2(reinterpret_cast<Bytef*>(output), &dest_len,
                          reinterpret_cast<const Bytef*>(input.data()),
                          static_cast<uLong>(input.size()), Z_DEFAULT_COMPRESSION);
        return r == Z_OK ? static_cast<size_t>(dest_len) : 0;
    }

    Status decompress(std::span<const uint8_t> compressed,
                      uint8_t* output, size_t output_size) const noexcept override {
        uLongf dest_len = static_cast<uLongf>(output_size);
        int r = uncompress(reinterpret_cast<Bytef*>(output), &dest_len,
                           reinterpret_cast<const Bytef*>(compressed.data()),
                           static_cast<uLong>(compressed.size()));
        if (r != Z_OK) return Status::Corruption("GZip decompress failed");
        return Status::OK();
    }
};

// ─── Factory ──────────────────────────────────────────────────────────────────

std::unique_ptr<Compressor> Compressor::create(Compression c) {
    switch (c) {
    case Compression::None:   return std::make_unique<NoneCompressor>();
    case Compression::LZ4:    return std::make_unique<LZ4Compressor>();
    case Compression::Zstd:   return std::make_unique<ZstdCompressor>();
    case Compression::Snappy: return std::make_unique<SnappyCompressor>();
    case Compression::GZip:   return std::make_unique<GZipCompressor>();
    }
    return std::make_unique<NoneCompressor>();
}

} // namespace codec
} // namespace hfile
