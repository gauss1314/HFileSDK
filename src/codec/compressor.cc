#include "compressor.h"
#include <cstring>
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

// ─── GZip (RFC 1952 gzip format — required by HBase RegionServer) ─────────────
//
// HBase "GZ" compression has two code paths that expect DIFFERENT formats:
//
//   1. Native Hadoop zlib path (ZlibDecompressor with DEFAULT_HEADER):
//      Uses windowBits=15, expects RFC 1950 zlib format (header 0x78 xx).
//      Active when native-hadoop libraries are installed.
//
//   2. Pure-Java fallback (BuiltInGzipDecompressor):
//      Strictly requires RFC 1952 gzip format (magic 0x1f 0x8b).
//      Active when native-hadoop libraries are NOT installed on that node.
//
// Hadoop assumes all nodes in the cluster have consistent native-lib status.
// In practice, RegionServer nodes may lack native libs while the local
// machine (where `hbase hfile` runs) has them.  This causes the classic
// "works locally, fails on RS" symptom.
//
// We produce RFC 1952 gzip format because BuiltInGzipDecompressor (the more
// restrictive path) is what RS nodes without native libs use.
//
// If local `hbase hfile` verification fails with "unknown compression method",
// disable native zlib for the local test:
//   HADOOP_OPTS="-Dhadoop.native.lib=false" hbase hfile -m -v -f <path>
//
// For performance, use zlib-ng (https://github.com/zlib-ng/zlib-ng) compiled
// in compat mode.  Same zlib.h API, SIMD-optimized, typically 2-4x faster.

class GZipCompressor final : public Compressor {
public:
    /// @param level 1 (fastest) to 9 (best ratio).  0 = Z_DEFAULT_COMPRESSION (6).
    explicit GZipCompressor(int level = Z_DEFAULT_COMPRESSION)
        : Compressor(Compression::GZip)
        , level_(level <= 0 ? Z_DEFAULT_COMPRESSION : level) {}

    ~GZipCompressor() override {
        shutdown();
    }

    size_t max_compressed_size(size_t n) const noexcept override {
        // Match JDK GZIPOutputStream layout:
        // fixed 10-byte header + raw deflate payload + 8-byte trailer.
        return compressBound(static_cast<uLong>(n)) + 18;
    }

    size_t compress(std::span<const uint8_t> input,
                    uint8_t* output, size_t capacity) const noexcept override {
        return compress_impl(input, output, capacity, false, 0);
    }

    size_t compress_with_crc32(std::span<const uint8_t> input,
                               uint8_t* output,
                               size_t capacity,
                               uint32_t precomputed_crc32) const noexcept override {
        return compress_impl(input, output, capacity, true, precomputed_crc32);
    }

private:
    size_t compress_impl(std::span<const uint8_t> input,
                         uint8_t* output,
                         size_t capacity,
                         bool use_precomputed_crc32,
                         uint32_t precomputed_crc32) const noexcept {
        static constexpr uint8_t kGzipHeader[10] = {
            0x1f, 0x8b, 0x08, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0xff
        };
        if (capacity < sizeof(kGzipHeader) + 8) return 0;

        if (!ensure_initialized()) return 0;
        if (deflateReset(&strm_) != Z_OK) {
            shutdown();
            if (!ensure_initialized()) return 0;
            if (deflateReset(&strm_) != Z_OK) return 0;
        }

        std::memcpy(output, kGzipHeader, sizeof(kGzipHeader));

        strm_.next_in   = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input.data()));
        strm_.avail_in  = static_cast<uInt>(input.size());
        strm_.next_out  = reinterpret_cast<Bytef*>(output + sizeof(kGzipHeader));
        strm_.avail_out = static_cast<uInt>(capacity - sizeof(kGzipHeader) - 8);

        int ret = Z_OK;
        while (strm_.avail_in > 0) {
            ret = deflate(&strm_, Z_NO_FLUSH);
            if (ret != Z_OK) {
                return 0;
            }
            if (strm_.avail_out == 0) {
                return 0;
            }
        }

        do {
            ret = deflate(&strm_, Z_FINISH);
            if (ret != Z_OK && ret != Z_STREAM_END) {
                return 0;
            }
            if (ret != Z_STREAM_END && strm_.avail_out == 0) {
                return 0;
            }
        } while (ret != Z_STREAM_END);

        size_t deflated_size = strm_.total_out;

        const uint32_t crc = use_precomputed_crc32
            ? precomputed_crc32
            : static_cast<uint32_t>(
                ::crc32(0L, reinterpret_cast<const Bytef*>(input.data()),
                        static_cast<uInt>(input.size())));
        const uint32_t input_size = static_cast<uint32_t>(input.size());
        uint8_t* trailer = output + sizeof(kGzipHeader) + deflated_size;
        trailer[0] = static_cast<uint8_t>(crc & 0xff);
        trailer[1] = static_cast<uint8_t>((crc >> 8) & 0xff);
        trailer[2] = static_cast<uint8_t>((crc >> 16) & 0xff);
        trailer[3] = static_cast<uint8_t>((crc >> 24) & 0xff);
        trailer[4] = static_cast<uint8_t>(input_size & 0xff);
        trailer[5] = static_cast<uint8_t>((input_size >> 8) & 0xff);
        trailer[6] = static_cast<uint8_t>((input_size >> 16) & 0xff);
        trailer[7] = static_cast<uint8_t>((input_size >> 24) & 0xff);
        return sizeof(kGzipHeader) + deflated_size + 8;
    }

public:
    Status decompress(std::span<const uint8_t> compressed,
                      uint8_t* output, size_t output_size) const noexcept override {
        uint8_t scratch = 0;
        z_stream strm{};
        strm.next_in   = const_cast<Bytef*>(
            reinterpret_cast<const Bytef*>(compressed.data()));
        strm.avail_in  = static_cast<uInt>(compressed.size());
        strm.next_out  = reinterpret_cast<Bytef*>(output_size > 0 ? output : &scratch);
        strm.avail_out = static_cast<uInt>(output_size > 0 ? output_size : 1);

        // windowBits = MAX_WBITS + 32 (47) → auto-detect zlib or gzip format.
        // This lets our decompress() handle files written by either format,
        // useful for unit tests and hfile-verify.
        int ret = inflateInit2(&strm, MAX_WBITS + 32);
        if (ret != Z_OK) return Status::Corruption("inflateInit2 failed");

        for (;;) {
            ret = inflate(&strm, Z_NO_FLUSH);
            if (ret == Z_STREAM_END) {
                break;
            }
            if (ret == Z_OK) {
                if (strm.avail_out == 0) {
                    inflateEnd(&strm);
                    return Status::InvalidArg("GZip decompress: output buffer too small");
                }
                continue;
            }
            if (ret == Z_BUF_ERROR && strm.avail_in == 0) {
                inflateEnd(&strm);
                return Status::Corruption("GZip decompress truncated input");
            }
            inflateEnd(&strm);
            return Status::Corruption("GZip decompress failed (inflate returned "
                                      + std::to_string(ret) + ")");
        }
        const auto total_out = static_cast<size_t>(strm.total_out);
        inflateEnd(&strm);

        if (total_out != output_size) {
            return Status::Corruption("GZip decompressed size mismatch (expected "
                                      + std::to_string(output_size) + ", got "
                                      + std::to_string(total_out) + ")");
        }
        return Status::OK();
    }

private:
    int level_;
    bool ensure_initialized() const noexcept {
        if (initialized_) return true;
        std::memset(&strm_, 0, sizeof(strm_));
        const int ret = deflateInit2(&strm_, level_, Z_DEFLATED,
                                     -MAX_WBITS, /*memLevel=*/8,
                                     Z_DEFAULT_STRATEGY);
        initialized_ = (ret == Z_OK);
        return initialized_;
    }

    void shutdown() const noexcept {
        if (!initialized_) return;
        deflateEnd(&strm_);
        initialized_ = false;
        std::memset(&strm_, 0, sizeof(strm_));
    }

    mutable z_stream strm_{};
    mutable bool initialized_{false};
};

// ─── Factory ──────────────────────────────────────────────────────────────────

std::unique_ptr<Compressor> Compressor::create(Compression c, int level) {
    switch (c) {
    case Compression::None:   return std::make_unique<NoneCompressor>();
    case Compression::GZip:   return std::make_unique<GZipCompressor>(level);
    }
    return std::make_unique<NoneCompressor>();
}

} // namespace codec
} // namespace hfile
