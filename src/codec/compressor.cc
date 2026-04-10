#include "compressor.h"
#include <cstring>
#include <algorithm>
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
        // Hadoop BlockCompressorStream framing (repeated blocks):
        // [chunk_uncompressed_len:4][chunk_compressed_len:4][chunk_bytes]...
        const size_t chunks = (n + kMaxInputSize - 1) / kMaxInputSize;
        const size_t worst_per_chunk =
            8 + static_cast<size_t>(LZ4_compressBound(static_cast<int>(kMaxInputSize)));
        return chunks * worst_per_chunk;
    }

    size_t compress(std::span<const uint8_t> input,
                    uint8_t* output, size_t capacity) const noexcept override {
        if (capacity < max_compressed_size(input.size())) return 0;

        uint8_t* p = output;

        size_t off = 0;
        while (off < input.size()) {
            const size_t chunk_in_len = std::min(kMaxInputSize, input.size() - off);
            const size_t chunk_bound = static_cast<size_t>(
                LZ4_compressBound(static_cast<int>(chunk_in_len)));
            if (static_cast<size_t>(p - output) + 8 + chunk_bound > capacity) return 0;

            int chunk_out_len = LZ4_compress_default(
                reinterpret_cast<const char*>(input.data() + off),
                reinterpret_cast<char*>(p + 8),
                static_cast<int>(chunk_in_len),
                static_cast<int>(chunk_bound));
            if (chunk_out_len <= 0) return 0;

            write_be32(p, static_cast<uint32_t>(chunk_in_len));
            write_be32(p + 4, static_cast<uint32_t>(chunk_out_len));
            p += 8 + static_cast<size_t>(chunk_out_len);
            off += chunk_in_len;
        }
        return static_cast<size_t>(p - output);
    }

    Status decompress(std::span<const uint8_t> compressed,
                      uint8_t* output, size_t output_size) const noexcept override {
        if (compressed.empty()) {
            return Status::OK();
        }
        if (compressed.size() < 8) {
            return Status::Corruption("LZ4 framed block too small");
        }

        size_t in_off = 0;
        size_t out_off = 0;
        while (in_off < compressed.size()) {
            if (in_off + 8 > compressed.size())
                return Status::Corruption("LZ4 missing chunk header");
            const uint32_t chunk_uncompressed_len = read_be32(compressed.data() + in_off);
            const uint32_t chunk_len = read_be32(compressed.data() + in_off + 4);
            in_off += 8;
            if (in_off + chunk_len > compressed.size())
                return Status::Corruption("LZ4 framed chunk length overflow");
            if (out_off + chunk_uncompressed_len > output_size)
                return Status::InvalidArg("LZ4 output buffer too small");

            int r = LZ4_decompress_safe(
                reinterpret_cast<const char*>(compressed.data() + in_off),
                reinterpret_cast<char*>(output + out_off),
                static_cast<int>(chunk_len),
                static_cast<int>(chunk_uncompressed_len));
            if (r < 0) return Status::Corruption("LZ4 decompress failed");
            if (static_cast<uint32_t>(r) != chunk_uncompressed_len) {
                return Status::Corruption("LZ4 decompressed chunk size mismatch");
            }
            out_off += static_cast<size_t>(r);
            in_off += chunk_len;
        }
        return Status::OK();
    }

private:
    // Match Hadoop Lz4Compressor default direct buffer sizing behavior.
    static constexpr size_t kDefaultBufferSize = 64 * 1024;
    static constexpr size_t kCompressionOverhead = (kDefaultBufferSize / 255) + 16;
    static constexpr size_t kMaxInputSize = kDefaultBufferSize - kCompressionOverhead;
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

    size_t max_compressed_size(size_t n) const noexcept override {
        // gzip adds 10-byte header + 8-byte trailer on top of deflate bound
        return compressBound(static_cast<uLong>(n)) + 32;
    }

    size_t compress(std::span<const uint8_t> input,
                    uint8_t* output, size_t capacity) const noexcept override {
        z_stream strm{};
        strm.next_in   = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input.data()));
        strm.avail_in  = static_cast<uInt>(input.size());
        strm.next_out  = reinterpret_cast<Bytef*>(output);
        strm.avail_out = static_cast<uInt>(capacity);

        // windowBits = MAX_WBITS + 16 (31) → RFC 1952 gzip format.
        //
        // HBase clusters may or may not have native Hadoop zlib:
        //   - WITH native zlib:  ZlibDecompressor(DEFAULT_HEADER, windowBits=15)
        //                        → only handles RFC 1950 zlib format
        //   - WITHOUT native:    BuiltInGzipDecompressor (pure Java)
        //                        → strictly requires RFC 1952 gzip (checks 0x1f 0x8b)
        //
        // We produce gzip format because BuiltInGzipDecompressor is the more
        // restrictive consumer.  If the local `hbase hfile` tool fails because
        // the local machine HAS native zlib, disable it for testing:
        //   HADOOP_OPTS="-Dhadoop.native.lib=false" hbase hfile -m -v -f <path>
        int ret = deflateInit2(&strm, level_, Z_DEFLATED,
                               MAX_WBITS + 16, /*memLevel=*/8,
                               Z_DEFAULT_STRATEGY);
        if (ret != Z_OK) return 0;

        ret = deflate(&strm, Z_FINISH);
        size_t compressed_size = strm.total_out;
        deflateEnd(&strm);

        if (ret != Z_STREAM_END) return 0;
        return compressed_size;
    }

    Status decompress(std::span<const uint8_t> compressed,
                      uint8_t* output, size_t output_size) const noexcept override {
        z_stream strm{};
        strm.next_in   = const_cast<Bytef*>(
            reinterpret_cast<const Bytef*>(compressed.data()));
        strm.avail_in  = static_cast<uInt>(compressed.size());
        strm.next_out  = reinterpret_cast<Bytef*>(output);
        strm.avail_out = static_cast<uInt>(output_size);

        // windowBits = MAX_WBITS + 32 (47) → auto-detect zlib or gzip format.
        // This lets our decompress() handle files written by either format,
        // useful for unit tests and hfile-verify.
        int ret = inflateInit2(&strm, MAX_WBITS + 32);
        if (ret != Z_OK) return Status::Corruption("inflateInit2 failed");

        ret = inflate(&strm, Z_FINISH);
        inflateEnd(&strm);

        if (ret != Z_STREAM_END)
            return Status::Corruption("GZip decompress failed (inflate returned "
                                      + std::to_string(ret) + ")");
        return Status::OK();
    }

private:
    int level_;
};

// ─── Factory ──────────────────────────────────────────────────────────────────

std::unique_ptr<Compressor> Compressor::create(Compression c, int level) {
    switch (c) {
    case Compression::None:   return std::make_unique<NoneCompressor>();
    case Compression::LZ4:    return std::make_unique<LZ4Compressor>();
    case Compression::Zstd:   return std::make_unique<ZstdCompressor>();
    case Compression::Snappy: return std::make_unique<SnappyCompressor>();
    case Compression::GZip:   return std::make_unique<GZipCompressor>(level);
    }
    return std::make_unique<NoneCompressor>();
}

} // namespace codec
} // namespace hfile