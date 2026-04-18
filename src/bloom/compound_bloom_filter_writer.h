#pragma once

#include <hfile/types.h>
#include <hfile/status.h>
#include "checksum/crc32c.h"
#include "codec/compressor.h"
#include <vector>
#include <span>
#include <cstdint>
#include <cstring>
#include <cmath>

namespace hfile {
namespace bloom {

/// HBase-compatible MurmurHash (same algorithm as org.apache.hadoop.hbase.util.MurmurHash).
inline int32_t murmur_hash(const uint8_t* data, size_t len, int32_t seed = 0) noexcept {
    constexpr int32_t m = 0x5bd1e995;
    constexpr int32_t r = 24;
    int32_t h = seed ^ static_cast<int32_t>(len);

    const size_t len_4 = len >> 2;
    for (size_t i = 0; i < len_4; ++i) {
        const size_t i_4 = i << 2;
        int32_t k = static_cast<int32_t>(data[i_4 + 3]);
        k <<= 8;
        k |= static_cast<int32_t>(data[i_4 + 2] & 0xff);
        k <<= 8;
        k |= static_cast<int32_t>(data[i_4 + 1] & 0xff);
        k <<= 8;
        k |= static_cast<int32_t>(data[i_4 + 0] & 0xff);
        k *= m;
        k ^= static_cast<int32_t>(static_cast<uint32_t>(k) >> r);
        k *= m;
        h *= m;
        h ^= k;
    }

    const size_t len_m = len_4 << 2;
    const size_t left = len - len_m;
    if (left != 0) {
        if (left >= 3) h ^= static_cast<int32_t>(data[len_m + 2]) << 16;
        if (left >= 2) h ^= static_cast<int32_t>(data[len_m + 1]) << 8;
        if (left >= 1) h ^= static_cast<int32_t>(data[len_m]);
        h *= m;
    }

    h ^= static_cast<int32_t>(static_cast<uint32_t>(h) >> 13);
    h *= m;
    h ^= static_cast<int32_t>(static_cast<uint32_t>(h) >> 15);
    return h;
}

struct BloomWriteResult {
    int64_t  bloom_meta_offset{-1};
    int64_t  bloom_data_offset{-1};
    int64_t  last_block_offset{-1};
    uint32_t total_key_count{0};
    uint64_t bloom_chunk_uncompressed_bytes_with_headers{0};
    uint64_t bloom_meta_uncompressed_bytes_with_headers{0};
    bool     enabled{false};
};

/// Compound (chunked) Bloom Filter writer compatible with HBase HFile v3.
/// Each bloom filter chunk corresponds to roughly one data block.
class CompoundBloomFilterWriter {
public:
    static constexpr int     kBloomFormatVersion = 3;
    static constexpr int     kHashTypeMurmur3 = 1;
    static constexpr double  kDefaultErrorRate  = 0.01;
    static constexpr size_t  kMaxChunkSize      = 128 * 1024;  // 128 KB per chunk

    CompoundBloomFilterWriter(BloomType type        = BloomType::Row,
                              double    error_rate   = kDefaultErrorRate,
                              size_t    expected_kvs = 1'000'000,
                              std::string comparator_class_name = {})
        : type_{type},
          error_rate_{error_rate},
          comparator_class_name_{std::move(comparator_class_name)} {
        if (type == BloomType::None) return;
        chunk_byte_size_ = compute_foldable_byte_size(static_cast<long long>(kMaxChunkSize * 8), kMaxFold);
        long long bit_size = static_cast<long long>(chunk_byte_size_) * 8;
        chunk_keys_ = static_cast<uint32_t>(compute_ideal_max_keys(bit_size, error_rate_));
        hash_count_ = compute_optimal_function_count(static_cast<int>(chunk_keys_), bit_size);
        chunk_keys_ = static_cast<uint32_t>(compute_max_keys(bit_size, error_rate_, hash_count_));
        if (expected_kvs > 0 && chunk_keys_ > expected_kvs) {
            chunk_keys_ = static_cast<uint32_t>(expected_kvs);
        }
        init_chunk();
    }

    /// Add a key to the bloom filter.
    void add(std::span<const uint8_t> key) {
        if (type_ == BloomType::None) return;
        if (cur_keys_ >= chunk_keys_) finish_chunk();
        if (cur_keys_ == 0) {
            cur_first_key_.assign(key.begin(), key.end());
        }
        set_bits(key);
        ++cur_keys_;
        ++total_keys_;
    }

    /// Add a row+col key (for RowCol bloom type).
    void add_row_col(std::span<const uint8_t> row,
                     std::span<const uint8_t> qualifier) {
        if (type_ == BloomType::RowCol) {
            // Concatenate row + qualifier for hashing
            std::vector<uint8_t> combined(row.size() + qualifier.size());
            std::memcpy(combined.data(), row.data(), row.size());
            std::memcpy(combined.data() + row.size(), qualifier.data(), qualifier.size());
            add(combined);
        } else {
            add(row);
        }
    }

    /// Seal the current chunk (called automatically when chunk fills up,
    /// and must be called explicitly by the writer when a data block is finalized).
    void finish_chunk() {
        if (type_ == BloomType::None || cur_keys_ == 0) return;
        compact_chunk();
        chunks_.push_back(std::move(cur_chunk_));
        chunk_key_counts_.push_back(cur_keys_);
        chunk_max_keys_.push_back(cur_chunk_max_keys_);
        chunk_first_keys_.push_back(std::move(cur_first_key_));
        cur_keys_ = 0;
        init_chunk();
    }

    /// Flush pending chunk and serialise all BLMFBLK2 blocks into `blocks_out`.
    /// Records each chunk's absolute file offset (needed by the meta block).
    /// Call from the writer's non-scanned section, after all data blocks.
    /// Returns true if any bloom data was produced.
    bool flush_ready_data_blocks(std::vector<uint8_t>& blocks_out,
                                 int64_t current_file_offset,
                                 int64_t prev_block_offset,
                                 BloomWriteResult* result,
                                 codec::Compressor* compressor = nullptr) {
        if (type_ == BloomType::None || chunks_.empty()) return false;

        if (result && result->bloom_data_offset < 0) {
            result->bloom_data_offset = current_file_offset;
        }

        chunk_offsets_.reserve(chunk_offsets_.size() + chunks_.size());
        chunk_on_disk_sizes_.reserve(chunk_on_disk_sizes_.size() + chunks_.size());
        chunk_byte_sizes_.reserve(chunk_byte_sizes_.size() + chunks_.size());

        int64_t current_prev_block_offset = prev_block_offset;
        for (size_t i = 0; i < chunks_.size(); ++i) {
            int64_t block_offset = current_file_offset + static_cast<int64_t>(blocks_out.size());
            chunk_offsets_.push_back(block_offset);
            chunk_byte_sizes_.push_back(static_cast<uint32_t>(chunks_[i].size()));
            size_t before_size = blocks_out.size();
            write_bloom_chunk_block(blocks_out, chunks_[i], current_prev_block_offset, compressor);
            chunk_on_disk_sizes_.push_back(
                static_cast<uint32_t>(blocks_out.size() - before_size));
            current_prev_block_offset = block_offset;
            if (result) {
                result->last_block_offset = block_offset;
                result->bloom_chunk_uncompressed_bytes_with_headers +=
                    static_cast<uint64_t>(kBlockHeaderSize + chunks_[i].size());
            }
        }

        chunks_.clear();
        return true;
    }

    bool finish_data_blocks(std::vector<uint8_t>& blocks_out,
                            int64_t current_file_offset,
                            int64_t prev_block_offset,
                            BloomWriteResult* result,
                            codec::Compressor* compressor = nullptr) {
        if (type_ == BloomType::None) return false;
        if (cur_keys_ > 0) finish_chunk();
        return flush_ready_data_blocks(
            blocks_out, current_file_offset, prev_block_offset, result, compressor);
    }

    /// Serialise the BLMFMET2 bloom meta block into `meta_out`.
    /// Must be called after finish_data_blocks().
    /// `meta_block_file_offset` is the absolute file offset where this block starts
    /// (used to build the meta root index entry pointing to BLMFMET2).
    void finish_meta_block(std::vector<uint8_t>& meta_out,
                           int64_t prev_block_offset,
                           BloomWriteResult* result = nullptr,
                           codec::Compressor* compressor = nullptr) {
        write_bloom_meta_block(meta_out, prev_block_offset, result, compressor);
    }

    bool      is_enabled()     const noexcept { return type_ != BloomType::None; }
    bool      has_data()       const noexcept {
        return !chunks_.empty() || !chunk_offsets_.empty() || cur_keys_ > 0;
    }
    bool      has_ready_chunks() const noexcept { return !chunks_.empty(); }
    uint32_t  total_keys()     const noexcept { return total_keys_; }
    BloomType bloom_type()     const noexcept { return type_; }

    /// Legacy combined API (chunk blocks + meta in one buffer).
    /// Retained for unit tests that only care about serialised content, not layout.
    BloomWriteResult finish(std::vector<uint8_t>& meta_blocks_out,
                            int64_t current_offset) {
        BloomWriteResult result;
        if (type_ == BloomType::None || (chunks_.empty() && cur_keys_ == 0))
            return result;

            if (!finish_data_blocks(meta_blocks_out, current_offset, 0, &result)) return result;

        result.enabled         = true;
        result.total_key_count = total_keys_;
        result.bloom_data_offset = current_offset;
        result.bloom_meta_offset =
            current_offset + static_cast<int64_t>(meta_blocks_out.size());
        finish_meta_block(meta_blocks_out, result.last_block_offset, &result);
        return result;
    }

private:
    void init_chunk() {
        cur_chunk_.assign(chunk_byte_size_, 0);
        cur_chunk_max_keys_ = chunk_keys_;
    }

    void set_bits(std::span<const uint8_t> key) noexcept {
        if (cur_chunk_.empty()) return;
        int32_t hash1 = murmur_hash(key.data(), key.size(), 0);
        int32_t hash2 = murmur_hash(key.data(), key.size(), hash1);
        int32_t composite_hash = hash1;
        int32_t nbits = static_cast<int32_t>(cur_chunk_.size() * 8);
        for (int i = 0; i < hash_count_; ++i) {
            int32_t bit = composite_hash % nbits;
            if (bit < 0) bit = -bit;
            cur_chunk_[static_cast<size_t>(bit >> 3)] |= static_cast<uint8_t>(1u << (bit & 7));
            composite_hash += hash2;
        }
    }

    void compact_chunk() {
        if (cur_chunk_.empty() || cur_keys_ == 0) return;
        size_t pieces = 1;
        size_t new_byte_size = cur_chunk_.size();
        uint32_t new_max_keys = cur_chunk_max_keys_;
        while ((new_byte_size & 1u) == 0u && new_max_keys > (cur_keys_ << 1)) {
            pieces <<= 1;
            new_byte_size >>= 1;
            new_max_keys >>= 1;
        }
        if (pieces <= 1) return;

        for (size_t piece = 1; piece < pieces; ++piece) {
            size_t src_off = piece * new_byte_size;
            for (size_t pos = 0; pos < new_byte_size; ++pos) {
                cur_chunk_[pos] |= cur_chunk_[src_off + pos];
            }
        }
        cur_chunk_.resize(new_byte_size);
        cur_chunk_max_keys_ = new_max_keys;
    }

    static constexpr int kMaxFold = 7;
    static constexpr double kLog2Squared = 0.4804530139182014;

    static size_t compute_foldable_byte_size(long long bit_size, int fold_factor) {
        long long byte_size = (bit_size + 7) / 8;
        int mask = (1 << fold_factor) - 1;
        if ((byte_size & mask) != 0) {
            byte_size >>= fold_factor;
            ++byte_size;
            byte_size <<= fold_factor;
        }
        return static_cast<size_t>(byte_size);
    }

    static long long compute_ideal_max_keys(long long bit_size, double error_rate) {
        return static_cast<long long>(bit_size * (kLog2Squared / -std::log(error_rate)));
    }

    static long long compute_max_keys(long long bit_size, double error_rate, int hash_count) {
        return static_cast<long long>(
            -bit_size * 1.0 / hash_count *
            std::log(1 - std::exp(std::log(error_rate) / hash_count)));
    }

    static int compute_optimal_function_count(int max_keys, long long bit_size) {
        long long ratio = bit_size / std::max(1, max_keys);
        return std::max(1, static_cast<int>(std::ceil(std::log(2.0) * ratio)));
    }

    void write_bloom_chunk_block(std::vector<uint8_t>& out,
                                 const std::vector<uint8_t>& chunk_data,
                                 int64_t prev_block_offset,
                                 codec::Compressor* compressor = nullptr) {
        // Optionally compress the chunk data
        std::span<const uint8_t> payload{chunk_data.data(), chunk_data.size()};
        std::vector<uint8_t> compressed;
        if (compressor && compressor->type() != Compression::None) {
            compressed.resize(compressor->max_compressed_size(chunk_data.size()));
            size_t comp_len = compressor->compress(
                {chunk_data.data(), chunk_data.size()},
                compressed.data(), compressed.size());
            if (comp_len > 0) {
                compressed.resize(comp_len);
                payload = {compressed.data(), compressed.size()};
            }
        }

        size_t off = out.size();
        size_t on_disk_data_with_header = kBlockHeaderSize + payload.size();
        size_t n_chunks = (on_disk_data_with_header + kBytesPerChecksum - 1) / kBytesPerChecksum;
        std::vector<uint8_t> checksum_buf(n_chunks * 4);
        out.resize(off + kBlockHeaderSize + payload.size() + checksum_buf.size());
        uint8_t* p = out.data() + off;

        std::memcpy(p, kBloomChunkMagic.data(), 8); p += 8;
        write_be32(p, static_cast<uint32_t>(payload.size() + checksum_buf.size())); p += 4;
        write_be32(p, static_cast<uint32_t>(chunk_data.size()));  p += 4; // uncompressed original size
        write_be64(p, static_cast<uint64_t>(prev_block_offset));   p += 8;
        *p++ = kChecksumTypeCRC32C;
        write_be32(p, kBytesPerChecksum); p += 4;
        write_be32(p, static_cast<uint32_t>(on_disk_data_with_header)); p += 4;

        std::memcpy(p, payload.data(), payload.size());
        checksum::compute_hfile_checksums(
            out.data() + off, kBlockHeaderSize + payload.size(), kBytesPerChecksum, checksum_buf.data());
        std::memcpy(out.data() + off + kBlockHeaderSize + payload.size(),
                    checksum_buf.data(), checksum_buf.size());
    }

    void write_bloom_meta_block(std::vector<uint8_t>& out,
                                int64_t prev_block_offset,
                                BloomWriteResult* result,
                                codec::Compressor* compressor = nullptr) {
        // Bloom meta block format (HBase compatible):
        // version(4B BE) + totalByteSize(8B BE) + hashCount(4B BE) + hashType(4B BE)
        // + totalKeyCount(8B BE) + totalMaxKeys(8B BE) + numChunks(4B BE)
        // + comparatorClassName(bytes with writable-vint length prefix)
        // + bloom index root payload:
        //   [chunkOffset(8B BE) + chunkByteSize(4B BE) + firstKey(bytes with writable-vint length prefix)] * N

        // ── Step 1: serialize bloom meta data into temp buffer ────────────────
        uint8_t comparator_len_buf[10];
        const int comparator_len_size = encode_writable_vint(
            comparator_len_buf, static_cast<int64_t>(comparator_class_name_.size()));
        size_t index_payload_size = 0;
        for (const auto& first_key : chunk_first_keys_) {
            uint8_t key_len_buf[10];
            const int key_len_size = encode_writable_vint(
                key_len_buf, static_cast<int64_t>(first_key.size()));
            index_payload_size += 8 + 4 + static_cast<size_t>(key_len_size) + first_key.size();
        }
        size_t data_size = 4 + 8 + 4 + 4 + 8 + 8 + 4
                         + static_cast<size_t>(comparator_len_size)
                         + comparator_class_name_.size()
                         + index_payload_size;

        std::vector<uint8_t> raw_data(data_size);
        uint8_t* dp = raw_data.data();

        write_be32(dp, kBloomFormatVersion); dp += 4;
        uint64_t total_bytes = 0;
        for (uint32_t size : chunk_byte_sizes_) total_bytes += size;
        write_be64(dp, total_bytes); dp += 8;
        write_be32(dp, static_cast<uint32_t>(hash_count_)); dp += 4;
        write_be32(dp, static_cast<uint32_t>(kHashTypeMurmur3)); dp += 4;
        write_be64(dp, static_cast<uint64_t>(total_keys_)); dp += 8;
        uint64_t total_max_keys = 0;
        for (uint32_t c : chunk_max_keys_) total_max_keys += c;
        write_be64(dp, total_max_keys); dp += 8;
        write_be32(dp, static_cast<uint32_t>(chunk_offsets_.size())); dp += 4;
        std::memcpy(dp, comparator_len_buf, static_cast<size_t>(comparator_len_size));
        dp += comparator_len_size;
        if (!comparator_class_name_.empty()) {
            std::memcpy(dp, comparator_class_name_.data(), comparator_class_name_.size());
            dp += comparator_class_name_.size();
        }
        for (size_t i = 0; i < chunk_offsets_.size(); ++i) {
            uint8_t key_len_buf[10];
            const int key_len_size = encode_writable_vint(
                key_len_buf, static_cast<int64_t>(chunk_first_keys_[i].size()));
            write_be64(dp, static_cast<uint64_t>(chunk_offsets_[i])); dp += 8;
            write_be32(dp, chunk_on_disk_sizes_[i]); dp += 4;
            std::memcpy(dp, key_len_buf, static_cast<size_t>(key_len_size));
            dp += key_len_size;
            std::memcpy(dp, chunk_first_keys_[i].data(), chunk_first_keys_[i].size());
            dp += chunk_first_keys_[i].size();
        }

        // ── Step 2: optionally compress ───────────────────────────────────────
        std::span<const uint8_t> payload{raw_data.data(), raw_data.size()};
        std::vector<uint8_t> compressed;
        if (compressor && compressor->type() != Compression::None) {
            compressed.resize(compressor->max_compressed_size(data_size));
            size_t comp_len = compressor->compress(
                {raw_data.data(), raw_data.size()},
                compressed.data(), compressed.size());
            if (comp_len > 0) {
                compressed.resize(comp_len);
                payload = {compressed.data(), compressed.size()};
            }
        }

        // ── Step 3: write block header + payload + checksums ──────────────────
        size_t off = out.size();
        size_t on_disk_data_with_header = kBlockHeaderSize + payload.size();
        size_t n_chunks = (on_disk_data_with_header + kBytesPerChecksum - 1) / kBytesPerChecksum;
        std::vector<uint8_t> checksum_buf(n_chunks * 4);
        out.resize(off + kBlockHeaderSize + payload.size() + checksum_buf.size());
        uint8_t* p = out.data() + off;

        // Block header — BLMFMET2
        std::memcpy(p, kBloomMetaMagic.data(), 8); p += 8;
        write_be32(p, static_cast<uint32_t>(payload.size() + checksum_buf.size())); p += 4;
        write_be32(p, static_cast<uint32_t>(data_size)); p += 4;  // uncompressed original size
        write_be64(p, static_cast<uint64_t>(prev_block_offset)); p += 8;
        *p++ = kChecksumTypeCRC32C;
        write_be32(p, kBytesPerChecksum); p += 4;
        write_be32(p, static_cast<uint32_t>(on_disk_data_with_header)); p += 4;

        // Payload (compressed or raw)
        std::memcpy(p, payload.data(), payload.size());

        // Checksums
        checksum::compute_hfile_checksums(
            out.data() + off,
            kBlockHeaderSize + payload.size(),
            kBytesPerChecksum,
            checksum_buf.data());
        std::memcpy(out.data() + off + kBlockHeaderSize + payload.size(),
                    checksum_buf.data(), checksum_buf.size());
        if (result) {
            result->bloom_meta_uncompressed_bytes_with_headers =
                static_cast<uint64_t>(kBlockHeaderSize + raw_data.size());
        }
    }

    BloomType              type_;
    double                 error_rate_;
    int                    bits_per_key_{10};
    uint32_t               chunk_keys_{4096};
    int                    hash_count_{5};
    size_t                 chunk_byte_size_{kMaxChunkSize};
    uint32_t               cur_chunk_max_keys_{0};

    std::vector<uint8_t>                 cur_chunk_;
    std::vector<uint8_t>                 cur_first_key_;
    uint32_t                             cur_keys_{0};
    uint32_t                             total_keys_{0};
    std::string                          comparator_class_name_;
    std::vector<std::vector<uint8_t>>    chunks_;
    std::vector<std::vector<uint8_t>>    chunk_first_keys_;
    std::vector<uint32_t>                chunk_key_counts_;
    std::vector<uint32_t>                chunk_max_keys_;
    std::vector<uint32_t>                chunk_byte_sizes_;
    std::vector<uint32_t>                chunk_on_disk_sizes_;
    std::vector<int64_t>                 chunk_offsets_;
};

} // namespace bloom
} // namespace hfile
