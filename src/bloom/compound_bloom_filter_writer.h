#pragma once

#include <hfile/types.h>
#include <hfile/status.h>
#include "checksum/crc32c.h"
#include <vector>
#include <span>
#include <cstdint>
#include <cstring>
#include <cmath>

namespace hfile {
namespace bloom {

/// Murmur3 32-bit hash (fast, good distribution).
inline uint32_t murmur3_32(const uint8_t* data, size_t len, uint32_t seed = 0) noexcept {
    constexpr uint32_t c1 = 0xCC9E2D51u;
    constexpr uint32_t c2 = 0x1B873593u;
    uint32_t h = seed;
    size_t   nblocks = len / 4;

    for (size_t i = 0; i < nblocks; ++i) {
        uint32_t k;
        std::memcpy(&k, data + i * 4, 4);
        k *= c1; k = (k << 15) | (k >> 17); k *= c2;
        h ^= k;  h = (h << 13) | (h >> 19); h = h * 5 + 0xE6546B64u;
    }
    const uint8_t* tail = data + nblocks * 4;
    uint32_t k = 0;
    switch (len & 3) {
    case 3: k ^= static_cast<uint32_t>(tail[2]) << 16; [[fallthrough]];
    case 2: k ^= static_cast<uint32_t>(tail[1]) << 8;  [[fallthrough]];
    case 1: k ^= tail[0];
        k *= c1; k = (k << 15) | (k >> 17); k *= c2; h ^= k;
    }
    h ^= static_cast<uint32_t>(len);
    h ^= h >> 16; h *= 0x85EBCA6Bu; h ^= h >> 13; h *= 0xC2B2AE35u; h ^= h >> 16;
    return h;
}

struct BloomWriteResult {
    int64_t  bloom_meta_offset{-1};
    int64_t  bloom_data_offset{-1};
    uint32_t total_key_count{0};
    bool     enabled{false};
};

/// Compound (chunked) Bloom Filter writer compatible with HBase HFile v3.
/// Each bloom filter chunk corresponds to roughly one data block.
class CompoundBloomFilterWriter {
public:
    static constexpr int     kNumHashFunctions = 5;
    static constexpr int     kBloomFormatVersion = 3;
    static constexpr int     kHashTypeMurmur3 = 1;
    static constexpr double  kDefaultErrorRate  = 0.01;
    static constexpr size_t  kMaxChunkSize      = 128 * 1024;  // 128 KB per chunk

    CompoundBloomFilterWriter(BloomType type        = BloomType::Row,
                              double    error_rate   = kDefaultErrorRate,
                              size_t    expected_kvs = 1'000'000)
        : type_{type}, error_rate_{error_rate} {
        if (type == BloomType::None) return;
        // bits_per_key = -log(errorRate) / (ln2)^2
        double bpk   = -std::log(error_rate) / (0.693147 * 0.693147);
        bits_per_key_ = static_cast<int>(std::ceil(bpk));
        chunk_keys_  = static_cast<uint32_t>(
            std::min<size_t>(expected_kvs / 4 + 1, kMaxChunkSize * 8 / bits_per_key_));
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
        chunks_.push_back(std::move(cur_chunk_));
        chunk_key_counts_.push_back(cur_keys_);
        chunk_first_keys_.push_back(std::move(cur_first_key_));
        cur_keys_ = 0;
        init_chunk();
    }

    /// Flush pending chunk and serialise all BLMFBLK2 blocks into `blocks_out`.
    /// Records each chunk's absolute file offset (needed by the meta block).
    /// Call from the writer's non-scanned section, after all data blocks.
    /// Returns true if any bloom data was produced.
    bool finish_data_blocks(std::vector<uint8_t>& blocks_out,
                            int64_t current_file_offset) {
        if (type_ == BloomType::None) return false;
        if (cur_keys_ > 0) finish_chunk();
        if (chunks_.empty()) return false;

        chunk_offsets_.reserve(chunks_.size());
        for (size_t i = 0; i < chunks_.size(); ++i) {
            chunk_offsets_.push_back(
                current_file_offset + static_cast<int64_t>(blocks_out.size()));
            write_bloom_chunk_block(blocks_out, chunks_[i]);
        }
        return true;
    }

    /// Serialise the BLMFMET2 bloom meta block into `meta_out`.
    /// Must be called after finish_data_blocks().
    /// `meta_block_file_offset` is the absolute file offset where this block starts
    /// (used to build the meta root index entry pointing to BLMFMET2).
    void finish_meta_block(std::vector<uint8_t>& meta_out) {
        write_bloom_meta_block(meta_out);
    }

    bool      is_enabled()     const noexcept { return type_ != BloomType::None; }
    bool      has_data()       const noexcept { return !chunks_.empty() || cur_keys_ > 0; }
    uint32_t  total_keys()     const noexcept { return total_keys_; }
    BloomType bloom_type()     const noexcept { return type_; }

    /// Legacy combined API (chunk blocks + meta in one buffer).
    /// Retained for unit tests that only care about serialised content, not layout.
    BloomWriteResult finish(std::vector<uint8_t>& meta_blocks_out,
                            int64_t current_offset) {
        BloomWriteResult result;
        if (type_ == BloomType::None || (chunks_.empty() && cur_keys_ == 0))
            return result;

        if (!finish_data_blocks(meta_blocks_out, current_offset)) return result;

        result.enabled         = true;
        result.total_key_count = total_keys_;
        result.bloom_data_offset = current_offset;
        result.bloom_meta_offset =
            current_offset + static_cast<int64_t>(meta_blocks_out.size());
        finish_meta_block(meta_blocks_out);
        return result;
    }

private:
    void init_chunk() {
        size_t bits = static_cast<size_t>(chunk_keys_) * bits_per_key_;
        bits = (bits + 63) & ~63u;  // round up to 64
        cur_chunk_.assign((bits + 7) / 8, 0);
    }

    void set_bits(std::span<const uint8_t> key) noexcept {
        if (cur_chunk_.empty()) return;
        uint32_t h1 = murmur3_32(key.data(), key.size(), 0);
        uint32_t h2 = murmur3_32(key.data(), key.size(), h1);
        uint32_t nbits = static_cast<uint32_t>(cur_chunk_.size() * 8);
        for (int i = 0; i < kNumHashFunctions; ++i) {
            uint32_t bit = (h1 + static_cast<uint32_t>(i) * h2) % nbits;
            cur_chunk_[bit >> 3] |= 1u << (bit & 7);
        }
    }

    void write_bloom_chunk_block(std::vector<uint8_t>& out,
                                 const std::vector<uint8_t>& chunk_data) {
        // HFile block header (33 bytes) + chunk_data
        // Magic: BLMFBLK2
        size_t off = out.size();
        size_t on_disk_data_with_header = kBlockHeaderSize + chunk_data.size();
        size_t n_chunks = (on_disk_data_with_header + kBytesPerChecksum - 1) / kBytesPerChecksum;
        std::vector<uint8_t> checksum_buf(n_chunks * 4);
        out.resize(off + kBlockHeaderSize + chunk_data.size() + checksum_buf.size());
        uint8_t* p = out.data() + off;

        std::memcpy(p, kBloomChunkMagic.data(), 8); p += 8;
        write_be32(p, static_cast<uint32_t>(chunk_data.size() + checksum_buf.size())); p += 4;
        write_be32(p, static_cast<uint32_t>(chunk_data.size()));  p += 4; // uncompSz
        write_be64(p, 0);                                          p += 8; // prevOffset
        *p++ = kChecksumTypeCRC32C;
        write_be32(p, kBytesPerChecksum); p += 4;
        write_be32(p, static_cast<uint32_t>(on_disk_data_with_header)); p += 4;

        std::memcpy(p, chunk_data.data(), chunk_data.size());
        checksum::compute_hfile_checksums(
            out.data() + off, kBlockHeaderSize + chunk_data.size(), kBytesPerChecksum, checksum_buf.data());
        std::memcpy(out.data() + off + kBlockHeaderSize + chunk_data.size(),
                    checksum_buf.data(), checksum_buf.size());
    }

    void write_bloom_meta_block(std::vector<uint8_t>& out) {
        // Bloom meta block format (HBase compatible):
        // version(4B BE) + totalByteSize(8B BE) + hashCount(4B BE) + hashType(4B BE)
        // + totalKeyCount(8B BE) + totalMaxKeys(8B BE) + numChunks(4B BE)
        // + comparatorClassName(bytes with writable-vint length prefix)
        // + bloom index root payload:
        //   entryCount(4B BE) + secondaryIndexOffsets((N+1)*4B BE)
        //   + [chunkOffset(8B BE) + chunkByteSize(4B BE) + firstKey] * N
        size_t off = out.size();
        std::vector<uint32_t> secondary_offsets;
        secondary_offsets.reserve(chunks_.size() + 1);
        uint32_t entry_offset = 0;
        secondary_offsets.push_back(entry_offset);
        for (size_t i = 0; i < chunks_.size(); ++i) {
            entry_offset += static_cast<uint32_t>(8 + 4 + chunk_first_keys_[i].size());
            secondary_offsets.push_back(entry_offset);
        }
        uint8_t comparator_len_buf[10];
        const int comparator_len_size = encode_writable_vint(comparator_len_buf, 0);
        const size_t index_payload_size = 4 + secondary_offsets.size() * 4 + entry_offset;
        size_t data_size = 4 + 8 + 4 + 4 + 8 + 8 + 4
                         + static_cast<size_t>(comparator_len_size)
                         + index_payload_size;
        size_t on_disk_data_with_header = kBlockHeaderSize + data_size;
        size_t n_chunks = (on_disk_data_with_header + kBytesPerChecksum - 1) / kBytesPerChecksum;
        std::vector<uint8_t> checksum_buf(n_chunks * 4);
        out.resize(off + kBlockHeaderSize + data_size + checksum_buf.size());
        uint8_t* p = out.data() + off;

        // Block header — BLMFMET2
        std::memcpy(p, kBloomMetaMagic.data(), 8); p += 8;
        write_be32(p, static_cast<uint32_t>(data_size + checksum_buf.size())); p += 4;
        write_be32(p, static_cast<uint32_t>(data_size)); p += 4;
        write_be64(p, 0);                                 p += 8;
        *p++ = kChecksumTypeCRC32C;
        write_be32(p, kBytesPerChecksum); p += 4;
        write_be32(p, static_cast<uint32_t>(on_disk_data_with_header)); p += 4;

        // Bloom meta data
        write_be32(p, kBloomFormatVersion); p += 4;

        uint64_t total_bytes = 0;
        for (const auto& c : chunks_) total_bytes += c.size();
        write_be64(p, total_bytes); p += 8;

        write_be32(p, static_cast<uint32_t>(kNumHashFunctions)); p += 4;
        write_be32(p, static_cast<uint32_t>(kHashTypeMurmur3)); p += 4;
        write_be64(p, static_cast<uint64_t>(total_keys_)); p += 8;
        uint64_t total_max_keys = 0;
        for (uint32_t c : chunk_key_counts_) total_max_keys += c;
        write_be64(p, total_max_keys); p += 8;
        write_be32(p, static_cast<uint32_t>(chunks_.size())); p += 4;
        std::memcpy(p, comparator_len_buf, static_cast<size_t>(comparator_len_size));
        p += comparator_len_size;

        write_be32(p, static_cast<uint32_t>(chunks_.size())); p += 4;
        for (uint32_t mark : secondary_offsets) {
            write_be32(p, mark);
            p += 4;
        }
        for (size_t i = 0; i < chunks_.size(); ++i) {
            write_be64(p, static_cast<uint64_t>(chunk_offsets_[i])); p += 8;
            write_be32(p, static_cast<uint32_t>(chunks_[i].size())); p += 4;
            std::memcpy(p, chunk_first_keys_[i].data(), chunk_first_keys_[i].size());
            p += chunk_first_keys_[i].size();
        }
        checksum::compute_hfile_checksums(
            out.data() + off,
            kBlockHeaderSize + data_size,
            kBytesPerChecksum,
            checksum_buf.data());
        std::memcpy(out.data() + off + kBlockHeaderSize + data_size,
                    checksum_buf.data(), checksum_buf.size());
    }

    BloomType              type_;
    double                 error_rate_;
    int                    bits_per_key_{10};
    uint32_t               chunk_keys_{4096};

    std::vector<uint8_t>                 cur_chunk_;
    std::vector<uint8_t>                 cur_first_key_;
    uint32_t                             cur_keys_{0};
    uint32_t                             total_keys_{0};
    std::vector<std::vector<uint8_t>>    chunks_;
    std::vector<std::vector<uint8_t>>    chunk_first_keys_;
    std::vector<uint32_t>                chunk_key_counts_;
    std::vector<int64_t>                 chunk_offsets_;
};

} // namespace bloom
} // namespace hfile
