#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <span>
#include <array>
#include <vector>
#include <bit>

namespace hfile {

// ─── Byte-order helpers (HFile uses Big-Endian for all multi-byte integers) ──

inline void write_be16(uint8_t* dst, uint16_t v) noexcept {
    dst[0] = static_cast<uint8_t>(v >> 8);
    dst[1] = static_cast<uint8_t>(v);
}
inline void write_be32(uint8_t* dst, uint32_t v) noexcept {
    dst[0] = static_cast<uint8_t>(v >> 24);
    dst[1] = static_cast<uint8_t>(v >> 16);
    dst[2] = static_cast<uint8_t>(v >>  8);
    dst[3] = static_cast<uint8_t>(v);
}
inline void write_be64(uint8_t* dst, uint64_t v) noexcept {
    dst[0] = static_cast<uint8_t>(v >> 56);
    dst[1] = static_cast<uint8_t>(v >> 48);
    dst[2] = static_cast<uint8_t>(v >> 40);
    dst[3] = static_cast<uint8_t>(v >> 32);
    dst[4] = static_cast<uint8_t>(v >> 24);
    dst[5] = static_cast<uint8_t>(v >> 16);
    dst[6] = static_cast<uint8_t>(v >>  8);
    dst[7] = static_cast<uint8_t>(v);
}

inline uint16_t read_be16(const uint8_t* src) noexcept {
    return (static_cast<uint16_t>(src[0]) << 8) | src[1];
}
inline uint32_t read_be32(const uint8_t* src) noexcept {
    return (static_cast<uint32_t>(src[0]) << 24) |
           (static_cast<uint32_t>(src[1]) << 16) |
           (static_cast<uint32_t>(src[2]) <<  8) |
            static_cast<uint32_t>(src[3]);
}
inline uint64_t read_be64(const uint8_t* src) noexcept {
    return (static_cast<uint64_t>(src[0]) << 56) |
           (static_cast<uint64_t>(src[1]) << 48) |
           (static_cast<uint64_t>(src[2]) << 40) |
           (static_cast<uint64_t>(src[3]) << 32) |
           (static_cast<uint64_t>(src[4]) << 24) |
           (static_cast<uint64_t>(src[5]) << 16) |
           (static_cast<uint64_t>(src[6]) <<  8) |
            static_cast<uint64_t>(src[7]);
}

// ─── VarInt encoding (used for MVCC/MemstoreTS) ──────────────────────────────

/// Returns number of bytes written (1..10)
inline int encode_varint64(uint8_t* dst, uint64_t v) noexcept {
    int n = 0;
    while (v > 0x7F) {
        dst[n++] = static_cast<uint8_t>((v & 0x7F) | 0x80);
        v >>= 7;
    }
    dst[n++] = static_cast<uint8_t>(v);
    return n;
}

/// Decode a base-128 varint from `src`.
/// Returns the number of bytes consumed (1–10), or -1 on malformed input
/// (more than 10 continuation bytes — a uint64_t can hold at most 10 × 7 bits).
/// On error `out` is set to the partially decoded value.
inline int decode_varint64(const uint8_t* src, uint64_t& out) noexcept {
    uint64_t result = 0;
    int      shift  = 0;
    int      n      = 0;
    // A valid uint64_t varint is at most 10 bytes (ceil(64/7) = 10).
    // Byte 10 may only have bits 0–1 set (64 − 9×7 = 1 valid bit + sign).
    // Any byte beyond 10 with the continuation bit set is malformed.
    static constexpr int kMaxBytes = 10;
    while (n < kMaxBytes) {
        uint8_t b = src[n++];
        result |= static_cast<uint64_t>(b & 0x7F) << shift;
        if (!(b & 0x80)) {
            out = result;
            return n;     // normal exit
        }
        shift += 7;
    }
    // Consumed 10 bytes but the MSB of byte 10 was still set → malformed.
    out = result;
    return -1;
}

inline int writable_vint_size(int64_t v) noexcept {
    if (v >= -112 && v <= 127)
        return 1;
    int size = 1;
    uint64_t x;
    if (v < 0) {
        x = static_cast<uint64_t>(~v);
    } else {
        x = static_cast<uint64_t>(v);
    }
    while (x != 0) {
        x >>= 8;
        ++size;
    }
    return size;
}

inline int encode_writable_vint(uint8_t* dst, int64_t v) noexcept {
    if (v >= -112 && v <= 127) {
        dst[0] = static_cast<uint8_t>(static_cast<int8_t>(v));
        return 1;
    }

    int8_t prefix = -112;
    uint64_t x;
    if (v < 0) {
        x = static_cast<uint64_t>(~v);
        prefix = -120;
    } else {
        x = static_cast<uint64_t>(v);
    }

    uint64_t tmp = x;
    while (tmp != 0) {
        tmp >>= 8;
        --prefix;
    }

    int total = 1 + (prefix < -120 ? -(prefix + 120) : -(prefix + 112));
    dst[0] = static_cast<uint8_t>(prefix);
    for (int i = 1; i < total; ++i) {
        int shift = (total - i - 1) * 8;
        dst[i] = static_cast<uint8_t>((x >> shift) & 0xFFu);
    }
    return total;
}

inline int decode_writable_vint(const uint8_t* src, int64_t& out) noexcept {
    int8_t first = static_cast<int8_t>(src[0]);
    if (first >= -112) {
        out = first;
        return 1;
    }

    int total = first < -120 ? -119 - first : -111 - first;
    uint64_t value = 0;
    for (int i = 1; i < total; ++i) {
        value = (value << 8) | static_cast<uint64_t>(src[i]);
    }
    if (first < -120)
        value = ~value;
    out = static_cast<int64_t>(value);
    return total;
}

// ─── HFile v3 Block-Type Magic Strings ───────────────────────────────────────

inline constexpr size_t kBlockMagicSize = 8;

inline constexpr std::array<uint8_t, 8> kDataBlockMagic   = {'D','A','T','A','B','L','K','*'};
inline constexpr std::array<uint8_t, 8> kEncodedDataBlockMagic = {'D','A','T','A','B','L','K','E'};
inline constexpr std::array<uint8_t, 8> kLeafIndexMagic   = {'I','D','X','L','E','A','F','2'};
inline constexpr std::array<uint8_t, 8> kRootIndexMagic   = {'I','D','X','R','O','O','T','2'};
inline constexpr std::array<uint8_t, 8> kIntermedIdxMagic = {'I','D','X','I','N','T','E','2'};
inline constexpr std::array<uint8_t, 8> kMetaBlockMagic   = {'M','E','T','A','B','L','K','c'};
inline constexpr std::array<uint8_t, 8> kFileInfoMagic    = {'F','I','L','E','I','N','F','2'};
inline constexpr std::array<uint8_t, 8> kBloomChunkMagic  = {'B','L','M','F','B','L','K','2'};
inline constexpr std::array<uint8_t, 8> kBloomMetaMagic   = {'B','L','M','F','M','E','T','2'};
inline constexpr std::array<uint8_t, 8> kTrailerBlockMagic = {'T','R','A','B','L','K','"','$'};

// ─── Block Header size ────────────────────────────────────────────────────────
// 8 (magic) + 4 (compSz) + 4 (uncompSz) + 8 (prevBlockOffset)
// + 1 (checksumType) + 4 (bytesPerChecksum) + 4 (onDiskDataSize)
inline constexpr size_t kBlockHeaderSize = 33;

// ─── Checksum ─────────────────────────────────────────────────────────────────
inline constexpr uint8_t  kChecksumTypeCRC32C = 2;
inline constexpr uint32_t kBytesPerChecksum   = 16 * 1024;

// ─── HFile versions ──────────────────────────────────────────────────────────
inline constexpr uint32_t kHFileMajorVersion = 3;
inline constexpr uint32_t kHFileMinorVersion = 3;

inline constexpr size_t kTrailerVersionSize = 4;
inline constexpr size_t kTrailerFixedSize = 4096;

// ─── Default block size ──────────────────────────────────────────────────────
inline constexpr size_t kDefaultBlockSize = 64 * 1024;  // 64 KB

// ─── Key Type codes ──────────────────────────────────────────────────────────
enum class KeyType : uint8_t {
    Put              = 4,
    Delete           = 8,
    DeleteColumn     = 12,
    DeleteFamily     = 14,
    Maximum          = 255,
};

// ─── Compression codec IDs (matching HBase enum) ─────────────────────────────
enum class Compression : uint32_t {
    None   = 0,
    LZ4    = 4,
    Snappy = 2,
    Zstd   = 7,
    GZip   = 1,
};

// ─── Data block encoding types ───────────────────────────────────────────────
enum class Encoding : uint8_t {
    None     = 0,
    Prefix   = 1,
    Diff     = 2,
    FastDiff = 3,
};

// ─── Bloom filter types ───────────────────────────────────────────────────────
enum class BloomType : uint8_t {
    None   = 0,
    Row    = 1,
    RowCol = 2,
};

// ─── FileInfo key constants ───────────────────────────────────────────────────
namespace fileinfo {
inline constexpr std::string_view kLastKey           = "hfile.LASTKEY";
inline constexpr std::string_view kAvgKeyLen         = "hfile.AVG_KEY_LEN";
inline constexpr std::string_view kAvgValueLen       = "hfile.AVG_VALUE_LEN";
inline constexpr std::string_view kCreateTimeTs      = "hfile.CREATE_TIME_TS";
inline constexpr std::string_view kTagsCompressed    = "hfile.TAGS_COMPRESSED";
inline constexpr std::string_view kKeyOfBiggestCell  = "hfile.KEY_OF_BIGGEST_CELL";
inline constexpr std::string_view kMaxTagsLen        = "hfile.MAX_TAGS_LEN";
inline constexpr std::string_view kLenOfBiggestCell  = "hfile.LEN_OF_BIGGEST_CELL";
inline constexpr std::string_view kKeyValueVersion   = "KEY_VALUE_VERSION";
inline constexpr std::string_view kMaxMemstoreTsKey  = "MAX_MEMSTORE_TS_KEY";
inline constexpr std::string_view kComparator        = "hfile.COMPARATOR";
inline constexpr std::string_view kDataBlockEncoding = "DATA_BLOCK_ENCODING";
inline constexpr std::string_view kBloomFilterType   = "BLOOM_FILTER_TYPE";
inline constexpr std::string_view kLastBloomKey      = "LAST_BLOOM_KEY";
inline constexpr std::string_view kDeleteFamilyCount = "DELETE_FAMILY_COUNT";
inline constexpr std::string_view kHistorical        = "HISTORICAL";
} // namespace fileinfo

// ─── Comparator class names ───────────────────────────────────────────────────
inline constexpr std::string_view kCellComparator =
    "org.apache.hadoop.hbase.InnerStoreCellComparator";
inline constexpr std::string_view kCellComparatorImpl =
    "org.apache.hadoop.hbase.CellComparatorImpl";
inline constexpr std::string_view kMetaCellComparator =
    "org.apache.hadoop.hbase.MetaCellComparator";
inline constexpr std::string_view kLegacyKeyValueComparator =
    "org.apache.hadoop.hbase.KeyValue$KVComparator";
inline constexpr std::string_view kLegacyMetaKeyValueComparator =
    "org.apache.hadoop.hbase.KeyValue$MetaComparator";

// ─── KeyValue structure ───────────────────────────────────────────────────────
/// Non-owning view of a single HBase KeyValue.
/// All spans must remain valid for the lifetime of this struct.
struct KeyValue {
    std::span<const uint8_t> row;
    std::span<const uint8_t> family;
    std::span<const uint8_t> qualifier;
    int64_t                  timestamp{0};
    KeyType                  key_type{KeyType::Put};
    std::span<const uint8_t> value;
    // Tags (usually empty in Bulk Load)
    std::span<const uint8_t> tags{};
    // MVCC / MemstoreTS (always 0 for Bulk Load)
    uint64_t                 memstore_ts{0};
    bool                     has_memstore_ts{false};

    /// Total serialized key length (HBase internal key format)
    uint32_t key_length() const noexcept {
        return static_cast<uint32_t>(
            2 + row.size() + 1 + family.size() + qualifier.size() + 8 + 1);
    }

    /// Total serialized cell length on disk (v3 with tags + MVCC)
    size_t encoded_size() const noexcept {
        return 4 + 4                          // keyLen + valueLen
             + key_length()                   // key
             + value.size()                   // value
             + 2 + tags.size()                // tagsLen + tags  (v3)
             + (has_memstore_ts
                    ? static_cast<size_t>(writable_vint_size(static_cast<int64_t>(memstore_ts)))
                    : 0);                     // optional MVCC vint
    }
};

/// Owning version of KeyValue for sort buffers
struct OwnedKeyValue {
    std::vector<uint8_t> row;
    std::vector<uint8_t> family;
    std::vector<uint8_t> qualifier;
    int64_t              timestamp{0};
    KeyType              key_type{KeyType::Put};
    std::vector<uint8_t> value;
    std::vector<uint8_t> tags;
    uint64_t             memstore_ts{0};
    bool                 has_memstore_ts{false};

    KeyValue as_view() const noexcept {
        return KeyValue{
            .row        = {row.data(),       row.size()},
            .family     = {family.data(),    family.size()},
            .qualifier  = {qualifier.data(), qualifier.size()},
            .timestamp  = timestamp,
            .key_type   = key_type,
            .value      = {value.data(),     value.size()},
            .tags       = {tags.data(),      tags.size()},
            .memstore_ts = memstore_ts,
            .has_memstore_ts = has_memstore_ts,
        };
    }
};

// ─── HBase Key comparison (Row ASC → Family ASC → Qualifier ASC
//                           → Timestamp DESC → Type DESC) ────────────────────
inline int compare_keys(const KeyValue& a, const KeyValue& b) noexcept {
    // Row
    int r = std::memcmp(a.row.data(), b.row.data(),
                        std::min(a.row.size(), b.row.size()));
    if (r != 0) return r;
    if (a.row.size() != b.row.size())
        return a.row.size() < b.row.size() ? -1 : 1;

    // Family
    r = std::memcmp(a.family.data(), b.family.data(),
                    std::min(a.family.size(), b.family.size()));
    if (r != 0) return r;
    if (a.family.size() != b.family.size())
        return a.family.size() < b.family.size() ? -1 : 1;

    // Qualifier
    r = std::memcmp(a.qualifier.data(), b.qualifier.data(),
                    std::min(a.qualifier.size(), b.qualifier.size()));
    if (r != 0) return r;
    if (a.qualifier.size() != b.qualifier.size())
        return a.qualifier.size() < b.qualifier.size() ? -1 : 1;

    // Timestamp (descending)
    if (a.timestamp != b.timestamp)
        return a.timestamp > b.timestamp ? -1 : 1;

    // Type (descending)
    if (a.key_type != b.key_type)
        return static_cast<uint8_t>(a.key_type) > static_cast<uint8_t>(b.key_type) ? -1 : 1;

    return 0;
}

} // namespace hfile
