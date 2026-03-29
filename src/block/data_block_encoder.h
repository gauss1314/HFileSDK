#pragma once

#include <hfile/types.h>
#include <hfile/status.h>
#include <span>
#include <vector>
#include <memory>
#include <cstdint>

namespace hfile {
namespace block {

/// Serializes a KeyValue into the raw HFile v3 wire format.
/// Returns number of bytes written.
inline size_t serialize_kv(const KeyValue& kv, uint8_t* dst) noexcept {
    uint8_t* p = dst;

    uint32_t key_len   = kv.key_length();
    uint32_t value_len = static_cast<uint32_t>(kv.value.size());

    write_be32(p, key_len);   p += 4;
    write_be32(p, value_len); p += 4;

    // Row
    write_be16(p, static_cast<uint16_t>(kv.row.size())); p += 2;
    std::memcpy(p, kv.row.data(), kv.row.size());        p += kv.row.size();

    // Family
    *p++ = static_cast<uint8_t>(kv.family.size());
    std::memcpy(p, kv.family.data(), kv.family.size()); p += kv.family.size();

    // Qualifier
    std::memcpy(p, kv.qualifier.data(), kv.qualifier.size()); p += kv.qualifier.size();

    // Timestamp (8B big-endian)
    write_be64(p, static_cast<uint64_t>(kv.timestamp)); p += 8;

    // Key type (1B)
    *p++ = static_cast<uint8_t>(kv.key_type);

    // Value
    std::memcpy(p, kv.value.data(), kv.value.size()); p += kv.value.size();

    // HFile v3: Tags Length (2B) + Tags
    write_be16(p, static_cast<uint16_t>(kv.tags.size())); p += 2;
    if (!kv.tags.empty()) {
        std::memcpy(p, kv.tags.data(), kv.tags.size()); p += kv.tags.size();
    }

    if (kv.has_memstore_ts)
        p += encode_writable_vint(p, static_cast<int64_t>(kv.memstore_ts));

    return static_cast<size_t>(p - dst);
}

/// Serialize only the HBase "internal key" portion of a KeyValue.
/// Format: rowLen(2) + row + familyLen(1) + family + qualifier + timestamp(8) + keyType(1)
inline size_t serialize_key(const KeyValue& kv, uint8_t* dst) noexcept {
    uint8_t* p = dst;
    write_be16(p, static_cast<uint16_t>(kv.row.size())); p += 2;
    std::memcpy(p, kv.row.data(), kv.row.size());        p += kv.row.size();
    *p++ = static_cast<uint8_t>(kv.family.size());
    std::memcpy(p, kv.family.data(), kv.family.size()); p += kv.family.size();
    std::memcpy(p, kv.qualifier.data(), kv.qualifier.size()); p += kv.qualifier.size();
    write_be64(p, static_cast<uint64_t>(kv.timestamp)); p += 8;
    *p++ = static_cast<uint8_t>(kv.key_type);
    return static_cast<size_t>(p - dst);
}

// ─── Abstract encoder ─────────────────────────────────────────────────────────

class DataBlockEncoder {
public:
    virtual ~DataBlockEncoder() = default;

    /// Append a KeyValue to the current block.
    /// Returns false if the block is full (caller should call finish_block first).
    virtual bool append(const KeyValue& kv) = 0;

    /// Finalise the current block and return its raw (uncompressed) bytes.
    virtual std::span<const uint8_t> finish_block() = 0;

    /// Reset for the next block.
    virtual void reset() = 0;

    /// First key in the current block (for index building).
    virtual std::span<const uint8_t> first_key() const = 0;

    /// Current uncompressed block size estimate.
    virtual size_t current_size() const = 0;

    /// Number of KVs in the current block.
    virtual uint32_t num_kvs() const = 0;

    /// Whether the current block is empty.
    bool empty() const { return num_kvs() == 0; }

    static std::unique_ptr<DataBlockEncoder> create(Encoding enc, size_t block_size);
};

} // namespace block
} // namespace hfile
