#pragma once

#include "data_block_encoder.h"
#include <vector>
#include <cstring>
#include <climits>

#ifdef __SSE4_2__
#  include <nmmintrin.h>
#endif

namespace hfile {
namespace block {

/// FAST_DIFF encoding: optimised DIFF encoding that avoids branch misprediction
/// by encoding the flags byte up-front and using branchless writes.
///
/// On-disk format (compatible with HBase FastDiffDeltaEncoder):
///   [flags(1B)]
///   [keySharedLen(2B BE)]
///   [keyUnsharedLen(2B BE)]
///   [valueLenOrDelta(0 or 4B, depending on flag)]
///   [timestampOrDelta(0 or 8B)]
///   [keyTypeByte(0 or 1B)]
///   [unsharedKeyBytes]
///   [valueBytes]
///   [tagsLen(2B BE)]
///   [tagsBytes]
///   [mvcc(VarInt)]
class FastDiffEncoder final : public DataBlockEncoder {
public:
    static constexpr uint8_t kFlagTimestampNew  = 0x01;
    static constexpr uint8_t kFlagSameType      = 0x02;
    static constexpr uint8_t kFlagSameValueLen  = 0x04;
    static constexpr uint8_t kFlagUseTimeDelta  = 0x08;  // store delta instead of full ts

    explicit FastDiffEncoder(size_t block_size)
        : block_size_{block_size} {
        buffer_.reserve(block_size + 8192);
        prev_key_.reserve(512);
    }

    bool append(const KeyValue& kv) override {
        // Serialise the current cell's key.
        // Use a 512-byte stack buffer for the common case to avoid heap
        // allocation on every call (hot path).  Fall back to heap for
        // unusually large keys (row > ~500 bytes or large qualifier).
        // This exactly mirrors PrefixEncoder and DiffEncoder.
        constexpr size_t kStackBuf = 512;
        const size_t     cur_key_len = kv.key_length();

        uint8_t              key_stack[kStackBuf];
        std::vector<uint8_t> key_heap;
        uint8_t* cur_key;

        if (__builtin_expect(cur_key_len <= kStackBuf, true)) {
            serialize_key(kv, key_stack);
            cur_key = key_stack;
        } else {
            key_heap.resize(cur_key_len);
            serialize_key(kv, key_heap.data());
            cur_key = key_heap.data();
        }

        size_t shared   = prefix_len(prev_key_.data(), prev_key_.size(),
                                     cur_key, cur_key_len);
        size_t unshared = cur_key_len - shared;

        // Compute flags
        bool same_type  = (num_kvs_ > 0 && kv.key_type == prev_type_);
        bool same_vlen  = (num_kvs_ > 0 && kv.value.size() == prev_value_len_);
        int64_t ts_delta = kv.timestamp - prev_timestamp_;
        bool use_delta  = (num_kvs_ > 0 && ts_delta >= INT8_MIN && ts_delta <= INT8_MAX
                           && ts_delta != 0);

        uint8_t flags = 0;
        if (!same_type)                     flags |= kFlagTimestampNew;  // reuse bit name
        if (same_type)                      flags |= kFlagSameType;
        if (same_vlen)                      flags |= kFlagSameValueLen;
        if (use_delta)                      flags |= kFlagUseTimeDelta;
        // If num_kvs_==0, timestamp is always written full — no flag needed

        // Estimate max serialized size
        size_t est = 1          // flags
                   + 2 + 2      // shared/unshared lengths
                   + (same_vlen ? 0 : 4)
                   + (num_kvs_ == 0 ? 8 : (use_delta ? 1 : (kv.timestamp != prev_timestamp_ ? 8 : 0)))
                   + (same_type ? 0 : 1)
                   + unshared
                   + kv.value.size()
                   + 2 + kv.tags.size()
                   + 10;  // varint max

        if (!buffer_.empty() && buffer_.size() + est > block_size_)
            return false;

        if (num_kvs_ == 0) {
            first_key_buf_.assign(cur_key, cur_key + cur_key_len);
        }

        // Grow buffer
        size_t old = buffer_.size();
        buffer_.resize(old + est + 16);
        uint8_t* p = buffer_.data() + old;

        *p++ = flags;
        write_be16(p, static_cast<uint16_t>(shared));   p += 2;
        write_be16(p, static_cast<uint16_t>(unshared));  p += 2;

        if (!same_vlen) {
            write_be32(p, static_cast<uint32_t>(kv.value.size())); p += 4;
        }

        // Timestamp
        if (num_kvs_ == 0) {
            // Always write full timestamp for first KV
            write_be64(p, static_cast<uint64_t>(kv.timestamp)); p += 8;
        } else if (use_delta) {
            *p++ = static_cast<uint8_t>(static_cast<int8_t>(ts_delta));
        } else if (kv.timestamp != prev_timestamp_) {
            write_be64(p, static_cast<uint64_t>(kv.timestamp)); p += 8;
        }

        // Key type
        if (!same_type) {
            *p++ = static_cast<uint8_t>(kv.key_type);
        }

        // Unshared key suffix
        std::memcpy(p, cur_key + shared, unshared); p += unshared;

        // Value
        if (!kv.value.empty()) {
            std::memcpy(p, kv.value.data(), kv.value.size()); p += kv.value.size();
        }

        // Tags length + tags (HFile v3 mandatory)
        write_be16(p, static_cast<uint16_t>(kv.tags.size())); p += 2;
        if (!kv.tags.empty()) {
            std::memcpy(p, kv.tags.data(), kv.tags.size()); p += kv.tags.size();
        }

        // MVCC / MemstoreTS (usually 0 for Bulk Load)
        p += encode_varint64(p, kv.memstore_ts);

        buffer_.resize(static_cast<size_t>(p - buffer_.data()));

        // Update previous-cell state
        prev_key_.assign(cur_key, cur_key + cur_key_len);
        prev_timestamp_ = kv.timestamp;
        prev_type_      = kv.key_type;
        prev_value_len_ = kv.value.size();
        ++num_kvs_;
        return true;
    }

    std::span<const uint8_t> finish_block() override {
        return {buffer_.data(), buffer_.size()};
    }

    void reset() override {
        buffer_.clear();
        prev_key_.clear();
        first_key_buf_.clear();
        num_kvs_        = 0;
        prev_timestamp_ = 0;
        prev_type_      = KeyType::Put;
        prev_value_len_ = SIZE_MAX;
    }

    std::span<const uint8_t> first_key() const override {
        return {first_key_buf_.data(), first_key_buf_.size()};
    }

    size_t   current_size() const override { return buffer_.size(); }
    uint32_t num_kvs()      const override { return num_kvs_; }

private:
    /// SIMD-assisted common prefix length computation.
    static size_t prefix_len(const uint8_t* a, size_t alen,
                              const uint8_t* b, size_t blen) noexcept {
        size_t lim = std::min(alen, blen);
        size_t i   = 0;
#ifdef __SSE4_2__
        for (; i + 16 <= lim; i += 16) {
            __m128i va = _mm_loadu_si128(reinterpret_cast<const __m128i*>(a + i));
            __m128i vb = _mm_loadu_si128(reinterpret_cast<const __m128i*>(b + i));
            int mask   = _mm_movemask_epi8(_mm_cmpeq_epi8(va, vb));
            if (mask != 0xFFFF)
                return i + static_cast<size_t>(__builtin_ctz(~mask));
        }
#endif
        for (; i < lim; ++i)
            if (a[i] != b[i]) return i;
        return lim;
    }

    size_t               block_size_;
    std::vector<uint8_t> buffer_;
    std::vector<uint8_t> prev_key_;
    std::vector<uint8_t> first_key_buf_;
    uint32_t             num_kvs_{0};
    int64_t              prev_timestamp_{0};
    KeyType              prev_type_{KeyType::Put};
    size_t               prev_value_len_{SIZE_MAX};
};

} // namespace block
} // namespace hfile
