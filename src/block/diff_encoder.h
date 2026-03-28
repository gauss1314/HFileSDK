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

/// DIFF encoding: builds on PREFIX encoding and further encodes the
/// timestamp and key-type as deltas from the previous cell.
///
/// Per-KV flags byte:
///   bit 0 : timestamp is non-zero / different from prev (full 8B follows)
///   bit 1 : key-type is same as prev (no type byte follows)
///   bit 2 : value length is same as prev (no vlen field follows)
class DiffEncoder final : public DataBlockEncoder {
public:
    explicit DiffEncoder(size_t block_size)
        : block_size_{block_size} {
        buffer_.reserve(block_size + 8192);
        prev_key_.reserve(512);
    }

    bool append(const KeyValue& kv) override {
        // Serialise the current key — stack buffer avoids heap allocation on
        // every call (hot path).
        constexpr size_t kStackBuf = 512;
        const size_t     key_len   = kv.key_length();

        uint8_t              key_stack[kStackBuf];
        std::vector<uint8_t> key_heap;
        uint8_t* cur_key;

        if (__builtin_expect(key_len <= kStackBuf, true)) {
            serialize_key(kv, key_stack);
            cur_key = key_stack;
        } else {
            key_heap.resize(key_len);
            serialize_key(kv, key_heap.data());
            cur_key = key_heap.data();
        }

        const size_t shared   = prefix_len(prev_key_.data(), prev_key_.size(),
                                            cur_key, key_len);
        const size_t unshared = key_len - shared;

        // Compute flags
        const bool    same_type  = (prev_num_kvs_ > 0 && kv.key_type == prev_type_);
        const bool    same_vlen  = (prev_num_kvs_ > 0 && kv.value.size() == prev_value_len_);
        const int64_t ts_delta   = kv.timestamp - prev_timestamp_;

        uint8_t flags = 0;
        if (ts_delta != 0) flags |= 0x01;
        if (same_type)     flags |= 0x02;
        if (same_vlen)     flags |= 0x04;

        const size_t est = 1 + 2 + 2
                         + (same_vlen ? 0 : 4)
                         + ((flags & 0x01) ? 8 : 0)
                         + (same_type ? 0 : 1)
                         + unshared + kv.value.size()
                         + 2 + kv.tags.size() + 10;

        if (!buffer_.empty() && buffer_.size() + est > block_size_)
            return false;

        if (num_kvs_ == 0)
            first_key_buf_.assign(cur_key, cur_key + key_len);

        const size_t old = buffer_.size();
        buffer_.resize(old + est + 16);   // +16: generous slack for varint
        uint8_t* p = buffer_.data() + old;

        *p++ = flags;
        write_be16(p, static_cast<uint16_t>(shared));    p += 2;
        write_be16(p, static_cast<uint16_t>(unshared));  p += 2;

        if (!same_vlen) {
            write_be32(p, static_cast<uint32_t>(kv.value.size())); p += 4;
        }
        if (flags & 0x01) {
            write_be64(p, static_cast<uint64_t>(kv.timestamp)); p += 8;
        }
        if (!same_type) {
            *p++ = static_cast<uint8_t>(kv.key_type);
        }

        std::memcpy(p, cur_key + shared, unshared);        p += unshared;
        std::memcpy(p, kv.value.data(), kv.value.size());  p += kv.value.size();

        write_be16(p, static_cast<uint16_t>(kv.tags.size())); p += 2;
        if (!kv.tags.empty()) {
            std::memcpy(p, kv.tags.data(), kv.tags.size()); p += kv.tags.size();
        }
        p += encode_varint64(p, kv.memstore_ts);

        buffer_.resize(static_cast<size_t>(p - buffer_.data()));

        // Update state (one copy per KV to store prev_key_, unavoidable)
        prev_key_.assign(cur_key, cur_key + key_len);
        prev_timestamp_ = kv.timestamp;
        prev_type_      = kv.key_type;
        prev_value_len_ = kv.value.size();
        ++num_kvs_;
        ++prev_num_kvs_;
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
        prev_num_kvs_   = 0;
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
    static size_t prefix_len(const uint8_t* a, size_t alen,
                              const uint8_t* b, size_t blen) noexcept {
        const size_t lim = std::min(alen, blen);
        size_t i = 0;
#ifdef __SSE4_2__
        for (; i + 16 <= lim; i += 16) {
            __m128i va   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(a + i));
            __m128i vb   = _mm_loadu_si128(reinterpret_cast<const __m128i*>(b + i));
            int     mask = _mm_movemask_epi8(_mm_cmpeq_epi8(va, vb));
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
    uint32_t             prev_num_kvs_{0};
    int64_t              prev_timestamp_{0};
    KeyType              prev_type_{KeyType::Put};
    size_t               prev_value_len_{SIZE_MAX};
};

} // namespace block
} // namespace hfile
