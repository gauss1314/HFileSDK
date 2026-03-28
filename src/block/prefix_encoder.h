#pragma once

#include "data_block_encoder.h"
#include <vector>
#include <cstring>

#ifdef __SSE4_2__
#  include <nmmintrin.h>
#endif

namespace hfile {
namespace block {

/// PREFIX encoding: each KV stores only the suffix of the key not shared
/// with the previous key. Reduces block size by ~30-50% for sorted data.
///
/// On-disk format per KV (PREFIX):
///   [keySharedLen(2B BE)] [keyUnsharedLen(2B BE)] [valueLen(4B BE)]
///   [unsharedKeyBytes]  [valueBytes]
///   [tagsLen(2B BE)] [tagsBytes] [mvcc(VarInt)]
class PrefixEncoder final : public DataBlockEncoder {
public:
    explicit PrefixEncoder(size_t block_size)
        : block_size_{block_size} {
        buffer_.reserve(block_size + 4096);
        prev_key_.reserve(512);
    }

    bool append(const KeyValue& kv) override {
        // Serialise the current cell's key.
        // Use a stack buffer for the common case (key < 512 bytes) to avoid
        // a heap allocation on every call — this is the hot path.
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

        // Conservative size estimate
        const size_t est = 2 + 2 + 4 + unshared + kv.value.size()
                         + 2 + kv.tags.size() + 10 /*varint max*/;
        if (!buffer_.empty() && buffer_.size() + est > block_size_)
            return false;

        if (num_kvs_ == 0)
            first_key_buf_.assign(cur_key, cur_key + key_len);

        // Grow output buffer
        const size_t off = buffer_.size();
        buffer_.resize(off + est);
        uint8_t* p = buffer_.data() + off;

        write_be16(p, static_cast<uint16_t>(shared));    p += 2;
        write_be16(p, static_cast<uint16_t>(unshared));  p += 2;
        write_be32(p, static_cast<uint32_t>(kv.value.size())); p += 4;

        std::memcpy(p, cur_key + shared, unshared);        p += unshared;
        std::memcpy(p, kv.value.data(), kv.value.size());  p += kv.value.size();

        // Tags length + tags (HFile v3 mandatory)
        write_be16(p, static_cast<uint16_t>(kv.tags.size())); p += 2;
        if (!kv.tags.empty()) {
            std::memcpy(p, kv.tags.data(), kv.tags.size()); p += kv.tags.size();
        }

        // MVCC VarInt (Bulk Load = 0)
        p += encode_varint64(p, kv.memstore_ts);

        // Trim to actual written size
        buffer_.resize(static_cast<size_t>(p - buffer_.data()));

        // Update previous key (one copy per KV, unavoidable)
        prev_key_.assign(cur_key, cur_key + key_len);
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
        num_kvs_ = 0;
    }

    std::span<const uint8_t> first_key() const override {
        return {first_key_buf_.data(), first_key_buf_.size()};
    }

    size_t   current_size() const override { return buffer_.size(); }
    uint32_t num_kvs()      const override { return num_kvs_; }

private:
    /// SIMD-accelerated common-prefix length computation.
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
    std::vector<uint8_t> prev_key_;       // previous cell's serialised key
    std::vector<uint8_t> first_key_buf_;  // first key in current block
    uint32_t             num_kvs_{0};
};

} // namespace block
} // namespace hfile
