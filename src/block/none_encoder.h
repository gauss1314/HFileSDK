#pragma once

#include "data_block_encoder.h"
#include <algorithm>
#include <vector>
#include <zlib.h>

namespace hfile {
namespace block {

/// NONE encoding: KVs written sequentially with no prefix compression.
/// Fastest write path. Each block is a raw concatenation of serialized KVs.
class NoneEncoder final : public DataBlockEncoder {
public:
    explicit NoneEncoder(size_t block_size)
        : block_size_{block_size} {
        buffer_.resize(block_size + 4096);
        crc32_ = static_cast<uint32_t>(::crc32(0L, Z_NULL, 0));
    }

    bool append(const KeyValue& kv) override {
        return append_sized(kv, kv.encoded_size());
    }

    bool append_sized(const KeyValue& kv, size_t kv_size) override {
        ensure_capacity(used_ + kv_size);
        const uint32_t key_len = key_length_from_encoded_size(kv, kv_size);
        serialize_kv_sized(kv, key_len, buffer_.data() + used_);
        crc32_ = static_cast<uint32_t>(
            ::crc32(crc32_, buffer_.data() + used_, static_cast<uInt>(kv_size)));
        used_ += kv_size;

        if (num_kvs_ == 0) {
            // Save first key for index
            first_key_buf_.resize(key_len);
            serialize_key(kv, first_key_buf_.data());
        }
        ++num_kvs_;
        return true;
    }

    std::span<const uint8_t> finish_block() override {
        return {buffer_.data(), used_};
    }

    void reset() override {
        first_key_buf_.clear();
        num_kvs_ = 0;
        used_ = 0;
        crc32_ = static_cast<uint32_t>(::crc32(0L, Z_NULL, 0));
    }

    std::span<const uint8_t> first_key() const override {
        return {first_key_buf_.data(), first_key_buf_.size()};
    }

    size_t current_size() const override { return used_; }

    uint32_t num_kvs() const override { return num_kvs_; }
    bool supports_block_crc32() const noexcept override { return true; }
    uint32_t current_block_crc32() const noexcept override { return crc32_; }

private:
    void ensure_capacity(size_t required) {
        if (required <= buffer_.size()) {
            return;
        }
        size_t new_size = std::max(required, buffer_.size() * 2);
        buffer_.resize(new_size);
    }

    size_t               block_size_;
    std::vector<uint8_t> buffer_;
    std::vector<uint8_t> first_key_buf_;
    uint32_t             num_kvs_{0};
    size_t               used_{0};
    uint32_t             crc32_{0};
};

} // namespace block
} // namespace hfile
