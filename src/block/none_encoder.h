#pragma once

#include "data_block_encoder.h"
#include <vector>

namespace hfile {
namespace block {

/// NONE encoding: KVs written sequentially with no prefix compression.
/// Fastest write path. Each block is a raw concatenation of serialized KVs.
class NoneEncoder final : public DataBlockEncoder {
public:
    explicit NoneEncoder(size_t block_size)
        : block_size_{block_size} {
        buffer_.reserve(block_size + 4096);
    }

    bool append(const KeyValue& kv) override {
        size_t kv_size = kv.encoded_size();
        if (!buffer_.empty() && buffer_.size() + kv_size > block_size_)
            return false;  // block full

        size_t old_size = buffer_.size();
        buffer_.resize(old_size + kv_size);
        serialize_kv(kv, buffer_.data() + old_size);

        if (num_kvs_ == 0) {
            // Save first key for index
            first_key_buf_.resize(kv.key_length());
            serialize_key(kv, first_key_buf_.data());
        }
        ++num_kvs_;
        return true;
    }

    std::span<const uint8_t> finish_block() override {
        return {buffer_.data(), buffer_.size()};
    }

    void reset() override {
        buffer_.clear();
        first_key_buf_.clear();
        num_kvs_ = 0;
    }

    std::span<const uint8_t> first_key() const override {
        return {first_key_buf_.data(), first_key_buf_.size()};
    }

    size_t current_size() const override { return buffer_.size(); }

    uint32_t num_kvs() const override { return num_kvs_; }

private:
    size_t               block_size_;
    std::vector<uint8_t> buffer_;
    std::vector<uint8_t> first_key_buf_;
    uint32_t             num_kvs_{0};
};

} // namespace block
} // namespace hfile
