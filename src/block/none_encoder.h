#pragma once

#include "data_block_encoder.h"
#include <algorithm>
#include <limits>
#include <vector>

namespace hfile {
namespace block {

/// NONE encoding: KVs written sequentially with no prefix compression.
/// Fastest write path. Each block is a raw concatenation of serialized KVs.
class NoneEncoder final : public DataBlockEncoder {
public:
    explicit NoneEncoder(size_t block_size)
        : block_size_{block_size} {
        buffer_.resize(initial_buffer_size(block_size));
    }

    bool append(const KeyValue& kv) override {
        return append_sized(kv, kv.encoded_size());
    }

    bool append_sized(const KeyValue& kv, size_t kv_size) override {
        resize_buffer_storage(required_buffer_storage(kv_size));
        const uint32_t key_len = key_length_from_encoded_size(kv, kv_size);
        serialize_kv_sized(kv, key_len, buffer_.data() + used_);
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
    }

    memory::AlignedByteBuffer take_finished_buffer(
            memory::AlignedByteBuffer replacement) noexcept override {
        memory::AlignedByteBuffer finished = std::move(buffer_);
        buffer_ = std::move(replacement);
        reset();
        return finished;
    }

    size_t buffer_storage_size() const noexcept override {
        return buffer_.size();
    }

    size_t required_buffer_storage(size_t encoded_size) const noexcept override {
        const size_t max_size = std::numeric_limits<size_t>::max();
        const size_t required = encoded_size > max_size - used_
            ? max_size
            : used_ + encoded_size;
        if (required <= buffer_.size()) {
            return buffer_.size();
        }
        const size_t doubled = buffer_.size() > max_size / 2
            ? max_size
            : buffer_.size() * 2;
        return std::max(required, doubled);
    }

    void resize_buffer_storage(size_t size) override {
        if (size > buffer_.size()) {
            buffer_.resize(size);
        }
    }

    std::span<const uint8_t> first_key() const override {
        return {first_key_buf_.data(), first_key_buf_.size()};
    }

    size_t current_size() const override { return used_; }

    uint32_t num_kvs() const override { return num_kvs_; }

private:
    static size_t initial_buffer_size(size_t block_size) noexcept {
        constexpr size_t kSlack = 4096;
        return block_size > std::numeric_limits<size_t>::max() - kSlack
            ? block_size
            : block_size + kSlack;
    }

    size_t               block_size_;
    memory::AlignedByteBuffer buffer_;
    std::vector<uint8_t> first_key_buf_;
    uint32_t             num_kvs_{0};
    size_t               used_{0};
};

} // namespace block
} // namespace hfile
