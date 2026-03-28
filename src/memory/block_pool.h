#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include <mutex>
#include <memory>
#include <new>
#include <cassert>
#include <unordered_set>

namespace hfile {
namespace memory {

/// Pre-allocated pool of fixed-size, 64-byte aligned byte buffers.
///
/// Thread safety:
///   acquire() and release() are fully thread-safe via an internal mutex.
///   The *contents* of an acquired buffer are owned by a single thread;
///   the caller must not share the buffer without external synchronisation.
///
/// Correctness invariants enforced in debug builds:
///   - Double-release is detected (assert fires).
///   - Releasing a pointer not belonging to this pool is detected (assert fires).
class BlockPool {
public:
    struct Buffer {
        alignas(64) uint8_t data[1];  // placeholder; actual size is buffer_size_
    };

    explicit BlockPool(size_t buffer_size, size_t pool_count)
        : buffer_size_{buffer_size}, pool_count_{pool_count} {
        raw_.reserve(pool_count);
        free_.reserve(pool_count);
        for (size_t i = 0; i < pool_count; ++i) {
            auto* buf = static_cast<uint8_t*>(
                ::operator new[](buffer_size, std::align_val_t{64}));
            raw_.push_back(buf);
            free_.push_back(buf);
#ifdef HFILE_DEBUG_POOL
            owned_.insert(buf);
#endif
        }
    }

    ~BlockPool() {
        for (auto* buf : raw_)
            ::operator delete[](buf, std::align_val_t{64});
    }

    BlockPool(const BlockPool&)            = delete;
    BlockPool& operator=(const BlockPool&) = delete;

    /// Acquire a buffer from the pool.
    /// Returns nullptr if the pool is exhausted — caller must handle this.
    [[nodiscard]] uint8_t* acquire() noexcept {
        std::lock_guard<std::mutex> lk{mu_};
        if (free_.empty()) return nullptr;
        auto* buf = free_.back();
        free_.pop_back();
#ifdef HFILE_DEBUG_POOL
        in_use_.insert(buf);
#endif
        return buf;
    }

    /// Return a buffer to the pool.
    /// The buffer must have been obtained from this pool via acquire().
    void release(uint8_t* buf) noexcept {
        assert(buf != nullptr);
        std::lock_guard<std::mutex> lk{mu_};

#ifdef HFILE_DEBUG_POOL
        // Detect releasing a pointer that doesn't belong to this pool
        assert(owned_.count(buf) > 0
               && "BlockPool::release: pointer not owned by this pool");
        // Detect double-release
        assert(in_use_.count(buf) > 0
               && "BlockPool::release: double-release detected");
        in_use_.erase(buf);
#endif

        free_.push_back(buf);
    }

    size_t buffer_size() const noexcept { return buffer_size_; }

    size_t available() const noexcept {
        std::lock_guard<std::mutex> lk{mu_};
        return free_.size();
    }

private:
    size_t                  buffer_size_;
    size_t                  pool_count_;
    std::vector<uint8_t*>   raw_;    // all allocated buffers (never changes)
    mutable std::mutex      mu_;
    std::vector<uint8_t*>   free_;   // currently available buffers

#ifdef HFILE_DEBUG_POOL
    std::unordered_set<uint8_t*> owned_;   // all pointers this pool ever issued
    std::unordered_set<uint8_t*> in_use_;  // pointers currently acquired
#endif
};

} // namespace memory
} // namespace hfile
