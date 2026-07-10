#pragma once

#include <hfile/status.h>
#include <cstddef>
#include <atomic>
#include <algorithm>
#include <cassert>

namespace hfile {
namespace memory {

/// Thread-safe memory budget tracker.
///
/// All allocations that should count against the budget call reserve();
/// the corresponding release() frees the quota.  If the requested amount
/// would exceed max_bytes, reserve() returns a MemoryLimitExceeded error
/// and the caller must take remedial action (flush buffers, shrink batch
/// size, etc.) before retrying.
///
/// This class is intentionally lightweight — it does NOT intercept
/// operator new / malloc.  Callers are responsible for calling reserve()
/// before any large allocation and release() afterwards.
class MemoryBudget {
public:
    static constexpr size_t kUnlimited = SIZE_MAX;

    explicit MemoryBudget(size_t max_bytes = kUnlimited) noexcept
        : max_bytes_{max_bytes}, used_{0}, peak_{0} {}

    // Non-copyable, movable
    MemoryBudget(const MemoryBudget&)            = delete;
    MemoryBudget& operator=(const MemoryBudget&) = delete;

    /// Try to reserve `bytes` bytes of quota.
    /// Returns error if used + bytes > max_bytes.
    Status reserve(size_t bytes) {
        if (bytes == 0) return Status::OK();
        size_t current = used_.load(std::memory_order_relaxed);
        size_t next = 0;
        for (;;) {
            // Checked addition prevents size_t wraparound from bypassing the
            // configured ceiling near SIZE_MAX.
            if (current > max_bytes_ || bytes > max_bytes_ - current) {
                return Status::InvalidArg(
                    "MemoryBudget: limit exceeded ("
                    + std::to_string(max_bytes_ / 1024 / 1024) + " MB)");
            }
            next = current + bytes;
            if (used_.compare_exchange_weak(
                    current, next,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                break;
            }
        }

        size_t p = peak_.load(std::memory_order_relaxed);
        while (next > p &&
               !peak_.compare_exchange_weak(
                   p, next,
                   std::memory_order_relaxed,
                   std::memory_order_relaxed)) {}
        return Status::OK();
    }

    /// Release `bytes` bytes of previously reserved quota.
    void release(size_t bytes) noexcept {
        if (bytes == 0) return;
        size_t current = used_.load(std::memory_order_relaxed);
        for (;;) {
            assert(current >= bytes && "MemoryBudget double release");
            const size_t next = current >= bytes ? current - bytes : 0;
            if (used_.compare_exchange_weak(
                    current, next,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                return;
            }
        }
    }

    /// RAII guard: reserves on construction, releases on destruction.
    struct Guard {
        Guard(MemoryBudget& b, size_t bytes)
            : budget_{b}, bytes_{0} {
            if (b.reserve(bytes).ok()) bytes_ = bytes;
        }
        ~Guard() noexcept { budget_.release(bytes_); }
        bool ok() const noexcept { return bytes_ > 0; }
    private:
        MemoryBudget& budget_;
        size_t        bytes_;
    };

    size_t used()      const noexcept { return used_.load(std::memory_order_relaxed); }
    size_t peak()      const noexcept { return peak_.load(std::memory_order_relaxed); }
    size_t remaining() const noexcept {
        size_t u = used();
        return u < max_bytes_ ? max_bytes_ - u : 0;
    }
    size_t max_bytes() const noexcept { return max_bytes_; }
    bool   unlimited() const noexcept { return max_bytes_ == kUnlimited; }

    void reset_peak() noexcept { peak_.store(0, std::memory_order_relaxed); }

private:
    size_t             max_bytes_;
    std::atomic<size_t> used_;
    std::atomic<size_t> peak_;
};

} // namespace memory
} // namespace hfile
