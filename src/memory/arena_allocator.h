#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include <span>
#include <cassert>
#include <new>

namespace hfile {
namespace memory {

/// Thread-local bump-pointer arena allocator.
/// Zero heap allocation on hot path after initial setup.
/// All allocations are freed at once when the arena is reset or destroyed.
class ArenaAllocator {
public:
    static constexpr size_t kDefaultChunkSize = 256 * 1024;  // 256 KB

    explicit ArenaAllocator(size_t chunk_size = kDefaultChunkSize)
        : chunk_size_{chunk_size} {
        new_chunk();
    }

    ~ArenaAllocator() {
        for (auto* chunk : chunks_)
            ::operator delete[](chunk, std::align_val_t{64});
    }

    // Non-copyable, movable
    ArenaAllocator(const ArenaAllocator&)            = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&&) noexcept        = default;
    ArenaAllocator& operator=(ArenaAllocator&&) noexcept = default;

    /// Allocate `size` bytes aligned to `align` (must be power of 2).
    /// Hot path: single comparison + pointer bump. Never allocates on success.
    [[nodiscard]] uint8_t* allocate(size_t size, size_t align = 8) {
        assert((align & (align - 1)) == 0);
        uintptr_t cur = reinterpret_cast<uintptr_t>(ptr_);
        uintptr_t aligned = (cur + align - 1) & ~(align - 1);
        uint8_t* result = reinterpret_cast<uint8_t*>(aligned);
        uint8_t* next   = result + size;
        if (__builtin_expect(next <= end_, true)) {
            ptr_ = next;
            return result;
        }
        // Overflow: allocate a new chunk
        size_t chunk = std::max(chunk_size_, size + align);
        new_chunk(chunk);
        return allocate(size, align);
    }

    template<typename T>
    [[nodiscard]] T* allocate_typed(size_t count = 1) {
        return reinterpret_cast<T*>(allocate(sizeof(T) * count, alignof(T)));
    }

    /// Copy data into arena-owned memory, return span.
    [[nodiscard]] std::span<uint8_t> copy(std::span<const uint8_t> src) {
        auto* dst = allocate(src.size(), 1);
        __builtin_memcpy(dst, src.data(), src.size());
        return {dst, src.size()};
    }

    /// Reset: free all but the first chunk, reset pointers.
    void reset() noexcept {
        for (size_t i = 1; i < chunks_.size(); ++i)
            ::operator delete[](chunks_[i], std::align_val_t{64});
        chunks_.resize(1);
        ptr_ = chunks_[0];
        end_ = ptr_ + chunk_size_;
    }

    size_t bytes_used() const noexcept {
        size_t total = 0;
        for (size_t i = 0; i + 1 < chunks_.size(); ++i)
            total += chunk_size_;
        total += static_cast<size_t>(ptr_ - chunks_.back());
        return total;
    }

private:
    void new_chunk(size_t size = 0) {
        if (size == 0) size = chunk_size_;
        // 64-byte aligned chunk (cache-line)
        auto* chunk = static_cast<uint8_t*>(
            ::operator new[](size, std::align_val_t{64}));
        chunks_.push_back(chunk);
        ptr_ = chunk;
        end_ = chunk + size;
    }

    size_t                 chunk_size_;
    std::vector<uint8_t*>  chunks_;
    uint8_t*               ptr_{nullptr};
    uint8_t*               end_{nullptr};
};

} // namespace memory
} // namespace hfile
