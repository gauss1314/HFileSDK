#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <type_traits>
#include <vector>

namespace hfile::memory {

/// Minimal C++20 allocator backed by aligned operator new/delete.  It works on
/// Linux, macOS, and MSYS2 clang without platform-specific allocation APIs.
template <typename T, size_t Alignment> class AlignedAllocator {
public:
    static_assert(Alignment >= alignof(T));
    static_assert((Alignment & (Alignment - 1)) == 0,
                  "alignment must be a power of two");

    using value_type = T;
    using is_always_equal = std::true_type;

    AlignedAllocator() noexcept = default;

    template <typename U>
    AlignedAllocator(const AlignedAllocator<U, Alignment> &) noexcept {}

    template <typename U> struct rebind {
        using other = AlignedAllocator<U, Alignment>;
    };

    [[nodiscard]] T *allocate(size_t count) {
        if (count > std::numeric_limits<size_t>::max() / sizeof(T)) {
            throw std::bad_array_new_length();
        }
        if (count == 0)
            return nullptr;
        return static_cast<T *>(
            ::operator new(count * sizeof(T), std::align_val_t{Alignment}));
    }

    void deallocate(T *ptr, size_t) noexcept {
        ::operator delete(ptr, std::align_val_t{Alignment});
    }
};

template <typename T, typename U, size_t Alignment>
constexpr bool operator==(const AlignedAllocator<T, Alignment> &,
                          const AlignedAllocator<U, Alignment> &) noexcept {
    return true;
}

template <typename T, typename U, size_t Alignment>
constexpr bool operator!=(const AlignedAllocator<T, Alignment> &,
                          const AlignedAllocator<U, Alignment> &) noexcept {
    return false;
}

using AlignedByteBuffer = std::vector<uint8_t, AlignedAllocator<uint8_t, 64>>;

} // namespace hfile::memory
