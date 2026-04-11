#include <gtest/gtest.h>

#include "memory/arena_allocator.h"
#include "memory/memory_budget.h"

#include <algorithm>
#include <cstdint>
#include <vector>

using namespace hfile::memory;

TEST(MemoryBudget, ReserveReleaseAndPeakTracking) {
    MemoryBudget budget(128);
    EXPECT_EQ(budget.used(), 0u);
    EXPECT_EQ(budget.peak(), 0u);
    EXPECT_TRUE(budget.reserve(32).ok());
    EXPECT_TRUE(budget.reserve(64).ok());
    EXPECT_EQ(budget.used(), 96u);
    EXPECT_EQ(budget.peak(), 96u);
    budget.release(40);
    EXPECT_EQ(budget.used(), 56u);
    EXPECT_EQ(budget.remaining(), 72u);
    EXPECT_EQ(budget.peak(), 96u);
}

TEST(MemoryBudget, GuardAndOverLimitBehavior) {
    MemoryBudget budget(64);
    {
        MemoryBudget::Guard guard(budget, 48);
        ASSERT_TRUE(guard.ok());
        EXPECT_EQ(budget.used(), 48u);
    }
    EXPECT_EQ(budget.used(), 0u);
    EXPECT_FALSE(budget.reserve(65).ok());
    EXPECT_EQ(budget.used(), 0u);
}

TEST(ArenaAllocator, AlignsAllocationsAndCopiesData) {
    ArenaAllocator arena(64);
    auto* aligned = arena.allocate(8, 32);
    ASSERT_NE(aligned, nullptr);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(aligned) % 32u, 0u);

    std::vector<uint8_t> input = {1, 2, 3, 4, 5};
    auto copied = arena.copy(input);
    ASSERT_EQ(copied.size(), input.size());
    EXPECT_TRUE(std::equal(copied.begin(), copied.end(), input.begin()));
    EXPECT_GT(arena.bytes_used(), 0u);
}

TEST(ArenaAllocator, ResetReusesFirstChunkAndHandlesLargeAllocations) {
    ArenaAllocator arena(32);
    auto* first = arena.allocate(16, 8);
    auto* second = arena.allocate(64, 16);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_GT(arena.bytes_used(), 32u);

    arena.reset();

    EXPECT_EQ(arena.bytes_used(), 0u);
    auto* after_reset = arena.allocate(16, 8);
    ASSERT_NE(after_reset, nullptr);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(after_reset),
              reinterpret_cast<std::uintptr_t>(first));
}
