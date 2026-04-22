#include <gtest/gtest.h>

#include "memory/block_pool.h"

#include <cstdint>

using namespace hfile::memory;

TEST(BlockPoolExtra, AcquireReleaseAndExhaustion) {
    BlockPool pool(128, 2);
    EXPECT_EQ(pool.buffer_size(), 128u);
    EXPECT_EQ(pool.available(), 2u);

    uint8_t* a = pool.acquire();
    uint8_t* b = pool.acquire();
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_NE(a, b);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(a) % 64u, 0u);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(b) % 64u, 0u);
    EXPECT_EQ(pool.available(), 0u);
    EXPECT_EQ(pool.acquire(), nullptr);

    pool.release(a);
    EXPECT_EQ(pool.available(), 1u);
    uint8_t* c = pool.acquire();
    ASSERT_EQ(c, a);
    EXPECT_EQ(pool.available(), 0u);

    pool.release(b);
    pool.release(c);
    EXPECT_EQ(pool.available(), 2u);
}
