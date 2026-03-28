#include <gtest/gtest.h>
#include <hfile/region_partitioner.h>
#include <vector>
#include <string>

using namespace hfile;

static std::vector<uint8_t> key(const char* s) {
    return std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(s),
                                 reinterpret_cast<const uint8_t*>(s) + strlen(s));
}

TEST(RegionPartitioner, None) {
    auto p = RegionPartitioner::none();
    EXPECT_EQ(p->num_regions(), 1);

    auto k = key("anything");
    EXPECT_EQ(p->region_for(k), 0);
    EXPECT_TRUE(p->split_points().empty());
}

TEST(RegionPartitioner, SingleSplit) {
    std::vector<std::vector<uint8_t>> splits = {key("m")};  // split on 'm'
    auto p = RegionPartitioner::from_splits(splits);
    EXPECT_EQ(p->num_regions(), 2);

    EXPECT_EQ(p->region_for(key("a")), 0);   // "a" < "m" → region 0
    EXPECT_EQ(p->region_for(key("m")), 1);   // "m" == split → region 1 (upper_bound)
    EXPECT_EQ(p->region_for(key("z")), 1);   // "z" > "m" → region 1
}

TEST(RegionPartitioner, MultipleSplits) {
    std::vector<std::vector<uint8_t>> splits = {
        key("d"), key("h"), key("n"), key("t")
    };
    auto p = RegionPartitioner::from_splits(splits);
    EXPECT_EQ(p->num_regions(), 5);

    EXPECT_EQ(p->region_for(key("a")),  0);
    EXPECT_EQ(p->region_for(key("d")),  1);
    EXPECT_EQ(p->region_for(key("e")),  1);
    EXPECT_EQ(p->region_for(key("h")),  2);
    EXPECT_EQ(p->region_for(key("m")),  2);
    EXPECT_EQ(p->region_for(key("n")),  3);
    EXPECT_EQ(p->region_for(key("s")),  3);
    EXPECT_EQ(p->region_for(key("t")),  4);
    EXPECT_EQ(p->region_for(key("zzz")),4);
}

TEST(RegionPartitioner, UnsortedSplitsThrow) {
    std::vector<std::vector<uint8_t>> splits = {key("z"), key("a")};
    EXPECT_THROW(RegionPartitioner::from_splits(splits), std::invalid_argument);
}

TEST(RegionPartitioner, BinaryKeyLookup) {
    // Test with binary keys
    std::vector<std::vector<uint8_t>> splits;
    for (int i = 1; i < 10; ++i) {
        uint8_t k[4];
        hfile::write_be32(k, static_cast<uint32_t>(i * 1000));
        splits.emplace_back(k, k + 4);
    }
    auto p = RegionPartitioner::from_splits(splits);
    EXPECT_EQ(p->num_regions(), 10);

    uint8_t k0[4]; hfile::write_be32(k0, 0);
    EXPECT_EQ(p->region_for({k0, 4}), 0);

    uint8_t k5000[4]; hfile::write_be32(k5000, 5000);
    EXPECT_EQ(p->region_for({k5000, 4}), 5);

    uint8_t kmax[4]; hfile::write_be32(kmax, 99999);
    EXPECT_EQ(p->region_for({kmax, 4}), 9);
}
