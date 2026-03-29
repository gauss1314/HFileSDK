#include <gtest/gtest.h>

#include "partition/cf_grouper.h"

#include <vector>

using namespace hfile;
using namespace hfile::partition;

TEST(CFGrouper, KnownFamiliesAreReturnedSorted) {
    CFGrouper grouper;
    grouper.register_cf("cf_b");
    grouper.register_cf("cf_a");
    grouper.register_cf("cf_c");

    const auto& families = grouper.known_families();
    ASSERT_EQ(families.size(), 3u);
    EXPECT_EQ(families[0], "cf_a");
    EXPECT_EQ(families[1], "cf_b");
    EXPECT_EQ(families[2], "cf_c");
}

TEST(CFGrouper, ValidateFamilyMatchesRegisteredNames) {
    CFGrouper grouper;
    grouper.register_cf("cf");

    std::vector<uint8_t> row = {'r'};
    std::vector<uint8_t> family = {'c', 'f'};
    std::vector<uint8_t> qualifier = {'q'};
    std::vector<uint8_t> value = {'v'};
    KeyValue kv;
    kv.row = row;
    kv.family = family;
    kv.qualifier = qualifier;
    kv.timestamp = 1;
    kv.key_type = KeyType::Put;
    kv.value = value;

    EXPECT_TRUE(grouper.has_cf("cf"));
    EXPECT_TRUE(grouper.validate_family(kv));

    kv.family = std::vector<uint8_t>{'o', 't', 'h', 'e', 'r'};
    EXPECT_FALSE(grouper.validate_family(kv));
}

TEST(CFGrouper, RebuildListRefreshesCachedFamilyOrder) {
    CFGrouper grouper;
    grouper.register_cf("cf_b");
    const auto& before = grouper.known_families();
    ASSERT_EQ(before.size(), 1u);
    EXPECT_EQ(before[0], "cf_b");

    grouper.register_cf("cf_a");
    grouper.rebuild_list();
    const auto& after = grouper.known_families();
    ASSERT_EQ(after.size(), 2u);
    EXPECT_EQ(after[0], "cf_a");
    EXPECT_EQ(after[1], "cf_b");
}
