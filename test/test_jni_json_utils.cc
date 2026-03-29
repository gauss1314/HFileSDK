#include <gtest/gtest.h>

#include "jni/json_utils.h"

using namespace hfile;

TEST(JniJsonUtils, ParsesStrictConfigObject) {
    jni::JsonConfigObject cfg;
    auto s = jni::parse_json_config(
        "{\"compression\":\"zstd\",\"block_size\":65536,\"column_family\":\"cf\"}",
        &cfg);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(jni::config_string(cfg, "compression").has_value());
    EXPECT_EQ(*jni::config_string(cfg, "compression"), "zstd");
    ASSERT_TRUE(jni::config_int(cfg, "block_size").has_value());
    EXPECT_EQ(*jni::config_int(cfg, "block_size"), 65536);
    ASSERT_TRUE(jni::config_string(cfg, "column_family").has_value());
    EXPECT_EQ(*jni::config_string(cfg, "column_family"), "cf");
}

TEST(JniJsonUtils, RejectsDuplicateKeys) {
    jni::JsonConfigObject cfg;
    auto s = jni::parse_json_config(
        "{\"compression\":\"zstd\",\"compression\":\"lz4\"}",
        &cfg);
    EXPECT_FALSE(s.ok());
}

TEST(JniJsonUtils, RejectsUnsupportedSyntax) {
    jni::JsonConfigObject cfg;
    auto s = jni::parse_json_config(
        "{\"compression\":true}",
        &cfg);
    EXPECT_FALSE(s.ok());
}

TEST(JniJsonUtils, EscapesControlCharacters) {
    auto escaped = jni::json_escape("line1\n\"line2\"\t");
    EXPECT_EQ(escaped, "line1\\n\\\"line2\\\"\\t");
}
