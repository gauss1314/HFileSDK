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

// ─── Array parsing regression tests (added with #include <vector> fix) ────────

TEST(JniJsonUtils, ParsesStringArray) {
    jni::JsonConfigObject cfg;
    auto s = jni::parse_json_config(
        "{\"excluded_column_prefixes\":[\"_hoodie\",\"_cdc_\"]}",
        &cfg);
    ASSERT_TRUE(s.ok()) << s.message();
    auto arr = jni::config_string_array(cfg, "excluded_column_prefixes");
    ASSERT_TRUE(arr.has_value());
    ASSERT_EQ(arr->size(), 2u);
    EXPECT_EQ((*arr)[0], "_hoodie");
    EXPECT_EQ((*arr)[1], "_cdc_");
}

TEST(JniJsonUtils, ParsesEmptyStringArray) {
    jni::JsonConfigObject cfg;
    auto s = jni::parse_json_config(
        "{\"excluded_columns\":[]}",
        &cfg);
    ASSERT_TRUE(s.ok()) << s.message();
    auto arr = jni::config_string_array(cfg, "excluded_columns");
    ASSERT_TRUE(arr.has_value());
    EXPECT_EQ(arr->size(), 0u);
}

TEST(JniJsonUtils, MixedConfigWithArray) {
    jni::JsonConfigObject cfg;
    auto s = jni::parse_json_config(
        "{\"compression\":\"lz4\","
        "\"block_size\":65536,"
        "\"excluded_column_prefixes\":[\"_hoodie\"]}",
        &cfg);
    ASSERT_TRUE(s.ok()) << s.message();
    EXPECT_EQ(*jni::config_string(cfg, "compression"), "lz4");
    EXPECT_EQ(*jni::config_int(cfg, "block_size"), 65536);
    auto arr = jni::config_string_array(cfg, "excluded_column_prefixes");
    ASSERT_TRUE(arr.has_value());
    ASSERT_EQ(arr->size(), 1u);
    EXPECT_EQ((*arr)[0], "_hoodie");
}

TEST(JniJsonUtils, ConfigStringArrayReturnNulloptForNonArray) {
    jni::JsonConfigObject cfg;
    jni::parse_json_config("{\"compression\":\"lz4\"}", &cfg);
    // "compression" is a string, not an array → nullopt
    EXPECT_FALSE(jni::config_string_array(cfg, "compression").has_value());
    // missing key → nullopt
    EXPECT_FALSE(jni::config_string_array(cfg, "no_such_key").has_value());
}

TEST(JniJsonUtils, RejectsNonStringArrayElements) {
    jni::JsonConfigObject cfg;
    auto s = jni::parse_json_config(
        "{\"cols\":[\"ok\",123]}",
        &cfg);
    EXPECT_FALSE(s.ok());
}
