#include <algorithm>
#include <sstream>
#include "arrow/row_key_builder.h"
#include "hfile/types.h"
#include <cassert>
#include <cstdio>
#include <string>
#include <vector>
#include <array>
using namespace hfile::arrow_convert;

static int T=0, P=0;
#define EXPECT(c) do{++T;if(c){++P;}else{\
    fprintf(stderr,"  FAIL %s:%d  %s\n",__FILE__,__LINE__,#c);}}while(0)
#define EXPECT_EQ(a,b) do{\
    ++T; auto _a=(a); auto _b=(b);\
    if(_a==_b){++P;}else{\
        fprintf(stderr,"  FAIL %s:%d  [%s] != [%s]\n",__FILE__,__LINE__,\
            ([](auto v)->std::string{if constexpr(std::is_integral_v<decltype(v)>)return std::to_string(v);else return std::string(v);}(_a).c_str()),([](auto v)->std::string{if constexpr(std::is_integral_v<decltype(v)>)return std::to_string(v);else return std::string(v);}(_b).c_str()));}}while(0)

static std::string b64(std::span<const uint8_t> data) {
    static constexpr char kAlphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((data.size() + 2) / 3) * 4);
    for (size_t i = 0; i < data.size(); i += 3) {
        uint32_t n = static_cast<uint32_t>(data[i]) << 16;
        bool have_b1 = i + 1 < data.size();
        bool have_b2 = i + 2 < data.size();
        if (have_b1) n |= static_cast<uint32_t>(data[i + 1]) << 8;
        if (have_b2) n |= static_cast<uint32_t>(data[i + 2]);
        out.push_back(kAlphabet[(n >> 18) & 0x3F]);
        out.push_back(kAlphabet[(n >> 12) & 0x3F]);
        out.push_back(have_b1 ? kAlphabet[(n >> 6) & 0x3F] : '=');
        out.push_back(have_b2 ? kAlphabet[n & 0x3F] : '=');
    }
    return out;
}

static int16_t java_hash_string(std::string_view value) {
    long long tmp = std::stoll(std::string(value));
    long long part1 = tmp >> 32;
    long long part2 = tmp & ((2LL << 32) - 1LL);
    long long result = 1LL;
    result = 31LL * result + part1;
    result = 31LL * result + part2;
    return static_cast<int16_t>(result % 65535LL);
}

// ─── split_row_value ──────────────────────────────────────────────────────────
void test_split_basic(){
    auto f = split_row_value("a|b|c");
    EXPECT_EQ(f.size(), 3u);
    EXPECT_EQ(std::string(f[0]),"a");
    EXPECT_EQ(std::string(f[1]),"b");
    EXPECT_EQ(std::string(f[2]),"c");
}
void test_split_empty_tokens(){
    auto f = split_row_value("a||c");
    EXPECT_EQ(f.size(), 3u);
    EXPECT_EQ(std::string(f[1]),"");
}
void test_split_max_count(){
    // splitPreserveAllTokens with maxCount=3
    auto f = split_row_value("a|b|c|d|e", 3);
    EXPECT_EQ(f.size(), 3u);
    EXPECT_EQ(std::string(f[2]),"c|d|e");
}

// ─── RowKeyBuilder::compile error cases ───────────────────────────────────────
void test_compile_empty_rule(){
    auto [b, s] = RowKeyBuilder::compile("");
    EXPECT(!s.ok());
}
void test_compile_too_few_fields(){
    auto [b, s] = RowKeyBuilder::compile("STARTTIME,0,false");
    EXPECT(!s.ok());
}
void test_compile_bad_index(){
    auto [b, s] = RowKeyBuilder::compile("COL,abc,false,10");
    EXPECT(!s.ok());
}
void test_compile_bad_padlen(){
    auto [b, s] = RowKeyBuilder::compile("COL,0,false,xyz");
    EXPECT(!s.ok());
}

// ─── Document example: full 4-segment rule ───────────────────────────────────
// rowKeyRule: "STARTTIME,0,false,10#IMSI,1,true,15#MSISDN,2,false,11,RIGHT,#$RND$,3,false,4"
// rowValue:   "20240301123000|460001234567890|13800138000|120|1024000|2048000"
// Expected:
//   STARTTIME → "20240301123000"     (14 > 10 → no padding)
//   IMSI      → "098765432100064"    (reverse "460001234567890")
//   MSISDN    → "13800138000"        (11 == 11 → no padding)
//   $RND$     → 4 random digits
void test_doc_example(){
    auto [b, s] = RowKeyBuilder::compile(
        "STARTTIME,0,false,10#IMSI,1,true,15#MSISDN,2,false,11,RIGHT,#$RND$,3,false,4");
    EXPECT(s.ok());
    EXPECT_EQ(b.max_col_index(), 2);  // max of {0,1,2,3} but $RND$ is ignored

    auto fields = split_row_value("20240301123000|460001234567890|13800138000|120|1024000|2048000");
    std::string key = b.build(fields);

    // key = STARTTIME + IMSI_reversed + MSISDN + RND(4)
    EXPECT_EQ(key.size(), 14u + 15u + 11u + 4u);

    // Part 1: no padding needed (14 > 10)
    EXPECT_EQ(key.substr(0, 14), "20240301123000");

    // Part 2: reverse of "460001234567890"
    EXPECT_EQ(key.substr(14, 15), "098765432100064");

    // Part 3: no padding needed (11 == 11)
    EXPECT_EQ(key.substr(29, 11), "13800138000");

    // Part 4: 4 random digits, each 0-8
    std::string rnd_part = key.substr(40, 4);
    EXPECT_EQ(rnd_part.size(), 4u);
    for (char c : rnd_part) {
        EXPECT(c >= '0' && c <= '8');
    }
}

// ─── Individual segment behaviours ───────────────────────────────────────────
void test_left_pad(){
    auto [b, s] = RowKeyBuilder::compile("F,0,false,8");
    EXPECT(s.ok());
    auto f = split_row_value("123");
    std::string k = b.build(f);
    EXPECT_EQ(k, "00000123");
}
void test_right_pad(){
    auto [b, s] = RowKeyBuilder::compile("F,0,false,8,RIGHT");
    EXPECT(s.ok());
    auto f = split_row_value("123");
    EXPECT_EQ(b.build(f), "12300000");
}
void test_custom_pad_char(){
    auto [b, s] = RowKeyBuilder::compile("F,0,false,5,LEFT, ");  // space pad
    EXPECT(s.ok());
    auto f = split_row_value("ab");
    EXPECT_EQ(b.build(f), "   ab");
}
void test_no_truncation_when_value_longer(){
    // Value longer than padLen — should NOT be truncated (Java behaviour)
    auto [b, s] = RowKeyBuilder::compile("F,0,false,5");
    EXPECT(s.ok());
    auto f = split_row_value("1234567890");
    EXPECT_EQ(b.build(f), "1234567890");  // longer than 5, no truncation
}
void test_reverse_before_pad(){
    // Java: pad first, then reverse (setValue does pad then reverse)
    auto [b, s] = RowKeyBuilder::compile("F,0,true,6");
    EXPECT(s.ok());
    auto f = split_row_value("abc");
    // padded → "000abc" → reversed → "cba000"
    EXPECT_EQ(b.build(f), "cba000");
}
void test_reverse_exact_length(){
    auto [b, s] = RowKeyBuilder::compile("F,0,true,3");
    EXPECT(s.ok());
    auto f = split_row_value("xyz");
    EXPECT_EQ(b.build(f), "zyx");
}
void test_rnd_is_digits_0_to_8(){
    auto [b, s] = RowKeyBuilder::compile("$RND$,0,false,10");
    EXPECT(s.ok());
    auto f = split_row_value("ignored");
    // Run multiple times to verify range
    for (int i = 0; i < 20; ++i) {
        std::string rnd = b.build(f);
        EXPECT_EQ(rnd.size(), 10u);
        for (char c : rnd) EXPECT(c >= '0' && c <= '8');
    }
}
void test_multiple_segments_concatenation(){
    auto [b, s] = RowKeyBuilder::compile("A,0,false,0#B,1,false,0#C,2,false,0");
    EXPECT(s.ok());
    auto f = split_row_value("hello|world|!");
    EXPECT_EQ(b.build(f), "helloworld!");
}
void test_out_of_range_col_index_gives_empty(){
    auto [b, s] = RowKeyBuilder::compile("F,9,false,0");
    EXPECT(s.ok());
    auto f = split_row_value("only_two|fields");
    EXPECT_EQ(b.build(f), "");  // index 9 out of range → empty
    std::string out;
    auto bs = b.build_checked(f, &out);
    EXPECT(!bs.ok());
    EXPECT(std::string(bs.message()).find("missing field index 9") != std::string::npos);
}
void test_empty_field_value(){
    auto [b, s] = RowKeyBuilder::compile("F,1,false,5");  // index 1 is empty
    EXPECT(s.ok());
    auto f = split_row_value("a||c");
    // empty value, left-padded to 5 with '0'
    EXPECT_EQ(b.build(f), "00000");
}
void test_max_col_index(){
    auto [b, s] = RowKeyBuilder::compile("A,0,false,0#B,5,false,0#C,3,false,0");
    EXPECT(s.ok());
    EXPECT_EQ(b.max_col_index(), 5);
}
void test_rnd_dollar_variants(){
    // Both "$RND$" and "RND$" should be treated as random
    for (const std::string& name : std::vector<std::string>{"$RND$","RND$"}) {
        std::string rule = name + ",0,false,3";
        auto [b, s] = RowKeyBuilder::compile(rule);
        EXPECT(s.ok());
        auto f = split_row_value("x");
        std::string r = b.build(f);
        EXPECT_EQ(r.size(), 3u);
        for (char c : r) EXPECT(c >= '0' && c <= '8');
    }
}
void test_fill_alias_uses_empty_then_padding(){
    auto [b, s] = RowKeyBuilder::compile("FILL,9,false,4");
    EXPECT(s.ok());
    auto f = split_row_value("hello|world");
    EXPECT_EQ(b.build(f), "0000");
}
void test_random_alias_supported(){
    auto [b, s] = RowKeyBuilder::compile("RANDOM,0,false,5");
    EXPECT(s.ok());
    auto f = split_row_value("x");
    auto r = b.build(f);
    EXPECT_EQ(r.size(), 5u);
    for (char c : r) EXPECT(c >= '0' && c <= '8');
}
void test_random_col_alias_supported(){
    auto [b, s] = RowKeyBuilder::compile("RANDOM_COL,0,false,5");
    EXPECT(s.ok());
    auto f = split_row_value("x");
    auto r = b.build(f);
    EXPECT_EQ(r.size(), 5u);
    for (char c : r) EXPECT(c >= '0' && c <= '8');
}
void test_fill_col_alias_uses_empty_then_padding(){
    auto [b, s] = RowKeyBuilder::compile("FILL_COL,9,true,4");
    EXPECT(s.ok());
    auto f = split_row_value("hello|world");
    EXPECT_EQ(b.build(f), "0000");
}
void test_short_hash_matches_java_semantics(){
    auto [b, s] = RowKeyBuilder::compile("short(hash),0,false,0");
    EXPECT(s.ok());
    auto f = split_row_value("123456789");
    int16_t hashed = java_hash_string("123456789");
    std::array<uint8_t, 2> bytes{};
    hfile::write_be16(bytes.data(), static_cast<uint16_t>(hashed));
    EXPECT_EQ(b.build(f), b64(bytes));
}
void test_long_plain_matches_java_semantics(){
    auto [b, s] = RowKeyBuilder::compile("long(),0,false,0");
    EXPECT(s.ok());
    auto f = split_row_value("123456789");
    std::array<uint8_t, 8> bytes{};
    hfile::write_be64(bytes.data(), static_cast<uint64_t>(123456789LL));
    EXPECT_EQ(b.build(f), b64(bytes));
}
void test_long_hash_matches_java_semantics(){
    auto [b, s] = RowKeyBuilder::compile("long(hash),0,false,0");
    EXPECT(s.ok());
    auto f = split_row_value("123456789");
    int16_t hashed = java_hash_string("123456789");
    std::array<uint8_t, 8> bytes{};
    hfile::write_be64(bytes.data(), static_cast<uint64_t>(static_cast<int64_t>(hashed)));
    EXPECT_EQ(b.build(f), b64(bytes));
}
void test_negative_index_rejected(){
    auto [b, s] = RowKeyBuilder::compile("ID,-1,false,0");
    EXPECT(!s.ok());
    EXPECT(std::string(s.message()).find("index must be >= 0") != std::string::npos);
}
void test_negative_pad_len_rejected(){
    auto [b, s] = RowKeyBuilder::compile("ID,0,false,-1");
    EXPECT(!s.ok());
    EXPECT(std::string(s.message()).find("padLen must be >= 0") != std::string::npos);
}
void test_invalid_is_reverse_rejected(){
    auto [b, s] = RowKeyBuilder::compile("ID,0,maybe,0");
    EXPECT(!s.ok());
    EXPECT(std::string(s.message()).find("isReverse must be true or false") != std::string::npos);
}
void test_invalid_pad_mode_rejected(){
    auto [b, s] = RowKeyBuilder::compile("ID,0,false,0,CENTER");
    EXPECT(!s.ok());
    EXPECT(std::string(s.message()).find("padMode must be LEFT or RIGHT") != std::string::npos);
}
void test_long_plain_invalid_numeric_rejected_at_build(){
    auto [b, s] = RowKeyBuilder::compile("long(),0,false,0");
    EXPECT(s.ok());
    auto f = split_row_value("abc");
    std::string out;
    auto bs = b.build_checked(f, &out);
    EXPECT(!bs.ok());
    EXPECT(std::string(bs.message()).find("cannot parse int64 value") != std::string::npos);
}
void test_short_plain_overflow_rejected_at_build(){
    auto [b, s] = RowKeyBuilder::compile("short(),0,false,0");
    EXPECT(s.ok());
    auto f = split_row_value("40000");
    std::string out;
    auto bs = b.build_checked(f, &out);
    EXPECT(!bs.ok());
    EXPECT(std::string(bs.message()).find("cannot parse int16 value") != std::string::npos);
}

// ─── AutoSort: verify that identical rowKeyRule applied to sorted/unsorted
//     rows produces the same output order ──────────────────────────────────────
void test_sort_invariant(){
    // Rows in random order; after sorting by row key they should be ascending
    auto [b, s] = RowKeyBuilder::compile("F,0,false,10");
    EXPECT(s.ok());

    std::vector<std::string> raw_vals = {"300","100","200","050","999"};
    std::vector<std::string> keys;
    for (const auto& v : raw_vals) {
        auto f = split_row_value(v);
        keys.push_back(b.build(f));
    }
    // Sort keys
    std::vector<std::string> sorted_keys = keys;
    std::sort(sorted_keys.begin(), sorted_keys.end());

    // After sort, keys must be ascending (lexicographic)
    for (size_t i = 1; i < sorted_keys.size(); ++i)
        EXPECT(sorted_keys[i-1] <= sorted_keys[i]);

    // All 5 expected padded values must be present
    EXPECT(sorted_keys[0] == "0000000050");  // "050" left-padded to 10
    EXPECT(sorted_keys[1] == "0000000100");
    EXPECT(sorted_keys[4] == "0000000999");
}

// ─── B-13 regression: duplicate col_index in rule → concat (dedup is in converter)
void test_b13_duplicate_col_index_produces_concat() {
    // Two segments referencing the same Arrow column index is valid at the rule
    // level — the converter's GroupedCell dedup handles actual HBase qualifier dedup.
    auto [b, s] = RowKeyBuilder::compile("A,0,false,0#B,0,false,0");
    EXPECT(s.ok());
    auto f = split_row_value("hello");
    std::string key = b.build(f);
    EXPECT_EQ(key, std::string("hellohello"));
}


int main(){
    test_b13_duplicate_col_index_produces_concat();
    printf("\n=== RowKeyBuilder tests ===\n\n");
    test_split_basic();
    test_split_empty_tokens();
    test_split_max_count();
    test_compile_empty_rule();
    test_compile_too_few_fields();
    test_compile_bad_index();
    test_compile_bad_padlen();
    test_doc_example();
    test_left_pad();
    test_right_pad();
    test_custom_pad_char();
    test_no_truncation_when_value_longer();
    test_reverse_before_pad();
    test_reverse_exact_length();
    test_rnd_is_digits_0_to_8();
    test_multiple_segments_concatenation();
    test_out_of_range_col_index_gives_empty();
    test_empty_field_value();
    test_max_col_index();
    test_rnd_dollar_variants();
    test_fill_alias_uses_empty_then_padding();
    test_random_alias_supported();
    test_random_col_alias_supported();
    test_fill_col_alias_uses_empty_then_padding();
    test_short_hash_matches_java_semantics();
    test_long_plain_matches_java_semantics();
    test_long_hash_matches_java_semantics();
    test_negative_index_rejected();
    test_negative_pad_len_rejected();
    test_invalid_is_reverse_rejected();
    test_invalid_pad_mode_rejected();
    test_long_plain_invalid_numeric_rejected_at_build();
    test_short_plain_overflow_rejected_at_build();
    test_sort_invariant();
    printf("Tests run: %d  Passed: %d  Failed: %d\n\n", T, P, T-P);
    return (P==T) ? 0 : 1;
}
