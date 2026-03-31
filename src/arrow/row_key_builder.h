#pragma once

#include <hfile/status.h>
#include <string>
#include <string_view>
#include <vector>
#include <span>
#include <random>
#include <cstdint>

namespace hfile {
namespace arrow_convert {

/// One segment parsed from the rowKeyRule string.
///
/// rowKeyRule format (mirrors UniverseHbaseBeanUtil.parseKeyBeans):
///   "SEG1#SEG2#SEG3..."
///   Each SEG: "name,index,isReverse,padLen[,padMode][,padContent]"
///
/// Special names:
///   $RND$ / RANDOM / RANDOM_COL — generate padLen random digits (0–8), index field ignored
///   FILL / FILL_COL             — use empty string, then still apply pad/reverse
///
/// Java-compatible encoded names:
///   long(...), short(...)
/// with optional nested transforms such as:
///   short(hash)
///   long(hash(...))
///
/// Examples:
///   "STARTTIME,0,false,10"              → col[0], no reverse, left-pad to 10 with '0'
///   "IMSI,1,true,15"                    → col[1], reverse, no pad (len==padLen)
///   "MSISDN,2,false,11,RIGHT"           → col[2], right-pad to 11 with '0'
///   "$RND$,3,false,4"                   → 4 random digits (0–8)
struct RowKeySegment {
    enum class Type { ColumnRef, Random, Fill, EncodedColumn };
    enum class EncodeKind { None, Int64Base64, Int16Base64 };
    enum class Transform { Hash };

    Type        type       = Type::ColumnRef;
    std::string name;           // original column name (informational)
    int         col_index  = 0; // index into pipe-separated rowValue fields
    bool        reverse    = false;
    int         pad_len    = 0; // target length; 0 = no padding
    bool        pad_right  = false;  // false = LEFT (default), true = RIGHT
    char        pad_char   = '0';
    EncodeKind  encode_kind = EncodeKind::None;
    std::vector<Transform> transforms;
};

/// Compiled Row Key builder from a rowKeyRule expression.
///
/// Usage:
///   auto [builder, s] = RowKeyBuilder::compile(rule);
///   if (!s.ok()) { ... }
///   std::string key = builder.build(fields);  // fields = pipe-split rowValue
class RowKeyBuilder {
public:
    /// Parse and compile a rowKeyRule string.
    /// Returns error if the rule is syntactically invalid.
    static std::pair<RowKeyBuilder, Status> compile(const std::string& rule);

    /// Build a row key from the pipe-split fields of one Arrow row.
    /// `fields[i]` corresponds to the i-th pipe-delimited token in rowValue.
    ///
    /// If any col_index is out of range, that segment is treated as empty string.
    std::string build(const std::vector<std::string_view>& fields);
    Status build_checked(const std::vector<std::string_view>& fields, std::string* out);

    /// Maximum column index referenced by any segment (+1 = minimum field count).
    int max_col_index() const noexcept { return max_col_index_; }

    bool empty() const noexcept { return segments_.empty(); }

    const std::vector<RowKeySegment>& segments() const noexcept { return segments_; }

private:
    std::vector<RowKeySegment> segments_;
    int                        max_col_index_ = -1;

    // Shared PRNG — seeded once at construction, NOT thread-safe.
    // Each RowKeyBuilder instance has its own RNG.
    std::mt19937 rng_{std::random_device{}()};

    /// Apply pad + reverse to one segment value.
    static std::string apply_segment(const RowKeySegment& seg, std::string val);

    /// Generate `len` random digits (0–8), matching UniverseHbaseBeanUtil.getRandomValue().
    std::string random_digits(int len);
};

// ─── rowValue helper ─────────────────────────────────────────────────────────

/// Split a pipe-delimited rowValue string into views.
/// Matches UniverseHbaseBeanUtil: StringUtils.splitPreserveAllTokens(rowValue, "|", maxCount)
/// maxCount: maximum number of tokens to split into (remaining text goes into last token).
/// maxCount <= 0 means split all.
std::vector<std::string_view> split_row_value(std::string_view row_value,
                                               int max_count = -1);

} // namespace arrow_convert
} // namespace hfile
