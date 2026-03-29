#include "row_key_builder.h"

#include <algorithm>
#include <stdexcept>
#include <sstream>
#include <charconv>

namespace hfile {
namespace arrow_convert {

// ─── split helpers ────────────────────────────────────────────────────────────

/// Split `s` by `delim`, returning string_views into `s`.
static std::vector<std::string_view> split_sv(std::string_view s, char delim,
                                               int max_count = -1) {
    std::vector<std::string_view> parts;
    size_t start = 0;
    while (start <= s.size()) {
        if (max_count > 0 && static_cast<int>(parts.size()) + 1 >= max_count) {
            parts.emplace_back(s.data() + start, s.size() - start);
            break;
        }
        size_t end = s.find(delim, start);
        if (end == std::string_view::npos) end = s.size();
        parts.emplace_back(s.data() + start, end - start);
        start = end + 1;
    }
    return parts;
}

/// Case-insensitive string comparison
static bool iequal(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i)
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) return false;
    return true;
}

// ─── RowKeyBuilder::compile ───────────────────────────────────────────────────

std::pair<RowKeyBuilder, Status> RowKeyBuilder::compile(const std::string& rule) {
    RowKeyBuilder builder;

    if (rule.empty())
        return {std::move(builder), Status::InvalidArg("rowKeyRule is empty")};

    // Split by '#' to get individual segments
    auto seg_strs = split_sv(rule, '#');
    if (seg_strs.empty())
        return {std::move(builder), Status::InvalidArg("rowKeyRule has no segments")};

    for (const auto& seg_sv : seg_strs) {
        if (seg_sv.empty()) continue;  // skip empty segments (trailing #)

        // Split segment by ',' to get fields
        auto fields = split_sv(seg_sv, ',');
        if (fields.size() < 4)
            return {std::move(builder),
                    Status::InvalidArg(
                        "rowKeyRule segment too short (need ≥4 fields): " +
                        std::string(seg_sv))};

        RowKeySegment seg;
        seg.name = std::string(fields[0]);

        // index — field[1]
        {
            int idx = 0;
            auto [ptr, ec] = std::from_chars(fields[1].data(),
                                              fields[1].data() + fields[1].size(), idx);
            if (ec != std::errc{})
                return {std::move(builder),
                        Status::InvalidArg("rowKeyRule: invalid index '" +
                                           std::string(fields[1]) + "'")};
            seg.col_index = idx;
        }

        // isReverse — field[2]: "true" / "false"
        seg.reverse = iequal(fields[2], "true");

        // padLen — field[3]
        {
            int pl = 0;
            auto [ptr, ec] = std::from_chars(fields[3].data(),
                                              fields[3].data() + fields[3].size(), pl);
            if (ec != std::errc{})
                return {std::move(builder),
                        Status::InvalidArg("rowKeyRule: invalid padLen '" +
                                           std::string(fields[3]) + "'")};
            seg.pad_len = pl;
        }

        // padMode — field[4] (optional, default LEFT)
        if (fields.size() >= 5)
            seg.pad_right = iequal(fields[4], "RIGHT");

        // padContent — field[5] (optional, default '0')
        if (fields.size() >= 6 && !fields[5].empty())
            seg.pad_char = fields[5][0];

        // Detect special names
        if (seg.name == "$RND$" || seg.name == "RND$" || seg.name == "$RND")
            seg.type = RowKeySegment::Type::Random;
        else
            seg.type = RowKeySegment::Type::ColumnRef;

        if (seg.type == RowKeySegment::Type::ColumnRef &&
            seg.col_index > builder.max_col_index_)
            builder.max_col_index_ = seg.col_index;

        builder.segments_.push_back(std::move(seg));
    }

    if (builder.segments_.empty())
        return {std::move(builder), Status::InvalidArg("rowKeyRule produced no segments")};

    return {std::move(builder), Status::OK()};
}

// ─── RowKeyBuilder::build ─────────────────────────────────────────────────────

std::string RowKeyBuilder::build(const std::vector<std::string_view>& fields) {
    std::string result;
    result.reserve(64);

    for (const auto& seg : segments_) {
        if (seg.type == RowKeySegment::Type::Random) {
            result += random_digits(seg.pad_len);
            continue;
        }

        // ColumnRef: get field value
        std::string val;
        if (seg.col_index < static_cast<int>(fields.size()))
            val = std::string(fields[static_cast<size_t>(seg.col_index)]);
        // else val = "" (out of range → empty string, like Java's null handling)

        result += apply_segment(seg, std::move(val));
    }
    return result;
}

// ─── RowKeyBuilder::apply_segment ─────────────────────────────────────────────

std::string RowKeyBuilder::apply_segment(const RowKeySegment& seg, std::string val) {
    // 1. Padding (only if padLen > 0 and value is shorter)
    if (seg.pad_len > 0 && static_cast<int>(val.size()) < seg.pad_len) {
        int deficit = seg.pad_len - static_cast<int>(val.size());
        std::string pad(static_cast<size_t>(deficit), seg.pad_char);
        if (seg.pad_right)
            val += pad;        // StringUtils.rightPad
        else
            val = pad + val;   // StringUtils.leftPad
    }

    // 2. Reverse (after padding, matching Java setValue order)
    if (seg.reverse) {
        std::reverse(val.begin(), val.end());
    }

    return val;
}

// ─── RowKeyBuilder::random_digits ────────────────────────────────────────────

std::string RowKeyBuilder::random_digits(int len) {
    // Matches UniverseHbaseBeanUtil.getRandomValue():
    //   RANDOM.nextInt(SEED) where SEED = 9  → digits 0–8 (NOT 0–9)
    std::uniform_int_distribution<int> dist(0, 8);
    std::string result;
    result.reserve(static_cast<size_t>(len));
    for (int i = 0; i < len; ++i)
        result += static_cast<char>('0' + dist(rng_));
    return result;
}

// ─── split_row_value ──────────────────────────────────────────────────────────

std::vector<std::string_view> split_row_value(std::string_view row_value,
                                               int max_count) {
    return split_sv(row_value, '|', max_count);
}

} // namespace arrow_convert
} // namespace hfile
