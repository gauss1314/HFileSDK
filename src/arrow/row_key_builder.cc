#include "row_key_builder.h"
#include <hfile/types.h>

#include <algorithm>
#include <charconv>
#include <array>
#include <cctype>
#include <limits>

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

static bool is_random_name(std::string_view name) noexcept {
    return name == "$RND$" || name == "RND$" || name == "$RND" ||
           iequal(name, "RANDOM") || iequal(name, "RANDOM_COL");
}

static bool is_fill_name(std::string_view name) noexcept {
    return iequal(name, "FILL") || iequal(name, "FILL_COL");
}

static bool parse_encode_expr(std::string_view name, RowKeySegment& seg, Status& err) {
    auto open = name.find('(');
    if (open == std::string_view::npos)
        return false;
    if (name.back() != ')') {
        err = Status::InvalidArg("rowKeyRule: malformed encoded segment '" + std::string(name) + "'");
        return true;
    }

    auto outer = name.substr(0, open);
    if (iequal(outer, "long")) {
        seg.type = RowKeySegment::Type::EncodedColumn;
        seg.encode_kind = RowKeySegment::EncodeKind::Int64Base64;
    } else if (iequal(outer, "short")) {
        seg.type = RowKeySegment::Type::EncodedColumn;
        seg.encode_kind = RowKeySegment::EncodeKind::Int16Base64;
    } else {
        err = Status::InvalidArg("rowKeyRule: unsupported encoded segment '" + std::string(name) + "'");
        return true;
    }

    std::string_view inner = name.substr(open + 1, name.size() - open - 2);
    while (!inner.empty()) {
        auto next = inner.find('(');
        std::string_view fn = next == std::string_view::npos ? inner : inner.substr(0, next);
        if (iequal(fn, "hash")) {
            seg.transforms.push_back(RowKeySegment::Transform::Hash);
        } else {
            err = Status::InvalidArg("rowKeyRule: unsupported transform '" + std::string(fn) + "'");
            return true;
        }
        if (next == std::string_view::npos)
            break;
        inner = inner.substr(next + 1);
        if (inner.empty()) {
            err = Status::InvalidArg("rowKeyRule: malformed encoded segment '" + std::string(name) + "'");
            return true;
        }
        if (inner.back() != ')') {
            err = Status::InvalidArg("rowKeyRule: malformed encoded segment '" + std::string(name) + "'");
            return true;
        }
        inner.remove_suffix(1);
    }
    return true;
}

static bool java_hash_numeric_string(std::string_view value, int16_t& out) {
    long long tmp = 0;
    auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), tmp);
    if (ec != std::errc{} || ptr != value.data() + value.size())
        return false;

    long long part1 = tmp >> 32;
    long long part2 = tmp & ((2LL << 32) - 1LL);
    long long result = 1LL;
    result = 31LL * result + part1;
    result = 31LL * result + part2;
    out = static_cast<int16_t>(result % 65535LL);
    return true;
}

static bool parse_i64(std::string_view sv, int64_t& out) {
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), out);
    return ec == std::errc{} && ptr == sv.data() + sv.size();
}

static bool parse_i16(std::string_view sv, int16_t& out) {
    int tmp = 0;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), tmp);
    if (ec != std::errc{} || ptr != sv.data() + sv.size())
        return false;
    if (tmp < static_cast<int>(std::numeric_limits<int16_t>::min()) ||
        tmp > static_cast<int>(std::numeric_limits<int16_t>::max()))
        return false;
    out = static_cast<int16_t>(tmp);
    return true;
}

static std::string base64_encode(std::span<const uint8_t> data) {
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

static Status encode_long_or_short(const RowKeySegment& seg,
                                   std::string_view value,
                                   std::string* out) {
    std::string encoded(value);
    for (auto it = seg.transforms.rbegin(); it != seg.transforms.rend(); ++it) {
        switch (*it) {
        case RowKeySegment::Transform::Hash:
            int16_t hashed = 0;
            if (!java_hash_numeric_string(encoded, hashed))
                return Status::InvalidArg("rowKeyRule encoded segment '" + seg.name +
                                          "' requires numeric input, got '" + encoded + "'");
            encoded = std::to_string(hashed);
            break;
        }
    }

    if (seg.encode_kind == RowKeySegment::EncodeKind::Int64Base64) {
        int64_t parsed = 0;
        if (!parse_i64(encoded, parsed))
            return Status::InvalidArg("rowKeyRule encoded segment '" + seg.name +
                                      "' cannot parse int64 value '" + encoded + "'");
        std::array<uint8_t, 8> bytes{};
        write_be64(bytes.data(), static_cast<uint64_t>(parsed));
        *out = base64_encode(bytes);
        return Status::OK();
    }

    int16_t parsed = 0;
    if (!parse_i16(encoded, parsed))
        return Status::InvalidArg("rowKeyRule encoded segment '" + seg.name +
                                  "' cannot parse int16 value '" + encoded + "'");
    std::array<uint8_t, 2> bytes{};
    write_be16(bytes.data(), static_cast<uint16_t>(parsed));
    *out = base64_encode(bytes);
    return Status::OK();
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
            if (ec != std::errc{} || ptr != fields[1].data() + fields[1].size())
                return {std::move(builder),
                        Status::InvalidArg("rowKeyRule: invalid index '" +
                                           std::string(fields[1]) + "'")};
            if (idx < 0)
                return {std::move(builder),
                        Status::InvalidArg("rowKeyRule: index must be >= 0")};
            seg.col_index = idx;
        }

        // isReverse — field[2]: "true" / "false"
        if (iequal(fields[2], "true")) seg.reverse = true;
        else if (iequal(fields[2], "false")) seg.reverse = false;
        else
            return {std::move(builder),
                    Status::InvalidArg("rowKeyRule: isReverse must be true or false, got '" +
                                       std::string(fields[2]) + "'")};

        // padLen — field[3]
        {
            int pl = 0;
            auto [ptr, ec] = std::from_chars(fields[3].data(),
                                              fields[3].data() + fields[3].size(), pl);
            if (ec != std::errc{} || ptr != fields[3].data() + fields[3].size())
                return {std::move(builder),
                        Status::InvalidArg("rowKeyRule: invalid padLen '" +
                                           std::string(fields[3]) + "'")};
            if (pl < 0)
                return {std::move(builder),
                        Status::InvalidArg("rowKeyRule: padLen must be >= 0")};
            seg.pad_len = pl;
        }

        // padMode — field[4] (optional, default LEFT)
        if (fields.size() >= 5) {
            if (iequal(fields[4], "RIGHT")) seg.pad_right = true;
            else if (iequal(fields[4], "LEFT")) seg.pad_right = false;
            else
                return {std::move(builder),
                        Status::InvalidArg("rowKeyRule: padMode must be LEFT or RIGHT, got '" +
                                           std::string(fields[4]) + "'")};
        }

        // padContent — field[5] (optional, default '0')
        if (fields.size() >= 6 && !fields[5].empty())
            seg.pad_char = fields[5][0];

        // Detect special names
        Status expr_status = Status::OK();
        if (parse_encode_expr(seg.name, seg, expr_status)) {
            if (!expr_status.ok())
                return {std::move(builder), expr_status};
        } else if (is_random_name(seg.name))
            seg.type = RowKeySegment::Type::Random;
        else if (is_fill_name(seg.name))
            seg.type = RowKeySegment::Type::Fill;
        else
            seg.type = RowKeySegment::Type::ColumnRef;

        if ((seg.type == RowKeySegment::Type::ColumnRef ||
             seg.type == RowKeySegment::Type::EncodedColumn) &&
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
    std::string out;
    auto status = build_checked(fields, &out);
    if (!status.ok()) return "";
    return out;
}

Status RowKeyBuilder::build_checked(const std::vector<std::string_view>& fields, std::string* out) {
    out->clear();
    out->reserve(64);

    for (const auto& seg : segments_) {
        if (seg.type == RowKeySegment::Type::Random) {
            *out += random_digits(seg.pad_len);
            continue;
        }

        std::string val;
        if (seg.type == RowKeySegment::Type::Fill) {
            val.clear();
        } else {
            if (seg.col_index >= static_cast<int>(fields.size()))
                return Status::InvalidArg("rowKeyRule references missing field index " +
                                          std::to_string(seg.col_index));
            val = std::string(fields[static_cast<size_t>(seg.col_index)]);
        }

        if (seg.type == RowKeySegment::Type::EncodedColumn) {
            HFILE_RETURN_IF_ERROR(encode_long_or_short(seg, val, &val));
        }

        *out += apply_segment(seg, std::move(val));
    }
    return Status::OK();
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
