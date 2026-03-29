#pragma once

#include <hfile/status.h>

#include <cctype>
#include <cstdio>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace hfile {
namespace jni {

struct JsonConfigValue {
    bool        is_string{false};
    std::string text;
};

using JsonConfigObject = std::unordered_map<std::string, JsonConfigValue>;

inline std::string json_escape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (unsigned char c : s) {
        switch (c) {
        case '\\': out += "\\\\"; break;
        case '"':  out += "\\\""; break;
        case '\b': out += "\\b"; break;
        case '\f': out += "\\f"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default:
            if (c < 0x20) {
                char buf[7];
                std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                out += buf;
            } else {
                out += static_cast<char>(c);
            }
        }
    }
    return out;
}

inline void skip_ws(std::string_view s, size_t* pos) {
    while (*pos < s.size() &&
           std::isspace(static_cast<unsigned char>(s[*pos]))) {
        ++(*pos);
    }
}

inline Status parse_json_string(std::string_view s, size_t* pos, std::string* out) {
    if (*pos >= s.size() || s[*pos] != '"')
        return Status::InvalidArg("JSON parse error: expected string");
    ++(*pos);
    out->clear();
    while (*pos < s.size()) {
        char c = s[*pos];
        ++(*pos);
        if (c == '"') return Status::OK();
        if (c == '\\') {
            if (*pos >= s.size())
                return Status::InvalidArg("JSON parse error: incomplete escape");
            char esc = s[*pos];
            ++(*pos);
            switch (esc) {
            case '"':  out->push_back('"'); break;
            case '\\': out->push_back('\\'); break;
            case '/':  out->push_back('/'); break;
            case 'b':  out->push_back('\b'); break;
            case 'f':  out->push_back('\f'); break;
            case 'n':  out->push_back('\n'); break;
            case 'r':  out->push_back('\r'); break;
            case 't':  out->push_back('\t'); break;
            default:
                return Status::InvalidArg("JSON parse error: unsupported escape");
            }
            continue;
        }
        out->push_back(c);
    }
    return Status::InvalidArg("JSON parse error: unterminated string");
}

inline Status parse_json_integer(std::string_view s, size_t* pos, std::string* out) {
    size_t start = *pos;
    if (*pos < s.size() && (s[*pos] == '-' || s[*pos] == '+')) ++(*pos);
    size_t digits_start = *pos;
    while (*pos < s.size() && std::isdigit(static_cast<unsigned char>(s[*pos])))
        ++(*pos);
    if (digits_start == *pos)
        return Status::InvalidArg("JSON parse error: expected integer");
    out->assign(s.substr(start, *pos - start));
    return Status::OK();
}

inline Status parse_json_config(std::string_view json, JsonConfigObject* out) {
    out->clear();
    size_t pos = 0;
    skip_ws(json, &pos);
    if (pos >= json.size() || json[pos] != '{')
        return Status::InvalidArg("JSON parse error: expected '{'");
    ++pos;
    skip_ws(json, &pos);
    if (pos < json.size() && json[pos] == '}') {
        ++pos;
        skip_ws(json, &pos);
        return pos == json.size()
            ? Status::OK()
            : Status::InvalidArg("JSON parse error: trailing characters");
    }

    while (pos < json.size()) {
        std::string key;
        HFILE_RETURN_IF_ERROR(parse_json_string(json, &pos, &key));
        skip_ws(json, &pos);
        if (pos >= json.size() || json[pos] != ':')
            return Status::InvalidArg("JSON parse error: expected ':'");
        ++pos;
        skip_ws(json, &pos);

        JsonConfigValue value;
        if (pos < json.size() && json[pos] == '"') {
            value.is_string = true;
            HFILE_RETURN_IF_ERROR(parse_json_string(json, &pos, &value.text));
        } else {
            HFILE_RETURN_IF_ERROR(parse_json_integer(json, &pos, &value.text));
        }

        if (out->find(key) != out->end())
            return Status::InvalidArg("JSON parse error: duplicate key '" + key + "'");
        out->emplace(std::move(key), std::move(value));

        skip_ws(json, &pos);
        if (pos >= json.size())
            return Status::InvalidArg("JSON parse error: expected ',' or '}'");
        if (json[pos] == '}') {
            ++pos;
            skip_ws(json, &pos);
            return pos == json.size()
                ? Status::OK()
                : Status::InvalidArg("JSON parse error: trailing characters");
        }
        if (json[pos] != ',')
            return Status::InvalidArg("JSON parse error: expected ','");
        ++pos;
        skip_ws(json, &pos);
    }
    return Status::InvalidArg("JSON parse error: unterminated object");
}

inline std::optional<std::string> config_string(
        const JsonConfigObject& obj, const std::string& key) {
    auto it = obj.find(key);
    if (it == obj.end()) return std::nullopt;
    if (!it->second.is_string) return std::nullopt;
    return it->second.text;
}

inline std::optional<int64_t> config_int(
        const JsonConfigObject& obj, const std::string& key) {
    auto it = obj.find(key);
    if (it == obj.end()) return std::nullopt;
    if (it->second.is_string) return std::nullopt;
    try {
        return std::stoll(it->second.text);
    } catch (...) {
        return std::nullopt;
    }
}

} // namespace jni
} // namespace hfile
