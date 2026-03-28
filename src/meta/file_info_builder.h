#pragma once

#include <hfile/types.h>
#include <hfile/status.h>
#include <map>
#include <string>
#include <vector>
#include <span>
#include <cstdint>
#include <cstring>
#include <chrono>

namespace hfile {
namespace meta {

/// Builds the FileInfo block required by HBase 2.6.1.
/// All mandatory fields must be set before calling finish().
class FileInfoBuilder {
public:
    FileInfoBuilder() = default;

    void set_last_key(std::span<const uint8_t> key) {
        set_bytes(std::string(fileinfo::kLastKey), key);
    }

    void set_avg_key_len(uint32_t v) {
        uint8_t buf[4]; write_be32(buf, v);
        set_bytes(std::string(fileinfo::kAvgKeyLen), {buf, 4});
    }

    void set_avg_value_len(uint32_t v) {
        uint8_t buf[4]; write_be32(buf, v);
        set_bytes(std::string(fileinfo::kAvgValueLen), {buf, 4});
    }

    void set_max_tags_len(uint32_t v) {
        uint8_t buf[4]; write_be32(buf, v);
        set_bytes(std::string(fileinfo::kMaxTagsLen), {buf, 4});
    }

    void set_key_value_version(uint32_t v = 1) {
        uint8_t buf[4]; write_be32(buf, v);
        set_bytes(std::string(fileinfo::kKeyValueVersion), {buf, 4});
    }

    void set_max_memstore_ts(uint64_t v = 0) {
        uint8_t buf[8]; write_be64(buf, v);
        set_bytes(std::string(fileinfo::kMaxMemstoreTsKey), {buf, 8});
    }

    void set_comparator(std::string_view cmp) {
        set_bytes(std::string(fileinfo::kComparator),
                  {reinterpret_cast<const uint8_t*>(cmp.data()), cmp.size()});
    }

    void set_data_block_encoding(Encoding enc) {
        const char* name = "";
        switch (enc) {
        case Encoding::None:     name = "NONE";      break;
        case Encoding::Prefix:   name = "PREFIX";    break;
        case Encoding::Diff:     name = "DIFF";      break;
        case Encoding::FastDiff: name = "FAST_DIFF"; break;
        }
        set_bytes(std::string(fileinfo::kDataBlockEncoding),
                  {reinterpret_cast<const uint8_t*>(name), strlen(name)});
    }

    void set_create_time(int64_t ts_ms = -1) {
        if (ts_ms < 0) {
            ts_ms = static_cast<int64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
        }
        uint8_t buf[8]; write_be64(buf, static_cast<uint64_t>(ts_ms));
        set_bytes(std::string(fileinfo::kCreateTimeTs), {buf, 8});
    }

    void set_len_of_biggest_cell(uint64_t v) {
        uint8_t buf[8]; write_be64(buf, v);
        set_bytes(std::string(fileinfo::kLenOfBiggestCell), {buf, 8});
    }

    /// Serialize the FileInfo block into `out`.
    /// Format: [numEntries(4B BE)] + sorted([keyLen(2B) + key + valueLen(4B) + value] …)
    void finish(std::vector<uint8_t>& out) const {
        size_t off = out.size();
        // Reserve space for entry count (4B)
        out.resize(off + 4);

        uint32_t count = 0;
        for (const auto& [k, v] : entries_) {
            size_t entry_off = out.size();
            out.resize(entry_off + 2 + k.size() + 4 + v.size());
            uint8_t* p = out.data() + entry_off;
            write_be16(p, static_cast<uint16_t>(k.size()));  p += 2;
            std::memcpy(p, k.data(), k.size());               p += k.size();
            write_be32(p, static_cast<uint32_t>(v.size()));   p += 4;
            std::memcpy(p, v.data(), v.size());
            ++count;
        }
        write_be32(out.data() + off, count);
    }

private:
    void set_bytes(std::string key, std::span<const uint8_t> value) {
        entries_[std::move(key)] = std::vector<uint8_t>(value.begin(), value.end());
    }

    // Sorted map → deterministic output
    std::map<std::string, std::vector<uint8_t>> entries_;
};

} // namespace meta
} // namespace hfile
