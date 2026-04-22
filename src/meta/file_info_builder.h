#pragma once

#include <hfile/types.h>
#include <hfile/status.h>
#include "hfile_file_info.pb.h"
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
        (void)enc;
        static constexpr char name[] = "NONE";
        set_bytes(std::string(fileinfo::kDataBlockEncoding),
                  {reinterpret_cast<const uint8_t*>(name), sizeof(name) - 1});
    }

    void set_bloom_filter_type(BloomType type) {
        const char* name = "";
        switch (type) {
        case BloomType::None:   name = "NONE"; break;
        case BloomType::Row:    name = "ROW"; break;
        case BloomType::RowCol: name = "ROWCOL"; break;
        }
        set_bytes(std::string(fileinfo::kBloomFilterType),
                  {reinterpret_cast<const uint8_t*>(name), strlen(name)});
    }

    void set_last_bloom_key(std::span<const uint8_t> key) {
        set_bytes(std::string(fileinfo::kLastBloomKey), key);
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

    void set_key_of_biggest_cell(std::span<const uint8_t> key) {
        set_bytes(std::string(fileinfo::kKeyOfBiggestCell), key);
    }

    void set_tags_compressed(bool compressed) {
        uint8_t value = compressed ? 1 : 0;
        set_bytes(std::string(fileinfo::kTagsCompressed), {&value, 1});
    }

    void set_delete_family_count(uint64_t v) {
        uint8_t buf[8]; write_be64(buf, v);
        set_bytes(std::string(fileinfo::kDeleteFamilyCount), {buf, 8});
    }

    void set_historical(bool historical) {
        uint8_t value = historical ? 1 : 0;
        set_bytes(std::string(fileinfo::kHistorical), {&value, 1});
    }

    Status validate_required_fields() const {
        static const std::string kRequiredKeys[] = {
            std::string(fileinfo::kLastKey),
            std::string(fileinfo::kAvgKeyLen),
            std::string(fileinfo::kAvgValueLen),
            std::string(fileinfo::kCreateTimeTs),
            std::string(fileinfo::kKeyOfBiggestCell),
            std::string(fileinfo::kLenOfBiggestCell),
            std::string(fileinfo::kDeleteFamilyCount),
            std::string(fileinfo::kHistorical),
        };
        for (const auto& key : kRequiredKeys) {
            if (!entries_.count(key))
                return Status::InvalidArg("FileInfo missing mandatory field: " + key);
        }
        if (entries_.count(std::string(fileinfo::kMaxTagsLen))
            && !entries_.count(std::string(fileinfo::kTagsCompressed))) {
            return Status::InvalidArg("FileInfo missing mandatory field: "
                                      + std::string(fileinfo::kTagsCompressed));
        }
        return Status::OK();
    }

    /// Serialize the FileInfo block into `out`.
    /// Format: [PBUF magic][delimited FileInfoProto]
    void finish(std::vector<uint8_t>& out) const {
        static constexpr uint8_t kPBMagic[] = {'P','B','U','F'};

        pb::FileInfoProto proto;
        for (const auto& [k, v] : entries_) {
            auto* entry = proto.add_map_entry();
            entry->set_first(k);
            entry->set_second(v.data(), v.size());
        }

        std::string pb_bytes;
        proto.SerializeToString(&pb_bytes);
        uint8_t varint_buf[10];
        int varint_len = encode_varint64(varint_buf, pb_bytes.size());

        size_t off = out.size();
        out.resize(off + sizeof(kPBMagic) + static_cast<size_t>(varint_len) + pb_bytes.size());
        uint8_t* p = out.data() + off;
        std::memcpy(p, kPBMagic, sizeof(kPBMagic));
        p += sizeof(kPBMagic);
        std::memcpy(p, varint_buf, varint_len);
        p += varint_len;
        std::memcpy(p, pb_bytes.data(), pb_bytes.size());
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
