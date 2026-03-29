#pragma once

#include <hfile/types.h>
#include <hfile/status.h>
#include <vector>
#include <cstdint>
#include <string>

// Generated protobuf header (built by CMake)
#include "hfile_trailer.pb.h"

namespace hfile {
namespace meta {

/// Builds and serializes the HFile v3 Trailer.
///
/// Layout at file tail:
///   [TRABLK"$]                                 (8 bytes)
///   [delimited FileTrailerProto]               (variable)
///   [zero padding]                             (to fixed size - 4)
///   [version: uint32 LE-like materialized int] (4 bytes)
class TrailerBuilder {
public:
    TrailerBuilder() = default;

    void set_file_info_offset(uint64_t v)              { proto_.set_file_info_offset(v); }
    void set_load_on_open_offset(uint64_t v)           { proto_.set_load_on_open_data_offset(v); }
    void set_uncompressed_data_index_size(uint64_t v)  { proto_.set_uncompressed_data_index_size(v); }
    void set_total_uncompressed_bytes(uint64_t v)      { proto_.set_total_uncompressed_bytes(v); }
    void set_data_index_count(uint32_t v)              { proto_.set_data_index_count(v); }
    void set_meta_index_count(uint32_t v)              { proto_.set_meta_index_count(v); }
    void set_entry_count(uint64_t v)                   { proto_.set_entry_count(v); }
    void set_num_data_index_levels(uint32_t v)         { proto_.set_num_data_index_levels(v); }
    void set_first_data_block_offset(uint64_t v)       { proto_.set_first_data_block_offset(v); }
    void set_last_data_block_offset(uint64_t v)        { proto_.set_last_data_block_offset(v); }
    void set_comparator_class_name(const std::string& v) { proto_.set_comparator_class_name(v); }
    void set_compression_codec(uint32_t v)             { proto_.set_compression_codec(v); }

    /// Serialize trailer and append to `out`.
    Status finish(std::vector<uint8_t>& out) const {
        std::string pb_bytes;
        if (!proto_.SerializeToString(&pb_bytes))
            return Status::Internal("Failed to serialize FileTrailerProto");

        uint8_t delimited_prefix[10];
        int prefix_len = encode_varint64(delimited_prefix, pb_bytes.size());
        size_t payload_size = kBlockMagicSize + static_cast<size_t>(prefix_len)
                            + pb_bytes.size() + kTrailerVersionSize;
        if (payload_size > kTrailerFixedSize) {
            return Status::Internal("Trailer exceeds fixed HBase trailer size");
        }

        size_t off = out.size();
        out.resize(off + kTrailerFixedSize, 0);
        uint8_t* p = out.data() + off;

        std::memcpy(p, kTrailerBlockMagic.data(), kTrailerBlockMagic.size());
        p += kTrailerBlockMagic.size();
        std::memcpy(p, delimited_prefix, prefix_len);
        p += prefix_len;
        std::memcpy(p, pb_bytes.data(), pb_bytes.size());

        uint32_t materialized_version =
            (static_cast<uint32_t>(kHFileMinorVersion) << 24) |
            (static_cast<uint32_t>(kHFileMajorVersion) & 0x00FFFFFFu);
        write_be32(out.data() + off + kTrailerFixedSize - kTrailerVersionSize,
                   materialized_version);

        return Status::OK();
    }

private:
    hfile::pb::FileTrailerProto proto_;
};

} // namespace meta
} // namespace hfile
