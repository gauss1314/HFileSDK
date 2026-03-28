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
///   [ProtoBuf bytes of FileTrailerProto]       (variable)
///   [protobuf_start_offset: uint32 BE]         (4 bytes)
///   [major_version: uint32 BE = 3]             (4 bytes)
///   [minor_version: uint32 BE = 3]             (4 bytes)
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
    /// Must be called after all other data is written; `file_size_before_trailer`
    /// is the current file position (== total bytes before this call).
    Status finish(std::vector<uint8_t>& out) const {
        std::string pb_bytes;
        if (!proto_.SerializeToString(&pb_bytes))
            return Status::Internal("Failed to serialize FileTrailerProto");

        size_t pb_size = pb_bytes.size();
        size_t off     = out.size();

        // PB bytes + 4 (pb_offset) + 4 (major) + 4 (minor)
        out.resize(off + pb_size + kTrailerTailSize);
        uint8_t* p = out.data() + off;

        std::memcpy(p, pb_bytes.data(), pb_size);
        p += pb_size;

        // protobuf_start_offset: offset from end of file to start of PB
        // = pb_size + 12  (includes the 12 fixed tail bytes)
        uint32_t pb_offset = static_cast<uint32_t>(pb_size + kTrailerTailSize);
        write_be32(p, pb_offset);       p += 4;
        write_be32(p, kHFileMajorVersion); p += 4;
        write_be32(p, kHFileMinorVersion); p += 4;

        return Status::OK();
    }

private:
    hfile::pb::FileTrailerProto proto_;
};

} // namespace meta
} // namespace hfile
