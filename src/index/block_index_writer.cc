#include "block_index_writer.h"
#include <hfile/types.h>
#include <cassert>
#include <cstring>

namespace hfile {
namespace index {

// ─── add_entry ────────────────────────────────────────────────────────────────

void BlockIndexWriter::add_entry(std::span<const uint8_t> first_key,
                                 int64_t offset,
                                 int32_t data_size) {
    IndexEntry e;
    e.first_key.assign(first_key.begin(), first_key.end());
    e.offset    = offset;
    e.data_size = data_size;
    entries_.push_back(std::move(e));
}

// ─── Entry serialisation ──────────────────────────────────────────────────────
//
// Each index entry:   offset(8B BE) + dataSize(4B BE) + keyLen(4B BE) + key

void BlockIndexWriter::write_entry(const IndexEntry& e, std::vector<uint8_t>& buf) {
    const size_t entry_size = 8 + 4 + 4 + e.first_key.size();
    size_t off = buf.size();
    buf.resize(off + entry_size);
    uint8_t* p = buf.data() + off;
    write_be64(p, static_cast<uint64_t>(e.offset));           p += 8;
    write_be32(p, static_cast<uint32_t>(e.data_size));         p += 4;
    write_be32(p, static_cast<uint32_t>(e.first_key.size()));  p += 4;
    std::memcpy(p, e.first_key.data(), e.first_key.size());
}

// ─── Intermediate block serialisation ────────────────────────────────────────
//
// Format:  [Block Header 33B][entryCount(4B BE)][entry_0][entry_1]...[entry_N]
// Magic:   IDXINTE2

size_t BlockIndexWriter::write_intermediate_block(
        std::span<const IndexEntry* const> entries,
        int64_t prev_block_offset,
        std::vector<uint8_t>& buf) {

    // Compute payload size (entries only, no block header)
    size_t payload = 4;  // entry count (4B)
    for (const auto* e : entries)
        payload += 8 + 4 + 4 + e->first_key.size();

    // Write block header
    const size_t start = buf.size();
    buf.resize(start + kBlockHeaderSize + payload);
    uint8_t* p = buf.data() + start;

    std::memcpy(p, kIntermedIdxMagic.data(), 8); p += 8;
    write_be32(p, static_cast<uint32_t>(payload)); p += 4;  // compressedSz
    write_be32(p, static_cast<uint32_t>(payload)); p += 4;  // uncompressedSz
    write_be64(p, static_cast<uint64_t>(prev_block_offset)); p += 8;
    *p++ = kChecksumTypeCRC32C;
    write_be32(p, kBytesPerChecksum); p += 4;
    write_be32(p, static_cast<uint32_t>(payload)); p += 4;  // onDiskDataSz

    // Write entry count
    write_be32(p, static_cast<uint32_t>(entries.size())); p += 4;

    // Write entries
    for (const auto* e : entries) {
        write_be64(p, static_cast<uint64_t>(e->offset));           p += 8;
        write_be32(p, static_cast<uint32_t>(e->data_size));         p += 4;
        write_be32(p, static_cast<uint32_t>(e->first_key.size()));  p += 4;
        std::memcpy(p, e->first_key.data(), e->first_key.size());
        p += e->first_key.size();
    }

    assert(static_cast<size_t>(p - (buf.data() + start)) == kBlockHeaderSize + payload);
    return kBlockHeaderSize + payload;
}

// ─── finish ───────────────────────────────────────────────────────────────────

IndexWriteResult BlockIndexWriter::finish(int64_t              intermed_start_offset,
                                           std::vector<uint8_t>& intermed_blocks_out,
                                           std::vector<uint8_t>& root_out) {
    IndexWriteResult result;
    result.num_root_entries = static_cast<uint32_t>(entries_.size());

    // ── 1-level: all entries fit directly in the root ─────────────────────────
    if (entries_.size() <= max_per_block_) {
        result.num_levels = 1;

        // root payload: count(4B) + entries
        root_out.resize(root_out.size() + 4);
        write_be32(root_out.data() + root_out.size() - 4,
                   static_cast<uint32_t>(entries_.size()));
        for (const auto& e : entries_)
            write_entry(e, root_out);

        result.uncompressed_size = static_cast<uint64_t>(root_out.size());
        return result;
    }

    // ── 2-level: split entries into intermediate blocks ───────────────────────
    result.num_levels = 2;

    // Build a root entry per intermediate block.
    // Each intermediate block covers up to max_per_block_ data block entries.
    std::vector<IndexEntry> root_entries;

    int64_t  prev_intermed_offset = -1;
    int64_t  cur_offset           = intermed_start_offset;
    size_t   i                    = 0;

    while (i < entries_.size()) {
        size_t chunk_end = std::min(i + max_per_block_, entries_.size());

        // Collect pointers for this chunk
        std::vector<const IndexEntry*> chunk_ptrs;
        chunk_ptrs.reserve(chunk_end - i);
        for (size_t j = i; j < chunk_end; ++j)
            chunk_ptrs.push_back(&entries_[j]);

        // Write intermediate block into intermed_blocks_out
        size_t block_bytes = write_intermediate_block(
            std::span<const IndexEntry* const>{chunk_ptrs.data(), chunk_ptrs.size()},
            prev_intermed_offset,
            intermed_blocks_out);

        // Build root entry pointing to this intermediate block.
        // Root entry first_key = first data-block key covered by this chunk.
        IndexEntry re;
        re.first_key = entries_[i].first_key;
        re.offset    = cur_offset;
        re.data_size = static_cast<int32_t>(block_bytes);
        root_entries.push_back(std::move(re));

        prev_intermed_offset = cur_offset;
        cur_offset          += static_cast<int64_t>(block_bytes);
        i                    = chunk_end;
    }

    // Write root payload: count(4B) + root_entries
    size_t root_start = root_out.size();
    root_out.resize(root_start + 4);
    write_be32(root_out.data() + root_start,
               static_cast<uint32_t>(root_entries.size()));
    for (const auto& re : root_entries)
        write_entry(re, root_out);

    result.num_root_entries  = static_cast<uint32_t>(root_entries.size());
    result.uncompressed_size = static_cast<uint64_t>(
        intermed_blocks_out.size() + root_out.size());
    return result;
}

} // namespace index
} // namespace hfile
