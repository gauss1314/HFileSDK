#pragma once

#include <hfile/types.h>
#include <hfile/status.h>
#include <vector>
#include <span>
#include <cstdint>
#include <memory>

namespace hfile {
namespace index {

struct IndexEntry {
    std::vector<uint8_t> first_key;
    int64_t              offset{0};
    int32_t              data_size{0};
};

struct IndexWriteResult {
    int64_t  root_index_offset{0};   // offset of the root index block in the file
    uint32_t num_root_entries{0};    // entries in the root index
    int32_t  num_levels{1};          // 1 = inline root only; 2 = intermediate + root
    uint64_t uncompressed_size{0};   // total bytes of index data (for Trailer field)
};

/// Multi-level data block index writer (compatible with HBase HFile v2/v3).
///
/// Level structure:
///   1-level (entries ≤ max_entries_per_block):
///     Root index entries point directly to data blocks.
///
///   2-level (entries > max_entries_per_block):
///     Intermediate index blocks (magic IDXINTE2) are stored first.
///     Each holds up to max_entries_per_block entries pointing to data blocks.
///     The root index then points to the intermediate blocks.
///
/// Usage (from writer.cc):
///   // During normal writing:
///   index_writer_.add_entry(first_key, data_block_offset, data_block_size);
///
///   // At finish time:
///   std::vector<uint8_t> intermed_buf;   // receives serialised intermediate blocks
///   std::vector<uint8_t> root_buf;       // receives root-level index payload
///   int64_t intermed_start = writer->position();
///   auto result = index_writer_.finish(intermed_start, intermed_buf, root_buf);
///   if (!intermed_buf.empty())
///       writer->write(intermed_buf);     // write intermediate blocks first
///   // caller then wraps root_buf in a block header with kRootIndexMagic
class BlockIndexWriter {
public:
    /// max_entries_per_block: how many entries fit in one index block before
    /// a new intermediate block is started (and the root level is used).
    /// HBase default corresponds to ~128 KB per index block; with ~30 bytes
    /// per entry that is ~4000 entries.  A value of 128 is a reasonable
    /// smaller default that triggers 2-level indexes earlier for testing.
    explicit BlockIndexWriter(size_t max_entries_per_block = 128)
        : max_per_block_{max_entries_per_block} {}

    /// Record a data block at file position `offset`.
    void add_entry(std::span<const uint8_t> first_key,
                   int64_t offset,
                   int32_t data_size);

    /// Build the index.
    ///
    /// @param intermed_start_offset  File position where the caller will write
    ///                               `intermed_blocks_out` (needed so the root
    ///                               entries carry correct offsets).
    /// @param intermed_blocks_out    Receives raw bytes of intermediate index
    ///                               blocks (empty for 1-level indexes).
    /// @param root_out               Receives the root index payload (the caller
    ///                               wraps this in a block with kRootIndexMagic).
    IndexWriteResult finish(int64_t              intermed_start_offset,
                            std::vector<uint8_t>& intermed_blocks_out,
                            std::vector<uint8_t>& root_out);

    size_t num_entries() const noexcept { return entries_.size(); }

private:
    /// Append one entry's bytes to `buf`.
    static void write_entry(const IndexEntry& e, std::vector<uint8_t>& buf);

    /// Append a full intermediate index block (header + entries) to `buf`.
    /// Returns the total byte size written.
    static size_t write_intermediate_block(std::span<const IndexEntry* const> entries,
                                            int64_t prev_block_offset,
                                            std::vector<uint8_t>& buf);

    size_t                  max_per_block_;
    std::vector<IndexEntry> entries_;
};

} // namespace index
} // namespace hfile
