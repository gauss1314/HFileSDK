#pragma once

#include "types.h"
#include "status.h"
#include "writer_options.h"
#include <chrono>
#include <memory>
#include <string>
#include <span>

namespace hfile {

class HFileWriterImpl;  // forward declaration of impl

struct WriterStats {
    std::chrono::nanoseconds data_block_encode_ns{0};
    std::chrono::nanoseconds data_block_compress_ns{0};
    std::chrono::nanoseconds data_block_write_ns{0};
    std::chrono::nanoseconds leaf_index_write_ns{0};
    std::chrono::nanoseconds bloom_chunk_write_ns{0};
    std::chrono::nanoseconds load_on_open_write_ns{0};

    uint32_t data_block_count{0};
    uint32_t leaf_index_block_count{0};
    uint32_t bloom_chunk_flush_count{0};
    uint32_t load_on_open_block_count{0};
};

/// Single-file, single-CF HFile v3 writer.
/// Thread safety: NOT thread-safe. Use one writer per thread or serialize access.
class HFileWriter {
public:
    ~HFileWriter();

    // Non-copyable, movable
    HFileWriter(const HFileWriter&)            = delete;
    HFileWriter& operator=(const HFileWriter&) = delete;
    HFileWriter(HFileWriter&&) noexcept;
    HFileWriter& operator=(HFileWriter&&) noexcept;

    /// Append a single KeyValue cell.
    /// KVs must be appended in strictly ascending HBase key order
    /// (Row ASC → Family ASC → Qualifier ASC → Timestamp DESC → Type DESC).
    /// Returns an error if the sort invariant is violated (in Verified mode).
    Status append(const KeyValue& kv);

    /// Convenience overload with individual fields.
    Status append(
        std::span<const uint8_t> row,
        std::span<const uint8_t> family,
        std::span<const uint8_t> qualifier,
        int64_t                  timestamp,
        std::span<const uint8_t> value,
        KeyType                  key_type   = KeyType::Put,
        std::span<const uint8_t> tags       = {},
        uint64_t                 memstore_ts = 0);

    /// Fast path for callers that already guarantee:
    /// - row/family/value size validity
    /// - matching column family
    /// - final KV order
    ///
    /// Intended for high-throughput internal conversion paths.
    Status append_trusted(const KeyValue& kv);

    /// Trusted fast path for the first KV of a new row. This lets the writer
    /// skip previous-row comparison work in ROW bloom and last-key tracking.
    Status append_trusted_new_row(const KeyValue& kv);

    /// Trusted fast path for a KV that is guaranteed to continue the same row
    /// as the immediately previous KV. This lets the writer skip row-bloom
    /// change detection on subsequent qualifiers of the same row.
    Status append_trusted_same_row(const KeyValue& kv);

    /// Convenience overload for the trusted fast path.
    Status append_trusted(
        std::span<const uint8_t> row,
        std::span<const uint8_t> family,
        std::span<const uint8_t> qualifier,
        int64_t                  timestamp,
        std::span<const uint8_t> value,
        KeyType                  key_type   = KeyType::Put,
        std::span<const uint8_t> tags       = {},
        uint64_t                 memstore_ts = 0);

    Status append_trusted_new_row(
        std::span<const uint8_t> row,
        std::span<const uint8_t> family,
        std::span<const uint8_t> qualifier,
        int64_t                  timestamp,
        std::span<const uint8_t> value,
        KeyType                  key_type   = KeyType::Put,
        std::span<const uint8_t> tags       = {},
        uint64_t                 memstore_ts = 0);

    Status append_trusted_same_row(
        std::span<const uint8_t> row,
        std::span<const uint8_t> family,
        std::span<const uint8_t> qualifier,
        int64_t                  timestamp,
        std::span<const uint8_t> value,
        KeyType                  key_type   = KeyType::Put,
        std::span<const uint8_t> tags       = {},
        uint64_t                 memstore_ts = 0);

    /// Finalise the file (flush remaining block, write index/bloom/fileinfo/trailer).
    Status finish();

    /// Current file offset (bytes written so far).
    int64_t position() const noexcept;

    /// Number of KeyValue cells appended.
    uint64_t entry_count() const noexcept;

    /// Internal write-path timing and block counters.
    WriterStats stats() const noexcept;

    // ─── Builder ─────────────────────────────────────────────────────────────
    class Builder {
    public:
        Builder& set_path(std::string path);
        Builder& set_column_family(std::string cf);
        Builder& set_compression(Compression c);
        Builder& set_compression_level(int level);
        Builder& set_block_size(size_t sz);
        Builder& set_data_block_encoding(Encoding enc);
        Builder& set_bloom_type(BloomType bt);
        Builder& set_bloom_error_rate(double r);
        Builder& set_comparator(std::string cmp);
        Builder& set_file_create_time_ms(int64_t ts_ms);
        Builder& set_sort_mode(WriterOptions::SortMode m);
        Builder& set_include_tags(bool v);
        Builder& set_include_mvcc(bool v);

        // ── Production setters ───────────────────────────────────────────────
        Builder& set_fsync_policy(FsyncPolicy p);
        Builder& set_error_policy(ErrorPolicy p);
        Builder& set_max_error_count(uint64_t n);
        Builder& set_error_callback(std::function<void(const RowError&)> cb);
        Builder& set_max_row_key_bytes(uint32_t n);
        Builder& set_max_value_bytes(size_t n);
        Builder& set_max_memory(size_t bytes);
        Builder& set_compression_threads(uint32_t n);
        Builder& set_compression_queue_depth(uint32_t n);
        Builder& set_min_free_disk(size_t bytes);
        Builder& set_disk_check_interval(size_t bytes);
        Builder& set_max_open_files(int n);

        /// Build and open the writer.  Returns error if path can't be opened.
        std::pair<std::unique_ptr<HFileWriter>, Status> build();

    private:
        std::string    path_;
        WriterOptions  opts_;
    };

    static Builder builder() { return Builder{}; }

private:
    explicit HFileWriter(std::unique_ptr<HFileWriterImpl> impl);
    std::unique_ptr<HFileWriterImpl> impl_;
};

} // namespace hfile
