#pragma once

#include "types.h"
#include "status.h"
#include "writer_options.h"
#include <memory>
#include <string>
#include <span>

namespace hfile {

class HFileWriterImpl;  // forward declaration of impl

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

    /// Finalise the file (flush remaining block, write index/bloom/fileinfo/trailer).
    Status finish();

    /// Current file offset (bytes written so far).
    int64_t position() const noexcept;

    /// Number of KeyValue cells appended.
    uint64_t entry_count() const noexcept;

    // ─── Builder ─────────────────────────────────────────────────────────────
    class Builder {
    public:
        Builder& set_path(std::string path);
        Builder& set_column_family(std::string cf);
        Builder& set_compression(Compression c);
        Builder& set_block_size(size_t sz);
        Builder& set_data_block_encoding(Encoding enc);
        Builder& set_bloom_type(BloomType bt);
        Builder& set_bloom_error_rate(double r);
        Builder& set_comparator(std::string cmp);
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
