#pragma once

#include "types.h"
#include "status.h"
#include "writer_options.h"
#include "region_partitioner.h"
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <chrono>

// Forward declare Arrow types
namespace arrow { class RecordBatch; class Schema; }

namespace hfile {

/// Mapping strategy for Arrow RecordBatch → HBase KeyValue
enum class MappingMode {
    /// Each row expands to multiple KVs (one per non-null column).
    /// Schema must include a `__row_key__` column.
    WideTable,

    /// Each row → one KV. Schema: row_key, cf, qualifier, timestamp, value.
    TallTable,

    /// Two columns: key (pre-encoded HBase key bytes) + value.
    RawKV,
};

struct BulkLoadResult {
    // ── Output ─────────────────────────────────────────────────────────────
    std::string staging_dir;
    std::vector<std::string> files;         // successfully written: "cf/hfile_N.hfile"
    std::vector<std::string> failed_files;  // files that could not be committed

    // ── Statistics ─────────────────────────────────────────────────────────
    uint64_t total_entries{0};   // KVs successfully written
    uint64_t total_bytes{0};     // final on-disk bytes of successfully committed HFiles
    uint64_t skipped_rows{0};    // rows skipped due to validation errors
    uint64_t total_rows{0};      // total rows processed (written + skipped)

    // ── Timing ─────────────────────────────────────────────────────────────
    std::chrono::milliseconds elapsed{0};

    // ── Partial success ────────────────────────────────────────────────────
    /// true when some files succeeded and some failed.
    bool partial_success() const noexcept {
        return !files.empty() && !failed_files.empty();
    }
};

/// Progress snapshot passed to the progress callback.
struct ProgressInfo {
    int64_t  total_kv_written{0};
    int64_t  total_bytes_written{0};
    int      files_completed{0};   // finished and committed HFiles
    int      files_in_progress{0}; // currently open active HFiles
    int64_t  skipped_rows{0};
    double   estimated_progress{0.0};     // 0.0–1.0
    std::chrono::milliseconds elapsed{0};
};

class BulkLoadWriterImpl;

/// High-level Bulk Load writer: auto-routes Arrow data to per-Region, per-CF HFiles.
class BulkLoadWriter {
public:
    ~BulkLoadWriter();

    BulkLoadWriter(const BulkLoadWriter&)            = delete;
    BulkLoadWriter& operator=(const BulkLoadWriter&) = delete;
    BulkLoadWriter(BulkLoadWriter&&) noexcept;
    BulkLoadWriter& operator=(BulkLoadWriter&&) noexcept;

    /// Write one Arrow RecordBatch.
    Status write_batch(const arrow::RecordBatch& batch, MappingMode mode);

    /// Flush, close all HFiles, return result.
    std::pair<BulkLoadResult, Status> finish();

    // ─── Builder ─────────────────────────────────────────────────────────────
    class Builder {
    public:
        Builder& set_table_name(std::string name);
        Builder& set_column_families(std::vector<std::string> cfs);
        Builder& set_output_dir(std::string dir);  // local path or hdfs://…
        Builder& set_partitioner(std::unique_ptr<RegionPartitioner> p);

        // ── HFile write options ──────────────────────────────────────────
        Builder& set_compression(Compression c);
        Builder& set_block_size(size_t sz);
        Builder& set_data_block_encoding(Encoding enc);
        Builder& set_bloom_type(BloomType bt);
        Builder& set_parallelism(int n);

        // ── Crash-safety [Production] ─────────────────────────────────────
        Builder& set_fsync_policy(FsyncPolicy p);

        // ── Input validation [Production] ─────────────────────────────────
        Builder& set_error_policy(ErrorPolicy p);
        Builder& set_max_error_count(uint64_t n);
        Builder& set_error_callback(std::function<void(const RowError&)> cb);
        Builder& set_max_row_key_bytes(uint32_t n);
        Builder& set_max_value_bytes(size_t n);

        // ── Resource governance [Production] ──────────────────────────────
        Builder& set_max_memory(size_t bytes);
        Builder& set_min_free_disk(size_t bytes);
        Builder& set_max_open_files(int n);

        // ── Observability [Production] ────────────────────────────────────
        Builder& set_progress_callback(
            std::function<void(const ProgressInfo&)> cb,
            std::chrono::seconds interval = std::chrono::seconds(10));

        std::pair<std::unique_ptr<BulkLoadWriter>, Status> build();

    private:
        std::string                        table_name_;
        std::vector<std::string>           column_families_;
        std::string                        output_dir_;
        std::unique_ptr<RegionPartitioner> partitioner_;
        WriterOptions                      opts_;
        int                                parallelism_{1};
        std::function<void(const ProgressInfo&)> progress_cb_;
        std::chrono::seconds               progress_interval_{10};
    };

    static Builder builder() { return Builder{}; }

private:
    explicit BulkLoadWriter(std::unique_ptr<BulkLoadWriterImpl> impl);
    std::unique_ptr<BulkLoadWriterImpl> impl_;
};

} // namespace hfile
