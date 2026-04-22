#pragma once

#include "types.h"
#include <string>
#include <cstdint>
#include <functional>
#include <cstddef>

namespace hfile {

// ─── FsyncPolicy ─────────────────────────────────────────────────────────────

/// Controls when fsync is called during HFile writing.
enum class FsyncPolicy : uint8_t {
    /// Full crash-safety: write to .tmp, fsync temp file, rename, fsync dir.
    /// Guarantees no corrupt files in the final output directory.
    Safe     = 0,

    /// Only fsync once at finish(). Faster, but a crash mid-write may leave
    /// data in OS cache. Acceptable when jobs are idempotent and re-runnable.
    Fast     = 1,

    /// Paranoid: additionally fsync after every N blocks (controlled by
    /// fsync_block_interval). For environments with unreliable storage.
    Paranoid = 2,
};

// ─── ErrorPolicy ─────────────────────────────────────────────────────────────

/// Controls how write_batch() handles rows that fail validation.
enum class ErrorPolicy : uint8_t {
    /// Stop immediately on the first validation error.
    Strict    = 0,

    /// Skip the offending row, continue with the rest of the batch.
    SkipRow   = 1,

    /// Skip the entire RecordBatch if any row in it fails, continue with next batch.
    SkipBatch = 2,
};

// ─── RowError (reported to the error callback) ───────────────────────────────

struct RowError {
    enum class Reason : uint8_t {
        RowKeyEmpty      = 1,
        RowKeyTooLong    = 2,
        ValueTooLarge    = 3,
        NegativeTimestamp= 4,
        SortOrderViolation=5,
        UnsupportedType  = 6,
        SchemaMismatch   = 7,
    };
    int64_t batch_index{-1};  // row index within the RecordBatch
    Reason  reason{};
    std::string message;
};

// ─── WriterOptions ────────────────────────────────────────────────────────────

struct WriterOptions {
    // ── Column family ──────────────────────────────────────────────────────
    std::string  column_family;

    // ── Block settings ─────────────────────────────────────────────────────
    size_t       block_size           = kDefaultBlockSize;
    Compression  compression          = Compression::GZip;
    /// Compression level: 1 (fastest) to 9 (best ratio).
    /// 0 = algorithm default. Only affects GZip (via zlib deflate level).
    /// Recommended: 1 or 2 for write-throughput-sensitive HFile generation.
    int          compression_level    = 1;
    /// Only NONE is supported for on-disk HFile blocks.
    Encoding     data_block_encoding  = Encoding::None;

    // ── Bloom filter ───────────────────────────────────────────────────────
    BloomType    bloom_type       = BloomType::Row;
    double       bloom_error_rate = 0.01;

    // ── HFile v3 cell tags / MVCC ──────────────────────────────────────────
    bool         include_tags  = true;   // false => strip user tags, still emit tags_length=0
    bool         include_mvcc  = true;   // false => force MemstoreTS to 0 on disk

    // ── Comparator & checksum ──────────────────────────────────────────────
    std::string  comparator          = std::string(kCellComparator);
    int64_t      file_create_time_ms = 0;
    uint32_t     bytes_per_checksum  = kBytesPerChecksum;

    // ── Sort mode ──────────────────────────────────────────────────────────
    enum class SortMode {
        PreSortedTrusted,   // caller guarantees order, no validation
        PreSortedVerified,  // caller guarantees order, stream-verify each KV
        AutoSort,           // HFileWriter buffers KVs in memory and sorts at finish()
    };
    // Default: AutoSort — input end does not guarantee order.
    // Use PreSortedTrusted / PreSortedVerified only when input is known sorted.
    SortMode sort_mode = SortMode::AutoSort;

    // ── Crash-safety & durability [Production] ────────────────────────────
    /// Controls fsync behaviour. Default: Safe (write-to-temp + rename).
    FsyncPolicy  fsync_policy          = FsyncPolicy::Safe;
    /// For Paranoid mode: fsync every N blocks. 0 = disabled.
    uint32_t     fsync_block_interval  = 0;

    // ── Input validation & error handling [Production] ─────────────────────
    ErrorPolicy  error_policy     = ErrorPolicy::SkipRow;
    /// Maximum cumulative row errors before aborting (0 = unlimited).
    uint64_t     max_error_count  = 1000;
    /// Optional callback invoked for each row-level validation error.
    std::function<void(const RowError&)> error_callback;

    // ── Input size limits [Production] ────────────────────────────────────
    /// Maximum row-key length in bytes (HBase hard limit: 65535).
    uint32_t     max_row_key_bytes  = 32 * 1024;        // 32 KB
    /// Maximum single-cell value size in bytes.
    size_t       max_value_bytes    = 10ULL * 1024 * 1024;  // 10 MB

    // ── Resource governance [Production] ──────────────────────────────────
    /// Maximum memory the writer may consume across buffers / AutoSort staging (0 = unlimited).
    size_t       max_memory_bytes  = 0;
    /// Number of background threads used to compress data blocks.
    /// 0 = disabled (fully synchronous current behavior).
    uint32_t     compression_threads = 0;
    /// Maximum number of compressed/unwritten data blocks allowed in flight.
    /// 0 = auto (currently max(2, compression_threads * 2) when threads > 0).
    uint32_t     compression_queue_depth = 0;
    /// Minimum free disk space required; writing stops if below this (0 = disabled).
    size_t       min_free_disk_bytes = 512ULL * 1024 * 1024;  // 512 MB
    /// Check disk space every N bytes written (0 = disable periodic check).
    size_t       disk_check_interval_bytes = 256ULL * 1024 * 1024;  // 256 MB
    /// Maximum simultaneously open file handles the writer pipeline may keep.
    /// This is retained for API compatibility with the single-file conversion path.
    int          max_open_files    = 64;
};

} // namespace hfile
