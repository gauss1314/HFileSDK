#pragma once

#include <hfile/status.h>
#include <hfile/writer_options.h>
#include <string>
#include <vector>
#include <cstdint>
#include <chrono>
#include <functional>

namespace hfile {

enum class NumericSortFastPathMode {
    Auto,
    On,
    Off
};

inline constexpr const char* numeric_sort_fast_path_mode_name(
        NumericSortFastPathMode mode) noexcept {
    switch (mode) {
    case NumericSortFastPathMode::Auto: return "auto";
    case NumericSortFastPathMode::On:   return "on";
    case NumericSortFastPathMode::Off:  return "off";
    }
    return "auto";
}

/// Options for a single Arrow IPC file → HFile conversion.
struct ConvertOptions {
    // ── Input/Output ──────────────────────────────────────────────────────
    std::string arrow_path;     // path to Arrow IPC Stream file on disk
    std::string hfile_path;     // output HFile path (written atomically)
    std::string table_name;     // HBase table name

    // ── Row Key rule ──────────────────────────────────────────────────────
    /// Row Key rule expression.
    /// Format: "SEG1#SEG2#SEG3..."
    /// Each SEG: "colName,index,isReverse,padLen[,padMode][,padContent]"
    /// Special: "$RND$,index,false,N" → N random digits (0–8)
    std::string row_key_rule;

    // ── Column filtering ──────────────────────────────────────────────────
    /// Column names to exclude from HBase KV output (exact match, case-sensitive).
    /// These columns are NOT written as HBase qualifiers.
    /// Does NOT affect row key construction — row key segments reference columns
    /// by index (from the original Arrow schema) which remains unchanged.
    ///
    /// Example: {"_hoodie_commit_time", "_hoodie_record_key"}
    std::vector<std::string> excluded_columns;

    /// Column name prefixes to exclude (e.g. "_hoodie" excludes all columns
    /// whose name starts with "_hoodie").  Takes effect after exact-name exclusions.
    /// Matching is prefix-only, case-sensitive.
    ///
    /// Example: {"_hoodie"}  →  drops all five Hudi metadata columns automatically
    std::vector<std::string> excluded_column_prefixes;

    // ── Column mapping ────────────────────────────────────────────────────
    std::string column_family   = "cf";    // all KVs go into this CF
    int64_t     default_timestamp = 0;     // 0 = use current time

    // ── HFile write options ───────────────────────────────────────────────
    WriterOptions writer_opts;

    // ── Converter fast paths ──────────────────────────────────────────────
    NumericSortFastPathMode numeric_sort_fast_path = NumericSortFastPathMode::Auto;

    // ── Observability ─────────────────────────────────────────────────────
    std::function<void(int64_t rows_done, int64_t total_rows)> progress_cb;
};

/// Result of a single conversion.
struct ConvertResult {
    int         error_code          = 0;    // 0 = success
    std::string error_message;

    int64_t     arrow_batches_read  = 0;
    int64_t     arrow_rows_read     = 0;
    int64_t     kv_written_count    = 0;
    int64_t     kv_skipped_count    = 0;
    int64_t     hfile_size_bytes    = 0;

    /// Number of HBase row keys that were produced by more than one Arrow source
    /// row (i.e. rowKeyRule collisions).  Each such group emits one WARN log line.
    /// Non-zero here means your rowKeyRule is not injective for this dataset;
    /// review the rule or add a uniqueness-guaranteeing segment (e.g. $RND$).
    int64_t     duplicate_key_count = 0;
    int64_t     memory_budget_bytes = 0;
    int64_t     tracked_memory_peak_bytes = 0;
    NumericSortFastPathMode numeric_sort_fast_path_mode = NumericSortFastPathMode::Auto;
    bool        numeric_sort_fast_path_used = false;

    std::chrono::milliseconds elapsed_ms{0};
    std::chrono::milliseconds sort_ms{0};
    std::chrono::milliseconds write_ms{0};
    std::chrono::milliseconds data_block_encode_ms{0};
    std::chrono::milliseconds data_block_compress_ms{0};
    std::chrono::milliseconds data_block_write_ms{0};
    std::chrono::milliseconds leaf_index_write_ms{0};
    std::chrono::milliseconds bloom_chunk_write_ms{0};
    std::chrono::milliseconds load_on_open_write_ms{0};

    uint32_t    data_block_count = 0;
    uint32_t    leaf_index_block_count = 0;
    uint32_t    bloom_chunk_flush_count = 0;
    uint32_t    load_on_open_block_count = 0;
};

/// Error codes (also used as JNI return values).
namespace ErrorCode {
    inline constexpr int OK                  = 0;
    inline constexpr int INVALID_ARGUMENT    = 1;
    inline constexpr int ARROW_FILE_ERROR    = 2;
    inline constexpr int SCHEMA_MISMATCH     = 3;
    inline constexpr int INVALID_ROW_KEY_RULE= 4;
    inline constexpr int SORT_VIOLATION      = 5;
    inline constexpr int IO_ERROR            = 10;
    inline constexpr int DISK_EXHAUSTED      = 11;
    inline constexpr int MEMORY_EXHAUSTED    = 12;
    inline constexpr int INTERNAL_ERROR      = 20;
}

} // namespace hfile
