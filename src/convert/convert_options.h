#pragma once

#include <hfile/status.h>
#include <hfile/writer_options.h>
#include <string>
#include <cstdint>
#include <chrono>
#include <functional>

namespace hfile {

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

    // ── Column mapping ────────────────────────────────────────────────────
    std::string column_family   = "cf";    // all KVs go into this CF
    int64_t     default_timestamp = 0;     // 0 = use current time

    // ── HFile write options ───────────────────────────────────────────────
    WriterOptions writer_opts;

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

    std::chrono::milliseconds elapsed_ms{0};
    std::chrono::milliseconds sort_ms{0};
    std::chrono::milliseconds write_ms{0};
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
