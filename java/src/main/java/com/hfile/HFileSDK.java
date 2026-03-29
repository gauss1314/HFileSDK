package com.hfile;

/**
 * JNI bridge to the C++ HFileSDK shared library ({@code libhfilesdk.so}).
 *
 * <p>Usage:
 * <pre>{@code
 * HFileSDK sdk = new HFileSDK();
 *
 * // Optional: global configuration (call once before first convert())
 * sdk.configure("""
 *     {
 *       "compression":          "lz4",
 *       "block_size":           65536,
 *       "column_family":        "cf",
 *       "data_block_encoding":  "FAST_DIFF"
 *     }
 *     """);
 *
 * // Convert Arrow IPC Stream file → HFile v3
 * int rc = sdk.convert(
 *     "/data/events_20240301.arrow",          // arrowPath
 *     "/staging/cf/events_20240301.hfile",    // hfilePath
 *     "events_table",                         // tableName
 *     "STARTTIME,0,false,10#IMSI,1,true,15#MSISDN,2,false,11,RIGHT,#$RND$,3,false,4"
 * );
 * if (rc != 0) {
 *     String detail = sdk.getLastResult();    // JSON with error detail
 *     throw new IOException("HFile conversion failed: " + detail);
 * }
 *
 * // Inspect metrics
 * String json = sdk.getLastResult();
 * }</pre>
 *
 * <h3>rowKeyRule format</h3>
 * Segments separated by {@code #}, each segment:
 * {@code colName,index,isReverse,padLen[,padMode][,padContent]}
 * <ul>
 *   <li>{@code colName} — Arrow column name (informational label)
 *   <li>{@code index}   — 0-based column index in the Arrow Schema
 *   <li>{@code isReverse} — {@code true} to reverse the string after padding
 *   <li>{@code padLen}  — target width; 0 = no padding; value longer than padLen is not truncated
 *   <li>{@code padMode} — {@code LEFT} (default) or {@code RIGHT}
 *   <li>{@code padContent} — pad character, default {@code 0}
 * </ul>
 * Special names:
 * <ul>
 *   <li>{@code $RND$} / {@code RANDOM} / {@code RANDOM_COL} — generate {@code padLen} random digits (0–8)
 *   <li>{@code FILL} / {@code FILL_COL} — use an empty string, then still apply padding/reverse rules
 *   <li>{@code long(...)} / {@code short(...)} — Java-compatible numeric encoding segments; current transforms include {@code hash}
 * </ul>
 *
 * <h3>Design note: rowValue removed</h3>
 * Earlier API versions accepted a {@code rowValue} parameter.
 * Since v4.0, row values are derived internally from the Arrow columns —
 * the SDK reads the Arrow IPC Stream file, converts each row to a
 * pipe-delimited string, and applies the rowKeyRule automatically.
 * Callers must NOT pass a rowValue.
 *
 * <h3>Sorting</h3>
 * The SDK performs a two-pass sort internally:
 * <ol>
 *   <li>First pass: read all batches, build row keys, collect (key, position) pairs.</li>
 *   <li>Sort by HBase key order (lexicographic on Row key).</li>
 *   <li>Second pass: write KVs in sorted order.</li>
 * </ol>
 * The input Arrow file does NOT need to be pre-sorted.
 *
 * @since 4.0
 */
public class HFileSDK {

    static {
        System.loadLibrary("hfilesdk");
    }

    // ── Error codes (mirror hfile::ErrorCode in convert_options.h) ──────────

    public static final int OK                   = 0;
    public static final int INVALID_ARGUMENT     = 1;
    public static final int ARROW_FILE_ERROR     = 2;
    public static final int SCHEMA_MISMATCH      = 3;
    public static final int INVALID_ROW_KEY_RULE = 4;
    public static final int SORT_VIOLATION       = 5;
    public static final int IO_ERROR             = 10;
    public static final int DISK_EXHAUSTED       = 11;
    public static final int MEMORY_EXHAUSTED     = 12;
    public static final int INTERNAL_ERROR       = 20;

    // ── Native methods ───────────────────────────────────────────────────────

    /**
     * Convert an Arrow IPC Stream file to a HFile v3 file.
     *
     * <p>The conversion performs automatic sorting: the input Arrow file
     * does not need to be pre-sorted. The SDK handles all ordering internally.
     *
     * @param arrowPath   Path to the Arrow IPC Stream file on local disk.
     * @param hfilePath   Desired output HFile path.
     *                    The file is written atomically (temp + rename).
     * @param tableName   HBase table name (used for logging and metadata).
     * @param rowKeyRule  Row Key rule expression (see class Javadoc for format).
     * @return {@code 0} on success; non-zero error code otherwise.
     *         Call {@link #getLastResult()} for a JSON description of the result.
     */
    public native int convert(String arrowPath,
                               String hfilePath,
                               String tableName,
                               String rowKeyRule);

    /**
     * Return a JSON string describing the result of the most recent
     * {@link #convert} call on this thread.
     *
     * <p>Fields:
     * <ul>
     *   <li>{@code error_code}         — 0 = success
     *   <li>{@code error_message}      — human-readable error detail
     *   <li>{@code arrow_batches_read} — number of Arrow RecordBatches processed
     *   <li>{@code arrow_rows_read}    — total Arrow rows read
     *   <li>{@code kv_written_count}   — KVs successfully written to HFile
     *   <li>{@code kv_skipped_count}   — rows skipped (empty key / oversized value)
     *   <li>{@code hfile_size_bytes}   — final HFile size on disk
     *   <li>{@code elapsed_ms}         — total elapsed milliseconds
     *   <li>{@code sort_ms}            — time spent sorting
     *   <li>{@code write_ms}           — time spent writing the HFile
     * </ul>
     */
    public native String getLastResult();

    /**
     * Set global writer configuration.
     * Call this once before the first {@link #convert} call.
     *
     * <p>Supported JSON keys:
     * <ul>
     *   <li>{@code compression}         — {@code "none" | "lz4" | "zstd" | "snappy" | "gzip"}
     *   <li>{@code block_size}          — data block size in bytes (default 65536)
     *   <li>{@code column_family}       — HBase column family name (default "cf")
     *   <li>{@code data_block_encoding} — {@code "NONE" | "PREFIX" | "DIFF" | "FAST_DIFF"}
     *   <li>{@code fsync_policy}        — {@code "safe" | "fast" | "paranoid"}
     *   <li>{@code error_policy}        — {@code "strict" | "skip_row" | "skip_batch"}
     *   <li>{@code bloom_type}          — {@code "none" | "row" | "rowcol"}
     * </ul>
     *
     * @param configJson JSON configuration string.
     * @return {@code 0} on success.
     */
    public native int configure(String configJson);

    // ── Convenience factory ──────────────────────────────────────────────────

    /**
     * Return a builder for fluent configuration.
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Fluent configuration builder around {@link HFileSDK}.
     */
    public static final class Builder {
        private String compression        = "lz4";
        private int    blockSize          = 65536;
        private String columnFamily       = "cf";
        private String dataBlockEncoding  = "FAST_DIFF";

        public Builder compression(String c)       { compression = c;       return this; }
        public Builder blockSize(int s)            { blockSize = s;         return this; }
        public Builder columnFamily(String cf)     { columnFamily = cf;     return this; }
        public Builder dataBlockEncoding(String e) { dataBlockEncoding = e; return this; }

        /**
         * Build and configure an {@link HFileSDK} instance.
         */
        public HFileSDK build() {
            HFileSDK sdk = new HFileSDK();
            String cfg = String.format(
                "{\"compression\":\"%s\",\"block_size\":%d," +
                "\"column_family\":\"%s\",\"data_block_encoding\":\"%s\"}",
                compression, blockSize, columnFamily, dataBlockEncoding);
            sdk.configure(cfg);
            return sdk;
        }
    }
}
