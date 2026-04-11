package io.hfilesdk.converter;

/**
 * Structured result of a single Arrow → HFile conversion.
 *
 * <p>Obtained from {@link ArrowToHFileConverter#convert}. Mirrors the JSON
 * returned by {@link com.hfile.HFileSDK#getLastResult()}.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * ConvertResult r = converter.convert(opts);
 * if (!r.isSuccess()) {
 *     throw new ConvertException(r);
 * }
 * log.info("Converted {} KVs ({} KB) in {}ms  [sort {}ms, write {}ms]",
 *     r.kvWrittenCount, r.hfileSizeBytes / 1024,
 *     r.elapsedMs, r.sortMs, r.writeMs);
 * }</pre>
 */
public final class ConvertResult {

    // ── Error ──────────────────────────────────────────────────────────────────
    public final int    errorCode;
    public final String errorMessage;

    // ── Arrow read stats ───────────────────────────────────────────────────────
    public final long arrowBatchesRead;
    public final long arrowRowsRead;

    // ── HFile write stats ──────────────────────────────────────────────────────
    public final long kvWrittenCount;
    public final long kvSkippedCount;
    public final long hfileSizeBytes;

    /**
     * Number of HBase row keys that were produced by more than one Arrow source
     * row (rowKeyRule collisions).  When this is non-zero, some source data was
     * silently dropped (first-in-sort-order wins for each qualifier).
     *
     * <p>Non-zero here is a signal to review the rowKeyRule for uniqueness.
     * Each collision emits exactly ONE WARN log line (not one per column).
     */
    public final long duplicateKeyCount;
    public final long memoryBudgetBytes;
    public final long trackedMemoryPeakBytes;

    // ── Timing (milliseconds) ──────────────────────────────────────────────────
    public final long elapsedMs;
    public final long sortMs;
    public final long writeMs;

    // ── Error code constants (mirror HFileSDK) ─────────────────────────────────
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

    private ConvertResult(int errorCode, String errorMessage,
                          long arrowBatchesRead, long arrowRowsRead,
                          long kvWrittenCount, long kvSkippedCount,
                          long duplicateKeyCount,
                          long memoryBudgetBytes,
                          long trackedMemoryPeakBytes,
                          long hfileSizeBytes,
                          long elapsedMs, long sortMs, long writeMs) {
        this.errorCode        = errorCode;
        this.errorMessage     = errorMessage;
        this.arrowBatchesRead = arrowBatchesRead;
        this.arrowRowsRead    = arrowRowsRead;
        this.kvWrittenCount   = kvWrittenCount;
        this.kvSkippedCount   = kvSkippedCount;
        this.duplicateKeyCount = duplicateKeyCount;
        this.memoryBudgetBytes = memoryBudgetBytes;
        this.trackedMemoryPeakBytes = trackedMemoryPeakBytes;
        this.hfileSizeBytes   = hfileSizeBytes;
        this.elapsedMs        = elapsedMs;
        this.sortMs           = sortMs;
        this.writeMs          = writeMs;
    }

    /** Returns {@code true} if the conversion completed without error. */
    public boolean isSuccess() { return errorCode == OK; }

    /**
     * Returns {@code true} if there were row key collisions (multiple Arrow rows
     * mapped to the same HBase row key).  When true, some source data was silently
     * dropped (first-in-sort-order wins per qualifier).  Review the rowKeyRule.
     */
    public boolean hasDuplicateKeys() { return duplicateKeyCount > 0; }

    /**
     * Returns a human-readable single-line summary of the result.
     * <pre>
     * OK  kvs=1523400  skipped=23  dupKeys=0  hfile=845MB  elapsed=1230ms  sort=450ms  write=260ms
     * </pre>
     */
    public String summary() {
        if (!isSuccess()) {
            return String.format("FAILED (code=%d) %s", errorCode, errorMessage);
        }
        String dupNote = duplicateKeyCount > 0
            ? String.format("  dupKeys=%,d(!)", duplicateKeyCount)
            : "  dupKeys=0";
        return String.format(
            "OK  kvs=%,d  skipped=%,d%s  hfile=%s  elapsed=%dms  sort=%dms  write=%dms",
            kvWrittenCount, kvSkippedCount, dupNote,
            humanBytes(hfileSizeBytes),
            elapsedMs, sortMs, writeMs);
    }

    /** Returns throughput in MB/s, or 0 if elapsed time is 0. */
    public double throughputMbps() {
        if (elapsedMs <= 0) return 0.0;
        return (hfileSizeBytes / 1024.0 / 1024.0) / (elapsedMs / 1000.0);
    }

    @Override
    public String toString() {
        return "ConvertResult{" + summary() + "}";
    }

    // ── Factory: parse from HFileSDK.getLastResult() JSON ─────────────────────

    /**
     * Parse a {@code ConvertResult} from the JSON string returned by
     * {@link com.hfile.HFileSDK#getLastResult()}.
     *
     * <p>The parser is intentionally simple: it handles the exact flat-object
     * JSON format produced by the C++ side and does not need a JSON library.
     *
     * @param json   JSON string from {@code HFileSDK.getLastResult()}.
     * @param sdkRc  Return code from {@code HFileSDK.convert()} (same as
     *               {@code error_code} in the JSON, included as a fallback).
     */
    static ConvertResult fromJson(String json, int sdkRc) {
        if (json == null || json.isBlank() || json.equals("{}")) {
            return new ConvertResult(sdkRc, "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }
        return new ConvertResult(
            (int) parseLong(json, "error_code",          sdkRc),
            parseString(json, "error_message",           ""),
            parseLong(json, "arrow_batches_read",         0),
            parseLong(json, "arrow_rows_read",            0),
            parseLong(json, "kv_written_count",           0),
            parseLong(json, "kv_skipped_count",           0),
            parseLong(json, "duplicate_key_count",        0),
            parseLong(json, "memory_budget_bytes",        0),
            parseLong(json, "tracked_memory_peak_bytes",  0),
            parseLong(json, "hfile_size_bytes",           0),
            parseLong(json, "elapsed_ms",                 0),
            parseLong(json, "sort_ms",                    0),
            parseLong(json, "write_ms",                   0)
        );
    }

    /** Construct a result representing a pre-convert failure (bad args, etc.). */
    static ConvertResult ofError(int code, String message) {
        return new ConvertResult(code, message, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    // ── Internal JSON helpers ──────────────────────────────────────────────────

    private static long parseLong(String json, String key, long defaultValue) {
        String tag = "\"" + key + "\":";
        int idx = json.indexOf(tag);
        if (idx < 0) return defaultValue;
        int start = idx + tag.length();
        // skip whitespace
        while (start < json.length() && json.charAt(start) == ' ') start++;
        // read until , } or end
        int end = start;
        while (end < json.length()) {
            char c = json.charAt(end);
            if (c == ',' || c == '}') break;
            end++;
        }
        String raw = json.substring(start, end).trim();
        // strip surrounding quotes if numeric field was accidentally quoted
        if (raw.startsWith("\"") && raw.endsWith("\""))
            raw = raw.substring(1, raw.length() - 1);
        try { return Long.parseLong(raw); }
        catch (NumberFormatException e) { return defaultValue; }
    }

    private static String parseString(String json, String key, String defaultValue) {
        String tag = "\"" + key + "\":\"";
        int idx = json.indexOf(tag);
        if (idx < 0) return defaultValue;
        int start = idx + tag.length();
        StringBuilder sb = new StringBuilder();
        boolean escape = false;
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (escape) {
                switch (c) {
                    case '"': sb.append('"'); break;
                    case '\\': sb.append('\\'); break;
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    default: sb.append(c);
                }
                escape = false;
            } else if (c == '\\') {
                escape = true;
            } else if (c == '"') {
                break;  // end of string value
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String humanBytes(long bytes) {
        if (bytes >= 1024L * 1024 * 1024)
            return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
        if (bytes >= 1024L * 1024)
            return String.format("%.1fMB", bytes / (1024.0 * 1024));
        if (bytes >= 1024L)
            return String.format("%.1fKB", bytes / 1024.0);
        return bytes + "B";
    }
}
