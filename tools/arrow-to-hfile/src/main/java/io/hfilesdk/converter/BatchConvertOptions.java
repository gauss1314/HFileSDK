package io.hfilesdk.converter;

import java.nio.file.Path;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Options for a batch Arrow → HFile conversion job.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * BatchConvertOptions opts = BatchConvertOptions.builder()
 *     .arrowDir(Path.of("/data/arrow/"))          // scan all *.arrow files
 *     .hfileDir(Path.of("/staging/job_20550/cf/")) // output dir for HFiles
 *     .tableName("tdr_signal_stor_20550")
 *     .rowKeyRule("REFID,0,false,15")
 *     .columnFamily("cf")
 *     .parallelism(4)                              // 4 concurrent conversions
 *     .build();
 * }</pre>
 */
public final class BatchConvertOptions {

    // ── Required ──────────────────────────────────────────────────────────────
    /** Directory containing Arrow IPC Stream files (*.arrow), or explicit list. */
    public final Path       arrowDir;
    /** Explicit list of Arrow files. Used when arrowDir is null. */
    public final List<Path> arrowFiles;
    /** Output directory for HFile files. Each HFile is named after its Arrow file. */
    public final Path       hfileDir;
    /** HBase table name. */
    public final String     tableName;
    /** Row key rule expression for HFileSDK. */
    public final String     rowKeyRule;

    // ── Writer settings (forwarded to per-file ConvertOptions) ───────────────
    public final String columnFamily;
    public final String compression;
    public final int    compressionLevel;
    public final String dataBlockEncoding;
    public final String bloomType;
    public final String errorPolicy;
    public final int    blockSize;
    public final long   maxMemoryBytes;
    public final int    compressionThreads;
    public final int    compressionQueueDepth;
    public final String numericSortFastPath;
    public final long   defaultTimestampMs;
    public final List<String> excludedColumnPrefixes;
    public final List<String> excludedColumns;

    // ── Batch control ─────────────────────────────────────────────────────────
    /**
     * Number of files to convert concurrently.
     * Default: {@code Runtime.getRuntime().availableProcessors()}.
     * Rule of thumb: set to CPU count (convert is CPU-bound for small files)
     * or to 2-4 for large files where I/O dominates.
     */
    public final int parallelism;

    /** If true, skip Arrow files for which the output HFile already exists. */
    public final boolean skipExisting;

    /**
     * Optional progress callback. Called after each file completes (success or failure).
     * Arguments: (arrowFilePath, convertResult).
     * The callback is invoked from worker threads — must be thread-safe.
     */
    public final BiConsumer<Path, ConvertResult> progressCallback;

    /** Absolute path to libhfilesdk.so. */
    public final String nativeLibPath;

    // ─────────────────────────────────────────────────────────────────────────

    private BatchConvertOptions(Builder b) {
        this.arrowDir              = b.arrowDir;
        this.arrowFiles            = b.arrowFiles != null ? List.copyOf(b.arrowFiles) : List.of();
        this.hfileDir              = requireNonNull(b.hfileDir, "hfileDir");
        this.tableName             = b.tableName  != null ? b.tableName : "";
        this.rowKeyRule            = requireNonNull(b.rowKeyRule, "rowKeyRule");
        this.columnFamily          = b.columnFamily;
        this.compression           = b.compression;
        this.compressionLevel      = b.compressionLevel;
        this.dataBlockEncoding     = b.dataBlockEncoding;
        this.bloomType             = b.bloomType;
        this.errorPolicy           = b.errorPolicy;
        this.blockSize             = b.blockSize;
        this.maxMemoryBytes        = requireNonNegative(b.maxMemoryBytes, "maxMemoryBytes");
        this.compressionThreads    = (int) requireNonNegative(b.compressionThreads, "compressionThreads");
        this.compressionQueueDepth = (int) requireNonNegative(b.compressionQueueDepth, "compressionQueueDepth");
        this.numericSortFastPath   = normalizeNumericSortFastPath(b.numericSortFastPath);
        this.defaultTimestampMs    = requireNonNegative(b.defaultTimestampMs, "defaultTimestampMs");
        this.excludedColumnPrefixes = b.excludedColumnPrefixes != null
                                      ? List.copyOf(b.excludedColumnPrefixes) : List.of();
        this.excludedColumns       = b.excludedColumns != null
                                      ? List.copyOf(b.excludedColumns) : List.of();
        this.parallelism           = b.parallelism > 0 ? b.parallelism
                                      : Runtime.getRuntime().availableProcessors();
        this.skipExisting          = b.skipExisting;
        this.progressCallback      = b.progressCallback;
        this.nativeLibPath         = b.nativeLibPath;

        if (arrowDir == null && arrowFiles.isEmpty()) {
            throw new IllegalArgumentException("Either arrowDir or arrowFiles must be set");
        }
    }

    private static <T> T requireNonNull(T v, String name) {
        if (v == null) throw new IllegalArgumentException(name + " must not be null");
        return v;
    }

    private static long requireNonNegative(long v, String name) {
        if (v < 0) throw new IllegalArgumentException(name + " must be >= 0");
        return v;
    }

    private static String normalizeNumericSortFastPath(String value) {
        String normalized = value == null ? "auto" : value.trim().toLowerCase();
        if (normalized.isEmpty()) normalized = "auto";
        if (!normalized.equals("auto") && !normalized.equals("on") && !normalized.equals("off")) {
            throw new IllegalArgumentException("numericSortFastPath must be one of: auto|on|off");
        }
        return normalized;
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private Path       arrowDir;
        private List<Path> arrowFiles;
        private Path       hfileDir;
        private String     tableName;
        private String     rowKeyRule;
        private String     columnFamily      = "cf";
        private String     compression       = "GZ";
        private int        compressionLevel  = 1;
        private String     dataBlockEncoding = "FAST_DIFF";
        private String     bloomType         = "row";
        private String     errorPolicy       = "skip_row";
        private int        blockSize         = 65536;
        private long       maxMemoryBytes;
        private int        compressionThreads;
        private int        compressionQueueDepth;
        private String     numericSortFastPath = "auto";
        private long       defaultTimestampMs;
        private List<String> excludedColumnPrefixes;
        private List<String> excludedColumns;
        private int        parallelism;
        private boolean    skipExisting;
        private BiConsumer<Path, ConvertResult> progressCallback;
        private String     nativeLibPath;

        /** Scan this directory for *.arrow files. */
        public Builder arrowDir(Path v)              { arrowDir = v;              return this; }
        /** Use an explicit list of Arrow files instead of a directory scan. */
        public Builder arrowFiles(List<Path> v)      { arrowFiles = v;            return this; }
        /** Output directory for HFile files. */
        public Builder hfileDir(Path v)              { hfileDir = v;              return this; }
        public Builder tableName(String v)           { tableName = v;             return this; }
        public Builder rowKeyRule(String v)          { rowKeyRule = v;            return this; }
        public Builder columnFamily(String v)        { columnFamily = v;          return this; }
        public Builder compression(String v)         { compression = v;           return this; }
        public Builder compressionLevel(int v)       { compressionLevel = v;      return this; }
        public Builder dataBlockEncoding(String v)   { dataBlockEncoding = v;     return this; }
        public Builder bloomType(String v)           { bloomType = v;             return this; }
        public Builder errorPolicy(String v)         { errorPolicy = v;           return this; }
        public Builder blockSize(int v)              { blockSize = v;             return this; }
        public Builder maxMemoryBytes(long v)        { maxMemoryBytes = v;        return this; }
        public Builder compressionThreads(int v)     { compressionThreads = v;    return this; }
        public Builder compressionQueueDepth(int v)  { compressionQueueDepth = v; return this; }
        public Builder numericSortFastPath(String v) { numericSortFastPath = v;   return this; }
        public Builder defaultTimestampMs(long v)    { defaultTimestampMs = v;    return this; }
        public Builder excludedColumnPrefixes(List<String> v) { excludedColumnPrefixes = v; return this; }
        public Builder excludedColumns(List<String> v)        { excludedColumns = v;        return this; }
        /** Number of concurrent conversions. Default: CPU count. */
        public Builder parallelism(int v)            { parallelism = v;           return this; }
        /** Skip files whose HFile output already exists. */
        public Builder skipExisting(boolean v)       { skipExisting = v;          return this; }
        /** Callback invoked after each file (success or failure). Thread-safe. */
        public Builder progressCallback(BiConsumer<Path, ConvertResult> v) {
            progressCallback = v; return this;
        }
        public Builder nativeLibPath(String v)       { nativeLibPath = v;         return this; }

        public BatchConvertOptions build() { return new BatchConvertOptions(this); }
    }
}
