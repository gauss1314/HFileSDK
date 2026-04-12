package io.hfilesdk.converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Immutable options for a single Arrow → HFile conversion.
 *
 * <h3>列过滤（Hudi / CDC 元数据列）</h3>
 * <p>Arrow 文件可能包含 Hudi 写入的元数据列（前5列为
 * {@code _hoodie_commit_time}, {@code _hoodie_commit_seqno},
 * {@code _hoodie_record_key}, {@code _hoodie_partition_path},
 * {@code _hoodie_file_name}）。这些列不应写入 HBase，否则：
 * <ol>
 *   <li>它们会变成无意义的 HBase qualifier，浪费存储空间</li>
 *   <li>它们导致列顺序偏移，使 rowKeyRule 中基于数字索引的列引用全部错位</li>
 * </ol>
 * 使用前缀一次性排除：
 * <pre>{@code
 * ConvertOptions.builder()
 *     .arrowPath("/data/events.arrow")
 *     .hfilePath("/staging/cf/events.hfile")
 *     .rowKeyRule("STARTTIME,0,false,10#IMSI,1,true,15")  // 索引基于过滤后的 schema
 *     .excludedColumnPrefix("_hoodie")                    // 丢弃全部5个 _hoodie_* 列
 *     .build();
 * }</pre>
 *
 * <p><strong>重要</strong>：列过滤在 rowKeyRule 索引解析之前生效。
 * {@code rowKeyRule} 中的 {@code index} 指向的是<strong>排除列之后</strong>的 Arrow Schema 位置。
 * 例如原始 schema 有 5 个 _hoodie 列 + STARTTIME + IMSI，排除后 STARTTIME 变为 index 0。
 *
 * <h3>rowKeyRule 格式</h3>
 * 用 {@code #} 分隔的若干段，每段：
 * {@code colName,index,isReverse,padLen[,padMode][,padContent]}
 */
public final class ConvertOptions {

    // ── Required ──────────────────────────────────────────────────────────────
    private final String arrowPath;
    private final String hfilePath;
    private final String tableName;
    private final String rowKeyRule;

    // ── HFile writer settings ─────────────────────────────────────────────────
    private final String columnFamily;
    private final String compression;
    private final int    compressionLevel;
    private final String dataBlockEncoding;
    private final String bloomType;
    private final String fsyncPolicy;
    private final String errorPolicy;
    private final int    blockSize;
    private final long   maxMemoryBytes;
    private final long   defaultTimestampMs;

    // ── Column exclusion ──────────────────────────────────────────────────────
    private final List<String> excludedColumns;
    private final List<String> excludedColumnPrefixes;

    // ── Native lib ────────────────────────────────────────────────────────────
    private final String nativeLibPath;

    private ConvertOptions(Builder b) {
        this.arrowPath              = requireNonBlank(b.arrowPath,  "arrowPath");
        this.hfilePath              = requireNonBlank(b.hfilePath,  "hfilePath");
        this.tableName              = b.tableName != null ? b.tableName : "";
        this.rowKeyRule             = requireNonBlank(b.rowKeyRule, "rowKeyRule");
        this.columnFamily           = b.columnFamily;
        this.compression            = b.compression;
        this.compressionLevel       = b.compressionLevel;
        this.dataBlockEncoding      = b.dataBlockEncoding;
        this.bloomType              = b.bloomType;
        this.fsyncPolicy            = b.fsyncPolicy;
        this.errorPolicy            = b.errorPolicy;
        this.blockSize              = b.blockSize;
        this.maxMemoryBytes         = requireNonNegative(b.maxMemoryBytes, "maxMemoryBytes");
        this.defaultTimestampMs     = requireNonNegative(b.defaultTimestampMs, "defaultTimestampMs");
        this.excludedColumns        = Collections.unmodifiableList(new ArrayList<>(b.excludedColumns));
        this.excludedColumnPrefixes = Collections.unmodifiableList(new ArrayList<>(b.excludedColumnPrefixes));
        this.nativeLibPath          = b.nativeLibPath;
    }

    private static String requireNonBlank(String v, String name) {
        if (v == null || v.isBlank())
            throw new IllegalArgumentException(name + " must not be null or blank");
        return v;
    }

    private static long requireNonNegative(long v, String name) {
        if (v < 0) throw new IllegalArgumentException(name + " must be >= 0");
        return v;
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public String       arrowPath()                { return arrowPath; }
    public String       hfilePath()                { return hfilePath; }
    public String       tableName()                { return tableName; }
    public String       rowKeyRule()               { return rowKeyRule; }
    public String       columnFamily()             { return columnFamily; }
    public String       compression()              { return compression; }
    public int          compressionLevel()         { return compressionLevel; }
    public String       dataBlockEncoding()        { return dataBlockEncoding; }
    public String       bloomType()                { return bloomType; }
    public String       fsyncPolicy()              { return fsyncPolicy; }
    public String       errorPolicy()              { return errorPolicy; }
    public int          blockSize()                { return blockSize; }
    public long         maxMemoryBytes()           { return maxMemoryBytes; }
    public long         defaultTimestampMs()       { return defaultTimestampMs; }
    public List<String> excludedColumns()          { return excludedColumns; }
    public List<String> excludedColumnPrefixes()   { return excludedColumnPrefixes; }
    public String       nativeLibPath()            { return nativeLibPath; }

    /**
     * Build the JSON config string passed to {@link com.hfile.HFileSDK#configure}.
     */
    String toConfigJson() {
        StringBuilder sb = new StringBuilder("{");
        appendStr(sb, "compression",         compression);
        if (compressionLevel > 0) {
            if (sb.length() > 1) sb.append(',');
            sb.append("\"compression_level\":").append(compressionLevel);
        }
        appendStr(sb, "column_family",       columnFamily);
        appendStr(sb, "data_block_encoding", dataBlockEncoding);
        appendStr(sb, "bloom_type",          bloomType);
        appendStr(sb, "fsync_policy",        fsyncPolicy);
        appendStr(sb, "error_policy",        errorPolicy);
        if (blockSize > 0) {
            if (sb.length() > 1) sb.append(',');
            sb.append("\"block_size\":").append(blockSize);
        }
        if (maxMemoryBytes > 0) {
            if (sb.length() > 1) sb.append(',');
            sb.append("\"max_memory_bytes\":").append(maxMemoryBytes);
        }
        if (defaultTimestampMs > 0) {
            if (sb.length() > 1) sb.append(',');
            sb.append("\"default_timestamp_ms\":").append(defaultTimestampMs);
        }
        appendStrArray(sb, "excluded_columns",         excludedColumns);
        appendStrArray(sb, "excluded_column_prefixes", excludedColumnPrefixes);
        sb.append('}');
        return sb.toString();
    }

    private static void appendStr(StringBuilder sb, String key, String value) {
        if (value == null || value.isBlank()) return;
        if (sb.length() > 1) sb.append(',');
        sb.append('"').append(key).append("\":\"").append(value).append('"');
    }

    private static void appendStrArray(StringBuilder sb, String key, List<String> values) {
        if (values == null || values.isEmpty()) return;
        if (sb.length() > 1) sb.append(',');
        sb.append('"').append(key).append("\":[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"');
            sb.append(values.get(i).replace("\\", "\\\\").replace("\"", "\\\""));
            sb.append('"');
        }
        sb.append(']');
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    public static final class Builder {

        private String arrowPath;
        private String hfilePath;
        private String tableName;
        private String rowKeyRule;
        private String columnFamily      = "cf";
        private String compression       = "GZ";
        private int    compressionLevel  = 1;
        private String dataBlockEncoding = "FAST_DIFF";
        private String bloomType         = "row";
        private String fsyncPolicy       = "safe";
        private String errorPolicy       = "skip_row";
        private int    blockSize         = 65536;
        private long   maxMemoryBytes;
        private long   defaultTimestampMs;
        private final ArrayList<String> excludedColumns        = new ArrayList<>();
        private final ArrayList<String> excludedColumnPrefixes = new ArrayList<>();
        private String nativeLibPath;

        public Builder arrowPath(String v)         { arrowPath = v;         return this; }
        public Builder hfilePath(String v)         { hfilePath = v;         return this; }
        public Builder tableName(String v)         { tableName = v;         return this; }

        /**
         * Row Key rule. The {@code index} in each segment refers to the column
         * position in the Arrow schema AFTER excluded columns have been removed.
         *
         * <p>Example — Hudi file with schema:
         * <pre>
         * [0] _hoodie_commit_time   ← excluded by prefix "_hoodie"
         * [1] _hoodie_commit_seqno  ← excluded
         * [2] _hoodie_record_key    ← excluded
         * [3] _hoodie_partition_path← excluded
         * [4] _hoodie_file_name     ← excluded
         * [5] STARTTIME             → becomes index 0 after exclusion
         * [6] IMSI                  → becomes index 1 after exclusion
         * [7] MSISDN                → becomes index 2 after exclusion
         * </pre>
         *
         * With {@code .excludedColumnPrefix("_hoodie")}, use:
         * <pre>{@code .rowKeyRule("STARTTIME,0,false,10#IMSI,1,true,15") }</pre>
         */
        public Builder rowKeyRule(String v)        { rowKeyRule = v;        return this; }

        public Builder columnFamily(String v)      { columnFamily = v;      return this; }
        public Builder compression(String v)       { compression = v;       return this; }
        public Builder compressionLevel(int v)     { compressionLevel = v;  return this; }
        public Builder dataBlockEncoding(String v) { dataBlockEncoding = v; return this; }
        public Builder bloomType(String v)         { bloomType = v;         return this; }
        public Builder fsyncPolicy(String v)       { fsyncPolicy = v;       return this; }
        public Builder errorPolicy(String v)       { errorPolicy = v;       return this; }
        public Builder blockSize(int v)            { blockSize = v;         return this; }
        public Builder maxMemoryBytes(long v)      { maxMemoryBytes = v;    return this; }
        public Builder defaultTimestampMs(long v)  { defaultTimestampMs = v; return this; }

        /**
         * Exclude one column by exact name from HBase KV output.
         * May be called multiple times.  Does NOT affect row key indices.
         */
        public Builder excludedColumn(String name) {
            excludedColumns.add(name);
            return this;
        }

        /** Exclude multiple columns by exact name. */
        public Builder excludedColumns(List<String> names) {
            excludedColumns.addAll(names);
            return this;
        }

        /**
         * Exclude all columns whose name starts with {@code prefix}.
         * May be called multiple times for multiple prefixes.
         *
         * <p>Hudi example — drop all five metadata columns at once:
         * <pre>{@code .excludedColumnPrefix("_hoodie") }</pre>
         */
        public Builder excludedColumnPrefix(String prefix) {
            excludedColumnPrefixes.add(prefix);
            return this;
        }

        /** Exclude all columns matching any of the given prefixes. */
        public Builder excludedColumnPrefixes(List<String> prefixes) {
            excludedColumnPrefixes.addAll(prefixes);
            return this;
        }

        /**
         * Absolute path to {@code libhfilesdk.so} / {@code .dylib} / {@code .dll}.
         * Falls back to {@code HFILESDK_NATIVE_LIB} env var or {@code java.library.path}.
         */
        public Builder nativeLibPath(String v)     { nativeLibPath = v;     return this; }

        public ConvertOptions build() { return new ConvertOptions(this); }
    }

    @Override
    public String toString() {
        return "ConvertOptions{arrow=" + arrowPath +
               ", hfile=" + hfilePath +
               ", cf=" + columnFamily +
               ", compression=" + compression +
               (excludedColumnPrefixes.isEmpty() ? "" : ", excludePfx=" + excludedColumnPrefixes) +
               (excludedColumns.isEmpty()        ? "" : ", excludeCols=" + excludedColumns) + "}";
    }
}
