package io.hfilesdk.converter.javaimpl;

import java.util.ArrayList;
import java.util.List;

public final class JavaConvertOptions {

    private final String arrowPath;
    private final String hfilePath;
    private final String tableName;
    private final String rowKeyRule;
    private final String columnFamily;
    private final String compression;
    private final int compressionLevel;
    private final String dataBlockEncoding;
    private final String bloomType;
    private final int blockSize;
    private final long defaultTimestampMs;
    private final List<String> excludedColumns;
    private final List<String> excludedPrefixes;

    private JavaConvertOptions(Builder builder) {
        this.arrowPath = requireText(builder.arrowPath, "arrowPath");
        this.hfilePath = requireText(builder.hfilePath, "hfilePath");
        this.tableName = builder.tableName == null ? "" : builder.tableName;
        this.rowKeyRule = requireText(builder.rowKeyRule, "rowKeyRule");
        this.columnFamily = builder.columnFamily == null || builder.columnFamily.isBlank() ? "cf" : builder.columnFamily;
        this.compression = builder.compression == null || builder.compression.isBlank() ? "GZ" : builder.compression;
        this.compressionLevel = requireCompressionLevel(builder.compressionLevel, "compressionLevel");
        this.dataBlockEncoding = builder.dataBlockEncoding == null || builder.dataBlockEncoding.isBlank()
            ? "NONE"
            : builder.dataBlockEncoding;
        this.bloomType = builder.bloomType == null || builder.bloomType.isBlank() ? "ROW" : builder.bloomType;
        this.blockSize = builder.blockSize <= 0 ? 65536 : builder.blockSize;
        this.defaultTimestampMs = requireNonNegative(builder.defaultTimestampMs, "defaultTimestampMs");
        this.excludedColumns = List.copyOf(builder.excludedColumns);
        this.excludedPrefixes = List.copyOf(builder.excludedPrefixes);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String arrowPath() {
        return arrowPath;
    }

    public String hfilePath() {
        return hfilePath;
    }

    public String tableName() {
        return tableName;
    }

    public String rowKeyRule() {
        return rowKeyRule;
    }

    public String columnFamily() {
        return columnFamily;
    }

    public String compression() {
        return compression;
    }

    public int compressionLevel() {
        return compressionLevel;
    }

    public String dataBlockEncoding() {
        return dataBlockEncoding;
    }

    public String bloomType() {
        return bloomType;
    }

    public int blockSize() {
        return blockSize;
    }

    public long defaultTimestampMs() {
        return defaultTimestampMs;
    }

    public List<String> excludedColumns() {
        return excludedColumns;
    }

    public List<String> excludedPrefixes() {
        return excludedPrefixes;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " 不能为空");
        }
        return value;
    }

    private static long requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " 不能小于 0");
        }
        return value;
    }

    private static int requireCompressionLevel(int value, String name) {
        if (value < 0 || value > 9) {
            throw new IllegalArgumentException(name + " 必须在 0-9 之间");
        }
        return value;
    }

    public static final class Builder {
        private String arrowPath;
        private String hfilePath;
        private String tableName = "";
        private String rowKeyRule;
        private String columnFamily = "cf";
        private String compression = "GZ";
        private int compressionLevel = 1;
        private String dataBlockEncoding = "NONE";
        private String bloomType = "ROW";
        private int blockSize = 65536;
        private long defaultTimestampMs;
        private final List<String> excludedColumns = new ArrayList<>();
        private final List<String> excludedPrefixes = new ArrayList<>();

        private Builder() {}

        public Builder arrowPath(String arrowPath) {
            this.arrowPath = arrowPath;
            return this;
        }

        public Builder hfilePath(String hfilePath) {
            this.hfilePath = hfilePath;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder rowKeyRule(String rowKeyRule) {
            this.rowKeyRule = rowKeyRule;
            return this;
        }

        public Builder columnFamily(String columnFamily) {
            this.columnFamily = columnFamily;
            return this;
        }

        public Builder compression(String compression) {
            this.compression = compression;
            return this;
        }

        public Builder compressionLevel(int compressionLevel) {
            this.compressionLevel = compressionLevel;
            return this;
        }

        public Builder dataBlockEncoding(String dataBlockEncoding) {
            this.dataBlockEncoding = dataBlockEncoding;
            return this;
        }

        public Builder bloomType(String bloomType) {
            this.bloomType = bloomType;
            return this;
        }

        public Builder blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder defaultTimestampMs(long defaultTimestampMs) {
            this.defaultTimestampMs = defaultTimestampMs;
            return this;
        }

        public Builder excludedColumns(List<String> excludedColumns) {
            this.excludedColumns.clear();
            if (excludedColumns != null) {
                this.excludedColumns.addAll(excludedColumns.stream().filter(value -> value != null && !value.isBlank()).toList());
            }
            return this;
        }

        public Builder excludedPrefixes(List<String> excludedPrefixes) {
            this.excludedPrefixes.clear();
            if (excludedPrefixes != null) {
                this.excludedPrefixes.addAll(excludedPrefixes.stream().filter(value -> value != null && !value.isBlank()).toList());
            }
            return this;
        }

        public Builder addExcludedColumn(String column) {
            if (column != null && !column.isBlank()) {
                this.excludedColumns.add(column);
            }
            return this;
        }

        public Builder addExcludedPrefix(String prefix) {
            if (prefix != null && !prefix.isBlank()) {
                this.excludedPrefixes.add(prefix);
            }
            return this;
        }

        public JavaConvertOptions build() {
            return new JavaConvertOptions(this);
        }
    }
}
