package io.hfilesdk.converter.javaimpl;

import java.util.Locale;

public final class JavaConvertResult {

    public static final int OK = 0;
    public static final int INVALID_ARGUMENT = 1;
    public static final int ARROW_FILE_ERROR = 2;
    public static final int SCHEMA_MISMATCH = 3;
    public static final int INVALID_ROW_KEY_RULE = 4;
    public static final int SORT_VIOLATION = 5;
    public static final int IO_ERROR = 10;
    public static final int INTERNAL_ERROR = 20;

    public final int errorCode;
    public final String errorMessage;
    public final String arrowPath;
    public final String hfilePath;
    public final long rowsRead;
    public final long kvWrittenCount;
    public final long hfileSizeBytes;
    public final long elapsedMs;
    public final long sortMs;
    public final long writeMs;

    private JavaConvertResult(
        int errorCode,
        String errorMessage,
        String arrowPath,
        String hfilePath,
        long rowsRead,
        long kvWrittenCount,
        long hfileSizeBytes,
        long elapsedMs,
        long sortMs,
        long writeMs
    ) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage == null ? "" : errorMessage;
        this.arrowPath = arrowPath == null ? "" : arrowPath;
        this.hfilePath = hfilePath == null ? "" : hfilePath;
        this.rowsRead = rowsRead;
        this.kvWrittenCount = kvWrittenCount;
        this.hfileSizeBytes = hfileSizeBytes;
        this.elapsedMs = elapsedMs;
        this.sortMs = sortMs;
        this.writeMs = writeMs;
    }

    public static JavaConvertResult success(String arrowPath,
                                            String hfilePath,
                                            long rowsRead,
                                            long kvWrittenCount,
                                            long hfileSizeBytes,
                                            long elapsedMs,
                                            long sortMs,
                                            long writeMs) {
        return new JavaConvertResult(
            OK, "", arrowPath, hfilePath, rowsRead, kvWrittenCount, hfileSizeBytes, elapsedMs, sortMs, writeMs);
    }

    public static JavaConvertResult error(int errorCode,
                                          String errorMessage,
                                          String arrowPath,
                                          String hfilePath,
                                          long rowsRead,
                                          long kvWrittenCount,
                                          long elapsedMs,
                                          long sortMs,
                                          long writeMs) {
        return new JavaConvertResult(
            errorCode, errorMessage, arrowPath, hfilePath, rowsRead, kvWrittenCount, 0L, elapsedMs, sortMs, writeMs);
    }

    public boolean isSuccess() {
        return errorCode == OK;
    }

    public double throughputMbps(long inputBytes) {
        if (elapsedMs <= 0L) {
            return 0.0;
        }
        return inputBytes / 1024.0 / 1024.0 / (elapsedMs / 1000.0);
    }

    public String summary() {
        if (isSuccess()) {
            return String.format(
                Locale.ROOT,
                "rows=%d kvs=%d hfile=%.2fMiB elapsed=%dms",
                rowsRead,
                kvWrittenCount,
                hfileSizeBytes / 1024.0 / 1024.0,
                elapsedMs
            ) + String.format(Locale.ROOT, " sort=%dms write=%dms", sortMs, writeMs);
        }
        return "errorCode=" + errorCode + ", errorMessage=" + errorMessage;
    }

    public String toJson() {
        return "{"
            + "\"error_code\":" + errorCode + ","
            + "\"error_message\":\"" + escape(errorMessage) + "\","
            + "\"arrow_path\":\"" + escape(arrowPath) + "\","
            + "\"hfile_path\":\"" + escape(hfilePath) + "\","
            + "\"rows_read\":" + rowsRead + ","
            + "\"kv_written_count\":" + kvWrittenCount + ","
            + "\"hfile_size_bytes\":" + hfileSizeBytes + ","
            + "\"elapsed_ms\":" + elapsedMs + ","
            + "\"sort_ms\":" + sortMs + ","
            + "\"write_ms\":" + writeMs
            + "}";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
