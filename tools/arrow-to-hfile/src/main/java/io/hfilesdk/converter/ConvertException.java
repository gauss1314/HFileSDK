package io.hfilesdk.converter;

/**
 * Thrown by {@link ArrowToHFileConverter#convertOrThrow} when the conversion
 * fails.  Carries the full {@link ConvertResult} for programmatic inspection.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * try {
 *     converter.convertOrThrow(opts);
 * } catch (ConvertException e) {
 *     ConvertResult r = e.getResult();
 *     if (r.errorCode == ConvertResult.ARROW_FILE_ERROR) {
 *         // handle missing/corrupt input file
 *     }
 *     log.error("Conversion failed: {}", e.getMessage());
 * }
 * }</pre>
 */
public class ConvertException extends RuntimeException {

    private final ConvertResult result;

    public ConvertException(ConvertResult result) {
        super(buildMessage(result));
        this.result = result;
    }

    public ConvertException(ConvertResult result, Throwable cause) {
        super(buildMessage(result), cause);
        this.result = result;
    }

    /** The full conversion result, including error code and all metrics. */
    public ConvertResult getResult() { return result; }

    /** The error code from {@link ConvertResult#errorCode}. */
    public int getErrorCode() { return result.errorCode; }

    private static String buildMessage(ConvertResult r) {
        String label = errorLabel(r.errorCode);
        if (r.errorMessage != null && !r.errorMessage.isBlank()) {
            return "Conversion failed [" + label + "]: " + r.errorMessage;
        }
        return "Conversion failed [" + label + "] (error code " + r.errorCode + ")";
    }

    private static String errorLabel(int code) {
        return switch (code) {
            case ConvertResult.OK                   -> "OK";
            case ConvertResult.INVALID_ARGUMENT     -> "INVALID_ARGUMENT";
            case ConvertResult.ARROW_FILE_ERROR     -> "ARROW_FILE_ERROR";
            case ConvertResult.SCHEMA_MISMATCH      -> "SCHEMA_MISMATCH";
            case ConvertResult.INVALID_ROW_KEY_RULE -> "INVALID_ROW_KEY_RULE";
            case ConvertResult.SORT_VIOLATION       -> "SORT_VIOLATION";
            case ConvertResult.IO_ERROR             -> "IO_ERROR";
            case ConvertResult.DISK_EXHAUSTED       -> "DISK_EXHAUSTED";
            case ConvertResult.MEMORY_EXHAUSTED     -> "MEMORY_EXHAUSTED";
            case ConvertResult.INTERNAL_ERROR       -> "INTERNAL_ERROR";
            default                                 -> "UNKNOWN";
        };
    }
}
