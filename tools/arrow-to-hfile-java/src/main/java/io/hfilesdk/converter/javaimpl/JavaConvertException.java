package io.hfilesdk.converter.javaimpl;

public final class JavaConvertException extends RuntimeException {

    private final JavaConvertResult result;

    public JavaConvertException(JavaConvertResult result) {
        super(result == null ? "unknown conversion error" : result.summary());
        this.result = result;
    }

    public JavaConvertResult result() {
        return result;
    }
}
