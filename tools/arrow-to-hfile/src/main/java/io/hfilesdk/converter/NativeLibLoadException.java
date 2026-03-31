package io.hfilesdk.converter;

/**
 * Thrown when the native {@code libhfilesdk} library cannot be located or loaded.
 */
public class NativeLibLoadException extends RuntimeException {

    public NativeLibLoadException(String message) {
        super(message);
    }

    public NativeLibLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
