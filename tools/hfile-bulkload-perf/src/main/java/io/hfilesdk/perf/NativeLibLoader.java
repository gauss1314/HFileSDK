package io.hfilesdk.perf;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public final class NativeLibLoader {

    private static volatile boolean loaded = false;
    private static final Object LOCK = new Object();

    private NativeLibLoader() {}

    public static void load(String libPath) {
        if (loaded) {
            return;
        }
        synchronized (LOCK) {
            if (loaded) {
                return;
            }
            if (libPath == null || libPath.isBlank()) {
                throw new IllegalArgumentException("native library path is required");
            }
            File file = new File(libPath);
            if (!file.isAbsolute()) {
                file = file.getAbsoluteFile();
            }
            Path path = file.toPath();
            if (!Files.exists(path)) {
                throw new IllegalArgumentException("native library not found: " + path);
            }
            if (!Files.isRegularFile(path)) {
                throw new IllegalArgumentException("native library is not a file: " + path);
            }
            System.load(path.toString());
            System.setProperty("hfilesdk.native.loaded", "true");
            loaded = true;
        }
    }
}
