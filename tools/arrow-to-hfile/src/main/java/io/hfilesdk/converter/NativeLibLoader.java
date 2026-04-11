package io.hfilesdk.converter;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Loads the native {@code libhfilesdk} shared library before any
 * {@link com.hfile.HFileSDK} class reference causes its static initializer
 * to run.
 *
 * <h3>Search order</h3>
 * <ol>
 *   <li>Explicit path passed to {@link #load(String)}: uses {@link System#load}.</li>
 *   <li>{@code HFILESDK_NATIVE_LIB} environment variable (absolute path to .so/.dll).</li>
 *   <li>{@code HFILESDK_NATIVE_DIR} environment variable (directory; resolves
 *       platform library name automatically).</li>
 *   <li>{@code java.library.path} system property — falls through to
 *       {@link System#loadLibrary} inside {@code HFileSDK}'s static block.</li>
 * </ol>
 *
 * <h3>Why this class is necessary</h3>
 * {@code HFileSDK} contains:
 * <pre>{@code
 * static { System.loadLibrary("hfilesdk"); }
 * }</pre>
 * {@code System.loadLibrary} only searches {@code java.library.path} and cannot
 * load from an arbitrary filesystem path.  This class solves the problem by:
 * <ol>
 *   <li>Calling {@link System#load(String)} with the absolute path (bypasses
 *       {@code java.library.path}).</li>
 *   <li>Appending the library directory to the JVM's internal {@code usr_paths}
 *       list via reflection, so the subsequent {@code loadLibrary("hfilesdk")}
 *       call in the static block succeeds (the OS-level {@code dlopen} sees the
 *       library is already mapped and returns the existing handle).</li>
 * </ol>
 */
public final class NativeLibLoader {

    private static volatile boolean loaded = false;
    private static final Object LOCK = new Object();

    private NativeLibLoader() {}

    /**
     * Load the native library from an explicit absolute path.
     * Call this <em>once</em> at application startup before any
     * {@code HFileSDK} reference.
     *
     * @param libPath absolute path to {@code libhfilesdk.so} /
     *                {@code libhfilesdk.dylib} / {@code hfilesdk.dll}.
     *                Pass {@code null} or empty string to skip explicit loading
     *                and rely on environment variables or {@code java.library.path}.
     * @throws NativeLibLoadException if the library cannot be found or loaded.
     */
    public static void load(String libPath) {
        if (loaded) return;
        synchronized (LOCK) {
            if (loaded) return;
            doLoad(libPath);
            loaded = true;
        }
    }

    /** Returns {@code true} if the library has already been loaded. */
    public static boolean isLoaded() { return loaded; }

    // ─────────────────────────────────────────────────────────────────────────

    private static void doLoad(String explicitPath) {
        // 1. Try explicit path argument
        if (explicitPath != null && !explicitPath.isBlank()) {
            loadAbsolute(explicitPath, "explicit --native-lib argument");
            return;
        }

        // 2. Try HFILESDK_NATIVE_LIB env var (absolute path to the .so)
        String envLib = System.getenv("HFILESDK_NATIVE_LIB");
        if (envLib != null && !envLib.isBlank()) {
            loadAbsolute(envLib, "HFILESDK_NATIVE_LIB environment variable");
            return;
        }

        // 3. Try HFILESDK_NATIVE_DIR env var (directory; auto-resolve filename)
        String envDir = System.getenv("HFILESDK_NATIVE_DIR");
        if (envDir != null && !envDir.isBlank()) {
            String libFile = resolveLibName("hfilesdk");
            Path candidate = Paths.get(envDir, libFile);
            if (Files.exists(candidate)) {
                loadAbsolute(candidate.toAbsolutePath().toString(),
                             "HFILESDK_NATIVE_DIR environment variable");
                return;
            }
            throw new NativeLibLoadException(
                "HFILESDK_NATIVE_DIR=" + envDir + " is set but " +
                libFile + " was not found there.");
        }

        // 4. Fall through — rely on java.library.path (set by user via -D flag)
        // HFileSDK's static block will call System.loadLibrary("hfilesdk").
        // Nothing to do here; if the library isn't on the path, the JVM will
        // throw UnsatisfiedLinkError when HFileSDK is first referenced.
    }

    private static void loadAbsolute(String path, String source) {
        File file = new File(path);
        if (!file.isAbsolute()) {
            file = file.getAbsoluteFile();
        }
        if (!file.exists()) {
            throw new NativeLibLoadException(
                "Native library not found at '" + file + "' (from " + source + ").");
        }
        if (!file.isFile()) {
            throw new NativeLibLoadException(
                "'" + file + "' is not a file (from " + source + ").");
        }

        String absolutePath = file.getAbsolutePath();

        // Step A: System.load() — loads the .so by absolute path.
        // The JVM's native library deduplication uses canonical paths, so a
        // subsequent System.loadLibrary("hfilesdk") from the same ClassLoader
        // that resolves to the same file will be a no-op.
        System.load(absolutePath);
        System.setProperty("hfilesdk.native.loaded", "true");

        // Step B: Append the directory to the JVM's internal usr_paths list so
        // that System.loadLibrary("hfilesdk") in HFileSDK's static block succeeds.
        // This uses reflection on ClassLoader's private field — requires
        //   --add-opens java.base/java.lang=ALL-UNNAMED
        // which the fat jar sets via MANIFEST.MF Add-Opens, and the shell scripts
        // pass explicitly.
        String dir = file.getParent();
        if (dir != null) {
            appendToUsrPaths(dir);
        }
    }

    /**
     * Append {@code dir} to the JVM's private {@code usr_paths} string array
     * (the in-memory equivalent of {@code java.library.path}).
     * This ensures {@code System.loadLibrary("hfilesdk")} can find the library
     * even though it was already loaded by {@code System.load(absolutePath)}.
     */
    private static void appendToUsrPaths(String dir) {
        try {
            Field usrPathsField = ClassLoader.class.getDeclaredField("usr_paths");
            usrPathsField.setAccessible(true);
            String[] current = (String[]) usrPathsField.get(null);

            // Already present?
            for (String p : current) {
                if (p.equals(dir)) return;
            }

            String[] extended = Arrays.copyOf(current, current.length + 1);
            extended[current.length] = dir;
            usrPathsField.set(null, extended);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // Reflection failed (can happen with strict module settings).
            // The library was already loaded by System.load(), so native method
            // calls will still work.  Only System.loadLibrary("hfilesdk") in
            // HFileSDK's static block might fail.  Swallow and let the JVM
            // throw UnsatisfiedLinkError if that happens (with a clear message).
        }
    }

    /** Returns the platform-specific library filename for the given base name. */
    static String resolveLibName(String baseName) {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("win")) {
            return baseName + ".dll";
        } else if (os.contains("mac") || os.contains("darwin")) {
            return "lib" + baseName + ".dylib";
        } else {
            return "lib" + baseName + ".so";
        }
    }
}
