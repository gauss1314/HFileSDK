package com.hfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class HFileSDKIntegrationTest {

    @Test
    void convertWritesReadableSortedHFile(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path arrowPath = tempDir.resolve("input.arrow");
        java.nio.file.Path hfilePath = tempDir.resolve("output.hfile");
        writeArrowStream(arrowPath, List.of("row2", "row1"), List.of("value2", "value1"));

        HFileSDK sdk = new HFileSDK();
        int configureRc = sdk.configure("""
            {
              "compression":"none",
              "column_family":"cf",
              "data_block_encoding":"NONE",
              "bloom_type":"row",
              "include_mvcc":0
            }
            """);
        assertEquals(HFileSDK.OK, configureRc);

        int rc = sdk.convert(
            arrowPath.toString(),
            hfilePath.toString(),
            "test_table",
            "ID,0,false,0");
        assertEquals(HFileSDK.OK, rc);

        String lastResult = sdk.getLastResult();
        assertTrue(lastResult.contains("\"error_code\":0"));
        assertTrue(lastResult.contains("\"kv_written_count\":2"));
        assertTrue(lastResult.contains("\"arrow_rows_read\":2"));
        assertTrue(Files.exists(hfilePath));
        assertTrue(Files.size(hfilePath) > 0);
    }

    @Test
    void configureRejectsInvalidJson() {
        HFileSDK sdk = new HFileSDK();
        int rc = sdk.configure("{\"compression\":\"bogus\"}");
        assertEquals(HFileSDK.INVALID_ARGUMENT, rc);
        String lastResult = sdk.getLastResult();
        assertTrue(lastResult.contains("\"error_code\":1"));
    }

    @Test
    void configureAcceptsCanonicalGzAndLegacyGzipAlias() {
        HFileSDK sdk = new HFileSDK();
        assertEquals(HFileSDK.OK, sdk.configure("{\"compression\":\"GZ\"}"));
        assertEquals(HFileSDK.OK, sdk.configure("{\"compression\":\"gzip\"}"));
    }

    @Test
    void configureRejectsUnsupportedCompressionModes() {
        HFileSDK sdk = new HFileSDK();
        assertEquals(HFileSDK.INVALID_ARGUMENT, sdk.configure("{\"compression\":\"lz4\"}"));
        assertTrue(sdk.getLastResult().contains("NONE, GZ, or gzip"));
    }

    @Test
    void configureRejectsNegativeMaxMemoryBytes() {
        HFileSDK sdk = new HFileSDK();
        int rc = sdk.configure("{\"max_memory_bytes\":-1}");
        assertEquals(HFileSDK.INVALID_ARGUMENT, rc);
        String lastResult = sdk.getLastResult();
        assertTrue(lastResult.contains("max_memory_bytes"));
    }

    @Test
    void configureRejectsNegativeCompressionPipelineSettings() {
        HFileSDK sdk = new HFileSDK();

        int threadRc = sdk.configure("{\"compression_threads\":-1}");
        assertEquals(HFileSDK.INVALID_ARGUMENT, threadRc);
        assertTrue(sdk.getLastResult().contains("compression_threads"));

        int queueRc = sdk.configure("{\"compression_queue_depth\":-1}");
        assertEquals(HFileSDK.INVALID_ARGUMENT, queueRc);
        assertTrue(sdk.getLastResult().contains("compression_queue_depth"));

        int oversizedQueueRc = sdk.configure(
            "{\"compression_queue_depth\":4097}");
        assertEquals(HFileSDK.INVALID_ARGUMENT, oversizedQueueRc);
        assertTrue(sdk.getLastResult().contains("0-4096"));
    }

    @Test
    void configureAcceptsCompressionPipelineSettings() {
        HFileSDK sdk = new HFileSDK();
        int rc = sdk.configure("""
            {
              "compression":"GZ",
              "compression_threads":2,
              "compression_queue_depth":4,
              "numeric_sort_fast_path":"on"
            }
            """);
        assertEquals(HFileSDK.OK, rc);
    }

    @Test
    void configureRejectsInvalidNumericSortFastPathMode() {
        HFileSDK sdk = new HFileSDK();
        int rc = sdk.configure("{\"numeric_sort_fast_path\":\"maybe\"}");
        assertEquals(HFileSDK.INVALID_ARGUMENT, rc);
        assertTrue(sdk.getLastResult().contains("numeric_sort_fast_path"));
    }

    @Test
    void configureFailureDoesNotPartiallyApplyNumericSortFastPath(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path arrowPath = tempDir.resolve("input.arrow");
        java.nio.file.Path hfilePath = tempDir.resolve("output.hfile");
        writeArrowStream(arrowPath, List.of("row2", "row1"), List.of("value2", "value1"));

        HFileSDK sdk = new HFileSDK();
        assertEquals(HFileSDK.OK, sdk.configure("{\"numeric_sort_fast_path\":\"off\"}"));

        int rc = sdk.configure("{\"numeric_sort_fast_path\":\"on\",\"default_timestamp_ms\":-1}");
        assertEquals(HFileSDK.INVALID_ARGUMENT, rc);
        assertTrue(sdk.getLastResult().contains("default_timestamp_ms"));

        assertEquals(HFileSDK.OK, sdk.convert(
            arrowPath.toString(),
            hfilePath.toString(),
            "test_table",
            "ID,0,false,0"));
    }

    @Test
    void configureAcceptsFixedDefaultTimestamp(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path arrowPath = tempDir.resolve("input.arrow");
        java.nio.file.Path hfilePath = tempDir.resolve("output.hfile");
        writeArrowStream(arrowPath, List.of("row2", "row1"), List.of("value2", "value1"));

        HFileSDK sdk = new HFileSDK();
        int configureRc = sdk.configure("""
            {
              "compression":"none",
              "column_family":"cf",
              "data_block_encoding":"NONE",
              "bloom_type":"row",
              "include_mvcc":0,
              "default_timestamp_ms":1715678900123
            }
            """);
        assertEquals(HFileSDK.OK, configureRc);

        assertEquals(HFileSDK.OK, sdk.convert(
            arrowPath.toString(),
            hfilePath.toString(),
            "test_table",
            "ID,0,false,0"));
        String lastResult = sdk.getLastResult();
        assertTrue(lastResult.contains("\"error_code\":0"));
        assertTrue(Files.exists(hfilePath));
        assertTrue(Files.size(hfilePath) > 0);
    }

    @Test
    void builderOmitsDefaultTimestampUnlessExplicitlyConfigured() {
        String defaultConfig = HFileSDK.builder().toConfigJson();
        assertFalse(defaultConfig.contains("default_timestamp_ms"), defaultConfig);
        assertFalse(defaultConfig.contains("\"max_memory_bytes\":0"), defaultConfig);

        String fixedConfig = HFileSDK.builder()
            .defaultTimestampMs(1_715_678_900_123L)
            .toConfigJson();
        assertTrue(fixedConfig.contains("\"default_timestamp_ms\":1715678900123"), fixedConfig);
    }

    @Test
    void instanceStateIsIsolatedAcrossSdkObjects() {
        HFileSDK sdk1 = new HFileSDK();
        HFileSDK sdk2 = new HFileSDK();

        assertEquals(HFileSDK.INVALID_ARGUMENT, sdk1.configure("{\"compression\":\"bogus\"}"));
        assertEquals(HFileSDK.INVALID_ARGUMENT, sdk2.convert("", "", "test_table", "ID,0,false,0"));

        String result1 = sdk1.getLastResult();
        String result2 = sdk2.getLastResult();
        assertTrue(result1.contains("compression"));
        assertTrue(result2.contains("must not be null/empty"));
    }

    @Test
    void closeDestroysNativeSessionAndRejectsLaterCalls() {
        HFileSDK sdk = new HFileSDK();
        long handle = sdk.nativeHandleForTesting();
        assertTrue(handle != 0L);
        assertTrue(HFileSDK.nativeSessionExistsForTesting(handle));

        sdk.close();
        sdk.close(); // idempotent

        assertFalse(HFileSDK.nativeSessionExistsForTesting(handle));
        assertEquals(HFileSDK.INTERNAL_ERROR, sdk.configure("{}"));
        assertEquals(HFileSDK.INTERNAL_ERROR,
            sdk.convert("input.arrow", "output.hfile", "table", "ID,0,false,0"));
        assertTrue(sdk.getLastResult().contains("closed or invalid"));
    }

    @Test
    void corruptedNativeHandleIsRejectedWithoutDereference() throws Exception {
        long allocatedHandle;
        try (HFileSDK sdk = new HFileSDK()) {
            allocatedHandle = sdk.nativeHandleForTesting();
            assertTrue(HFileSDK.nativeSessionExistsForTesting(allocatedHandle));

            Field handleField = HFileSDK.class.getDeclaredField("nativeHandle");
            handleField.setAccessible(true);
            handleField.setLong(sdk, Long.MAX_VALUE);

            assertEquals(HFileSDK.INTERNAL_ERROR, sdk.configure("{}"));
            assertEquals(HFileSDK.INTERNAL_ERROR,
                sdk.convert("input.arrow", "output.hfile", "table", "ID,0,false,0"));
            assertTrue(sdk.getLastResult().contains("closed or invalid"));

            // Cleaner state owns the real handle independently of the Java
            // field, so close still releases it after field corruption.
            assertTrue(HFileSDK.nativeSessionExistsForTesting(allocatedHandle));
        }
        assertFalse(HFileSDK.nativeSessionExistsForTesting(allocatedHandle));
    }

    @Test
    void legacyCompiledBridgeStillResolvesOriginalJniSymbols(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        String nativeLib = System.getenv("HFILESDK_NATIVE_LIB");
        Assumptions.assumeTrue(nativeLib != null && !nativeLib.isBlank());

        Path sourceRoot = tempDir.resolve("legacy-src");
        Path classes = tempDir.resolve("legacy-classes");
        Path packageDir = sourceRoot.resolve("com/hfile");
        Files.createDirectories(packageDir);
        Files.createDirectories(classes);

        // This is the pre-nativeHandle binary surface. It deliberately declares
        // the original methods as native and therefore resolves the three legacy
        // Java_com_hfile_HFileSDK_* symbols in a fresh JVM.
        Files.writeString(packageDir.resolve("HFileSDK.java"), """
            package com.hfile;
            public class HFileSDK {
                static { System.load(System.getProperty("hfilesdk.legacy.native")); }
                public native int convert(String arrow, String hfile, String table, String rule);
                public native String getLastResult();
                public native int configure(String json);
            }
            """, StandardCharsets.UTF_8);
        Files.writeString(sourceRoot.resolve("LegacyMain.java"), """
            import com.hfile.HFileSDK;
            public class LegacyMain {
                public static void main(String[] args) {
                    HFileSDK sdk = new HFileSDK();
                    if (sdk.configure("{}") != 0) throw new AssertionError("configure");
                    if (sdk.convert("", "", "table", "ID,0,false,0") != 1) {
                        throw new AssertionError("convert");
                    }
                    if (!sdk.getLastResult().contains("must not be null/empty")) {
                        throw new AssertionError("getLastResult");
                    }
                }
            }
            """, StandardCharsets.UTF_8);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertTrue(compiler != null, "Tests require a JDK, not a JRE");
        int compileRc = compiler.run(
            null, null, null,
            "-d", classes.toString(),
            packageDir.resolve("HFileSDK.java").toString(),
            sourceRoot.resolve("LegacyMain.java").toString());
        assertEquals(0, compileRc);

        Process process = new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Dhfilesdk.legacy.native=" + Path.of(nativeLib).toAbsolutePath(),
            "-cp", classes.toString(),
            "LegacyMain")
            .redirectErrorStream(true)
            .start();
        assertTrue(process.waitFor(1, TimeUnit.MINUTES));
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), output);
    }

    @Test
    void oneSdkSessionSupportsConcurrentIndependentConversions(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path arrowPath = tempDir.resolve("concurrent.arrow");
        writeArrowStream(
            arrowPath,
            List.of("row4", "row2", "row3", "row1"),
            List.of("value4", "value2", "value3", "value1"));

        try (HFileSDK sdk = new HFileSDK()) {
            assertEquals(HFileSDK.OK, sdk.configure("""
                {
                  "compression":"none",
                  "column_family":"cf",
                  "data_block_encoding":"NONE",
                  "bloom_type":"row",
                  "include_mvcc":0,
                  "default_timestamp_ms":1715678900123
                }
                """));

            ExecutorService pool = Executors.newFixedThreadPool(4);
            try {
                List<Future<Integer>> futures = new ArrayList<>();
                for (int i = 0; i < 4; ++i) {
                    java.nio.file.Path output = tempDir.resolve("concurrent-" + i + ".hfile");
                    futures.add(pool.submit(() -> sdk.convert(
                        arrowPath.toString(), output.toString(),
                        "test_table", "ID,0,false,0")));
                }
                pool.shutdown();
                assertTrue(pool.awaitTermination(2, TimeUnit.MINUTES));
                for (int i = 0; i < futures.size(); ++i) {
                    assertEquals(HFileSDK.OK, futures.get(i).get());
                    assertTrue(Files.size(tempDir.resolve("concurrent-" + i + ".hfile")) > 0);
                }
                assertTrue(sdk.getLastResult().contains("\"error_code\":0"));
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void concurrentCallsKeepReturnCodePairedWithCallingThreadResult(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path arrowPath = tempDir.resolve("paired.arrow");
        writeArrowStream(
            arrowPath,
            List.of("row2", "row1"),
            List.of("value2", "value1"));

        record CallResult(int returnCode, String resultJson) {}

        try (HFileSDK sdk = new HFileSDK()) {
            assertEquals(HFileSDK.OK, sdk.configure("""
                {
                  "compression":"none",
                  "column_family":"cf",
                  "data_block_encoding":"NONE",
                  "bloom_type":"none",
                  "include_mvcc":0,
                  "default_timestamp_ms":1715678900123
                }
                """));

            int callCount = 6;
            ExecutorService pool = Executors.newFixedThreadPool(callCount);
            CountDownLatch conversionsReturned = new CountDownLatch(callCount);
            CountDownLatch readResults = new CountDownLatch(1);
            try {
                List<Future<CallResult>> futures = new ArrayList<>();
                for (int i = 0; i < callCount; ++i) {
                    final int index = i;
                    futures.add(pool.submit(() -> {
                        boolean valid = (index & 1) == 0;
                        String input = valid
                            ? arrowPath.toString()
                            : tempDir.resolve("missing-" + index + ".arrow").toString();
                        int rc = sdk.convert(
                            input,
                            tempDir.resolve("paired-" + index + ".hfile").toString(),
                            "test_table",
                            "ID,0,false,0");
                        conversionsReturned.countDown();
                        assertTrue(readResults.await(1, TimeUnit.MINUTES));
                        return new CallResult(rc, sdk.getLastResult());
                    }));
                }

                assertTrue(conversionsReturned.await(2, TimeUnit.MINUTES));
                readResults.countDown();
                for (int i = 0; i < futures.size(); ++i) {
                    CallResult result = futures.get(i).get();
                    int expected = (i & 1) == 0
                        ? HFileSDK.OK
                        : HFileSDK.ARROW_FILE_ERROR;
                    assertEquals(expected, result.returnCode());
                    assertTrue(result.resultJson().contains(
                        "\"error_code\":" + expected), result.resultJson());
                }
            } finally {
                readResults.countDown();
                pool.shutdownNow();
            }
        }
    }

    @Test
    void convertReportsInvalidRule(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path arrowPath = tempDir.resolve("input.arrow");
        java.nio.file.Path hfilePath = tempDir.resolve("output.hfile");
        writeArrowStream(arrowPath, List.of("row1"), List.of("value1"));

        HFileSDK sdk = new HFileSDK();
        int rc = sdk.convert(
            arrowPath.toString(),
            hfilePath.toString(),
            "test_table",
            "bad_rule");
        assertEquals(HFileSDK.INVALID_ROW_KEY_RULE, rc);
        String lastResult = sdk.getLastResult();
        assertTrue(lastResult.contains("\"error_code\":4"));
        assertTrue(lastResult.contains("rowKeyRule"));
    }

    @Test
    void convertRejectsEmptyPaths() {
        HFileSDK sdk = new HFileSDK();
        int rc = sdk.convert("", "", "test_table", "ID,0,false,0");
        assertEquals(HFileSDK.INVALID_ARGUMENT, rc);
        String lastResult = sdk.getLastResult();
        assertTrue(lastResult.contains("\"error_code\":1"));
        assertTrue(lastResult.contains("must not be null/empty"));
    }

    @Test
    void convertReportsTrackedMemoryMetrics(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path arrowPath = tempDir.resolve("input.arrow");
        java.nio.file.Path hfilePath = tempDir.resolve("output.hfile");
        writeArrowStream(arrowPath, List.of("row1", "row2"), List.of("value1", "value2"));

        HFileSDK sdk = new HFileSDK();
        int configureRc = sdk.configure("""
            {
              "compression":"none",
              "column_family":"cf",
              "data_block_encoding":"NONE",
              "bloom_type":"row",
              "include_mvcc":0,
              "max_memory_bytes":33554432
            }
            """);
        assertEquals(HFileSDK.OK, configureRc);

        int rc = sdk.convert(
            arrowPath.toString(),
            hfilePath.toString(),
            "test_table",
            "ID,0,false,0");
        assertEquals(HFileSDK.OK, rc);

        String lastResult = sdk.getLastResult();
        assertTrue(lastResult.contains("\"memory_budget_bytes\":33554432"));
        assertTrue(lastResult.contains("\"tracked_memory_peak_bytes\":"));
    }

    @Test
    void convertWritesReaderValidationFixture() throws Exception {
        String arrowPathValue = System.getProperty("hfilesdk.e2e.arrowPath");
        String hfilePathValue = System.getProperty("hfilesdk.e2e.hfilePath");
        Assumptions.assumeTrue(arrowPathValue != null && !arrowPathValue.isBlank());
        Assumptions.assumeTrue(hfilePathValue != null && !hfilePathValue.isBlank());

        Path arrowPath = Path.of(arrowPathValue);
        Path hfilePath = Path.of(hfilePathValue);
        Files.createDirectories(arrowPath.getParent());
        Files.createDirectories(hfilePath.getParent());
        Files.deleteIfExists(arrowPath);
        Files.deleteIfExists(hfilePath);

        writeArrowStream(arrowPath, List.of("row2", "row1"), List.of("value2", "value1"));

        HFileSDK sdk = new HFileSDK();
        int configureRc = sdk.configure("""
            {
              "compression":"none",
              "column_family":"cf",
              "data_block_encoding":"NONE",
              "bloom_type":"row",
              "include_mvcc":0
            }
            """);
        assertEquals(HFileSDK.OK, configureRc);

        int rc = sdk.convert(
            arrowPath.toString(),
            hfilePath.toString(),
            "test_table",
            "ID,0,false,0");
        assertEquals(HFileSDK.OK, rc);

        String lastResult = sdk.getLastResult();
        assertTrue(lastResult.contains("\"error_code\":0"));
        assertTrue(lastResult.contains("\"kv_written_count\":2"));
        assertTrue(Files.exists(hfilePath));
    }

    @Test
    void configureColumnFamilyAffectsConvert(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path arrowPath = tempDir.resolve("input.arrow");
        java.nio.file.Path hfilePath = tempDir.resolve("output.hfile");
        writeArrowStream(arrowPath, List.of("row1"), List.of("value1"));

        HFileSDK sdk = new HFileSDK();
        int configureRc = sdk.configure("""
            {
              "compression":"none",
              "column_family":"altcf",
              "data_block_encoding":"NONE",
              "bloom_type":"none",
              "include_mvcc":0
            }
            """);
        assertEquals(HFileSDK.OK, configureRc);

        int rc = sdk.convert(
            arrowPath.toString(),
            hfilePath.toString(),
            "test_table",
            "ID,0,false,0");
        assertEquals(HFileSDK.OK, rc);

        String raw = Files.readString(hfilePath, StandardCharsets.ISO_8859_1);
        assertTrue(raw.contains("altcf"));
    }

    private static void writeArrowStream(Path path,
                                         List<String> ids,
                                         List<String> values) throws IOException {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VarCharVector idVector = new VarCharVector("id", allocator);
             VarCharVector valueVector = new VarCharVector("value", allocator)) {
            idVector.allocateNew(ids.size() * 16, ids.size());
            valueVector.allocateNew(values.size() * 32, values.size());
            for (int i = 0; i < ids.size(); ++i) {
                idVector.setSafe(i, ids.get(i).getBytes(StandardCharsets.UTF_8));
                valueVector.setSafe(i, values.get(i).getBytes(StandardCharsets.UTF_8));
            }
            idVector.setValueCount(ids.size());
            valueVector.setValueCount(values.size());
            try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(idVector, valueVector));
                 ArrowStreamWriter writer = new ArrowStreamWriter(
                     root, null, Channels.newChannel(Files.newOutputStream(path)))) {
                root.setRowCount(ids.size());
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }
    }
}
