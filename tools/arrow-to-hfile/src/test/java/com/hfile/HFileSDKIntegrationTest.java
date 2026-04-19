package com.hfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
        assertTrue(lastResult.contains("\"kv_written_count\":4"));
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
        assertTrue(lastResult.contains("\"kv_written_count\":4"));
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
