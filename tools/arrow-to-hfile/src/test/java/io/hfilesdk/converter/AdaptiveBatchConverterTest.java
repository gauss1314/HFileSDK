package io.hfilesdk.converter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AdaptiveBatchConverterTest {

    @Test
    void mergeAndDirectStrategiesPreserveFixedTimestampBytes(
            @TempDir Path tempDir) throws Exception {
        Path arrowDir = tempDir.resolve("arrow");
        Path directDir = tempDir.resolve("direct");
        Path mergedDir = tempDir.resolve("merged");
        Path mergeTmpDir = tempDir.resolve("merge-tmp");
        Files.createDirectories(arrowDir);
        writeArrowStream(arrowDir.resolve("part-001.arrow"));

        long fixedTimestamp = 1_715_678_900_123L;
        BatchConvertOptions directOptions = options(
            arrowDir, directDir, fixedTimestamp);
        BatchConvertResult direct =
            new BatchArrowToHFileConverter().convertAll(directOptions);
        assertTrue(direct.isFullSuccess(), direct.summary());

        BatchConvertOptions mergeOptions = options(
            arrowDir, mergedDir, fixedTimestamp);
        AdaptiveBatchConverter.Policy mergePolicy =
            AdaptiveBatchConverter.Policy.builder()
                .mergeThresholdMib(1024)
                .triggerSizeMib(512)
                .triggerCount(500)
                .triggerIntervalSeconds(180)
                .mergeTmpDir(mergeTmpDir)
                .build();
        BatchConvertResult merged = new AdaptiveBatchConverter()
            .convertAll(mergeOptions, mergePolicy);
        assertTrue(merged.isFullSuccess(), merged.summary());

        Path directHFile = onlyHFile(directDir);
        Path mergedHFile = onlyHFile(mergedDir);
        assertArrayEquals(
            Files.readAllBytes(directHFile),
            Files.readAllBytes(mergedHFile),
            "fixed timestamp must produce byte-identical HFiles across strategies");

        if (Files.exists(mergeTmpDir)) {
            try (var files = Files.list(mergeTmpDir)) {
                assertFalse(files.anyMatch(path -> path.toString().endsWith(".arrow")),
                    "temporary merged Arrow files must be deleted");
            }
        }
    }

    private static BatchConvertOptions options(
            Path arrowDir, Path hfileDir, long fixedTimestamp) {
        return BatchConvertOptions.builder()
            .arrowDir(arrowDir)
            .hfileDir(hfileDir)
            .tableName("strategy_test")
            .rowKeyRule("ID,0,false,0")
            .columnFamily("cf")
            .compression("NONE")
            .dataBlockEncoding("NONE")
            .bloomType("none")
            .parallelism(1)
            .defaultTimestampMs(fixedTimestamp)
            .build();
    }

    private static Path onlyHFile(Path dir) throws IOException {
        try (var files = Files.list(dir)) {
            List<Path> hfiles = files
                .filter(path -> path.toString().endsWith(".hfile"))
                .toList();
            if (hfiles.size() != 1) {
                throw new IOException("Expected exactly one HFile in " + dir +
                    ", found " + hfiles.size());
            }
            return hfiles.get(0);
        }
    }

    private static void writeArrowStream(Path path) throws IOException {
        List<String> ids = List.of("row2", "row1");
        List<String> values = List.of("value2", "value1");
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
