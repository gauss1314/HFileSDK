package io.hfilesdk.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArrowFileMergerTest {

    @Test
    void mergeCombinesArrowFilesInOrder(@TempDir Path tempDir) throws Exception {
        Path first = tempDir.resolve("part-1.arrow");
        Path second = tempDir.resolve("part-2.arrow");
        Path merged = tempDir.resolve("merged.arrow");

        writeArrowStream(first, "id", "value",
            List.of("row1", "row2"), List.of("value1", "value2"));
        writeArrowStream(second, "id", "value",
            List.of("row3"), List.of("value3"));

        Path result = ArrowFileMerger.merge(List.of(first, second), merged);

        assertEquals(merged, result);
        assertTrue(Files.exists(merged));
        assertEquals(
            List.of("row1=value1", "row2=value2", "row3=value3"),
            readRows(merged));
    }

    @Test
    void mergeRejectsSchemaMismatch(@TempDir Path tempDir) throws Exception {
        Path first = tempDir.resolve("part-1.arrow");
        Path second = tempDir.resolve("part-2.arrow");
        Path merged = tempDir.resolve("merged.arrow");

        writeArrowStream(first, "id", "value",
            List.of("row1"), List.of("value1"));
        writeArrowStream(second, "id", "payload",
            List.of("row2"), List.of("value2"));

        IOException ex = assertThrows(
            IOException.class,
            () -> ArrowFileMerger.merge(List.of(first, second), merged));

        assertTrue(ex.getMessage().contains("Schema mismatch"));
    }

    private static void writeArrowStream(Path path,
                                         String idColumnName,
                                         String valueColumnName,
                                         List<String> ids,
                                         List<String> values) throws IOException {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VarCharVector idVector = new VarCharVector(idColumnName, allocator);
             VarCharVector valueVector = new VarCharVector(valueColumnName, allocator)) {
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

    private static List<String> readRows(Path path) throws IOException {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ArrowStreamReader reader = new ArrowStreamReader(Files.newInputStream(path), allocator)) {
            List<String> rows = new ArrayList<>();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                VarCharVector idVector = (VarCharVector) root.getVector("id");
                VarCharVector valueVector = (VarCharVector) root.getVector("value");
                for (int i = 0; i < root.getRowCount(); i++) {
                    rows.add(
                        new String(idVector.get(i), StandardCharsets.UTF_8) + "=" +
                        new String(valueVector.get(i), StandardCharsets.UTF_8));
                }
            }
            return rows;
        }
    }
}
