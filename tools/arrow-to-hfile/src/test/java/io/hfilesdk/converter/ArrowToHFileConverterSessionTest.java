package io.hfilesdk.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArrowToHFileConverterSessionTest {

    @Test
    void workerSessionConfiguresOnceUntilOptionsChange(@TempDir Path tempDir)
            throws Exception {
        Path arrow = tempDir.resolve("input.arrow");
        writeArrowStream(arrow);

        ArrowToHFileConverter converter = new ArrowToHFileConverter();
        try {
            ConvertResult first = converter.convert(options(
                arrow, tempDir.resolve("first.hfile"), "none"));
            ConvertResult second = converter.convert(options(
                arrow, tempDir.resolve("second.hfile"), "none"));

            assertTrue(first.isSuccess(), first.errorMessage);
            assertTrue(second.isSuccess(), second.errorMessage);
            assertEquals(1, converter.preparedSessionCountForTesting());
            assertEquals(1L, converter.configureCallCountForTesting());

            // Changing a writer option creates and configures a fresh native
            // session. Paths alone are not part of the configuration key.
            ConvertResult changed = converter.convert(options(
                arrow, tempDir.resolve("changed.hfile"), "row"));
            assertTrue(changed.isSuccess(), changed.errorMessage);
            assertEquals(1, converter.preparedSessionCountForTesting());
            assertEquals(2L, converter.configureCallCountForTesting());
        } finally {
            converter.close();
        }

        ConvertResult afterClose = converter.convert(options(
            arrow, tempDir.resolve("closed.hfile"), "none"));
        assertEquals(ConvertResult.INTERNAL_ERROR, afterClose.errorCode);
        assertTrue(afterClose.errorMessage.contains("closed"));
    }

    @Test
    void boundedLeasePoolDoesNotRetainOneSessionPerCallingThread(
            @TempDir Path tempDir) throws Exception {
        Path arrow = tempDir.resolve("bounded.arrow");
        writeArrowStream(arrow);

        ArrowToHFileConverter converter = new ArrowToHFileConverter(null, 2);
        int taskCount = 16;
        ExecutorService callers = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<ConvertResult>> futures = new ArrayList<>();
            for (int i = 0; i < taskCount; ++i) {
                final int index = i;
                futures.add(callers.submit(() -> {
                    assertTrue(start.await(1, TimeUnit.MINUTES));
                    return converter.convert(options(
                        arrow, tempDir.resolve("bounded-" + index + ".hfile"), "none"));
                }));
            }
            start.countDown();
            for (Future<ConvertResult> future : futures) {
                ConvertResult result = future.get(2, TimeUnit.MINUTES);
                assertTrue(result.isSuccess(), result.errorMessage);
            }

            assertTrue(converter.preparedSessionCountForTesting() <= 2);
            assertEquals(
                converter.preparedSessionCountForTesting(),
                converter.configureCallCountForTesting());
        } finally {
            start.countDown();
            callers.shutdownNow();
            converter.close();
        }
        assertEquals(0, converter.preparedSessionCountForTesting());
    }

    private static ConvertOptions options(Path arrow, Path hfile, String bloomType) {
        return ConvertOptions.builder()
            .arrowPath(arrow.toString())
            .hfilePath(hfile.toString())
            .tableName("test_table")
            .rowKeyRule("ID,0,false,0")
            .columnFamily("cf")
            .compression("NONE")
            .dataBlockEncoding("NONE")
            .bloomType(bloomType)
            .defaultTimestampMs(1_715_678_900_123L)
            .build();
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
