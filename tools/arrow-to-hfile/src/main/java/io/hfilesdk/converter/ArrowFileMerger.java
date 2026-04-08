package io.hfilesdk.converter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.file.*;
import java.util.List;

/**
 * Merges multiple Arrow IPC Stream files (*.arrow) into a single output file.
 *
 * <h3>设计</h3>
 * <ul>
 *   <li>逐个读取每个输入文件，每次一个 RecordBatch，追加到输出文件</li>
 *   <li>所有输入文件必须具有相同的 Arrow Schema（列名、类型完全一致）</li>
 *   <li>内存占用：固定上限 ≈ 1 个 RecordBatch（通常几MB~几十MB），与输入总大小无关</li>
 *   <li>不做类型转换：每个 RecordBatch 按原始 Arrow Schema 原样写入输出文件</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * Path merged = ArrowFileMerger.merge(
 *     List.of(
 *         Path.of("/data/event_001.arrow"),
 *         Path.of("/data/event_002.arrow"),
 *         Path.of("/data/event_003.arrow")
 *     ),
 *     Path.of("/tmp/merged_20550.arrow"));
 * }</pre>
 */
public final class ArrowFileMerger {

    private ArrowFileMerger() {}

    /**
     * Merge multiple Arrow IPC Stream files into a single output file.
     *
     * <p>All input files must have the same Arrow schema.
     * The output file is written atomically: data goes to a {@code .tmp} file
     * first, which is renamed to {@code outputPath} only on success.
     *
     * @param inputFiles  list of Arrow IPC Stream files to merge (in order)
     * @param outputPath  path for the merged output file
     * @return {@code outputPath} on success (for chaining)
     * @throws IOException if any file cannot be read or written,
     *                     or if schemas are incompatible
     */
    public static Path merge(List<Path> inputFiles, Path outputPath) throws IOException {
        if (inputFiles == null || inputFiles.isEmpty()) {
            throw new IllegalArgumentException("inputFiles must not be empty");
        }
        if (inputFiles.size() == 1) {
            // Single file: no merge needed — just copy to output path
            Files.createDirectories(outputPath.getParent() != null
                ? outputPath.getParent() : Path.of("."));
            Files.copy(inputFiles.get(0), outputPath,
                       StandardCopyOption.REPLACE_EXISTING);
            return outputPath;
        }

        // Ensure output directory exists
        if (outputPath.getParent() != null) {
            Files.createDirectories(outputPath.getParent());
        }
        Path tmpPath = outputPath.resolveSibling(outputPath.getFileName() + ".tmp");

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (OutputStream fos = new BufferedOutputStream(
                     Files.newOutputStream(tmpPath), 256 * 1024);
                 ArrowStreamReader firstReader = new ArrowStreamReader(
                     new BufferedInputStream(Files.newInputStream(inputFiles.get(0))),
                     allocator)) {
                var expectedSchema = firstReader.getVectorSchemaRoot().getSchema();

                try (VectorSchemaRoot outputRoot = VectorSchemaRoot.create(expectedSchema, allocator);
                     ArrowStreamWriter writer = new ArrowStreamWriter(
                         outputRoot, null, Channels.newChannel(fos))) {
                    VectorLoader loader = new VectorLoader(outputRoot);

                    writer.start();

                    while (firstReader.loadNextBatch()) {
                        VectorSchemaRoot batchRoot = firstReader.getVectorSchemaRoot();
                        if (batchRoot.getRowCount() > 0) {
                            writeBatch(batchRoot, loader, outputRoot, writer);
                        }
                    }

                    for (int i = 1; i < inputFiles.size(); i++) {
                        Path file = inputFiles.get(i);
                        try (ArrowStreamReader reader = new ArrowStreamReader(
                                 new BufferedInputStream(Files.newInputStream(file)),
                                 allocator)) {
                            VectorSchemaRoot fileRoot = reader.getVectorSchemaRoot();
                            if (!schemasCompatible(expectedSchema, fileRoot.getSchema())) {
                                throw new IOException(
                                    "Schema mismatch: file[0] schema differs from " +
                                    file.getFileName() + ".\n" +
                                    "  Expected: " + expectedSchema + "\n" +
                                    "  Got:      " + fileRoot.getSchema());
                            }

                            while (reader.loadNextBatch()) {
                                if (fileRoot.getRowCount() > 0) {
                                    writeBatch(fileRoot, loader, outputRoot, writer);
                                }
                            }
                        }
                    }

                    writer.end();
                }
            }
        }

        // Atomic rename tmp → final
        Files.move(tmpPath, outputPath,
                   StandardCopyOption.REPLACE_EXISTING,
                   StandardCopyOption.ATOMIC_MOVE);

        return outputPath;
    }

    /**
     * Check whether two Arrow schemas are structurally compatible for merging
     * (same field names and types in the same order).
     */
    private static boolean schemasCompatible(
            org.apache.arrow.vector.types.pojo.Schema a,
            org.apache.arrow.vector.types.pojo.Schema b) {
        if (a.getFields().size() != b.getFields().size()) return false;
        for (int i = 0; i < a.getFields().size(); i++) {
            var fa = a.getFields().get(i);
            var fb = b.getFields().get(i);
            if (!fa.getName().equals(fb.getName())) return false;
            if (!fa.getType().equals(fb.getType()))  return false;
        }
        return true;
    }

    private static void writeBatch(
            VectorSchemaRoot batchRoot,
            VectorLoader loader,
            VectorSchemaRoot outputRoot,
            ArrowStreamWriter writer) throws IOException {
        try (ArrowRecordBatch batch = new VectorUnloader(batchRoot).getRecordBatch()) {
            loader.load(batch);
        }
        outputRoot.setRowCount(batchRoot.getRowCount());
        writer.writeBatch();
    }

    /**
     * Compute the total byte size of a list of files.
     * Used by callers to decide whether to merge or convert directly.
     */
    public static long totalBytes(List<Path> files) {
        long total = 0;
        for (Path f : files) {
            try { total += Files.size(f); } catch (IOException ignored) {}
        }
        return total;
    }
}
