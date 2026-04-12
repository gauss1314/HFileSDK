package io.hfilesdk.perf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.hfilesdk.converter.ArrowToHFileConverter;
import io.hfilesdk.converter.ConvertOptions;
import io.hfilesdk.converter.ConvertResult;
import io.hfilesdk.converter.javaimpl.ArrowToHFileJavaConverter;
import io.hfilesdk.converter.javaimpl.JavaConvertOptions;
import io.hfilesdk.converter.javaimpl.JavaConvertResult;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileReaderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class HFileConsistencyTest {
    private static final long FIXED_TIMESTAMP_MS = 1_715_678_900_123L;

    @Test
    void jniAndPureJavaProduceEquivalentReadableHFiles(@TempDir Path tempDir) throws Exception {
        Path nativeLib = findNativeLib();
        Assumptions.assumeTrue(nativeLib != null, "native lib not available for JNI comparison test");

        Path arrowPath = tempDir.resolve("input.arrow");
        Path jniHFile = tempDir.resolve("jni.hfile");
        Path javaHFile = tempDir.resolve("java.hfile");
        writeUnsortedArrowStream(arrowPath);

        ConvertResult jniResult = ArrowToHFileConverter.withNativeLib(nativeLib.toString()).convert(
            ConvertOptions.builder()
                .nativeLibPath(nativeLib.toString())
                .arrowPath(arrowPath.toString())
                .hfilePath(jniHFile.toString())
                .tableName("perf_table")
                .rowKeyRule("USER_ID,0,false,0")
                .columnFamily("cf")
                .compression("GZ")
                .compressionLevel(1)
                .dataBlockEncoding("NONE")
                .bloomType("row")
                .defaultTimestampMs(FIXED_TIMESTAMP_MS)
                .build()
        );
        assertTrue(jniResult.isSuccess(), jniResult.summary());

        JavaConvertResult javaResult = new ArrowToHFileJavaConverter().convert(
            JavaConvertOptions.builder()
                .arrowPath(arrowPath.toString())
                .hfilePath(javaHFile.toString())
                .tableName("perf_table")
                .rowKeyRule("USER_ID,0,false,0")
                .columnFamily("cf")
                .compression("GZ")
                .compressionLevel(1)
                .dataBlockEncoding("NONE")
                .bloomType("ROW")
                .defaultTimestampMs(FIXED_TIMESTAMP_MS)
                .build()
        );
        assertTrue(javaResult.isSuccess(), javaResult.summary());

        try (ReaderSnapshot jniReader = openReader(jniHFile);
             ReaderSnapshot javaReader = openReader(javaHFile)) {
            assertEquals(-1L, Files.mismatch(jniHFile, javaHFile), "JNI/Java HFile bytes should match exactly");
            assertEquals(jniReader.cells(), javaReader.cells());
            assertEquals(Compression.Algorithm.GZ, jniReader.reader().getFileContext().getCompression());
            assertEquals(Compression.Algorithm.GZ, javaReader.reader().getFileContext().getCompression());
            assertEquals(DataBlockEncoding.NONE, jniReader.reader().getFileContext().getDataBlockEncoding());
            assertEquals(DataBlockEncoding.NONE, javaReader.reader().getFileContext().getDataBlockEncoding());
            assertEquals(jniReader.reader().getEntries(), javaReader.reader().getEntries());
            assertEquals(jniReader.reader().getFileContext().isIncludesTags(),
                javaReader.reader().getFileContext().isIncludesTags());
            assertEquals(jniReader.reader().getFileContext().isIncludesMvcc(),
                javaReader.reader().getFileContext().isIncludesMvcc());
            assertNotNull(jniReader.reader().getGeneralBloomFilterMetadata());
            assertNotNull(javaReader.reader().getGeneralBloomFilterMetadata());

            assertCommonFileInfo(jniReader.reader());
            assertCommonFileInfo(javaReader.reader());
        }
    }

    private static void assertCommonFileInfo(HFile.Reader reader) {
        HFileInfo fileInfo = ((HFileReaderImpl) reader).getHFileInfo();
        for (String key : List.of(
            "hfile.LASTKEY",
            "hfile.AVG_KEY_LEN",
            "hfile.AVG_VALUE_LEN",
            "hfile.CREATE_TIME_TS",
            "hfile.LEN_OF_BIGGEST_CELL",
            "BLOOM_FILTER_TYPE",
            "LAST_BLOOM_KEY")) {
            assertNotNull(fileInfo.get(Bytes.toBytes(key)), "missing FileInfo key: " + key);
        }
        if (reader.getFileContext().isIncludesTags()) {
            assertNotNull(fileInfo.get(Bytes.toBytes("hfile.MAX_TAGS_LEN")));
        }
        if (reader.getFileContext().isIncludesMvcc()) {
            assertNotNull(fileInfo.get(Bytes.toBytes("KEY_VALUE_VERSION")));
            assertNotNull(fileInfo.get(Bytes.toBytes("MAX_MEMSTORE_TS_KEY")));
        }
        String comparator = ((HFileReaderImpl) reader).getTrailer().getComparatorClassName();
        assertNotNull(comparator);
        assertTrue(comparator.contains("CellComparator"), comparator);
    }

    private static ReaderSnapshot openReader(Path hfilePath) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = new RawLocalFileSystem();
        fs.initialize(java.net.URI.create("file:///"), conf);
        HFile.Reader reader = HFile.createReader(
            fs,
            new org.apache.hadoop.fs.Path(hfilePath.toString()),
            new CacheConfig(conf),
            true,
            conf
        );
        return new ReaderSnapshot(reader, scanCells(reader, conf));
    }

    private static List<String> scanCells(HFile.Reader reader, Configuration conf) throws Exception {
        List<String> cells = new ArrayList<>();
        HFileScanner scanner = reader.getScanner(conf, false, false);
        if (!scanner.seekTo()) {
            return cells;
        }
        do {
            Cell cell = scanner.getCell();
            cells.add(
                Bytes.toStringBinary(CellUtil.cloneRow(cell)) + "|" +
                Bytes.toStringBinary(CellUtil.cloneFamily(cell)) + "|" +
                Bytes.toStringBinary(CellUtil.cloneQualifier(cell)) + "|" +
                cell.getTimestamp() + "|" +
                Bytes.toStringBinary(CellUtil.cloneValue(cell)) + "|" +
                cell.getType()
            );
        } while (scanner.next());
        return cells;
    }

    private static void writeUnsortedArrowStream(Path path) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VarCharVector userId = new VarCharVector("USER_ID", allocator);
             BigIntVector eventTime = new BigIntVector("EVENT_TIME", allocator);
             VarCharVector payload = new VarCharVector("PAYLOAD", allocator);
             VectorSchemaRoot root = new VectorSchemaRoot(List.of(userId, eventTime, payload));
             ArrowStreamWriter writer = new ArrowStreamWriter(
                 root, null, Channels.newChannel(Files.newOutputStream(path)))) {
            userId.allocateNew();
            eventTime.allocateNew();
            payload.allocateNew();

            userId.setSafe(0, "user-0002".getBytes(StandardCharsets.UTF_8));
            eventTime.setSafe(0, 1002L);
            payload.setSafe(0, "payload-b".getBytes(StandardCharsets.UTF_8));

            userId.setSafe(1, "user-0001".getBytes(StandardCharsets.UTF_8));
            eventTime.setSafe(1, 1001L);
            payload.setSafe(1, "payload-a".getBytes(StandardCharsets.UTF_8));

            userId.setSafe(2, "user-0003".getBytes(StandardCharsets.UTF_8));
            eventTime.setSafe(2, 1003L);
            payload.setSafe(2, "payload-c".getBytes(StandardCharsets.UTF_8));

            userId.setValueCount(3);
            eventTime.setValueCount(3);
            payload.setValueCount(3);
            root.setRowCount(3);

            writer.start();
            writer.writeBatch();
            writer.end();
        }
    }

    private static Path findNativeLib() {
        String nativeDir = System.getenv("HFILESDK_NATIVE_DIR");
        if (nativeDir != null && !nativeDir.isBlank()) {
            Path resolved = findNativeLibInDir(Path.of(nativeDir));
            if (resolved != null) {
                return resolved;
            }
        }
        Path cwd = Path.of("").toAbsolutePath().normalize();
        for (Path candidate : List.of(cwd.resolve("../../build"), cwd.resolve("../build"), cwd.resolve("build"))) {
            Path resolved = findNativeLibInDir(candidate.normalize());
            if (resolved != null) {
                return resolved;
            }
        }
        return null;
    }

    private static Path findNativeLibInDir(Path dir) {
        for (String fileName : List.of("libhfilesdk.dylib", "libhfilesdk.so", "hfilesdk.dll")) {
            Path candidate = dir.resolve(fileName);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private record ReaderSnapshot(HFile.Reader reader, List<String> cells) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            reader.close();
        }
    }
}
