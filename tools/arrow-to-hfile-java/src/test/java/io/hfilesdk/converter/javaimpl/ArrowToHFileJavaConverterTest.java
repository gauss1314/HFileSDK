package io.hfilesdk.converter.javaimpl;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.junit.jupiter.api.Test;

import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ArrowToHFileJavaConverterTest {

    @Test
    void convertsArrowFileToReadableHFile() throws Exception {
        java.nio.file.Path tempDir = Files.createTempDirectory("arrow-to-hfile-java-test");
        java.nio.file.Path arrowFile = tempDir.resolve("input.arrow");
        java.nio.file.Path hfile = tempDir.resolve("output.hfile");

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VarCharVector userId = new VarCharVector("USER_ID", allocator);
             BigIntVector eventTime = new BigIntVector("EVENT_TIME", allocator);
             VarCharVector payload = new VarCharVector("PAYLOAD", allocator);
             VectorSchemaRoot root = new VectorSchemaRoot(java.util.List.of(userId, eventTime, payload));
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(Files.newOutputStream(arrowFile)))) {

            userId.allocateNew();
            eventTime.allocateNew();
            payload.allocateNew();

            userId.setSafe(0, "user-0001".getBytes(StandardCharsets.UTF_8));
            eventTime.setSafe(0, 1001L);
            payload.setSafe(0, "payload-a".getBytes(StandardCharsets.UTF_8));

            userId.setSafe(1, "user-0002".getBytes(StandardCharsets.UTF_8));
            eventTime.setSafe(1, 1002L);
            payload.setSafe(1, "payload-b".getBytes(StandardCharsets.UTF_8));

            userId.setValueCount(2);
            eventTime.setValueCount(2);
            payload.setValueCount(2);
            root.setRowCount(2);

            writer.start();
            writer.writeBatch();
            writer.end();
        }

        JavaConvertResult result = new ArrowToHFileJavaConverter().convert(
            JavaConvertOptions.builder()
                .arrowPath(arrowFile.toString())
                .hfilePath(hfile.toString())
                .tableName("perf_table")
                .rowKeyRule("USER_ID,0,false,0")
                .columnFamily("cf")
                .build()
        );

        assertTrue(result.isSuccess(), result.summary());
        assertTrue(Files.exists(hfile));
        assertTrue(Files.size(hfile) > 0L);

        Configuration conf = new Configuration();
        FileSystem fs = new RawLocalFileSystem();
        fs.initialize(java.net.URI.create("file:///"), conf);
        try (HFile.Reader reader = HFile.createReader(fs, new Path(hfile.toString()), new CacheConfig(conf), true, conf)) {
            assertEquals(6L, reader.getEntries());
            assertEquals(Compression.Algorithm.GZ, reader.getFileContext().getCompression());
            assertEquals(DataBlockEncoding.NONE, reader.getFileContext().getDataBlockEncoding());
            HFileScanner scanner = reader.getScanner(conf, false, false);
            assertTrue(scanner.seekTo());
            Cell firstCell = scanner.getCell();
            assertEquals("EVENT_TIME", new String(firstCell.getQualifierArray(), firstCell.getQualifierOffset(), firstCell.getQualifierLength(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void fallsBackToNoneEncodingWhenCallerRequestsFastDiff() throws Exception {
        java.nio.file.Path tempDir = Files.createTempDirectory("arrow-to-hfile-java-encoding-test");
        java.nio.file.Path arrowFile = tempDir.resolve("input.arrow");
        java.nio.file.Path hfile = tempDir.resolve("output.hfile");

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VarCharVector userId = new VarCharVector("USER_ID", allocator);
             VectorSchemaRoot root = new VectorSchemaRoot(java.util.List.of(userId));
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(Files.newOutputStream(arrowFile)))) {
            userId.allocateNew();
            userId.setSafe(0, "user-0001".getBytes(StandardCharsets.UTF_8));
            userId.setValueCount(1);
            root.setRowCount(1);
            writer.start();
            writer.writeBatch();
            writer.end();
        }

        JavaConvertResult result = new ArrowToHFileJavaConverter().convert(
            JavaConvertOptions.builder()
                .arrowPath(arrowFile.toString())
                .hfilePath(hfile.toString())
                .tableName("perf_table")
                .rowKeyRule("USER_ID,0,false,0")
                .columnFamily("cf")
                .compression("gzip")
                .dataBlockEncoding("FAST_DIFF")
                .build()
        );

        assertTrue(result.isSuccess(), result.summary());

        Configuration conf = new Configuration();
        FileSystem fs = new RawLocalFileSystem();
        fs.initialize(java.net.URI.create("file:///"), conf);
        try (HFile.Reader reader = HFile.createReader(fs, new Path(hfile.toString()), new CacheConfig(conf), true, conf)) {
            assertEquals(Compression.Algorithm.GZ, reader.getFileContext().getCompression());
            assertEquals(DataBlockEncoding.NONE, reader.getFileContext().getDataBlockEncoding());
        }
    }
}
