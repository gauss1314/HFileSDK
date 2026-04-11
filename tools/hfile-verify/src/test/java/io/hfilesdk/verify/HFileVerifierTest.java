package io.hfilesdk.verify;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

final class HFileVerifierTest {

    @Test
    void inspectLayoutReportsSectionSizesAndClosedTotals() throws Exception {
        Path tempDir = Files.createTempDirectory("hfile-verify-layout");
        Path hfile = tempDir.resolve("sample.hfile");

        Configuration configuration = new Configuration();
        FileSystem fileSystem = new RawLocalFileSystem();
        fileSystem.initialize(java.net.URI.create("file:///"), configuration);
        byte[] columnFamily = Bytes.toBytes("cf");
        HFileContext context = new HFileContextBuilder()
            .withCompression(Compression.Algorithm.GZ)
            .withDataBlockEncoding(DataBlockEncoding.NONE)
            .withBlockSize(65536)
            .withColumnFamily(columnFamily)
            .withCellComparator(CellComparatorImpl.COMPARATOR)
            .withIncludesTags(true)
            .withIncludesMvcc(true)
            .build();

        StoreFileWriter writer = new StoreFileWriter.Builder(configuration, new CacheConfig(configuration), fileSystem)
            .withFilePath(new org.apache.hadoop.fs.Path(hfile.toString()))
            .withBloomType(BloomType.ROW)
            .withMaxKeyCount(4)
            .withFileContext(context)
            .withCellComparator(context.getCellComparator())
            .withMaxVersions(1)
            .build();
        try {
            writer.append(new KeyValue(Bytes.toBytes("row-0001"), columnFamily, Bytes.toBytes("q1"), 1L, Bytes.toBytes("v1")));
            writer.append(new KeyValue(Bytes.toBytes("row-0002"), columnFamily, Bytes.toBytes("q1"), 1L, Bytes.toBytes("v2")));
        } finally {
            writer.close();
        }

        HFileVerifier.LayoutSummary layout = HFileVerifier.inspectLayout(hfile.toFile());

        assertTrue(layout.dataBytes > 0);
        assertTrue(layout.indexBytes > 0);
        assertTrue(layout.bloomBytes > 0);
        assertEquals(0L, layout.metaBytes);
        assertTrue(layout.fileInfoBytes > 0);
        assertEquals(1, layout.fileInfoBlocks);
        assertEquals(Files.size(hfile), layout.accountedBytes);
    }
}
