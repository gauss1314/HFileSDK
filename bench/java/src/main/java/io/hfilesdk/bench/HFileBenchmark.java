package io.hfilesdk.bench;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Java HBase HFile.Writer baseline benchmark.
 *
 * <p>Writes HFiles using the native Java HBase API under the same conditions
 * as {@code bm_e2e_write.cc}, producing apples-to-apples throughput numbers.
 * Outputs JSON in Google Benchmark format so {@code hfile-report.py} can
 * display the C++ / Java comparison side-by-side.
 *
 * <p>Usage (matches bench-runner.sh invocation):
 * <pre>
 *   java -Xmx8g -Xms8g -XX:+UseZGC \
 *       -jar hfile-bench-java.jar \
 *       --benchmark_format=json \
 *       --iterations=10
 * </pre>
 *
 * <p>Dataset correspondence with C++ benchmark:
 * <ul>
 *   <li>SmallKV_100B  — 100,000 KVs, 16-byte row key, 100-byte value, family=cf, qualifier=col
 *   <li>WideTable     — 20,000 rows × 20 columns, 8-byte row key, 50-byte value
 * </ul>
 */
public class HFileBenchmark {

    // ── Compression/Encoding codec identifiers (mirrors C++ Compression enum) ──
    private static final String[] COMPRESSIONS = {"NONE", "LZ4", "ZSTD", "SNAPPY"};
    private static final String[] ENCODINGS    = {"NONE", "FAST_DIFF"};

    // ── Dataset shape constants (must match bm_e2e_write.cc exactly) ──────────
    private static final int  SMALL_KV_COUNT  = 100_000;
    private static final int  SMALL_KV_ROWLEN = 16;
    private static final int  SMALL_KV_VALLEN = 100;

    private static final int  WIDE_ROW_COUNT  = 20_000;
    private static final int  WIDE_COL_COUNT  = 20;
    private static final int  WIDE_VALLEN     = 50;

    private static final byte[] CF  = Bytes.toBytes("cf");
    private static final byte[] COL = Bytes.toBytes("col");

    // ── Result accumulator ────────────────────────────────────────────────────
    private static final List<BmResult> RESULTS = new ArrayList<>();

    // ─────────────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt("benchmark_format")
            .hasArg().desc("Output format: json or text (default: text)").build());
        opts.addOption(Option.builder().longOpt("iterations")
            .hasArg().desc("Number of measurement iterations (default: 10)").build());
        opts.addOption(Option.builder().longOpt("benchmark_filter")
            .hasArg().desc("Regex filter on benchmark names").build());

        CommandLine cmd = new DefaultParser().parse(opts, args);
        boolean jsonOutput = "json".equalsIgnoreCase(
            cmd.getOptionValue("benchmark_format", "text"));
        int iterations = Integer.parseInt(cmd.getOptionValue("iterations", "10"));
        String filter  = cmd.getOptionValue("benchmark_filter", "");

        // Warm up the JVM with a couple of silent iterations before measuring.
        warmUp();

        // ── Run all benchmark combinations ────────────────────────────────────
        runSmallKVBenchmarks(iterations, filter);
        runWideTableBenchmark(iterations, filter);

        // ── Emit results ──────────────────────────────────────────────────────
        if (jsonOutput) {
            printJson(System.out);
        } else {
            printText(System.out);
        }
    }

    // ─── Benchmark runners ────────────────────────────────────────────────────

    private static void runSmallKVBenchmarks(int iterations, String filter)
            throws Exception {
        // Match C++ BENCHMARK(BM_E2E_Write) matrix: 4 compression × NONE+FAST_DIFF
        String[][] configs = {
            {"NONE",   "NONE"},
            {"LZ4",    "FAST_DIFF"},
            {"ZSTD",   "FAST_DIFF"},
            {"SNAPPY", "FAST_DIFF"},
        };
        for (String[] cfg : configs) {
            String name = String.format("BM_E2E_Write/%d/%s/%s",
                SMALL_KV_COUNT, cfg[0], cfg[1]);
            if (!filter.isEmpty() && !name.toLowerCase(Locale.ROOT)
                    .contains(filter.toLowerCase(Locale.ROOT)))
                continue;

            System.err.printf("Running %-55s  (iterations=%d)%n", name, iterations);
            BmResult r = measureSmallKV(name, cfg[0], cfg[1], iterations);
            RESULTS.add(r);
            System.err.printf("  → %.1f MB/s  %.2f M KVs/s%n",
                r.bytesPerSecond / 1024.0 / 1024.0,
                r.itemsPerSecond / 1_000_000.0);
        }
    }

    private static void runWideTableBenchmark(int iterations, String filter)
            throws Exception {
        String name = "BM_E2E_WideTable";
        if (!filter.isEmpty() && !name.toLowerCase(Locale.ROOT)
                .contains(filter.toLowerCase(Locale.ROOT)))
            return;

        System.err.printf("Running %-55s  (iterations=%d)%n", name, iterations);
        BmResult r = measureWideTable(name, iterations);
        RESULTS.add(r);
        System.err.printf("  → %.1f MB/s  %.2f M KVs/s%n",
            r.bytesPerSecond / 1024.0 / 1024.0,
            r.itemsPerSecond / 1_000_000.0);
    }

    // ─── Core measurement ─────────────────────────────────────────────────────

    /** Measure writing SMALL_KV_COUNT KVs with the given compression/encoding. */
    private static BmResult measureSmallKV(String name, String compression,
                                            String encoding, int iterations)
            throws Exception {
        // Build dataset once
        List<KeyValue> kvs = buildSmallKVDataset();

        // Measurement loop: each iteration writes a fresh HFile and discards it
        long totalBytes = 0;
        long totalItems = 0;
        long totalNanos = 0;

        for (int iter = 0; iter < iterations; ++iter) {
            java.io.File tmpFile = File.createTempFile("hfile_java_bench_", ".hfile");
            tmpFile.deleteOnExit();

            long t0 = System.nanoTime();
            long fileSize = writeHFile(tmpFile.getAbsolutePath(), kvs,
                                       compression, encoding);
            long elapsed = System.nanoTime() - t0;

            totalNanos += elapsed;
            totalItems += kvs.size();
            // bytes processed = raw uncompressed data (mirrors C++ SetBytesProcessed)
            totalBytes += (long) kvs.size() * (SMALL_KV_ROWLEN + SMALL_KV_VALLEN + 20);
            tmpFile.delete();
        }

        return new BmResult(name,
            totalBytes * 1_000_000_000L / totalNanos,
            totalItems * 1_000_000_000L / totalNanos,
            totalNanos / iterations);
    }

    /** Measure writing the wide-table dataset (20K rows × 20 columns). */
    private static BmResult measureWideTable(String name, int iterations)
            throws Exception {
        List<KeyValue> kvs = buildWideTableDataset();

        long totalBytes = 0;
        long totalItems = 0;
        long totalNanos = 0;

        for (int iter = 0; iter < iterations; ++iter) {
            File tmpFile = File.createTempFile("hfile_java_wide_", ".hfile");
            tmpFile.deleteOnExit();

            long t0 = System.nanoTime();
            writeHFile(tmpFile.getAbsolutePath(), kvs, "LZ4", "FAST_DIFF");
            long elapsed = System.nanoTime() - t0;

            totalNanos += elapsed;
            totalItems += kvs.size();
            totalBytes += (long) kvs.size() * (8 + WIDE_VALLEN + 20);
            tmpFile.delete();
        }

        return new BmResult(name,
            totalBytes * 1_000_000_000L / totalNanos,
            totalItems * 1_000_000_000L / totalNanos,
            totalNanos / iterations);
    }

    // ─── HFile writing via HBase API ──────────────────────────────────────────

    /**
     * Write {@code kvs} into a local HFile using HBase's {@code HFile.Writer}.
     * Returns the file size in bytes.
     */
    private static long writeHFile(String path, List<KeyValue> kvs,
                                    String compressionName,
                                    String encodingName) throws IOException {
        Configuration conf = new Configuration();
        FileSystem     localFs = FileSystem.getLocal(conf);
        Path           hfilePath = new Path(path);

        Compression.Algorithm compression =
            Compression.getCompressionAlgorithmByName(compressionName.toLowerCase(Locale.ROOT));
        DataBlockEncoding encoding =
            DataBlockEncoding.valueOf(encodingName.replace('-', '_'));

        HFileContext ctx = new HFileContextBuilder()
            .withCompression(compression)
            .withDataBlockEncoding(encoding)
            .withCellComparator(CellComparatorImpl.COMPARATOR)
            .withIncludesTags(true)          // HFile v3
            .withColumnFamily(CF)
            .build();

        try (HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf))
                .withPath(localFs, hfilePath)
                .withFileContext(ctx)
                .create()) {

            for (KeyValue kv : kvs) {
                writer.append(kv);
            }
        }
        return localFs.getFileStatus(hfilePath).getLen();
    }

    // ─── Dataset builders (mirror bm_e2e_write.cc exactly) ───────────────────

    /**
     * DS-1: 100,000 KVs, row="%016d"%i (16B), family="cf", qualifier="col",
     * timestamp=N-i, value=100B filled with i&0xFF.
     * Pre-sorted: rows are lexicographically ascending (i=0..N-1).
     */
    private static List<KeyValue> buildSmallKVDataset() {
        int n = SMALL_KV_COUNT;
        List<KeyValue> kvs = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            byte[] row   = String.format("%016d", i).getBytes();
            byte[] value = new byte[SMALL_KV_VALLEN];
            java.util.Arrays.fill(value, (byte)(i & 0xFF));
            kvs.add(new KeyValue(row, CF, COL, (long)(n - i), value));
        }
        return kvs;
    }

    /**
     * DS-3: 20,000 rows × 20 columns.
     * Row = 8-byte big-endian int64 of row index.
     * Qualifier = "col%02d"%c, value = 50B filled with (i+c)&0xFF.
     * KVs are pre-sorted: row i, col 0..19, then row i+1, etc.
     */
    private static List<KeyValue> buildWideTableDataset() {
        int n = WIDE_ROW_COUNT;
        List<KeyValue> kvs = new ArrayList<>(n * WIDE_COL_COUNT);
        for (int i = 0; i < n; ++i) {
            byte[] row = ByteBuffer.allocate(8).putLong(i).array();
            for (int c = 0; c < WIDE_COL_COUNT; ++c) {
                byte[] qualifier = String.format("col%02d", c).getBytes();
                byte[] value     = new byte[WIDE_VALLEN];
                java.util.Arrays.fill(value, (byte)((i + c) & 0xFF));
                kvs.add(new KeyValue(row, CF, qualifier, (long)(n - i), value));
            }
        }
        return kvs;
    }

    // ─── Warm-up ─────────────────────────────────────────────────────────────

    private static void warmUp() throws Exception {
        System.err.println("JVM warm-up (5 iterations)...");
        List<KeyValue> kvs = buildSmallKVDataset();
        for (int i = 0; i < 5; ++i) {
            File f = File.createTempFile("hfile_warmup_", ".hfile");
            f.deleteOnExit();
            writeHFile(f.getAbsolutePath(), kvs, "NONE", "NONE");
            f.delete();
        }
        System.err.println("Warm-up complete.");
    }

    // ─── Output formatters ────────────────────────────────────────────────────

    /**
     * Emit results in Google Benchmark JSON format.
     * This is what hfile-report.py parses: fields used are
     * name, bytes_per_second, items_per_second, cpu_time.
     */
    private static void printJson(PrintStream out) {
        out.println("{");
        out.println("  \"context\": {");
        out.printf ("    \"date\": \"%s\",%n", java.time.LocalDateTime.now());
        out.println("    \"library_version\": \"HBase 2.6.1 HFile.Writer\",");
        out.println("    \"language\": \"Java\"");
        out.println("  },");
        out.println("  \"benchmarks\": [");
        for (int i = 0; i < RESULTS.size(); i++) {
            BmResult r = RESULTS.get(i);
            String comma = (i < RESULTS.size() - 1) ? "," : "";
            out.println("    {");
            out.printf ("      \"name\": \"%s\",%n", r.name);
            out.printf ("      \"run_name\": \"%s\",%n", r.name);
            out.printf ("      \"run_type\": \"iteration\",%n");
            out.printf ("      \"bytes_per_second\": %d,%n", r.bytesPerSecond);
            out.printf ("      \"items_per_second\": %d,%n", r.itemsPerSecond);
            out.printf ("      \"cpu_time\": %d,%n", r.cpuTimeNs);
            out.printf ("      \"real_time\": %d%n", r.cpuTimeNs);
            out.printf ("    }%s%n", comma);
        }
        out.println("  ]");
        out.println("}");
    }

    private static void printText(PrintStream out) {
        out.printf("%-55s  %12s  %14s%n",
            "Benchmark", "Throughput", "KV QPS");
        out.println("-".repeat(85));
        for (BmResult r : RESULTS) {
            out.printf("%-55s  %8.1f MB/s  %10.2f M/s%n",
                r.name,
                r.bytesPerSecond / 1024.0 / 1024.0,
                r.itemsPerSecond / 1_000_000.0);
        }
    }

    // ─── Result record ────────────────────────────────────────────────────────

    private static class BmResult {
        final String name;
        final long   bytesPerSecond;
        final long   itemsPerSecond;
        final long   cpuTimeNs;       // nanoseconds per iteration (median)

        BmResult(String name, long bytesPerSecond, long itemsPerSecond,
                 long cpuTimeNs) {
            this.name           = name;
            this.bytesPerSecond = bytesPerSecond;
            this.itemsPerSecond = itemsPerSecond;
            this.cpuTimeNs      = cpuTimeNs;
        }
    }
}
