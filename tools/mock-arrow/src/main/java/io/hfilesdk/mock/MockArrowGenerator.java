package io.hfilesdk.mock;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Generates Arrow IPC Stream files for HFileSDK end-to-end testing.
 *
 * <h2>CLI 用法</h2>
 * <pre>{@code
 * java -jar mock-arrow-1.0.0.jar \
 *   --output  /data/events.arrow \
 *   --table   tdr_signal_stor_20550 \
 *   --size    50                      # 50 MB (default 10 MB)
 *   --seed    42                      # 随机种子（默认 42）
 *   --batch   1000                    # 每个 RecordBatch 的行数（默认 1000）
 * }</pre>
 *
 * <h2>可选的表名</h2>
 * <ul>
 *   <li>{@code tdr_signal_stor_20550} — 新信令存储表（默认）</li>
 *   <li>{@code tdr_mock} — 原始 CDR 话单表</li>
 * </ul>
 *
 * <h2>文件大小控制</h2>
 * <p>通过 {@code --size} 指定目标文件大小（MiB）。生成器持续写入 RecordBatch 直到
 * 累计字节数达到目标。实际文件大小会在目标附近（±1 个 batch）。
 *
 * <h2>与 arrow-to-hfile 配合使用</h2>
 * <pre>{@code
 * # 生成 Arrow 文件
 * java -jar mock-arrow-1.0.0.jar \
 *   --output /data/tdr_20550.arrow --size 100
 *
 * # 转换为 HFile（_hoodie_* 列不存在，无需 exclude）
 * java -jar arrow-to-hfile-4.0.0.jar \
 *   --native-lib /opt/hfilesdk/libhfilesdk.so \
 *   --arrow /data/tdr_20550.arrow \
 *   --hfile /staging/cf/tdr_20550.hfile \
 *   --rule  "REFID,0,false,15"
 * }</pre>
 */
public class MockArrowGenerator {

    // ── Defaults ──────────────────────────────────────────────────────────────
    private static final int    DEFAULT_SIZE_MB    = 10;
    private static final int    DEFAULT_BATCH_ROWS = 1000;
    private static final long   DEFAULT_SEED       = 42L;
    private static final String DEFAULT_TABLE      = TableSchema.TDR_SIGNAL_STOR_20550.tableName;

    // ── CLI entry point ───────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        Options options = buildOptions();
        CommandLine cmd;
        try {
            cmd = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            printHelp(options);
            System.exit(1);
            return;
        }
        if (cmd.hasOption("help")) {
            printHelp(options);
            return;
        }

        // ── Parse arguments ───────────────────────────────────────────────
        String outputPath = cmd.getOptionValue("output");
        if (outputPath == null) {
            System.err.println("Error: --output is required.");
            printHelp(options);
            System.exit(1);
            return;
        }

        String tableName = cmd.getOptionValue("table", DEFAULT_TABLE);
        int    sizeMb    = parseInt(cmd.getOptionValue("size", String.valueOf(DEFAULT_SIZE_MB)), DEFAULT_SIZE_MB);
        int    batchRows = parseInt(cmd.getOptionValue("batch", String.valueOf(DEFAULT_BATCH_ROWS)), DEFAULT_BATCH_ROWS);
        long   seed      = parseLong(cmd.getOptionValue("seed",  String.valueOf(DEFAULT_SEED)), DEFAULT_SEED);

        TableSchema schema;
        try {
            schema = TableSchema.forTable(tableName);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
            return;
        }

        // ── Generate ──────────────────────────────────────────────────────
        Path out = Paths.get(outputPath);
        if (out.getParent() != null) Files.createDirectories(out.getParent());

        System.out.printf("Generating Arrow IPC Stream file%n");
        System.out.printf("  table  : %s%n", schema.tableName);
        System.out.printf("  output : %s%n", outputPath);
        System.out.printf("  target : %d MiB%n", sizeMb);
        System.out.printf("  batch  : %d rows%n", batchRows);
        System.out.printf("  seed   : %d%n%n", seed);
        System.out.printf("  rowKey : %s%n", schema.rowKeyRule);

        GenerateResult result = generate(schema, out, sizeMb, batchRows, seed);

        System.out.printf("%nDone:%n");
        System.out.printf("  rows written : %,d%n",   result.totalRows);
        System.out.printf("  batches      : %,d%n",   result.batches);
        System.out.printf("  file size    : %.2f MiB%n", result.fileSizeBytes / 1024.0 / 1024.0);
        System.out.printf("%nrowKeyRule for arrow-to-hfile:%n  %s%n", schema.rowKeyRule);
    }

    // ── Programmatic API ──────────────────────────────────────────────────────

    /**
     * Generate an Arrow IPC Stream file.
     *
     * @param schema    table schema to generate
     * @param output    output file path (parent directories are created if needed)
     * @param sizeMb    target file size in MiB; generation stops when exceeded
     * @param batchRows number of rows per Arrow RecordBatch
     * @param seed      random seed for reproducibility
     * @return summary of what was written
     */
    public static GenerateResult generate(
            TableSchema schema, Path output, int sizeMb, int batchRows, long seed)
            throws IOException {

        long targetBytes = (long) sizeMb * 1024 * 1024;
        RowGenerator gen = new RowGenerator(schema, seed);
        int estimatedRowBytes = gen.estimateRowBytes();
        // Aim for target size; account for Arrow format overhead (~5%)
        long adjustedTarget = (long) (targetBytes * 0.95);

        long totalRows  = 0;
        int  batches    = 0;

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             OutputStream fos = new BufferedOutputStream(
                     Files.newOutputStream(output), 256 * 1024);
             VectorSchemaRoot root = schema.allocateRoot(allocator);
             ArrowStreamWriter writer = new ArrowStreamWriter(
                     root, null, Channels.newChannel(fos))) {

            writer.start();

            long writtenBytes = 0;
            while (writtenBytes < adjustedTarget) {
                // Determine batch size: don't overshoot target significantly
                long remainingBytes = adjustedTarget - writtenBytes;
                int  rowsThisBatch  = (int) Math.min(batchRows,
                        Math.max(1, remainingBytes / estimatedRowBytes));

                fillBatch(schema, root, gen, rowsThisBatch);
                writer.writeBatch();
                batches++;
                totalRows    += rowsThisBatch;
                // Estimate written bytes using actual row count × avg row size
                writtenBytes += (long) rowsThisBatch * estimatedRowBytes;

                if (batches % 100 == 0) {
                    System.out.printf("  ... %,d rows written (%.1f MiB estimated)%n",
                        totalRows, writtenBytes / 1024.0 / 1024.0);
                }
            }

            writer.end();
        }

        long fileSize = Files.size(output);
        return new GenerateResult(totalRows, batches, fileSize);
    }

    // ── Batch filling ─────────────────────────────────────────────────────────

    /**
     * Fill {@code root} with {@code numRows} rows using data from {@code gen}.
     * Uses positional vector writes to avoid per-row object allocation.
     */
    private static void fillBatch(
            TableSchema schema, VectorSchemaRoot root,
            RowGenerator gen, int numRows) {

        root.allocateNew();
        List<FieldVector> vectors = root.getFieldVectors();

        switch (schema) {
            case TDR_SIGNAL_STOR_20550 -> fillBatch20550(vectors, gen, numRows);
            case TDR_MOCK -> fillBatchMock(vectors, gen, numRows);
        }

        root.setRowCount(numRows);
    }

    private static void fillBatch20550(List<FieldVector> vectors, RowGenerator gen, int n) {
        BigIntVector refidVec   = (BigIntVector)   vectors.get(0);
        BigIntVector timeVec    = (BigIntVector)   vectors.get(1);
        VarCharVector sigVec    = (VarCharVector)  vectors.get(2);
        BigIntVector bitmapVec  = (BigIntVector)   vectors.get(3);
        VarCharVector noVec     = (VarCharVector)  vectors.get(4);

        for (int i = 0; i < n; i++) {
            Object[] row = gen.nextRow();
            refidVec .setSafe(i, (Long)   row[0]);
            timeVec  .setSafe(i, (Long)   row[1]);
            byte[] sigBytes = ((String) row[2]).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            sigVec   .setSafe(i, sigBytes, 0, sigBytes.length);
            bitmapVec.setSafe(i, (Long)   row[3]);
            byte[] noBytes  = ((String) row[4]).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            noVec    .setSafe(i, noBytes, 0, noBytes.length);
        }
    }

    private static void fillBatchMock(List<FieldVector> vectors, RowGenerator gen, int n) {
        BigIntVector  startVec    = (BigIntVector)  vectors.get(0);
        VarCharVector imsiVec     = (VarCharVector) vectors.get(1);
        VarCharVector msisdnVec   = (VarCharVector) vectors.get(2);
        BigIntVector  durVec      = (BigIntVector)  vectors.get(3);
        BigIntVector  upVec       = (BigIntVector)  vectors.get(4);
        BigIntVector  dwVec       = (BigIntVector)  vectors.get(5);
        VarCharVector cellVec     = (VarCharVector) vectors.get(6);
        VarCharVector ratVec      = (VarCharVector) vectors.get(7);

        for (int i = 0; i < n; i++) {
            Object[] row = gen.nextRow();
            startVec.setSafe(i, (Long) row[0]);
            setUtf8(imsiVec,   i, (String) row[1]);
            setUtf8(msisdnVec, i, (String) row[2]);
            durVec.setSafe(i, (Long) row[3]);
            upVec .setSafe(i, (Long) row[4]);
            dwVec .setSafe(i, (Long) row[5]);
            setUtf8(cellVec, i, (String) row[6]);
            setUtf8(ratVec,  i, (String) row[7]);
        }
    }

    private static void setUtf8(VarCharVector vec, int idx, String value) {
        byte[] b = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        vec.setSafe(idx, b, 0, b.length);
    }

    // ── Result record ─────────────────────────────────────────────────────────

    public record GenerateResult(long totalRows, int batches, long fileSizeBytes) {}

    // ── CLI helpers ───────────────────────────────────────────────────────────

    private static Options buildOptions() {
        Options o = new Options();
        o.addOption(Option.builder().longOpt("output")
            .desc("Output Arrow IPC Stream file path  [required]")
            .hasArg().argName("FILE").build());
        o.addOption(Option.builder().longOpt("table")
            .desc("Table schema to generate. Available:\n" +
                  "  tdr_signal_stor_20550  (default) — new signal-store table\n" +
                  "  tdr_mock            — original CDR table")
            .hasArg().argName("TABLE").build());
        o.addOption(Option.builder().longOpt("size")
            .desc("Target file size in MiB              [default: " + DEFAULT_SIZE_MB + "]")
            .hasArg().argName("MiB").build());
        o.addOption(Option.builder().longOpt("batch")
            .desc("Rows per Arrow RecordBatch            [default: " + DEFAULT_BATCH_ROWS + "]")
            .hasArg().argName("ROWS").build());
        o.addOption(Option.builder().longOpt("seed")
            .desc("Random seed for reproducibility      [default: " + DEFAULT_SEED + "]")
            .hasArg().argName("SEED").build());
        o.addOption(Option.builder().longOpt("help")
            .desc("Print this help message").build());
        return o;
    }

    private static void printHelp(Options options) {
        System.out.println();
        new HelpFormatter().printHelp(
            "java -jar mock-arrow-1.0.0.jar",
            "\nGenerate Arrow IPC Stream files for HFileSDK end-to-end testing.\n\n",
            options,
            "\nExamples:\n" +
            "  # Generate 10 MiB (default) for tdr_signal_stor_20550:\n" +
            "  java -jar mock-arrow-1.0.0.jar --output /data/tdr_20550.arrow\n\n" +
            "  # Generate 100 MiB for the original CDR table:\n" +
            "  java -jar mock-arrow-1.0.0.jar \\\n" +
            "    --output /data/tdr_mock.arrow \\\n" +
            "    --table  tdr_mock \\\n" +
            "    --size   100\n\n" +
            "  # Then convert to HFile:\n" +
            "  java -jar arrow-to-hfile-4.0.0.jar \\\n" +
            "    --native-lib /opt/hfilesdk/libhfilesdk.so \\\n" +
            "    --arrow /data/tdr_20550.arrow \\\n" +
            "    --hfile /staging/cf/tdr_20550.hfile \\\n" +
            "    --rule  \"REFID,0,false,15\"\n",
            true);
    }

    private static int  parseInt (String s, int  def) { try { return Integer.parseInt(s);  } catch (Exception e) { return def; } }
    private static long parseLong (String s, long def) { try { return Long.parseLong(s);    } catch (Exception e) { return def; } }
}
