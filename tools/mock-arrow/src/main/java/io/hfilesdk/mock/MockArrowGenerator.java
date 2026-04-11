package io.hfilesdk.mock;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates Arrow IPC Stream files for HFileSDK end-to-end testing.
 *
 * <h2>单文件模式（原有功能 + ZSTD 压缩）</h2>
 * <pre>{@code
 * java -jar mock-arrow-1.0.0.jar \
 *   --output  /data/events.arrow \
 *   --table   tdr_signal_stor_20550 \
 *   --size    50                      # 50 MiB (default 10 MiB)
 *   --seed    42                      # 随机种子（默认 42）
 *   --batch   1000                    # 每个 RecordBatch 的行数（默认 1000）
 *   --compression ZSTD                # ZSTD|NONE，默认 ZSTD（与生产环境一致）
 * }</pre>
 *
 * <h2>批量模式（新增，跨文件无重复主键）</h2>
 * <pre>{@code
 * java -jar mock-arrow-1.0.0.jar \
 *   --dir   /data/batch \
 *   --count 10 \
 *   --size  200 \
 *   --table tdr_signal_stor_20550 \
 *   --compression ZSTD
 * }</pre>
 * <p>生成文件：{dir}/{table}_{0000..count-1}.arrow，文件名示例：
 * {@code tdr_signal_stor_20550_0000.arrow}。
 *
 * <h2>跨文件唯一键保证</h2>
 * <p>批量模式使用同一个 {@link RowGenerator} 实例跨所有文件持续生成数据。
 * {@code RowGenerator} 内部维护单调递增的逻辑主键来源（如 {@code refid} /
 * {@code starttime}），再在写入每个 RecordBatch 前做批内洗牌，因此：
 * <ul>
 *   <li>跨文件不会产生重复主键</li>
 *   <li>最终 Arrow 文件默认是乱序的，更贴近生产环境</li>
 * </ul>
 *
 * <h2>ZSTD 压缩说明</h2>
 * <p>Arrow IPC body 级别压缩：每个 RecordBatch 的 body buffers 经 ZSTD 压缩，
 * FlatBuffer 消息中写入 {@code BodyCompression{codec=ZSTD}}。
 * C++ 侧 {@code RecordBatchStreamReader::ReadNext()} 自动透明解压，
 * <b>HFileSDK C++ 无需任何修改</b>（前提：Arrow C++ 以 {@code -DARROW_WITH_ZSTD=ON} 编译，
 * 系统包默认开启）。
 *
 * <h2>可选的表名</h2>
 * <ul>
 *   <li>{@code tdr_signal_stor_20550} — 新信令存储表（默认）</li>
 *   <li>{@code tdr_mock} — 原始 CDR 话单表</li>
 * </ul>
 */
public class MockArrowGenerator {

    // ── Defaults ──────────────────────────────────────────────────────────────
    private static final int    DEFAULT_SIZE_MB    = 10;
    private static final int    DEFAULT_BATCH_ROWS = 1000;
    private static final int    DEFAULT_PAYLOAD_BYTES = 0;
    private static final long   DEFAULT_SEED       = 42L;
    private static final String DEFAULT_TABLE      = TableSchema.TDR_SIGNAL_STOR_20550.tableName;
    private static final String DEFAULT_COMPRESSION = "ZSTD";

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

        // ── Parse common arguments ────────────────────────────────────────
        String tableName  = cmd.getOptionValue("table",       DEFAULT_TABLE);
        int    sizeMb     = parseInt(cmd.getOptionValue("size",  String.valueOf(DEFAULT_SIZE_MB)),  DEFAULT_SIZE_MB);
        int    batchRows  = parseInt(cmd.getOptionValue("batch", String.valueOf(DEFAULT_BATCH_ROWS)), DEFAULT_BATCH_ROWS);
        int    payloadBytes = parseInt(cmd.getOptionValue("payload-bytes", String.valueOf(DEFAULT_PAYLOAD_BYTES)), DEFAULT_PAYLOAD_BYTES);
        long   seed       = parseLong(cmd.getOptionValue("seed", String.valueOf(DEFAULT_SEED)), DEFAULT_SEED);
        String compStr    = cmd.getOptionValue("compression", DEFAULT_COMPRESSION);

        TableSchema schema;
        try {
            schema = TableSchema.forTable(tableName);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
            return;
        }

        CompressionUtil.CodecType codec = parseCodec(compStr);

        boolean batchMode  = cmd.hasOption("dir");
        boolean singleMode = cmd.hasOption("output");

        if (batchMode && singleMode) {
            System.err.println("Error: --output 和 --dir 不能同时使用。");
            printHelp(options);
            System.exit(1);
            return;
        }
        if (!batchMode && !singleMode) {
            System.err.println("Error: 必须指定 --output（单文件模式）或 --dir（批量模式）。");
            printHelp(options);
            System.exit(1);
            return;
        }

        if (singleMode) {
            // ── 单文件模式 ────────────────────────────────────────────────
            String outputPath = cmd.getOptionValue("output");
            Path out = Paths.get(outputPath);
            if (out.getParent() != null) Files.createDirectories(out.getParent());

            System.out.printf("Generating Arrow IPC Stream file%n");
            System.out.printf("  table       : %s%n", schema.tableName);
            System.out.printf("  output      : %s%n", outputPath);
            System.out.printf("  target      : %d MiB%n", sizeMb);
            System.out.printf("  batch       : %d rows%n", batchRows);
            System.out.printf("  payload     : %s%n", payloadBytes > 0 ? payloadBytes + " bytes" : "schema default");
            System.out.printf("  seed        : %d%n", seed);
            System.out.printf("  compression : %s%n", compStr.toUpperCase());
            System.out.printf("  rowKey      : %s%n%n", schema.rowKeyRule);

            RowGenerator gen = new RowGenerator(schema, seed, payloadBytes);
            GenerateResult result = generate(schema, out, sizeMb, batchRows, gen, codec);

            System.out.printf("%nDone:%n");
            System.out.printf("  rows written : %,d%n",   result.totalRows);
            System.out.printf("  batches      : %,d%n",   result.batches);
            System.out.printf("  file size    : %.2f MiB%n", result.fileSizeBytes / 1024.0 / 1024.0);
            if (codec != null) {
                double uncompMib = (double) sizeMb * result.totalRows
                    / Math.max(1, sizeMb * 1024L * 1024L / gen.estimateRowBytes());
                System.out.printf("  compression  : ~%.1fx ratio%n",
                    (sizeMb * 1024.0 * 1024.0) / result.fileSizeBytes);
            }
            System.out.printf("%nrowKeyRule for arrow-to-hfile:%n  %s%n", schema.rowKeyRule);

        } else {
            // ── 批量模式 ──────────────────────────────────────────────────
            Path dir   = Paths.get(cmd.getOptionValue("dir"));
            int  count = parseInt(cmd.getOptionValue("count", "1"), 1);
            if (count < 1) {
                System.err.println("Error: --count 必须 >= 1");
                System.exit(1);
                return;
            }
            Files.createDirectories(dir);

            System.out.printf("Generating Arrow IPC Stream files (batch mode)%n");
            System.out.printf("  table       : %s%n", schema.tableName);
            System.out.printf("  dir         : %s%n", dir);
            System.out.printf("  count       : %d files%n", count);
            System.out.printf("  size/file   : %d MiB%n", sizeMb);
            System.out.printf("  batch       : %d rows%n", batchRows);
            System.out.printf("  payload     : %s%n", payloadBytes > 0 ? payloadBytes + " bytes" : "schema default");
            System.out.printf("  seed        : %d%n", seed);
            System.out.printf("  compression : %s%n", compStr.toUpperCase());
            System.out.printf("  rowKey      : %s%n", schema.rowKeyRule);
            System.out.printf("%n  [唯一键保证] 所有文件共享同一 RowGenerator，%n");
            System.out.printf("  REFID/STARTTIME 单调递增，跨文件绝无重复%n%n");

            // 关键：单一 RowGenerator 实例跨所有文件，保证主键全局唯一
            long startAll  = System.currentTimeMillis();
            DirectoryGenerateResult batchResult = generateDirectory(
                schema, dir, count, sizeMb, batchRows, seed, payloadBytes, codec);
            for (int i = 0; i < batchResult.files().size(); i++) {
                Path filePath = batchResult.files().get(i);
                GenerateResult fileResult = batchResult.fileResults().get(i);
                System.out.printf("  [%d/%d] %-50s  rows=%,d  size=%.1fMiB%n",
                    i + 1, count, filePath.getFileName(), fileResult.totalRows,
                    fileResult.fileSizeBytes / 1024.0 / 1024.0);
            }
            long totalMs = System.currentTimeMillis() - startAll;
            System.out.printf("%nDone: %d files  total rows=%,d  elapsed=%.1fs%n",
                count, batchResult.totalRows(), totalMs / 1000.0);
            System.out.printf("rowKeyRule: %s%n", schema.rowKeyRule);
        }
    }

    // ── Programmatic API ──────────────────────────────────────────────────────

    /**
     * 原有签名保持不变（向后兼容），compression = null（不压缩）。
     */
    public static GenerateResult generate(
            TableSchema schema, Path output, int sizeMb, int batchRows, long seed)
            throws IOException {
        return generate(schema, output, sizeMb, batchRows,
                        new RowGenerator(schema, seed), null);
    }

    public static GenerateResult generate(
            TableSchema schema, Path output, int sizeMb, int batchRows, long seed, int payloadBytes)
            throws IOException {
        return generate(schema, output, sizeMb, batchRows,
                        new RowGenerator(schema, seed, payloadBytes), null);
    }

    /**
     * 生成单个 Arrow IPC Stream 文件。
     *
     * @param schema     表 schema
     * @param output     输出路径
     * @param sizeMb     目标大小（MiB）
     * @param batchRows  每个 RecordBatch 的行数
     * @param gen        行数据生成器（批量模式传入共享实例以保证唯一性）
     * @param codec      压缩编解码器；null 表示不压缩
     */
    public static GenerateResult generate(
            TableSchema schema, Path output, int sizeMb, int batchRows,
            RowGenerator gen, CompressionUtil.CodecType codec)
            throws IOException {

        long targetBytes   = (long) sizeMb * 1024 * 1024;
        int  estimatedRowBytes = gen.estimateRowBytes();
        long adjustedTarget    = (long) (targetBytes * 0.95);

        long totalRows = 0;
        int  batches   = 0;

        if (output.getParent() != null) Files.createDirectories(output.getParent());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = schema.allocateRoot(allocator);
             OutputStream fos = new BufferedOutputStream(
                     Files.newOutputStream(output), 256 * 1024);
             ArrowStreamWriter writer = buildWriter(root, fos, codec)) {

            writer.start();

            long writtenBytes = 0;
            while (writtenBytes < adjustedTarget) {
                long remainingBytes = adjustedTarget - writtenBytes;
                int  rowsThisBatch  = (int) Math.min(batchRows,
                        Math.max(1, remainingBytes / estimatedRowBytes));

                fillBatch(schema, root, gen, rowsThisBatch);
                writer.writeBatch();
                batches++;
                totalRows    += rowsThisBatch;
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

    public static DirectoryGenerateResult generateDirectory(
            TableSchema schema,
            Path dir,
            int count,
            int sizeMb,
            int batchRows,
            long seed,
            int payloadBytes,
            CompressionUtil.CodecType codec) throws IOException {
        if (count < 1) {
            throw new IllegalArgumentException("count must be >= 1");
        }
        Files.createDirectories(dir);
        RowGenerator sharedGen = new RowGenerator(schema, seed, payloadBytes);
        List<Path> files = new ArrayList<>(count);
        List<GenerateResult> fileResults = new ArrayList<>(count);
        long totalRows = 0L;
        int totalBatches = 0;
        long totalFileSizeBytes = 0L;
        for (int index = 0; index < count; index++) {
            Path filePath = dir.resolve(String.format("%s_%04d.arrow", schema.tableName, index));
            GenerateResult result = generate(schema, filePath, sizeMb, batchRows, sharedGen, codec);
            files.add(filePath);
            fileResults.add(result);
            totalRows += result.totalRows;
            totalBatches += result.batches;
            totalFileSizeBytes += result.fileSizeBytes;
        }
        return new DirectoryGenerateResult(
            totalRows,
            totalBatches,
            totalFileSizeBytes,
            List.copyOf(files),
            List.copyOf(fileResults)
        );
    }

    // ── Writer factory ────────────────────────────────────────────────────────

    /**
     * 构建 ArrowStreamWriter，根据 codec 决定是否启用 body 压缩。
     *
     * <p>ZSTD 模式：使用 {@link CommonsCompressionFactory} 构造带压缩的 writer，
     * 在每个 RecordBatch FlatBuffer 中写入 {@code BodyCompression{codec=ZSTD}}。
     * C++ Arrow {@code RecordBatchStreamReader} 读取时自动解压，透明无感知。
     *
     * <p>NONE 模式（codec == null）：与原有逻辑完全相同。
     */
    private static ArrowStreamWriter buildWriter(
            VectorSchemaRoot root, OutputStream fos, CompressionUtil.CodecType codec)
            throws IOException {
        WritableByteChannel channel = Channels.newChannel(fos);
        if (codec != null) {
            // Arrow 16.x 正确构造器签名：(root, dict, channel, IpcOption, Factory, CodecType)
            return new ArrowStreamWriter(root, null, channel, IpcOption.DEFAULT,
                CommonsCompressionFactory.INSTANCE, codec);
        } else {
            return new ArrowStreamWriter(root, null, channel);
        }
    }

    // ── Batch filling（默认生成乱序数据）─────────────────────────────────────

    private static void fillBatch(
            TableSchema schema, VectorSchemaRoot root,
            RowGenerator gen, int numRows) {

        root.allocateNew();
        List<FieldVector> vectors = root.getFieldVectors();

        switch (schema) {
            case TDR_SIGNAL_STOR_20550 -> fillBatch20550(vectors, gen, numRows);
            case TDR_MOCK              -> fillBatchMock(vectors, gen, numRows);
        }

        root.setRowCount(numRows);
    }

    private static void fillBatch20550(List<FieldVector> vectors, RowGenerator gen, int n) {
        BigIntVector  refidVec   = (BigIntVector)  vectors.get(0);
        BigIntVector  timeVec    = (BigIntVector)  vectors.get(1);
        VarCharVector sigVec     = (VarCharVector) vectors.get(2);
        BigIntVector  bitmapVec  = (BigIntVector)  vectors.get(3);
        VarCharVector noVec      = (VarCharVector) vectors.get(4);

        Object[][] rows = new Object[n][];
        for (int i = 0; i < n; i++) {
            rows[i] = gen.nextRow();
        }
        gen.shuffleRows(rows);

        for (int i = 0; i < n; i++) {
            Object[] row = rows[i];
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
        BigIntVector  startVec  = (BigIntVector)  vectors.get(0);
        VarCharVector imsiVec   = (VarCharVector) vectors.get(1);
        VarCharVector msisdnVec = (VarCharVector) vectors.get(2);
        BigIntVector  durVec    = (BigIntVector)  vectors.get(3);
        BigIntVector  upVec     = (BigIntVector)  vectors.get(4);
        BigIntVector  dwVec     = (BigIntVector)  vectors.get(5);
        VarCharVector cellVec   = (VarCharVector) vectors.get(6);
        VarCharVector ratVec    = (VarCharVector) vectors.get(7);

        Object[][] rows = new Object[n][];
        for (int i = 0; i < n; i++) {
            rows[i] = gen.nextRow();
        }
        gen.shuffleRows(rows);

        for (int i = 0; i < n; i++) {
            Object[] row = rows[i];
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

    // ── Result record（原有，不变）────────────────────────────────────────────

    public record GenerateResult(long totalRows, int batches, long fileSizeBytes) {}

    public record DirectoryGenerateResult(
        long totalRows,
        int totalBatches,
        long totalFileSizeBytes,
        List<Path> files,
        List<GenerateResult> fileResults
    ) {}

    // ── CLI helpers ───────────────────────────────────────────────────────────

    /**
     * 解析压缩类型字符串，返回对应的 CodecType 或 null（NONE）。
     */
    private static CompressionUtil.CodecType parseCodec(String name) {
        return switch (name.toUpperCase().trim()) {
            case "NONE" -> null;
            case "ZSTD" -> CompressionUtil.CodecType.ZSTD;
            default -> throw new IllegalArgumentException(
                "Unknown compression: '" + name + "'. Use: ZSTD | NONE");
        };
    }

    private static Options buildOptions() {
        Options o = new Options();

        // 单文件模式
        o.addOption(Option.builder().longOpt("output")
            .desc("【单文件模式】输出 Arrow IPC Stream 文件路径  [required if no --dir]")
            .hasArg().argName("FILE").build());

        // 批量模式（新增）
        o.addOption(Option.builder().longOpt("dir")
            .desc("【批量模式】输出目录，与 --count 配合使用  [required if no --output]")
            .hasArg().argName("DIR").build());
        o.addOption(Option.builder().longOpt("count")
            .desc("【批量模式】生成文件数量  [default: 1]")
            .hasArg().argName("N").build());

        // 公共参数
        o.addOption(Option.builder().longOpt("table")
            .desc("Table schema to generate. Available:\n" +
                  "  tdr_signal_stor_20550  (default)\n" +
                  "  tdr_mock")
            .hasArg().argName("TABLE").build());
        o.addOption(Option.builder().longOpt("size")
            .desc("Target file size in MiB  [default: " + DEFAULT_SIZE_MB + "]")
            .hasArg().argName("MiB").build());
        o.addOption(Option.builder().longOpt("batch")
            .desc("Rows per Arrow RecordBatch  [default: " + DEFAULT_BATCH_ROWS + "]")
            .hasArg().argName("ROWS").build());
        o.addOption(Option.builder().longOpt("payload-bytes")
            .desc("Approximate payload bytes for large text columns; for tdr_signal_stor_20550 this tunes SIGSTORE width")
            .hasArg().argName("BYTES").build());
        o.addOption(Option.builder().longOpt("seed")
            .desc("Random seed for reproducibility  [default: " + DEFAULT_SEED + "]")
            .hasArg().argName("SEED").build());

        // 压缩（新增）
        o.addOption(Option.builder().longOpt("compression")
            .desc("Arrow IPC body 压缩  [default: ZSTD，与生产环境一致]\n" +
                  "可选: ZSTD | NONE")
            .hasArg().argName("CODEC").build());

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
            "  # 单文件，ZSTD 压缩（默认，与生产一致）:\n" +
            "  java -jar mock-arrow-1.0.0.jar \\\n" +
            "    --output /data/tdr_20550.arrow --size 50 --payload-bytes 512\n\n" +
            "  # 单文件，不压缩:\n" +
            "  java -jar mock-arrow-1.0.0.jar \\\n" +
            "    --output /data/tdr_20550.arrow --size 50 --compression NONE\n\n" +
            "  # 批量模式，生成 10 个文件，跨文件无重复主键:\n" +
            "  java -jar mock-arrow-1.0.0.jar \\\n" +
            "    --dir /data/batch --count 10 --size 200 --compression ZSTD\n\n" +
            "  # 然后用 arrow-to-hfile 转换:\n" +
            "  java -jar arrow-to-hfile-4.0.0.jar \\\n" +
            "    --native-lib /opt/hfilesdk/libhfilesdk.so \\\n" +
            "    --arrow /data/tdr_20550.arrow \\\n" +
            "    --hfile /staging/cf/tdr_20550.hfile \\\n" +
            "    --rule  \"REFID,0,false,15\"\n",
            true);
    }

    private static int  parseInt (String s, int  def) { try { return Integer.parseInt(s);  } catch (Exception e) { return def; } }
    private static long parseLong(String s, long def) { try { return Long.parseLong(s);    } catch (Exception e) { return def; } }
}
