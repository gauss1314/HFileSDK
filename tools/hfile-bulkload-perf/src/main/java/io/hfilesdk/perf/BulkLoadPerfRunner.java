package io.hfilesdk.perf;

import com.hfile.HFileSDK;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public final class BulkLoadPerfRunner {

    private static final int DEFAULT_ITERATIONS = 1;
    private static final int DEFAULT_TARGET_SIZE_MB = 1024;
    private static final int DEFAULT_BATCH_ROWS = 8192;
    private static final int DEFAULT_PAYLOAD_BYTES = 768;
    private static final String DEFAULT_RULE = "USER_ID,0,false,0#long(),1,false,0";
    private static final String DEFAULT_CF = "cf";
    private static final String DEFAULT_COMPRESSION = "lz4";
    private static final String DEFAULT_ENCODING = "FAST_DIFF";
    private static final String DEFAULT_BLOOM = "row";
    private static final String DEFAULT_FSYNC_POLICY = "safe";
    private static final String DEFAULT_ERROR_POLICY = "skip_row";
    private static final int DEFAULT_BLOCK_SIZE = 65536;
    private static final String DEFAULT_HBASE_BIN = "hbase";
    private static final String DEFAULT_WORK_DIR = "/tmp/hfilesdk-bulkload-perf";
    private static final String DEFAULT_BULKLOAD_DIR = "/tmp/hbase_bulkload";
    private static final Path DEFAULT_VERIFY_JAR = Path.of(
        "tools", "hfile-bulkload-verify", "target", "hfile-bulkload-verify-1.0.0.jar"
    ).toAbsolutePath().normalize();
    private static final String USER_ID_PREFIX = "user-";
    private static final String DEVICE_PREFIX = "device-";
    private static final String[] REGIONS = {"cn-north", "cn-east", "cn-south", "sg", "us-west"};

    private BulkLoadPerfRunner() {}

    public static void main(String[] args) throws Exception {
        if (ensureJavaNioOpen(args)) {
            return;
        }

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

        RunConfig config;
        try {
            config = parseConfig(cmd);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            printHelp(options);
            System.exit(1);
            return;
        }

        long totalStart = System.nanoTime();
        List<PerfResult> iterationResults = new ArrayList<>();
        Throwable failure = null;
        try {
            Files.createDirectories(config.workDir());
            Files.createDirectories(config.reportJson().getParent());
            if (!config.skipBulkLoad()) {
                TableCheckStats tableCheckStats = checkTableExists(config);
                if (!tableCheckStats.success()) {
                    throw new IllegalStateException(tableCheckStats.errorMessage());
                }
            }
            for (int i = 1; i <= config.iterations(); ++i) {
                RunConfig iterationConfig = config.forIteration(i);
                PerfResult iterationResult = runSingleIteration(iterationConfig);
                iterationResults.add(iterationResult);
                printIterationSummary(iterationResult, i, config.iterations());
                if (!iterationResult.success()) {
                    throw new IllegalStateException(iterationResult.errorMessage());
                }
            }
        } catch (Exception e) {
            failure = e;
        }

        long totalNanos = System.nanoTime() - totalStart;
        AggregateReport report = AggregateReport.from(config, iterationResults, totalNanos, failure);
        writeAggregateReport(report);
        printAggregateSummary(report);

        if (failure == null && !config.keepGeneratedFiles()) {
            cleanupGeneratedArtifacts(config);
        }
        if (failure != null) {
            System.err.println("FAILED: " + messageOf(failure));
            throw propagate(failure);
        }
    }

    private static PerfResult runSingleIteration(RunConfig config) throws Exception {
        GenerationStats generationStats = GenerationStats.empty();
        ConversionStats conversionStats = ConversionStats.notStarted();
        BulkLoadStats bulkLoadStats = BulkLoadStats.notStarted();
        VerifyStats verifyStats = VerifyStats.notStarted();
        Throwable failure = null;
        long totalStart = System.nanoTime();
        try {
            prepareDirectories(config);
            generationStats = generateArrowFile(config);
            conversionStats = convertToHFile(config);
            if (!conversionStats.success()) {
                throw new IllegalStateException(conversionStats.errorMessage());
            }
            bulkLoadStats = config.skipBulkLoad()
                ? BulkLoadStats.skippedResult()
                : runBulkLoad(config);
            if (!bulkLoadStats.success()) {
                throw new IllegalStateException(bulkLoadStats.errorMessage());
            }
            verifyStats = config.verifyBulkLoad()
                ? runVerify(config, generationStats.rows())
                : VerifyStats.skippedResult();
            if (!verifyStats.success()) {
                throw new IllegalStateException(verifyStats.errorMessage());
            }
        } catch (Exception e) {
            failure = e;
        }

        long totalNanos = System.nanoTime() - totalStart;
        PerfResult result = new PerfResult(
            config,
            failure == null,
            failure == null ? "" : messageOf(failure),
            generationStats.rows(),
            generationStats.batches(),
            Files.exists(config.arrowPath()) ? Files.size(config.arrowPath()) : 0L,
            Files.exists(config.hfilePath()) ? Files.size(config.hfilePath()) : 0L,
            generationStats.elapsedNanos(),
            conversionStats.elapsedNanos(),
            bulkLoadStats.elapsedNanos(),
            verifyStats.elapsedNanos(),
            totalNanos,
            bulkLoadStats.skipped(),
            bulkLoadStats.command(),
            bulkLoadStats.exitCode(),
            bulkLoadStats.output(),
            verifyStats.skipped(),
            verifyStats.command(),
            verifyStats.exitCode(),
            verifyStats.output(),
            conversionStats.sdkResultJson()
        );
        writeIterationReport(result);
        return result;
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("table").hasArg().argName("NAME").desc("HBase 表名").build());
        options.addOption(Option.builder().longOpt("native-lib").hasArg().argName("PATH").desc("libhfilesdk 动态库绝对路径").build());
        options.addOption(Option.builder().longOpt("work-dir").hasArg().argName("PATH").desc("Arrow 文件与报告输出目录").build());
        options.addOption(Option.builder().longOpt("bulkload-dir").hasArg().argName("PATH").desc("BulkLoad HFile staging 目录").build());
        options.addOption(Option.builder().longOpt("iterations").hasArg().argName("N").desc("完整端到端执行轮数，默认 1").build());
        options.addOption(Option.builder().longOpt("target-size-mb").hasArg().argName("MB").desc("目标 Arrow 文件大小，默认 1024").build());
        options.addOption(Option.builder().longOpt("batch-rows").hasArg().argName("N").desc("每个 Arrow batch 的行数，默认 8192").build());
        options.addOption(Option.builder().longOpt("payload-bytes").hasArg().argName("BYTES").desc("每行 PAYLOAD 列长度，默认 768").build());
        options.addOption(Option.builder().longOpt("cf").hasArg().argName("CF").desc("列族名，默认 cf").build());
        options.addOption(Option.builder().longOpt("rule").hasArg().argName("RULE").desc("rowKeyRule，默认 USER_ID + long()").build());
        options.addOption(Option.builder().longOpt("compression").hasArg().argName("ALG").desc("压缩算法，默认 lz4").build());
        options.addOption(Option.builder().longOpt("encoding").hasArg().argName("ENC").desc("Data Block Encoding，默认 FAST_DIFF").build());
        options.addOption(Option.builder().longOpt("bloom").hasArg().argName("TYPE").desc("Bloom 类型，默认 row").build());
        options.addOption(Option.builder().longOpt("fsync-policy").hasArg().argName("POLICY").desc("fsync 策略，默认 safe").build());
        options.addOption(Option.builder().longOpt("error-policy").hasArg().argName("POLICY").desc("错误策略，默认 skip_row").build());
        options.addOption(Option.builder().longOpt("block-size").hasArg().argName("BYTES").desc("HFile block size，默认 65536").build());
        options.addOption(Option.builder().longOpt("hbase-bin").hasArg().argName("BIN").desc("hbase 命令路径，默认 hbase").build());
        options.addOption(Option.builder().longOpt("verify-bulkload").desc("BulkLoad 完成后执行行数校验").build());
        options.addOption(Option.builder().longOpt("verify-jar").hasArg().argName("PATH").desc("BulkLoad 校验工具 jar 路径").build());
        options.addOption(Option.builder().longOpt("zookeeper").hasArg().argName("HOST:PORT").desc("BulkLoad 校验用 ZooKeeper 地址").build());
        options.addOption(Option.builder().longOpt("report-json").hasArg().argName("PATH").desc("性能报告 JSON 路径").build());
        options.addOption(Option.builder().longOpt("skip-bulkload").desc("仅生成 Arrow 并完成 JNI 转换，不执行 BulkLoad").build());
        options.addOption(Option.builder().longOpt("keep-generated-files").desc("保留 Arrow 文件与 staging HFile").build());
        options.addOption(Option.builder().longOpt("help").desc("显示帮助").build());
        return options;
    }

    private static RunConfig parseConfig(CommandLine cmd) {
        String tableName = required(cmd, "table");
        String nativeLib = required(cmd, "native-lib");
        Path workDir = Path.of(cmd.getOptionValue("work-dir", DEFAULT_WORK_DIR)).toAbsolutePath().normalize();
        Path bulkloadDir = Path.of(cmd.getOptionValue("bulkload-dir", DEFAULT_BULKLOAD_DIR)).toAbsolutePath().normalize();
        int iterations = parsePositiveInt(cmd.getOptionValue("iterations", Integer.toString(DEFAULT_ITERATIONS)), "iterations");
        int targetSizeMb = parsePositiveInt(cmd.getOptionValue("target-size-mb", Integer.toString(DEFAULT_TARGET_SIZE_MB)), "target-size-mb");
        int batchRows = parsePositiveInt(cmd.getOptionValue("batch-rows", Integer.toString(DEFAULT_BATCH_ROWS)), "batch-rows");
        int payloadBytes = parsePositiveInt(cmd.getOptionValue("payload-bytes", Integer.toString(DEFAULT_PAYLOAD_BYTES)), "payload-bytes");
        int blockSize = parsePositiveInt(cmd.getOptionValue("block-size", Integer.toString(DEFAULT_BLOCK_SIZE)), "block-size");
        String columnFamily = cmd.getOptionValue("cf", DEFAULT_CF);
        String rowKeyRule = cmd.getOptionValue("rule", DEFAULT_RULE);
        String compression = cmd.getOptionValue("compression", DEFAULT_COMPRESSION);
        String encoding = cmd.getOptionValue("encoding", DEFAULT_ENCODING);
        String bloom = cmd.getOptionValue("bloom", DEFAULT_BLOOM);
        String fsyncPolicy = cmd.getOptionValue("fsync-policy", DEFAULT_FSYNC_POLICY);
        String errorPolicy = cmd.getOptionValue("error-policy", DEFAULT_ERROR_POLICY);
        String hbaseBin = cmd.getOptionValue("hbase-bin", DEFAULT_HBASE_BIN);
        boolean verifyBulkLoad = cmd.hasOption("verify-bulkload");
        String zookeeper = cmd.getOptionValue("zookeeper", "");
        Path verifyJar = Path.of(cmd.getOptionValue("verify-jar", DEFAULT_VERIFY_JAR.toString())).toAbsolutePath().normalize();
        boolean skipBulkload = cmd.hasOption("skip-bulkload");
        boolean keepGeneratedFiles = cmd.hasOption("keep-generated-files");
        if (verifyBulkLoad && skipBulkload) {
            throw new IllegalArgumentException("--verify-bulkload 不能与 --skip-bulkload 同时使用");
        }
        if (verifyBulkLoad && zookeeper.isBlank()) {
            throw new IllegalArgumentException("--verify-bulkload 时必须提供 --zookeeper");
        }
        Path arrowPath = workDir.resolve(tableName + ".arrow");
        Path hfilePath = bulkloadDir.resolve(columnFamily).resolve("part-00000.hfile");
        Path reportJson = Path.of(cmd.getOptionValue("report-json", workDir.resolve(tableName + "-perf-report.json").toString()))
            .toAbsolutePath()
            .normalize();
        return new RunConfig(
            tableName,
            nativeLib,
            workDir,
            bulkloadDir,
            iterations,
            1,
            arrowPath,
            hfilePath,
            reportJson,
            targetSizeMb,
            batchRows,
            payloadBytes,
            blockSize,
            columnFamily,
            rowKeyRule,
            compression,
            encoding,
            bloom,
            fsyncPolicy,
            errorPolicy,
            hbaseBin,
            verifyBulkLoad,
            verifyJar,
            zookeeper,
            skipBulkload,
            keepGeneratedFiles
        );
    }

    private static void prepareDirectories(RunConfig config) throws IOException {
        Files.createDirectories(config.workDir());
        if (Files.exists(config.arrowPath())) {
            Files.delete(config.arrowPath());
        }
        deleteRecursively(config.bulkloadDir());
        Files.createDirectories(config.hfilePath().getParent());
        Files.createDirectories(config.reportJson().getParent());
    }

    private static GenerationStats generateArrowFile(RunConfig config) throws Exception {
        long start = System.nanoTime();
        long targetBytes = config.targetSizeMb() * 1024L * 1024L;
        byte[] payloadSuffix = buildPayloadSuffix(config.payloadBytes());
        long rows = 0;
        int batches = 0;

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VarCharVector userIdVector = new VarCharVector("USER_ID", allocator);
             BigIntVector eventTimeVector = new BigIntVector("EVENT_TIME", allocator);
             VarCharVector deviceIdVector = new VarCharVector("DEVICE_ID", allocator);
             VarCharVector regionVector = new VarCharVector("REGION", allocator);
             IntVector scoreVector = new IntVector("SCORE", allocator);
             VarCharVector payloadVector = new VarCharVector("PAYLOAD", allocator);
             VectorSchemaRoot root = new VectorSchemaRoot(List.of(
                 userIdVector,
                 eventTimeVector,
                 deviceIdVector,
                 regionVector,
                 scoreVector,
                 payloadVector
             ));
             ArrowStreamWriter writer = new ArrowStreamWriter(
                 root,
                 null,
                 Channels.newChannel(Files.newOutputStream(config.arrowPath())))) {

            allocateVectors(config, userIdVector, eventTimeVector, deviceIdVector, regionVector, scoreVector, payloadVector);
            writer.start();

            long eventBase = 1_775_000_000L;
            while (!Files.exists(config.arrowPath()) || Files.size(config.arrowPath()) < targetBytes) {
                fillBatch(config, rows, payloadSuffix, eventBase, userIdVector, eventTimeVector, deviceIdVector, regionVector, scoreVector, payloadVector);
                root.setRowCount(config.batchRows());
                writer.writeBatch();
                rows += config.batchRows();
                batches++;
            }
            writer.end();
        }

        return new GenerationStats(rows, batches, System.nanoTime() - start);
    }

    private static void allocateVectors(RunConfig config,
                                        VarCharVector userIdVector,
                                        BigIntVector eventTimeVector,
                                        VarCharVector deviceIdVector,
                                        VarCharVector regionVector,
                                        IntVector scoreVector,
                                        VarCharVector payloadVector) {
        int batchRows = config.batchRows();
        userIdVector.allocateNew(Math.max(256, batchRows * 24), batchRows);
        eventTimeVector.allocateNew(batchRows);
        deviceIdVector.allocateNew(Math.max(256, batchRows * 24), batchRows);
        regionVector.allocateNew(Math.max(256, batchRows * 12), batchRows);
        scoreVector.allocateNew(batchRows);
        payloadVector.allocateNew((long) batchRows * config.payloadBytes(), batchRows);
    }

    private static void fillBatch(RunConfig config,
                                  long rowOffset,
                                  byte[] payloadSuffix,
                                  long eventBase,
                                  VarCharVector userIdVector,
                                  BigIntVector eventTimeVector,
                                  VarCharVector deviceIdVector,
                                  VarCharVector regionVector,
                                  IntVector scoreVector,
                                  VarCharVector payloadVector) {
        int batchRows = config.batchRows();
        for (int i = 0; i < batchRows; ++i) {
            long rowId = rowOffset + i;
            userIdVector.setSafe(i, utf8(USER_ID_PREFIX + zeroPad(rowId, 12)));
            eventTimeVector.setSafe(i, eventBase + rowId);
            deviceIdVector.setSafe(i, utf8(DEVICE_PREFIX + zeroPad(rowId % 1_000_000L, 10)));
            regionVector.setSafe(i, utf8(REGIONS[(int) (rowId % REGIONS.length)]));
            scoreVector.setSafe(i, (int) (rowId % 10_000));
            payloadVector.setSafe(i, buildPayload(rowId, config.payloadBytes(), payloadSuffix));
        }
        userIdVector.setValueCount(batchRows);
        eventTimeVector.setValueCount(batchRows);
        deviceIdVector.setValueCount(batchRows);
        regionVector.setValueCount(batchRows);
        scoreVector.setValueCount(batchRows);
        payloadVector.setValueCount(batchRows);
    }

    private static ConversionStats convertToHFile(RunConfig config) {
        long start = System.nanoTime();
        NativeLibLoader.load(config.nativeLib());
        HFileSDK sdk = new HFileSDK();
        int configureRc = sdk.configure(buildConfigJson(config));
        if (configureRc != HFileSDK.OK) {
            long elapsed = System.nanoTime() - start;
            return new ConversionStats(
                elapsed,
                sdk.getLastResult(),
                configureRc,
                false,
                "configure failed: " + sdk.getLastResult()
            );
        }
        int rc = sdk.convert(
            config.arrowPath().toString(),
            config.hfilePath().toString(),
            config.tableName(),
            config.rowKeyRule()
        );
        String sdkResultJson = sdk.getLastResult();
        long elapsed = System.nanoTime() - start;
        return new ConversionStats(
            elapsed,
            sdkResultJson,
            rc,
            rc == HFileSDK.OK,
            rc == HFileSDK.OK ? "" : "convert failed: " + sdkResultJson
        );
    }

    private static BulkLoadStats runBulkLoad(RunConfig config) throws IOException, InterruptedException {
        List<String> command = List.of(
            config.hbaseBin(),
            "org.apache.hadoop.hbase.tool.BulkLoadHFilesTool",
            config.bulkloadDir().toString(),
            config.tableName()
        );
        CommandExecResult exec = runExternalCommand(command);
        return new BulkLoadStats(
            exec.elapsedNanos(),
            false,
            String.join(" ", command),
            exec.exitCode(),
            exec.output(),
            exec.exitCode() == 0,
            exec.exitCode() == 0 ? "" : "BulkLoadHFilesTool exited with code " + exec.exitCode()
        );
    }

    private static VerifyStats runVerify(RunConfig config, long expectedRows) throws IOException, InterruptedException {
        if (!Files.exists(config.verifyJar())) {
            return new VerifyStats(
                0L,
                false,
                "",
                -1,
                "",
                false,
                "verify jar not found: " + config.verifyJar()
            );
        }
        List<String> command = List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-jar",
            config.verifyJar().toString(),
            "--zookeeper",
            config.zookeeper(),
            "--table",
            config.tableName(),
            "--row-count",
            Long.toString(expectedRows)
        );
        CommandExecResult exec = runExternalCommand(command);
        return new VerifyStats(
            exec.elapsedNanos(),
            false,
            String.join(" ", command),
            exec.exitCode(),
            exec.output(),
            exec.exitCode() == 0,
            exec.exitCode() == 0 ? "" : "BulkLoad verify exited with code " + exec.exitCode()
        );
    }

    private static CommandExecResult runExternalCommand(List<String> command) throws IOException, InterruptedException {
        long start = System.nanoTime();
        Process process;
        try {
            process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        } catch (IOException e) {
            throw new IOException("cannot start command: " + String.join(" ", command), e);
        }

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                output.append(line).append(System.lineSeparator());
            }
        }

        int exitCode = process.waitFor();
        return new CommandExecResult(System.nanoTime() - start, exitCode, output.toString());
    }

    private static void writeIterationReport(PerfResult result) throws IOException {
        Files.writeString(result.config().reportJson(), result.toJson(), StandardCharsets.UTF_8);
    }

    private static void writeAggregateReport(AggregateReport report) throws IOException {
        Files.writeString(report.config().baseReportJson(), report.toJson(), StandardCharsets.UTF_8);
    }

    private static void printIterationSummary(PerfResult result, int iteration, int totalIterations) {
        System.out.println();
        System.out.println("Bulk load performance summary");
        System.out.println("  iteration             : " + iteration + "/" + totalIterations);
        System.out.println("  table                 : " + result.config().tableName());
        System.out.println("  arrow_file            : " + result.config().arrowPath());
        System.out.println("  hfile_path            : " + result.config().hfilePath());
        System.out.println("  report_json           : " + result.config().reportJson());
        System.out.println("  rows                  : " + result.rows());
        System.out.println("  batches               : " + result.batches());
        System.out.println("  arrow_size_mb         : " + formatMiB(result.arrowSizeBytes()));
        System.out.println("  hfile_size_mb         : " + formatMiB(result.hfileSizeBytes()));
        System.out.println("  status                : " + (result.success() ? "SUCCESS" : "FAILED"));
        System.out.println("  generate_ms           : " + nanosToMillis(result.generateNanos()));
        System.out.println("  convert_ms            : " + nanosToMillis(result.convertNanos()));
        System.out.println("  bulkload_ms           : " + (result.bulkLoadSkipped() ? "SKIPPED" : nanosToMillis(result.bulkLoadNanos())));
        System.out.println("  verify_ms             : " + (result.verifySkipped() ? "SKIPPED" : nanosToMillis(result.verifyNanos())));
        System.out.println("  total_ms              : " + nanosToMillis(result.totalNanos()));
        System.out.println("  generate_mb_per_sec   : " + formatRate(result.arrowSizeBytes(), result.generateNanos()));
        System.out.println("  convert_mb_per_sec    : " + formatRate(result.arrowSizeBytes(), result.convertNanos()));
        System.out.println("  bulkload_mb_per_sec   : " + (result.bulkLoadSkipped() ? "SKIPPED" : formatRate(result.hfileSizeBytes(), result.bulkLoadNanos())));
        System.out.println("  end_to_end_mb_per_sec : " + formatRate(result.arrowSizeBytes(), result.totalNanos()));
        if (!result.bulkLoadSkipped()) {
            System.out.println("  bulkload_command      : " + result.bulkLoadCommand());
        }
        if (!result.verifySkipped()) {
            System.out.println("  verify_command        : " + result.verifyCommand());
        }
        if (!result.success()) {
            System.out.println("  error_message         : " + result.errorMessage());
        }
    }

    private static void printAggregateSummary(AggregateReport report) {
        System.out.println();
        System.out.println("Bulk load aggregate summary");
        System.out.println("  table                 : " + report.config().tableName());
        System.out.println("  iterations            : " + report.iterationResults().size());
        System.out.println("  success               : " + report.success());
        System.out.println("  report_json           : " + report.config().baseReportJson());
        System.out.println("  total_ms              : " + nanosToMillis(report.totalNanos()));
        if (!report.iterationResults().isEmpty()) {
            System.out.println("  avg_generate_ms       : " + formatDouble(report.averageGenerateMs()));
            System.out.println("  avg_convert_ms        : " + formatDouble(report.averageConvertMs()));
            System.out.println("  avg_bulkload_ms       : " + formatOptionalDouble(report.averageBulkLoadMs(), report.anyBulkLoadExecuted()));
            System.out.println("  avg_verify_ms         : " + formatOptionalDouble(report.averageVerifyMs(), report.anyVerifyExecuted()));
            System.out.println("  avg_e2e_ms            : " + formatDouble(report.averageTotalMs()));
            System.out.println("  avg_e2e_mb_per_sec    : " + formatDouble(report.averageEndToEndMbps()));
            System.out.println("  min_e2e_mb_per_sec    : " + formatDouble(report.minEndToEndMbps()));
            System.out.println("  max_e2e_mb_per_sec    : " + formatDouble(report.maxEndToEndMbps()));
        }
        if (!report.success()) {
            System.out.println("  error_message         : " + report.errorMessage());
        }
    }

    private static void cleanupGeneratedArtifacts(RunConfig config) throws IOException {
        if (config.iterations() > 1) {
            for (int i = 1; i <= config.iterations(); ++i) {
                RunConfig iterationConfig = config.forIteration(i);
                deleteRecursively(iterationConfig.workDir());
                deleteRecursively(iterationConfig.bulkloadDir());
            }
            return;
        }
        if (Files.exists(config.arrowPath())) {
            Files.delete(config.arrowPath());
        }
        if (Files.exists(config.hfilePath())) {
            Files.delete(config.hfilePath());
        }
        deleteEmptyParents(config.hfilePath().getParent(), config.bulkloadDir());
    }

    private static void deleteEmptyParents(Path path, Path stopAt) throws IOException {
        Path current = path;
        while (current != null && !current.equals(stopAt.getParent())) {
            if (Files.isDirectory(current) && isDirectoryEmpty(current)) {
                Files.deleteIfExists(current);
            } else {
                break;
            }
            if (current.equals(stopAt)) {
                break;
            }
            current = current.getParent();
        }
    }

    private static boolean isDirectoryEmpty(Path dir) throws IOException {
        try (var stream = Files.list(dir)) {
            return stream.findAny().isEmpty();
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException ioException) {
                throw ioException;
            }
            throw e;
        }
    }

    private static String buildConfigJson(RunConfig config) {
        return "{"
            + "\"compression\":\"" + jsonEscape(config.compression()) + "\","
            + "\"block_size\":" + config.blockSize() + ","
            + "\"column_family\":\"" + jsonEscape(config.columnFamily()) + "\","
            + "\"data_block_encoding\":\"" + jsonEscape(config.encoding()) + "\","
            + "\"bloom_type\":\"" + jsonEscape(config.bloom()) + "\","
            + "\"fsync_policy\":\"" + jsonEscape(config.fsyncPolicy()) + "\","
            + "\"error_policy\":\"" + jsonEscape(config.errorPolicy()) + "\""
            + "}";
    }

    private static boolean ensureJavaNioOpen(String[] args) throws Exception {
        if (Buffer.class.getModule().isOpen("java.nio", BulkLoadPerfRunner.class.getModule())) {
            return false;
        }
        Path self = Path.of(BulkLoadPerfRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        if (self.toString().endsWith(".jar")) {
            command.add("-jar");
            command.add(self.toString());
        } else {
            command.add("-cp");
            command.add(System.getProperty("java.class.path"));
            command.add(BulkLoadPerfRunner.class.getName());
        }
        for (String arg : args) {
            command.add(arg);
        }
        Process process = new ProcessBuilder(command)
            .inheritIO()
            .start();
        int code = process.waitFor();
        System.exit(code);
        return true;
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp(
            "java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar",
            options
        );
    }

    private static String required(CommandLine cmd, String option) {
        String value = cmd.getOptionValue(option);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("--" + option + " is required");
        }
        return value;
    }

    private static int parsePositiveInt(String raw, String optionName) {
        int value;
        try {
            value = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + optionName + " must be a positive integer");
        }
        if (value <= 0) {
            throw new IllegalArgumentException("--" + optionName + " must be greater than 0");
        }
        return value;
    }

    private static String zeroPad(long value, int width) {
        return String.format(Locale.ROOT, "%0" + width + "d", value);
    }

    private static byte[] buildPayload(long rowId, int payloadBytes, byte[] suffix) {
        byte[] prefix = utf8("payload-" + Long.toUnsignedString(rowId, 36) + "-");
        if (payloadBytes <= prefix.length) {
            byte[] out = new byte[payloadBytes];
            System.arraycopy(prefix, 0, out, 0, payloadBytes);
            return out;
        }
        byte[] out = new byte[payloadBytes];
        System.arraycopy(prefix, 0, out, 0, prefix.length);
        System.arraycopy(suffix, 0, out, prefix.length, payloadBytes - prefix.length);
        return out;
    }

    private static byte[] buildPayloadSuffix(int payloadBytes) {
        byte[] out = new byte[payloadBytes];
        byte[] seed = utf8("abcdefghijklmnopqrstuvwxyz0123456789");
        for (int i = 0; i < out.length; ++i) {
            out[i] = seed[i % seed.length];
        }
        return out;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String jsonEscape(String value) {
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"");
    }

    private static String formatMiB(long bytes) {
        return String.format(Locale.ROOT, "%.2f", bytes / 1024.0 / 1024.0);
    }

    private static String formatRate(long bytes, long nanos) {
        if (nanos <= 0L) {
            return "INF";
        }
        double seconds = nanos / 1_000_000_000.0;
        double mibPerSec = bytes / 1024.0 / 1024.0 / seconds;
        return String.format(Locale.ROOT, "%.2f", mibPerSec);
    }

    private static long nanosToMillis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static String messageOf(Throwable throwable) {
        String message = throwable.getMessage();
        return message == null || message.isBlank() ? throwable.getClass().getSimpleName() : message;
    }

    private static RuntimeException propagate(Throwable throwable) {
        if (throwable instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new RuntimeException(throwable);
    }

    private static TableCheckStats checkTableExists(RunConfig config) throws IOException, InterruptedException {
        List<String> command = List.of(config.hbaseBin(), "shell", "-n");
        String script = "exists '" + config.tableName().replace("'", "\\'") + "'\nexit\n";
        CommandExecResult exec = runExternalCommand(command, script);
        String normalized = exec.output().toLowerCase(Locale.ROOT);
        boolean exists = normalized.contains("does exist") && !normalized.contains("does not exist");
        return new TableCheckStats(
            exec.elapsedNanos(),
            String.join(" ", command),
            exec.exitCode(),
            exec.output(),
            exec.exitCode() == 0 && exists,
            exec.exitCode() != 0
                ? "table existence check exited with code " + exec.exitCode()
                : exists ? "" : "target table does not exist or hbase shell output is unrecognized"
        );
    }

    private static CommandExecResult runExternalCommand(List<String> command, String stdin) throws IOException, InterruptedException {
        long start = System.nanoTime();
        Process process;
        try {
            process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        } catch (IOException e) {
            throw new IOException("cannot start command: " + String.join(" ", command), e);
        }

        var writer = process.getOutputStream();
        try {
            try {
                writer.write(stdin.getBytes(StandardCharsets.UTF_8));
                writer.flush();
            } catch (IOException ignored) {
            }
        } finally {
            try {
                writer.close();
            } catch (IOException ignored) {
            }
        }

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                output.append(line).append(System.lineSeparator());
            }
        }

        int exitCode = process.waitFor();
        return new CommandExecResult(System.nanoTime() - start, exitCode, output.toString());
    }

    private static String formatDouble(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    private static String formatOptionalDouble(double value, boolean enabled) {
        return enabled ? formatDouble(value) : "SKIPPED";
    }

    private record RunConfig(
        String tableName,
        String nativeLib,
        Path workDir,
        Path bulkloadDir,
        int iterations,
        int iterationIndex,
        Path arrowPath,
        Path hfilePath,
        Path reportJson,
        int targetSizeMb,
        int batchRows,
        int payloadBytes,
        int blockSize,
        String columnFamily,
        String rowKeyRule,
        String compression,
        String encoding,
        String bloom,
        String fsyncPolicy,
        String errorPolicy,
        String hbaseBin,
        boolean verifyBulkLoad,
        Path verifyJar,
        String zookeeper,
        boolean skipBulkLoad,
        boolean keepGeneratedFiles
    ) {
        private Path baseReportJson() {
            return reportJson;
        }

        private RunConfig forIteration(int iteration) {
            if (iterations <= 1) {
                return this;
            }
            String suffix = String.format(Locale.ROOT, "iter-%03d", iteration);
            Path iterationWorkDir = workDir.resolve(suffix);
            Path iterationBulkloadDir = bulkloadDir.resolve(suffix);
            Path iterationArrowPath = iterationWorkDir.resolve(tableName + ".arrow");
            Path iterationHfilePath = iterationBulkloadDir.resolve(columnFamily).resolve("part-00000.hfile");
            Path iterationReportPath = workDir.resolve(tableName + "-" + suffix + "-perf-report.json");
            return new RunConfig(
                tableName,
                nativeLib,
                iterationWorkDir,
                iterationBulkloadDir,
                iterations,
                iteration,
                iterationArrowPath,
                iterationHfilePath,
                iterationReportPath,
                targetSizeMb,
                batchRows,
                payloadBytes,
                blockSize,
                columnFamily,
                rowKeyRule,
                compression,
                encoding,
                bloom,
                fsyncPolicy,
                errorPolicy,
                hbaseBin,
                verifyBulkLoad,
                verifyJar,
                zookeeper,
                skipBulkLoad,
                keepGeneratedFiles
            );
        }
    }

    private record GenerationStats(long rows, int batches, long elapsedNanos) {
        private static GenerationStats empty() {
            return new GenerationStats(0L, 0, 0L);
        }
    }

    private record ConversionStats(long elapsedNanos, String sdkResultJson, int exitCode, boolean success, String errorMessage) {
        private static ConversionStats notStarted() {
            return new ConversionStats(0L, "", 0, true, "");
        }
    }

    private record BulkLoadStats(long elapsedNanos, boolean skipped, String command, int exitCode, String output, boolean success, String errorMessage) {
        private static BulkLoadStats skippedResult() {
            return new BulkLoadStats(0L, true, "", 0, "", true, "");
        }

        private static BulkLoadStats notStarted() {
            return new BulkLoadStats(0L, true, "", 0, "", true, "");
        }
    }

    private record VerifyStats(long elapsedNanos, boolean skipped, String command, int exitCode, String output, boolean success, String errorMessage) {
        private static VerifyStats skippedResult() {
            return new VerifyStats(0L, true, "", 0, "", true, "");
        }

        private static VerifyStats notStarted() {
            return new VerifyStats(0L, true, "", 0, "", true, "");
        }
    }

    private record TableCheckStats(long elapsedNanos, String command, int exitCode, String output, boolean success, String errorMessage) {}

    private record CommandExecResult(long elapsedNanos, int exitCode, String output) {}

    private record PerfResult(
        RunConfig config,
        boolean success,
        String errorMessage,
        long rows,
        int batches,
        long arrowSizeBytes,
        long hfileSizeBytes,
        long generateNanos,
        long convertNanos,
        long bulkLoadNanos,
        long verifyNanos,
        long totalNanos,
        boolean bulkLoadSkipped,
        String bulkLoadCommand,
        int bulkLoadExitCode,
        String bulkLoadOutput,
        boolean verifySkipped,
        String verifyCommand,
        int verifyExitCode,
        String verifyOutput,
        String sdkResultJson
    ) {
        String toJson() {
            return "{\n"
                + "  \"iteration_index\": " + config.iterationIndex() + ",\n"
                + "  \"success\": " + success + ",\n"
                + "  \"error_message\": \"" + jsonEscape(errorMessage) + "\",\n"
                + "  \"table_name\": \"" + jsonEscape(config.tableName()) + "\",\n"
                + "  \"column_family\": \"" + jsonEscape(config.columnFamily()) + "\",\n"
                + "  \"arrow_path\": \"" + jsonEscape(config.arrowPath().toString()) + "\",\n"
                + "  \"hfile_path\": \"" + jsonEscape(config.hfilePath().toString()) + "\",\n"
                + "  \"report_json\": \"" + jsonEscape(config.reportJson().toString()) + "\",\n"
                + "  \"target_size_mb\": " + config.targetSizeMb() + ",\n"
                + "  \"payload_bytes\": " + config.payloadBytes() + ",\n"
                + "  \"batch_rows\": " + config.batchRows() + ",\n"
                + "  \"rows\": " + rows + ",\n"
                + "  \"batches\": " + batches + ",\n"
                + "  \"arrow_size_bytes\": " + arrowSizeBytes + ",\n"
                + "  \"hfile_size_bytes\": " + hfileSizeBytes + ",\n"
                + "  \"generate_ms\": " + nanosToMillis(generateNanos) + ",\n"
                + "  \"convert_ms\": " + nanosToMillis(convertNanos) + ",\n"
                + "  \"bulkload_ms\": " + (bulkLoadSkipped ? 0 : nanosToMillis(bulkLoadNanos)) + ",\n"
                + "  \"verify_ms\": " + (verifySkipped ? 0 : nanosToMillis(verifyNanos)) + ",\n"
                + "  \"total_ms\": " + nanosToMillis(totalNanos) + ",\n"
                + "  \"generate_mb_per_sec\": " + formatRate(arrowSizeBytes, generateNanos) + ",\n"
                + "  \"convert_mb_per_sec\": " + formatRate(arrowSizeBytes, convertNanos) + ",\n"
                + "  \"bulkload_mb_per_sec\": " + (bulkLoadSkipped ? "\"SKIPPED\"" : formatRate(hfileSizeBytes, bulkLoadNanos)) + ",\n"
                + "  \"end_to_end_mb_per_sec\": " + formatRate(arrowSizeBytes, totalNanos) + ",\n"
                + "  \"bulkload_skipped\": " + bulkLoadSkipped + ",\n"
                + "  \"bulkload_exit_code\": " + bulkLoadExitCode + ",\n"
                + "  \"bulkload_command\": \"" + jsonEscape(bulkLoadCommand) + "\",\n"
                + "  \"bulkload_output\": \"" + jsonEscape(bulkLoadOutput) + "\",\n"
                + "  \"verify_skipped\": " + verifySkipped + ",\n"
                + "  \"verify_exit_code\": " + verifyExitCode + ",\n"
                + "  \"verify_command\": \"" + jsonEscape(verifyCommand) + "\",\n"
                + "  \"verify_output\": \"" + jsonEscape(verifyOutput) + "\",\n"
                + "  \"sdk_result\": " + (sdkResultJson == null || sdkResultJson.isBlank() ? "\"\"" : sdkResultJson) + "\n"
                + "}\n";
        }
    }

    private record AggregateReport(
        RunConfig config,
        boolean success,
        String errorMessage,
        long totalNanos,
        List<PerfResult> iterationResults
    ) {
        private static AggregateReport from(RunConfig config, List<PerfResult> iterationResults, long totalNanos, Throwable failure) {
            return new AggregateReport(
                config,
                failure == null && !iterationResults.isEmpty(),
                failure == null ? "" : messageOf(failure),
                totalNanos,
                List.copyOf(iterationResults)
            );
        }

        private double averageGenerateMs() { return averageMillis(iterationResults, PerfResult::generateNanos); }
        private double averageConvertMs() { return averageMillis(iterationResults, PerfResult::convertNanos); }
        private double averageBulkLoadMs() { return averageMillis(iterationResults, PerfResult::bulkLoadNanos, r -> !r.bulkLoadSkipped()); }
        private double averageVerifyMs() { return averageMillis(iterationResults, PerfResult::verifyNanos, r -> !r.verifySkipped()); }
        private double averageTotalMs() { return averageMillis(iterationResults, PerfResult::totalNanos); }
        private double averageEndToEndMbps() { return averageDouble(iterationResults, r -> mbPerSec(r.arrowSizeBytes(), r.totalNanos())); }
        private double minEndToEndMbps() { return extremumDouble(iterationResults, r -> mbPerSec(r.arrowSizeBytes(), r.totalNanos()), true); }
        private double maxEndToEndMbps() { return extremumDouble(iterationResults, r -> mbPerSec(r.arrowSizeBytes(), r.totalNanos()), false); }
        private boolean anyBulkLoadExecuted() { return iterationResults.stream().anyMatch(r -> !r.bulkLoadSkipped()); }
        private boolean anyVerifyExecuted() { return iterationResults.stream().anyMatch(r -> !r.verifySkipped()); }

        private String toJson() {
            StringBuilder builder = new StringBuilder();
            builder.append("{\n");
            builder.append("  \"success\": ").append(success).append(",\n");
            builder.append("  \"error_message\": \"").append(jsonEscape(errorMessage)).append("\",\n");
            builder.append("  \"table_name\": \"").append(jsonEscape(config.tableName())).append("\",\n");
            builder.append("  \"iterations\": ").append(iterationResults.size()).append(",\n");
            builder.append("  \"total_ms\": ").append(nanosToMillis(totalNanos)).append(",\n");
            builder.append("  \"aggregate\": {\n");
            builder.append("    \"avg_generate_ms\": ").append(formatDouble(averageGenerateMs())).append(",\n");
            builder.append("    \"avg_convert_ms\": ").append(formatDouble(averageConvertMs())).append(",\n");
            builder.append("    \"avg_bulkload_ms\": ").append(anyBulkLoadExecuted() ? formatDouble(averageBulkLoadMs()) : "\"SKIPPED\"").append(",\n");
            builder.append("    \"avg_verify_ms\": ").append(anyVerifyExecuted() ? formatDouble(averageVerifyMs()) : "\"SKIPPED\"").append(",\n");
            builder.append("    \"avg_e2e_ms\": ").append(formatDouble(averageTotalMs())).append(",\n");
            builder.append("    \"avg_e2e_mb_per_sec\": ").append(formatDouble(averageEndToEndMbps())).append(",\n");
            builder.append("    \"min_e2e_mb_per_sec\": ").append(formatDouble(minEndToEndMbps())).append(",\n");
            builder.append("    \"max_e2e_mb_per_sec\": ").append(formatDouble(maxEndToEndMbps())).append("\n");
            builder.append("  },\n");
            builder.append("  \"iteration_results\": [\n");
            for (int i = 0; i < iterationResults.size(); ++i) {
                builder.append(indentJson(iterationResults.get(i).toJson(), 4));
                if (i + 1 < iterationResults.size()) {
                    builder.append(",");
                }
                builder.append("\n");
            }
            builder.append("  ]\n");
            builder.append("}\n");
            return builder.toString();
        }
    }

    private static double averageMillis(List<PerfResult> results, java.util.function.ToLongFunction<PerfResult> extractor) {
        return averageMillis(results, extractor, r -> true);
    }

    private static double averageMillis(List<PerfResult> results,
                                        java.util.function.ToLongFunction<PerfResult> extractor,
                                        java.util.function.Predicate<PerfResult> predicate) {
        if (results.isEmpty()) {
            return 0.0;
        }
        long sum = 0L;
        int count = 0;
        for (PerfResult result : results) {
            if (predicate.test(result)) {
                sum += extractor.applyAsLong(result);
                count++;
            }
        }
        return count == 0 ? 0.0 : (sum / 1_000_000.0) / count;
    }

    private static double averageDouble(List<PerfResult> results, java.util.function.ToDoubleFunction<PerfResult> extractor) {
        if (results.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (PerfResult result : results) {
            sum += extractor.applyAsDouble(result);
        }
        return sum / results.size();
    }

    private static double extremumDouble(List<PerfResult> results,
                                         java.util.function.ToDoubleFunction<PerfResult> extractor,
                                         boolean min) {
        if (results.isEmpty()) {
            return 0.0;
        }
        double value = extractor.applyAsDouble(results.get(0));
        for (int i = 1; i < results.size(); ++i) {
            double current = extractor.applyAsDouble(results.get(i));
            value = min ? Math.min(value, current) : Math.max(value, current);
        }
        return value;
    }

    private static double mbPerSec(long bytes, long nanos) {
        if (nanos <= 0L) {
            return 0.0;
        }
        return bytes / 1024.0 / 1024.0 / (nanos / 1_000_000_000.0);
    }

    private static String indentJson(String json, int spaces) {
        String indent = " ".repeat(spaces);
        return indent + json.replace("\n", "\n" + indent).stripTrailing();
    }
}
