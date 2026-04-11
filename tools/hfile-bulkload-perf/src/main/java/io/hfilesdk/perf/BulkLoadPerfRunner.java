package io.hfilesdk.perf;

import io.hfilesdk.converter.AdaptiveBatchConverter;
import io.hfilesdk.converter.ArrowToHFileConverter;
import io.hfilesdk.converter.BatchConvertOptions;
import io.hfilesdk.converter.BatchConvertResult;
import io.hfilesdk.converter.ConvertOptions;
import io.hfilesdk.converter.ConvertResult;
import io.hfilesdk.converter.javaimpl.ArrowToHFileJavaConverter;
import io.hfilesdk.converter.javaimpl.JavaConvertOptions;
import io.hfilesdk.converter.javaimpl.JavaConvertResult;
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

import java.nio.Buffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class BulkLoadPerfRunner {

    private static final int FIXED_ITERATIONS = 3;
    private static final int DEFAULT_BATCH_ROWS = 8192;
    private static final int DEFAULT_PAYLOAD_BYTES = 768;
    private static final int DEFAULT_MERGE_THRESHOLD_MB = 100;
    private static final int DEFAULT_TRIGGER_SIZE_MB = 512;
    private static final int DEFAULT_TRIGGER_COUNT = 500;
    private static final int DEFAULT_TRIGGER_INTERVAL_SECONDS = 180;
    private static final String DEFAULT_RULE = "USER_ID,0,false,0";
    private static final String DEFAULT_CF = "cf";
    private static final String DEFAULT_COMPRESSION = "GZ";
    private static final String DEFAULT_ENCODING = "NONE";
    private static final String DEFAULT_BLOOM = "row";
    private static final String DEFAULT_ERROR_POLICY = "skip_row";
    private static final int DEFAULT_BLOCK_SIZE = 65536;
    private static final String DEFAULT_WORK_DIR = "/tmp/hfilesdk-bulkload-perf";
    private static final int FIXED_ARROW_OVERHEAD_BYTES = 256;
    private static final int FIXED_ROW_OVERHEAD_BYTES = 80;
    private static final String USER_ID_PREFIX = "user-";
    private static final String DEVICE_PREFIX = "device-";
    private static final String[] REGIONS = {"cn-north", "cn-east", "cn-south", "sg", "us-west"};
    private static final String IMPL_JNI = "arrow-to-hfile";
    private static final String IMPL_JAVA = "arrow-to-hfile-java";
    private static final String STRATEGY_DIRECT = "DIRECT-CONVERT";
    private static final String STRATEGY_PARALLEL = "PARALLEL-CONVERT";
    private static final String STRATEGY_MERGE = "MERGE-THEN-CONVERT";

    private BulkLoadPerfRunner() {}

    public static void main(String[] args) throws Exception {
        if (ensureRequiredOpens(args)) {
            return;
        }

        Options options = buildOptions();
        CommandLine commandLine;
        try {
            commandLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            printHelp(options);
            System.exit(1);
            return;
        }

        if (commandLine.hasOption("help")) {
            printHelp(options);
            return;
        }

        RunConfig config;
        try {
            config = parseConfig(commandLine);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            printHelp(options);
            System.exit(1);
            return;
        }

        List<ScenarioConfig> scenarios = buildScenarioMatrix(config.scenarioFilter());
        if (scenarios.isEmpty()) {
            throw new IllegalArgumentException("没有命中任何场景，请检查 --scenario-filter");
        }

        Files.createDirectories(config.workDir());
        if (config.reportJson().getParent() != null) {
            Files.createDirectories(config.reportJson().getParent());
        }

        long totalStart = System.nanoTime();
        List<ScenarioReport> scenarioReports = new ArrayList<>();
        for (ScenarioConfig scenario : scenarios) {
            ScenarioReport scenarioReport = runScenario(config, scenario);
            scenarioReports.add(scenarioReport);
            printScenarioSummary(scenarioReport);
        }

        ComparisonReport report = new ComparisonReport(config, scenarioReports, System.nanoTime() - totalStart);
        Files.writeString(config.reportJson(), report.toJson(), StandardCharsets.UTF_8);
        printAggregateSummary(report);
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("native-lib").hasArg().argName("PATH").desc("JNI 实现所需 libhfilesdk 动态库路径").build());
        options.addOption(Option.builder().longOpt("table").hasArg().argName("NAME").desc("表名，默认 hfilesdk_perf").build());
        options.addOption(Option.builder().longOpt("work-dir").hasArg().argName("PATH").desc("工作目录").build());
        options.addOption(Option.builder().longOpt("report-json").hasArg().argName("PATH").desc("结构化结果输出路径").build());
        options.addOption(Option.builder().longOpt("implementations").hasArg().argName("LIST").desc("实现列表：arrow-to-hfile,arrow-to-hfile-java").build());
        options.addOption(Option.builder().longOpt("scenario-filter").hasArg().argName("TEXT").desc("按场景 ID 过滤执行").build());
        options.addOption(Option.builder().longOpt("iterations").hasArg().argName("N").desc("固定为 3").build());
        options.addOption(Option.builder().longOpt("parallelism").hasArg().argName("N").desc("JNI 大文件并行转换线程数").build());
        options.addOption(Option.builder().longOpt("merge-threshold").hasArg().argName("MB").desc("JNI 小文件合并阈值").build());
        options.addOption(Option.builder().longOpt("trigger-size").hasArg().argName("MB").desc("JNI 合并攒批大小阈值").build());
        options.addOption(Option.builder().longOpt("trigger-count").hasArg().argName("N").desc("JNI 合并攒批文件数阈值").build());
        options.addOption(Option.builder().longOpt("trigger-interval").hasArg().argName("SEC").desc("JNI 合并时间阈值").build());
        options.addOption(Option.builder().longOpt("batch-rows").hasArg().argName("N").desc("每个 Arrow batch 行数").build());
        options.addOption(Option.builder().longOpt("payload-bytes").hasArg().argName("BYTES").desc("每行 PAYLOAD 字节数").build());
        options.addOption(Option.builder().longOpt("cf").hasArg().argName("CF").desc("列族名").build());
        options.addOption(Option.builder().longOpt("rule").hasArg().argName("RULE").desc("rowKeyRule").build());
        options.addOption(Option.builder().longOpt("compression").hasArg().argName("ALG").desc("压缩算法").build());
        options.addOption(Option.builder().longOpt("encoding").hasArg().argName("ENC").desc("Data Block Encoding").build());
        options.addOption(Option.builder().longOpt("bloom").hasArg().argName("TYPE").desc("Bloom 类型").build());
        options.addOption(Option.builder().longOpt("error-policy").hasArg().argName("POLICY").desc("JNI 错误策略").build());
        options.addOption(Option.builder().longOpt("block-size").hasArg().argName("BYTES").desc("HFile block size").build());
        options.addOption(Option.builder().longOpt("keep-generated-files").desc("保留中间产物").build());
        options.addOption(Option.builder().longOpt("help").desc("显示帮助").build());
        return options;
    }

    private static RunConfig parseConfig(CommandLine commandLine) {
        int iterations = parsePositiveInt(commandLine.getOptionValue("iterations", Integer.toString(FIXED_ITERATIONS)), "iterations");
        if (iterations != FIXED_ITERATIONS) {
            throw new IllegalArgumentException("当前规范要求 --iterations 固定为 3");
        }

        Path workDir = Path.of(commandLine.getOptionValue("work-dir", DEFAULT_WORK_DIR)).toAbsolutePath().normalize();
        Path reportJson = Path.of(commandLine.getOptionValue("report-json", workDir.resolve("perf-matrix-report.json").toString()))
            .toAbsolutePath()
            .normalize();
        List<String> implementations = parseImplementations(commandLine.getOptionValue("implementations", IMPL_JNI + "," + IMPL_JAVA));
        String nativeLib = commandLine.getOptionValue("native-lib", "").trim();
        if (implementations.contains(IMPL_JNI) && nativeLib.isBlank()) {
            throw new IllegalArgumentException("包含 arrow-to-hfile 时必须提供 --native-lib");
        }
        return new RunConfig(
            nativeLib,
            commandLine.getOptionValue("table", "hfilesdk_perf"),
            workDir,
            reportJson,
            implementations,
            commandLine.getOptionValue("scenario-filter", "").trim(),
            parsePositiveInt(commandLine.getOptionValue("parallelism", Integer.toString(Runtime.getRuntime().availableProcessors())), "parallelism"),
            parsePositiveInt(commandLine.getOptionValue("merge-threshold", Integer.toString(DEFAULT_MERGE_THRESHOLD_MB)), "merge-threshold"),
            parsePositiveInt(commandLine.getOptionValue("trigger-size", Integer.toString(DEFAULT_TRIGGER_SIZE_MB)), "trigger-size"),
            parsePositiveInt(commandLine.getOptionValue("trigger-count", Integer.toString(DEFAULT_TRIGGER_COUNT)), "trigger-count"),
            parsePositiveInt(commandLine.getOptionValue("trigger-interval", Integer.toString(DEFAULT_TRIGGER_INTERVAL_SECONDS)), "trigger-interval"),
            parsePositiveInt(commandLine.getOptionValue("batch-rows", Integer.toString(DEFAULT_BATCH_ROWS)), "batch-rows"),
            parsePositiveInt(commandLine.getOptionValue("payload-bytes", Integer.toString(DEFAULT_PAYLOAD_BYTES)), "payload-bytes"),
            commandLine.getOptionValue("cf", DEFAULT_CF),
            commandLine.getOptionValue("rule", DEFAULT_RULE),
            commandLine.getOptionValue("compression", DEFAULT_COMPRESSION),
            commandLine.getOptionValue("encoding", DEFAULT_ENCODING),
            commandLine.getOptionValue("bloom", DEFAULT_BLOOM),
            commandLine.getOptionValue("error-policy", DEFAULT_ERROR_POLICY),
            parsePositiveInt(commandLine.getOptionValue("block-size", Integer.toString(DEFAULT_BLOCK_SIZE)), "block-size"),
            commandLine.hasOption("keep-generated-files")
        );
    }

    private static List<String> parseImplementations(String raw) {
        List<String> implementations = new ArrayList<>();
        for (String token : raw.split(",")) {
            String implementation = token.trim();
            if (implementation.isEmpty()) {
                continue;
            }
            if (!implementation.equals(IMPL_JNI) && !implementation.equals(IMPL_JAVA)) {
                throw new IllegalArgumentException("不支持的实现类型: " + implementation);
            }
            if (!implementations.contains(implementation)) {
                implementations.add(implementation);
            }
        }
        if (implementations.isEmpty()) {
            throw new IllegalArgumentException("至少需要一个实现类型");
        }
        return List.copyOf(implementations);
    }

    private static List<ScenarioConfig> buildScenarioMatrix(String scenarioFilter) {
        List<ScenarioConfig> scenarios = List.of(
            new ScenarioConfig("single-001mb", "single_file", 1, 1),
            new ScenarioConfig("single-010mb", "single_file", 1, 10),
            new ScenarioConfig("single-100mb", "single_file", 1, 100),
            new ScenarioConfig("single-500mb", "single_file", 1, 500),
            new ScenarioConfig("directory-100x001mb", "directory", 100, 1),
            new ScenarioConfig("directory-100x010mb", "directory", 100, 10)
        );
        if (scenarioFilter == null || scenarioFilter.isBlank()) {
            return scenarios;
        }
        String normalized = scenarioFilter.toLowerCase(Locale.ROOT);
        return scenarios.stream()
            .filter(scenario -> scenario.scenarioId().toLowerCase(Locale.ROOT).contains(normalized))
            .toList();
    }

    private static ScenarioReport runScenario(RunConfig config, ScenarioConfig scenario) throws Exception {
        Path scenarioDir = config.workDir().resolve(scenario.scenarioId());
        Path inputDir = scenarioDir.resolve("input");
        Path outputDir = scenarioDir.resolve("output");
        deleteRecursively(outputDir);
        if (!config.keepGeneratedFiles()) {
            deleteRecursively(inputDir);
        }
        Files.createDirectories(inputDir);
        Files.createDirectories(outputDir);

        GenerationStats generationStats = generateArrowFiles(config, scenario, inputDir);
        List<ImplementationSummary> implementations = new ArrayList<>();
        for (String implementation : config.implementations()) {
            implementations.add(runImplementation(config, scenario, generationStats, outputDir.resolve(implementation), implementation));
        }
        return new ScenarioReport(scenario, generationStats, List.copyOf(implementations));
    }

    private static GenerationStats generateArrowFiles(RunConfig config, ScenarioConfig scenario, Path inputDir) throws Exception {
        long start = System.nanoTime();
        byte[] payloadSuffix = buildPayloadSuffix(config.payloadBytes());
        List<Path> arrowFiles = new ArrayList<>();
        long totalRows = 0L;
        int totalBatches = 0;
        long totalArrowSizeBytes = 0L;
        long nextRowOffset = 0L;

        for (int index = 0; index < scenario.arrowFileCount(); index++) {
            Path arrowFile = inputDir.resolve(String.format(Locale.ROOT, "%s-part-%03d.arrow", scenario.scenarioId(), index + 1));
            SingleFileGenerationStats fileStats = writeArrowFile(config, scenario.targetSizeMb(), arrowFile, nextRowOffset, payloadSuffix);
            arrowFiles.add(arrowFile);
            totalRows += fileStats.rows();
            totalBatches += fileStats.batches();
            totalArrowSizeBytes += fileStats.sizeBytes();
            nextRowOffset += fileStats.rows();
        }

        return new GenerationStats(
            totalRows,
            totalBatches,
            totalArrowSizeBytes,
            List.copyOf(arrowFiles),
            System.nanoTime() - start
        );
    }

    private static SingleFileGenerationStats writeArrowFile(RunConfig config,
                                                            int targetSizeMb,
                                                            Path arrowFile,
                                                            long rowOffset,
                                                            byte[] payloadSuffix) throws Exception {
        long targetBytes = targetSizeMb * 1024L * 1024L;
        long rows = 0L;
        int batches = 0;
        long eventBase = 1_775_000_000L;
        int estimatedRowBytes = Math.max(1, config.payloadBytes() + FIXED_ROW_OVERHEAD_BYTES);

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
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(Files.newOutputStream(arrowFile)))) {

            allocateVectors(config, userIdVector, eventTimeVector, deviceIdVector, regionVector, scoreVector, payloadVector);
            writer.start();
            while (!Files.exists(arrowFile) || Files.size(arrowFile) < targetBytes) {
                long currentSize = Files.exists(arrowFile) ? Files.size(arrowFile) : 0L;
                long remainingBytes = Math.max(estimatedRowBytes, targetBytes - currentSize - FIXED_ARROW_OVERHEAD_BYTES);
                int rowsThisBatch = (int) Math.min(config.batchRows(), Math.max(1L, remainingBytes / estimatedRowBytes));
                fillBatch(config, rowsThisBatch, rowOffset + rows, payloadSuffix, eventBase, userIdVector, eventTimeVector, deviceIdVector, regionVector, scoreVector, payloadVector);
                root.setRowCount(rowsThisBatch);
                writer.writeBatch();
                rows += rowsThisBatch;
                batches++;
            }
            writer.end();
        }

        return new SingleFileGenerationStats(rows, batches, Files.size(arrowFile));
    }

    private static ImplementationSummary runImplementation(RunConfig config,
                                                           ScenarioConfig scenario,
                                                           GenerationStats generationStats,
                                                           Path implementationDir,
                                                           String implementation) throws Exception {
        deleteRecursively(implementationDir);
        Files.createDirectories(implementationDir);
        List<IterationResult> iterationResults = new ArrayList<>();
        for (int iteration = 1; iteration <= FIXED_ITERATIONS; iteration++) {
            Path iterationDir = implementationDir.resolve(String.format(Locale.ROOT, "iter-%03d", iteration));
            deleteRecursively(iterationDir);
            Files.createDirectories(iterationDir);
            IterationResult result = implementation.equals(IMPL_JNI)
                ? runJniIteration(config, generationStats, iterationDir, iteration)
                : runJavaIteration(config, generationStats, iterationDir, iteration);
            iterationResults.add(result);
            if (!result.success()) {
                throw new IllegalStateException("场景 " + scenario.scenarioId() + " / " + implementation + " 失败: " + result.errorMessage());
            }
        }
        return new ImplementationSummary(implementation, List.copyOf(iterationResults));
    }

    private static IterationResult runJniIteration(RunConfig config,
                                                   GenerationStats generationStats,
                                                   Path iterationDir,
                                                   int iterationIndex) throws Exception {
        Path hfileDir = iterationDir.resolve(config.columnFamily());
        Path mergeTmpDir = iterationDir.resolve("merge-tmp");
        Files.createDirectories(hfileDir);
        Files.createDirectories(mergeTmpDir);

        if (generationStats.arrowFiles().size() == 1) {
            return runJniSingleFileIteration(config, generationStats.arrowFiles().getFirst(), hfileDir, iterationIndex);
        }

        long start = System.nanoTime();
        String strategy = decideStrategy(generationStats.arrowFiles(), config.mergeThresholdMb());
        BatchConvertOptions options = BatchConvertOptions.builder()
            .arrowFiles(generationStats.arrowFiles())
            .hfileDir(hfileDir)
            .tableName(config.tableName())
            .rowKeyRule(config.rowKeyRule())
            .columnFamily(config.columnFamily())
            .compression(config.compression())
            .dataBlockEncoding(config.encoding())
            .bloomType(config.bloom())
            .errorPolicy(config.errorPolicy())
            .blockSize(config.blockSize())
            .parallelism(config.parallelism())
            .nativeLibPath(config.nativeLib())
            .build();
        AdaptiveBatchConverter.Policy policy = AdaptiveBatchConverter.Policy.builder()
            .mergeThresholdMib(config.mergeThresholdMb())
            .triggerSizeMib(config.triggerSizeMb())
            .triggerCount(config.triggerCount())
            .triggerIntervalSeconds(config.triggerIntervalSeconds())
            .mergeTmpDir(mergeTmpDir)
            .build();
        BatchConvertResult batchResult = new AdaptiveBatchConverter().convertAll(options, policy);
        return new IterationResult(
            iterationIndex,
            batchResult.isFullSuccess(),
            batchResult.isFullSuccess() ? "" : "failed outputs: " + batchResult.failed.stream().map(path -> path.getFileName().toString()).sorted().toList(),
            strategy,
            System.nanoTime() - start,
            batchResult.totalHfileSizeBytes,
            batchResult.totalKvWritten,
            batchResult.results.size(),
            batchConvertResultToJson(strategy, batchResult)
        );
    }

    private static IterationResult runJniSingleFileIteration(RunConfig config,
                                                             Path arrowFile,
                                                             Path hfileDir,
                                                             int iterationIndex) throws Exception {
        String fileName = arrowFile.getFileName().toString();
        String hfileName = fileName.endsWith(".arrow") ? fileName.substring(0, fileName.length() - 6) + ".hfile" : fileName + ".hfile";
        Path hfilePath = hfileDir.resolve(hfileName);
        long start = System.nanoTime();
        ConvertResult result = new ArrowToHFileConverter(config.nativeLib()).convert(
            ConvertOptions.builder()
                .arrowPath(arrowFile.toString())
                .hfilePath(hfilePath.toString())
                .tableName(config.tableName())
                .rowKeyRule(config.rowKeyRule())
                .columnFamily(config.columnFamily())
                .compression(config.compression())
                .dataBlockEncoding(config.encoding())
                .bloomType(config.bloom())
                .errorPolicy(config.errorPolicy())
                .blockSize(config.blockSize())
                .nativeLibPath(config.nativeLib())
                .build()
        );
        String detailJson = "{"
            + "\"source\":\"" + jsonEscape(arrowFile.toString()) + "\","
            + "\"error_code\":" + result.errorCode + ","
            + "\"error_message\":\"" + jsonEscape(result.errorMessage) + "\","
            + "\"arrow_rows_read\":" + result.arrowRowsRead + ","
            + "\"kv_written_count\":" + result.kvWrittenCount + ","
            + "\"hfile_size_bytes\":" + result.hfileSizeBytes + ","
            + "\"elapsed_ms\":" + result.elapsedMs
            + "}";
        return new IterationResult(
            iterationIndex,
            result.isSuccess(),
            result.isSuccess() ? "" : result.errorMessage,
            STRATEGY_DIRECT,
            System.nanoTime() - start,
            result.hfileSizeBytes,
            result.kvWrittenCount,
            result.isSuccess() ? 1 : 0,
            detailJson
        );
    }

    private static IterationResult runJavaIteration(RunConfig config,
                                                    GenerationStats generationStats,
                                                    Path iterationDir,
                                                    int iterationIndex) throws Exception {
        Path hfileDir = iterationDir.resolve(config.columnFamily());
        Files.createDirectories(hfileDir);
        ArrowToHFileJavaConverter converter = new ArrowToHFileJavaConverter();
        long start = System.nanoTime();
        long totalSize = 0L;
        long totalKvWritten = 0L;
        List<String> resultJsons = new ArrayList<>();

        for (Path arrowFile : generationStats.arrowFiles()) {
            String fileName = arrowFile.getFileName().toString();
            String hfileName = fileName.endsWith(".arrow") ? fileName.substring(0, fileName.length() - 6) + ".hfile" : fileName + ".hfile";
            Path hfilePath = hfileDir.resolve(hfileName);
            JavaConvertResult result = converter.convert(
                JavaConvertOptions.builder()
                    .arrowPath(arrowFile.toString())
                    .hfilePath(hfilePath.toString())
                    .tableName(config.tableName())
                    .rowKeyRule(config.rowKeyRule())
                    .columnFamily(config.columnFamily())
                    .compression(config.compression())
                    .dataBlockEncoding(config.encoding())
                    .bloomType(config.bloom())
                    .blockSize(config.blockSize())
                    .build()
            );
            resultJsons.add(result.toJson());
            if (!result.isSuccess()) {
                return new IterationResult(
                    iterationIndex,
                    false,
                    result.errorMessage,
                    STRATEGY_DIRECT,
                    System.nanoTime() - start,
                    totalSize,
                    totalKvWritten,
                    resultJsons.size(),
                    "[" + String.join(",", resultJsons) + "]"
                );
            }
            totalSize += result.hfileSizeBytes;
            totalKvWritten += result.kvWrittenCount;
        }

        return new IterationResult(
            iterationIndex,
            true,
            "",
            STRATEGY_DIRECT,
            System.nanoTime() - start,
            totalSize,
            totalKvWritten,
            generationStats.arrowFiles().size(),
            "[" + String.join(",", resultJsons) + "]"
        );
    }

    private static void printScenarioSummary(ScenarioReport report) {
        System.out.println();
        System.out.println("Scenario: " + report.scenario().scenarioId());
        System.out.println("  input_mode          : " + report.scenario().inputMode());
        System.out.println("  arrow_file_count    : " + report.scenario().arrowFileCount());
        System.out.println("  target_size_mb      : " + report.scenario().targetSizeMb());
        System.out.println("  generated_size_mb   : " + formatMiB(report.generation().arrowSizeBytes()));
        System.out.println("  generate_ms         : " + nanosToMillis(report.generation().elapsedNanos()));
        for (ImplementationSummary implementation : report.implementations()) {
            System.out.println("  implementation      : " + implementation.implementation());
            System.out.println("    average_ms        : " + formatDouble(implementation.averageMillis()));
            System.out.println("    iteration_ms      : " + implementation.iterationMillisJson());
        }
    }

    private static void printAggregateSummary(ComparisonReport report) {
        System.out.println();
        System.out.println("Comparison report");
        System.out.println("  report_json         : " + report.config().reportJson());
        System.out.println("  scenarios           : " + report.scenarioReports().size());
        System.out.println("  implementations     : " + String.join(",", report.config().implementations()));
        System.out.println("  total_ms            : " + nanosToMillis(report.totalNanos()));
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
                                  int rowCount,
                                  long rowOffset,
                                  byte[] payloadSuffix,
                                  long eventBase,
                                  VarCharVector userIdVector,
                                  BigIntVector eventTimeVector,
                                  VarCharVector deviceIdVector,
                                  VarCharVector regionVector,
                                  IntVector scoreVector,
                                  VarCharVector payloadVector) {
        for (int index = 0; index < rowCount; index++) {
            long rowId = rowOffset + index;
            userIdVector.setSafe(index, utf8(USER_ID_PREFIX + zeroPad(rowId, 12)));
            eventTimeVector.setSafe(index, eventBase + rowId);
            deviceIdVector.setSafe(index, utf8(DEVICE_PREFIX + zeroPad(rowId % 1_000_000L, 10)));
            regionVector.setSafe(index, utf8(REGIONS[(int) (rowId % REGIONS.length)]));
            scoreVector.setSafe(index, (int) (rowId % 10_000));
            payloadVector.setSafe(index, buildPayload(rowId, config.payloadBytes(), payloadSuffix));
        }
        userIdVector.setValueCount(rowCount);
        eventTimeVector.setValueCount(rowCount);
        deviceIdVector.setValueCount(rowCount);
        regionVector.setValueCount(rowCount);
        scoreVector.setValueCount(rowCount);
        payloadVector.setValueCount(rowCount);
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
        for (int index = 0; index < out.length; index++) {
            out[index] = seed[index % seed.length];
        }
        return out;
    }

    private static String decideStrategy(List<Path> arrowFiles, int mergeThresholdMb) {
        if (arrowFiles.isEmpty()) {
            return STRATEGY_DIRECT;
        }
        long totalBytes = 0L;
        for (Path arrowFile : arrowFiles) {
            try {
                totalBytes += Files.size(arrowFile);
            } catch (Exception ignored) {
            }
        }
        long averageBytes = totalBytes / Math.max(1, arrowFiles.size());
        long thresholdBytes = mergeThresholdMb * 1024L * 1024L;
        return averageBytes < thresholdBytes ? STRATEGY_MERGE : STRATEGY_PARALLEL;
    }

    private static String batchConvertResultToJson(String strategy, BatchConvertResult batchResult) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("\"strategy\":\"").append(jsonEscape(strategy)).append("\",");
        builder.append("\"task_count\":").append(batchResult.results.size()).append(",");
        builder.append("\"success_count\":").append(batchResult.succeeded.size()).append(",");
        builder.append("\"failed_count\":").append(batchResult.failed.size()).append(",");
        builder.append("\"total_elapsed_ms\":").append(batchResult.totalElapsedMs).append(",");
        builder.append("\"total_kv_written\":").append(batchResult.totalKvWritten).append(",");
        builder.append("\"total_hfile_size_bytes\":").append(batchResult.totalHfileSizeBytes).append(",");
        builder.append("\"results\":[");
        int index = 0;
        for (Map.Entry<Path, ConvertResult> entry : batchResult.results.entrySet()) {
            if (index++ > 0) {
                builder.append(",");
            }
            ConvertResult result = entry.getValue();
            builder.append("{");
            builder.append("\"source\":\"").append(jsonEscape(entry.getKey().toString())).append("\",");
            builder.append("\"error_code\":").append(result.errorCode).append(",");
            builder.append("\"error_message\":\"").append(jsonEscape(result.errorMessage)).append("\",");
            builder.append("\"arrow_rows_read\":").append(result.arrowRowsRead).append(",");
            builder.append("\"kv_written_count\":").append(result.kvWrittenCount).append(",");
            builder.append("\"hfile_size_bytes\":").append(result.hfileSizeBytes).append(",");
            builder.append("\"elapsed_ms\":").append(result.elapsedMs);
            builder.append("}");
        }
        builder.append("]}");
        return builder.toString();
    }

    private static void deleteRecursively(Path path) throws Exception {
        if (!Files.exists(path)) {
            return;
        }
        try (var walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(current -> {
                try {
                    Files.deleteIfExists(current);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof Exception exception) {
                throw exception;
            }
            throw e;
        }
    }

    private static boolean ensureRequiredOpens(String[] args) throws Exception {
        boolean nioOpen = Buffer.class.getModule().isOpen("java.nio", BulkLoadPerfRunner.class.getModule());
        boolean langOpen = ClassLoader.class.getModule().isOpen("java.lang", BulkLoadPerfRunner.class.getModule());
        if (nioOpen && langOpen) {
            return false;
        }
        Path self = Path.of(BulkLoadPerfRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.lang=ALL-UNNAMED");
        if (self.toString().endsWith(".jar")) {
            command.add("-jar");
            command.add(self.toString());
        } else {
            command.add("-cp");
            command.add(System.getProperty("java.class.path"));
            command.add(BulkLoadPerfRunner.class.getName());
        }
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).inheritIO().start();
        int code = process.waitFor();
        System.exit(code);
        return true;
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp("java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar", options);
    }

    private static int parsePositiveInt(String raw, String optionName) {
        int value;
        try {
            value = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + optionName + " 必须为正整数");
        }
        if (value <= 0) {
            throw new IllegalArgumentException("--" + optionName + " 必须为正整数");
        }
        return value;
    }

    private static String zeroPad(long value, int width) {
        return String.format(Locale.ROOT, "%0" + width + "d", value);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static long nanosToMillis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static String formatMiB(long bytes) {
        return String.format(Locale.ROOT, "%.2f", bytes / 1024.0 / 1024.0);
    }

    private static String formatDouble(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    private static String jsonEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private record RunConfig(
        String nativeLib,
        String tableName,
        Path workDir,
        Path reportJson,
        List<String> implementations,
        String scenarioFilter,
        int parallelism,
        int mergeThresholdMb,
        int triggerSizeMb,
        int triggerCount,
        int triggerIntervalSeconds,
        int batchRows,
        int payloadBytes,
        String columnFamily,
        String rowKeyRule,
        String compression,
        String encoding,
        String bloom,
        String errorPolicy,
        int blockSize,
        boolean keepGeneratedFiles
    ) {}

    private record ScenarioConfig(String scenarioId, String inputMode, int arrowFileCount, int targetSizeMb) {}

    private record SingleFileGenerationStats(long rows, int batches, long sizeBytes) {}

    private record GenerationStats(long rows, int batches, long arrowSizeBytes, List<Path> arrowFiles, long elapsedNanos) {}

    private record IterationResult(
        int iterationIndex,
        boolean success,
        String errorMessage,
        String strategy,
        long elapsedNanos,
        long hfileSizeBytes,
        long kvWrittenCount,
        int hfileCount,
        String detailJson
    ) {
        private String toJson() {
            return "{"
                + "\"iteration_index\":" + iterationIndex + ","
                + "\"success\":" + success + ","
                + "\"error_message\":\"" + jsonEscape(errorMessage) + "\","
                + "\"strategy\":\"" + jsonEscape(strategy) + "\","
                + "\"elapsed_ms\":" + nanosToMillis(elapsedNanos) + ","
                + "\"hfile_size_bytes\":" + hfileSizeBytes + ","
                + "\"kv_written_count\":" + kvWrittenCount + ","
                + "\"hfile_count\":" + hfileCount + ","
                + "\"detail\":" + detailJson
                + "}";
        }
    }

    private record ImplementationSummary(String implementation, List<IterationResult> iterationResults) {
        private double averageMillis() {
            return iterationResults.stream().mapToLong(result -> result.elapsedNanos()).average().orElse(0.0) / 1_000_000.0;
        }

        private String iterationMillisJson() {
            return iterationResults.stream()
                .map(result -> Long.toString(nanosToMillis(result.elapsedNanos())))
                .collect(java.util.stream.Collectors.joining(",", "[", "]"));
        }

        private String toJson() {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            builder.append("\"implementation\":\"").append(jsonEscape(implementation)).append("\",");
            builder.append("\"average_ms\":").append(formatDouble(averageMillis())).append(",");
            builder.append("\"iteration_ms\":").append(iterationMillisJson()).append(",");
            builder.append("\"iterations\":[");
            for (int index = 0; index < iterationResults.size(); index++) {
                if (index > 0) {
                    builder.append(",");
                }
                builder.append(iterationResults.get(index).toJson());
            }
            builder.append("]}");
            return builder.toString();
        }
    }

    private record ScenarioReport(ScenarioConfig scenario, GenerationStats generation, List<ImplementationSummary> implementations) {
        private String toJson() {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            builder.append("\"scenario_id\":\"").append(jsonEscape(scenario.scenarioId())).append("\",");
            builder.append("\"input_mode\":\"").append(jsonEscape(scenario.inputMode())).append("\",");
            builder.append("\"arrow_file_count\":").append(scenario.arrowFileCount()).append(",");
            builder.append("\"target_size_mb\":").append(scenario.targetSizeMb()).append(",");
            builder.append("\"generated_rows\":").append(generation.rows()).append(",");
            builder.append("\"generated_batches\":").append(generation.batches()).append(",");
            builder.append("\"generated_arrow_size_bytes\":").append(generation.arrowSizeBytes()).append(",");
            builder.append("\"generate_ms\":").append(nanosToMillis(generation.elapsedNanos())).append(",");
            builder.append("\"implementations\":[");
            for (int index = 0; index < implementations.size(); index++) {
                if (index > 0) {
                    builder.append(",");
                }
                builder.append(implementations.get(index).toJson());
            }
            builder.append("]}");
            return builder.toString();
        }
    }

    private record ComparisonReport(RunConfig config, List<ScenarioReport> scenarioReports, long totalNanos) {
        private String toJson() {
            StringBuilder builder = new StringBuilder();
            builder.append("{\n");
            builder.append("  \"table_name\": \"").append(jsonEscape(config.tableName())).append("\",\n");
            builder.append("  \"implementations\": [");
            for (int index = 0; index < config.implementations().size(); index++) {
                if (index > 0) {
                    builder.append(",");
                }
                builder.append("\"").append(jsonEscape(config.implementations().get(index))).append("\"");
            }
            builder.append("],\n");
            builder.append("  \"iterations\": ").append(FIXED_ITERATIONS).append(",\n");
            builder.append("  \"total_ms\": ").append(nanosToMillis(totalNanos)).append(",\n");
            builder.append("  \"scenarios\": [\n");
            for (int index = 0; index < scenarioReports.size(); index++) {
                builder.append("    ").append(scenarioReports.get(index).toJson());
                if (index + 1 < scenarioReports.size()) {
                    builder.append(",");
                }
                builder.append("\n");
            }
            builder.append("  ]\n");
            builder.append("}\n");
            return builder.toString();
        }
    }
}
