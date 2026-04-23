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
import io.hfilesdk.mock.MockArrowGenerator;
import io.hfilesdk.mock.TableSchema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.nio.Buffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class BulkLoadPerfRunner {

    private static final int FIXED_ITERATIONS = 3;
    private static final int DEFAULT_BATCH_ROWS = 8192;
    private static final int DEFAULT_PAYLOAD_BYTES = 0;
    private static final int DEFAULT_MERGE_THRESHOLD_MB = 100;
    private static final int DEFAULT_TRIGGER_SIZE_MB = 512;
    private static final int DEFAULT_TRIGGER_COUNT = 500;
    private static final int DEFAULT_TRIGGER_INTERVAL_SECONDS = 180;
    private static final String DEFAULT_TABLE = TableSchema.TDR_SIGNAL_STOR_20550.tableName;
    private static final long DEFAULT_MOCK_ARROW_SEED = 42L;
    private static final String DEFAULT_CF = "cf";
    private static final String DEFAULT_COMPRESSION = "GZ";
    private static final String DEFAULT_ENCODING = "NONE";
    private static final String DEFAULT_BLOOM = "row";
    private static final String DEFAULT_ERROR_POLICY = "skip_row";
    private static final int DEFAULT_BLOCK_SIZE = 65536;
    private static final String DEFAULT_WORK_DIR = "/tmp/hfilesdk-bulkload-perf";
    private static final long DEFAULT_TIMESTAMP_MS = 0L;
    private static final String IMPL_JNI = "arrow-to-hfile";
    private static final String IMPL_JAVA = "arrow-to-hfile-java";
    private static final String STRATEGY_DIRECT = "DIRECT-CONVERT";
    private static final String STRATEGY_PARALLEL = "PARALLEL-CONVERT";
    private static final String STRATEGY_MERGE = "MERGE-THEN-CONVERT";
    private static final long PROCESS_SAMPLE_INTERVAL_MS = 50L;
    private static final long FALLBACK_CLK_TCK = 100L;

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

        if (commandLine.hasOption("worker-mode")) {
            runWorker(commandLine, config);
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
        options.addOption(Option.builder().longOpt("table").hasArg().argName("NAME").desc("mock-arrow 表名，默认 tdr_signal_stor_20550").build());
        options.addOption(Option.builder().longOpt("work-dir").hasArg().argName("PATH").desc("工作目录").build());
        options.addOption(Option.builder().longOpt("report-json").hasArg().argName("PATH").desc("结构化结果输出路径").build());
        options.addOption(Option.builder().longOpt("implementations").hasArg().argName("LIST").desc("实现列表：arrow-to-hfile,arrow-to-hfile-java").build());
        options.addOption(Option.builder().longOpt("scenario-filter").hasArg().argName("TEXT").desc("按场景 ID 过滤执行").build());
        options.addOption(Option.builder().longOpt("iterations").hasArg().argName("N").desc("固定为 3").build());
        options.addOption(Option.builder().longOpt("parallelism").hasArg().argName("N").desc("JNI 目录场景的 worker 内并行转换线程数；不同实现/轮次仍由父进程串行调度").build());
        options.addOption(Option.builder().longOpt("merge-threshold").hasArg().argName("MB").desc("JNI 小文件合并阈值").build());
        options.addOption(Option.builder().longOpt("trigger-size").hasArg().argName("MB").desc("JNI 合并攒批大小阈值").build());
        options.addOption(Option.builder().longOpt("trigger-count").hasArg().argName("N").desc("JNI 合并攒批文件数阈值").build());
        options.addOption(Option.builder().longOpt("trigger-interval").hasArg().argName("SEC").desc("JNI 合并时间阈值").build());
        options.addOption(Option.builder().longOpt("batch-rows").hasArg().argName("N").desc("每个 Arrow batch 行数").build());
        options.addOption(Option.builder().longOpt("payload-bytes").hasArg().argName("BYTES").desc("mock-arrow 大文本列近似字节数；tdr_signal_stor_20550 下对应 SIGSTORE 宽度").build());
        options.addOption(Option.builder().longOpt("default-timestamp-ms").hasArg().argName("MS").desc("默认 timestamp；0 表示使用当前时间").build());
        options.addOption(Option.builder().longOpt("cf").hasArg().argName("CF").desc("列族名").build());
        options.addOption(Option.builder().longOpt("rule").hasArg().argName("RULE").desc("rowKeyRule；未传时默认使用 mock-arrow schema 自带规则").build());
        options.addOption(Option.builder().longOpt("compression").hasArg().argName("ALG").desc("压缩算法").build());
        options.addOption(Option.builder().longOpt("encoding").hasArg().argName("ENC").desc("Data Block Encoding").build());
        options.addOption(Option.builder().longOpt("bloom").hasArg().argName("TYPE").desc("Bloom 类型").build());
        options.addOption(Option.builder().longOpt("error-policy").hasArg().argName("POLICY").desc("JNI 错误策略").build());
        options.addOption(Option.builder().longOpt("block-size").hasArg().argName("BYTES").desc("HFile block size").build());
        options.addOption(Option.builder().longOpt("cpu-set").hasArg().argName("LIST").desc("Linux taskset CPU 集合，例如 0-3 或 1,3").build());
        options.addOption(Option.builder().longOpt("process-memory-mb").hasArg().argName("MB").desc("子进程总内存硬限制，优先 cgroup v2，失败时退化为 prlimit").build());
        options.addOption(Option.builder().longOpt("jni-xmx-mb").hasArg().argName("MB").desc("JNI worker JVM -Xmx").build());
        options.addOption(Option.builder().longOpt("jni-direct-memory-mb").hasArg().argName("MB").desc("JNI worker JVM MaxDirectMemorySize").build());
        options.addOption(Option.builder().longOpt("jni-sdk-max-memory-mb").hasArg().argName("MB").desc("JNI/C++ SDK 内部 soft budget").build());
        options.addOption(Option.builder().longOpt("jni-sdk-compression-threads").hasArg().argName("N").desc("JNI/C++ SDK 数据块压缩后台线程数；0 表示关闭流水线").build());
        options.addOption(Option.builder().longOpt("jni-sdk-compression-queue-depth").hasArg().argName("N").desc("JNI/C++ SDK 压缩流水线最大 in-flight block 数；0 表示自动").build());
        options.addOption(Option.builder().longOpt("jni-sdk-numeric-sort-fast-path").hasArg().argName("MODE").desc("JNI/C++ SDK 数值 row key 排序快路径：auto|on|off").build());
        options.addOption(Option.builder().longOpt("java-xmx-mb").hasArg().argName("MB").desc("纯 Java worker JVM -Xmx").build());
        options.addOption(Option.builder().longOpt("java-direct-memory-mb").hasArg().argName("MB").desc("纯 Java worker JVM MaxDirectMemorySize").build());
        options.addOption(Option.builder().longOpt("keep-generated-files").desc("保留中间产物").build());
        options.addOption(Option.builder().longOpt("worker-mode").desc("internal").build());
        options.addOption(Option.builder().longOpt("worker-implementation").hasArg().argName("NAME").desc("internal").build());
        options.addOption(Option.builder().longOpt("worker-input-dir").hasArg().argName("PATH").desc("internal").build());
        options.addOption(Option.builder().longOpt("worker-output-dir").hasArg().argName("PATH").desc("internal").build());
        options.addOption(Option.builder().longOpt("worker-result-json").hasArg().argName("PATH").desc("internal").build());
        options.addOption(Option.builder().longOpt("worker-iteration-index").hasArg().argName("N").desc("internal").build());
        options.addOption(Option.builder().longOpt("help").desc("显示帮助").build());
        return options;
    }

    private static RunConfig parseConfig(CommandLine commandLine) {
        int iterations = parsePositiveInt(commandLine.getOptionValue("iterations", Integer.toString(FIXED_ITERATIONS)), "iterations");
        if (iterations != FIXED_ITERATIONS) {
            throw new IllegalArgumentException("当前规范要求 --iterations 固定为 3");
        }

        boolean workerMode = commandLine.hasOption("worker-mode");
        List<String> implementations = workerMode
            ? List.of(requireOption(commandLine, "worker-implementation"))
            : parseImplementations(commandLine.getOptionValue("implementations", IMPL_JNI + "," + IMPL_JAVA));
        String nativeLib = commandLine.getOptionValue("native-lib", "").trim();
        if (implementations.contains(IMPL_JNI) && nativeLib.isBlank()) {
            throw new IllegalArgumentException("包含 arrow-to-hfile 时必须提供 --native-lib");
        }

        Path workDir = Path.of(commandLine.getOptionValue("work-dir", DEFAULT_WORK_DIR)).toAbsolutePath().normalize();
        Path reportJson = Path.of(commandLine.getOptionValue("report-json", "perf-matrix-report.json"))
            .toAbsolutePath()
            .normalize();
        String tableName = commandLine.getOptionValue("table", DEFAULT_TABLE);
        String rowKeyRule = resolveDefaultRowKeyRule(tableName, workerMode, commandLine.getOptionValue("rule", ""));

        return new RunConfig(
            nativeLib,
            tableName,
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
            Math.toIntExact(parseNonNegativeLong(commandLine.getOptionValue("payload-bytes", Integer.toString(DEFAULT_PAYLOAD_BYTES)), "payload-bytes")),
            parseNonNegativeLong(commandLine.getOptionValue("default-timestamp-ms", Long.toString(DEFAULT_TIMESTAMP_MS)), "default-timestamp-ms"),
            commandLine.getOptionValue("cf", DEFAULT_CF),
            rowKeyRule,
            commandLine.getOptionValue("compression", DEFAULT_COMPRESSION),
            commandLine.getOptionValue("encoding", DEFAULT_ENCODING),
            commandLine.getOptionValue("bloom", DEFAULT_BLOOM),
            commandLine.getOptionValue("error-policy", DEFAULT_ERROR_POLICY),
            parsePositiveInt(commandLine.getOptionValue("block-size", Integer.toString(DEFAULT_BLOCK_SIZE)), "block-size"),
            commandLine.hasOption("keep-generated-files"),
            commandLine.getOptionValue("cpu-set", "").trim(),
            parseNonNegativeLong(commandLine.getOptionValue("process-memory-mb", "0"), "process-memory-mb"),
            parseNonNegativeLong(commandLine.getOptionValue("jni-xmx-mb", "0"), "jni-xmx-mb"),
            parseNonNegativeLong(commandLine.getOptionValue("jni-direct-memory-mb", "0"), "jni-direct-memory-mb"),
            parseNonNegativeLong(commandLine.getOptionValue("jni-sdk-max-memory-mb", "0"), "jni-sdk-max-memory-mb"),
            parseNonNegativeLong(commandLine.getOptionValue("jni-sdk-compression-threads", "0"), "jni-sdk-compression-threads"),
            parseNonNegativeLong(commandLine.getOptionValue("jni-sdk-compression-queue-depth", "0"), "jni-sdk-compression-queue-depth"),
            normalizeNumericSortFastPathMode(commandLine.getOptionValue("jni-sdk-numeric-sort-fast-path", "auto")),
            parseNonNegativeLong(commandLine.getOptionValue("java-xmx-mb", "0"), "java-xmx-mb"),
            parseNonNegativeLong(commandLine.getOptionValue("java-direct-memory-mb", "0"), "java-direct-memory-mb")
        );
    }

    private static String normalizeNumericSortFastPathMode(String raw) {
        String normalized = raw == null ? "auto" : raw.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            normalized = "auto";
        }
        if (!normalized.equals("auto") && !normalized.equals("on") && !normalized.equals("off")) {
            throw new IllegalArgumentException("jni-sdk-numeric-sort-fast-path must be one of: auto|on|off");
        }
        return normalized;
    }

    private static String resolveDefaultRowKeyRule(String tableName, boolean workerMode, String rawRule) {
        if (rawRule != null && !rawRule.isBlank()) {
            return rawRule;
        }
        if (workerMode) {
            return TableSchema.TDR_SIGNAL_STOR_20550.rowKeyRule;
        }
        return TableSchema.forTable(tableName).rowKeyRule;
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

        try {
            GenerationStats generationStats = generateArrowFiles(config, scenario, inputDir);
            List<ImplementationSummary> implementations = new ArrayList<>();
            // Keep different implementations serialized at the parent level so JNI and
            // pure-Java runs do not contend for disk IO or CPU in the same scenario.
            for (String implementation : config.implementations()) {
                implementations.add(runImplementation(config, scenario, generationStats, inputDir, outputDir.resolve(implementation), implementation));
            }
            return new ScenarioReport(scenario, generationStats, List.copyOf(implementations));
        } finally {
            if (!config.keepGeneratedFiles()) {
                deleteRecursively(scenarioDir);
            }
        }
    }

    private static GenerationStats generateArrowFiles(RunConfig config, ScenarioConfig scenario, Path inputDir) throws Exception {
        long start = System.nanoTime();
        TableSchema schema = TableSchema.forTable(config.tableName());
        MockArrowGenerator.DirectoryGenerateResult generationResult = MockArrowGenerator.generateDirectory(
            schema,
            inputDir,
            scenario.arrowFileCount(),
            scenario.targetSizeMb(),
            config.batchRows(),
            DEFAULT_MOCK_ARROW_SEED,
            config.payloadBytes(),
            null
        );

        return new GenerationStats(
            generationResult.totalRows(),
            generationResult.totalBatches(),
            generationResult.totalFileSizeBytes(),
            generationResult.files(),
            System.nanoTime() - start
        );
    }

    private static ImplementationSummary runImplementation(RunConfig config,
                                                           ScenarioConfig scenario,
                                                           GenerationStats generationStats,
                                                           Path inputDir,
                                                           Path implementationDir,
                                                           String implementation) throws Exception {
        deleteRecursively(implementationDir);
        Files.createDirectories(implementationDir);
        List<IterationResult> iterationResults = new ArrayList<>();
        for (int iteration = 1; iteration <= FIXED_ITERATIONS; iteration++) {
            Path iterationDir = implementationDir.resolve(String.format(Locale.ROOT, "iter-%03d", iteration));
            deleteRecursively(iterationDir);
            Files.createDirectories(iterationDir);
            IterationResult result = runWorkerIteration(config, scenario, generationStats, inputDir, iterationDir, implementation, iteration);
            iterationResults.add(result);
            if (!result.success()) {
                throw new IllegalStateException("场景 " + scenario.scenarioId() + " / " + implementation + " 失败: " + result.errorMessage());
            }
        }
        return new ImplementationSummary(implementation, List.copyOf(iterationResults));
    }

    private static IterationResult runWorkerIteration(RunConfig config,
                                                      ScenarioConfig scenario,
                                                      GenerationStats generationStats,
                                                      Path inputDir,
                                                      Path iterationDir,
                                                      String implementation,
                                                      int iterationIndex) throws Exception {
        Path resultJson = iterationDir.resolve("worker-result.json");
        Path logPath = iterationDir.resolve("worker.log");
        Files.deleteIfExists(resultJson);
        Files.deleteIfExists(logPath);

        List<String> command = buildWorkerJavaCommand(config, inputDir, iterationDir, resultJson, implementation, iterationIndex);
        ResourceLaunch launch = startWorkerProcess(command, logPath, config);
        ProcessMetrics processMetrics = waitForWorker(launch);

        WorkerResult workerResult;
        if (Files.exists(resultJson)) {
            workerResult = WorkerResult.fromJson(Files.readString(resultJson, StandardCharsets.UTF_8));
        } else {
            String errorMessage = "worker 未生成结果文件: " + resultJson;
            if (Files.exists(logPath)) {
                errorMessage += " log=" + logPath;
            }
            workerResult = WorkerResult.failure(
                implementation,
                iterationIndex,
                errorMessage,
                STRATEGY_DIRECT,
                0L,
                0L,
                0L,
                0,
                implementation.equals(IMPL_JNI) ? config.parallelism() : 1,
                0L,
                0L,
                implementation.equals(IMPL_JNI) ? config.jniSdkNumericSortFastPath() : "off",
                false,
                "{}"
            );
        }

        return new IterationResult(
            workerResult.iterationIndex(),
            workerResult.success(),
            workerResult.errorMessage(),
            workerResult.strategy(),
            workerResult.elapsedMs(),
            workerResult.hfileSizeBytes(),
            workerResult.kvWrittenCount(),
            workerResult.hfileCount(),
            processMetrics.processPeakRssBytes(),
            processMetrics.processUserCpuMs(),
            processMetrics.processSysCpuMs(),
            processMetrics.processExitCode(),
            processMetrics.processExitSignal(),
            workerResult.workerParallelism(),
            workerResult.sdkMemoryBudgetBytes(),
            workerResult.sdkTrackedMemoryPeakBytes(),
            workerResult.sdkNumericSortFastPathMode(),
            workerResult.sdkNumericSortFastPathUsed(),
            launch.controlMode(),
            launch.controlNote(),
            workerResult.detailJson()
        );
    }

    private static void runWorker(CommandLine commandLine, RunConfig config) throws Exception {
        String implementation = requireOption(commandLine, "worker-implementation");
        Path inputDir = Path.of(requireOption(commandLine, "worker-input-dir")).toAbsolutePath().normalize();
        Path outputDir = Path.of(requireOption(commandLine, "worker-output-dir")).toAbsolutePath().normalize();
        Path resultJson = Path.of(requireOption(commandLine, "worker-result-json")).toAbsolutePath().normalize();
        int iterationIndex = parsePositiveInt(requireOption(commandLine, "worker-iteration-index"), "worker-iteration-index");

        Files.createDirectories(outputDir);
        if (resultJson.getParent() != null) {
            Files.createDirectories(resultJson.getParent());
        }

        WorkerResult result;
        try {
            List<Path> arrowFiles = collectArrowFiles(inputDir);
            if (arrowFiles.isEmpty()) {
                throw new IllegalArgumentException("worker 输入目录中没有 .arrow 文件: " + inputDir);
            }
            result = implementation.equals(IMPL_JNI)
                ? runJniWorkerIteration(config, arrowFiles, outputDir, iterationIndex)
                : runJavaWorkerIteration(config, arrowFiles, outputDir, iterationIndex);
        } catch (Exception e) {
            result = WorkerResult.failure(
                implementation,
                iterationIndex,
                e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage(),
                STRATEGY_DIRECT,
                0L,
                0L,
                0L,
                0,
                implementation.equals(IMPL_JNI) ? config.parallelism() : 1,
                0L,
                0L,
                implementation.equals(IMPL_JNI) ? config.jniSdkNumericSortFastPath() : "off",
                false,
                "{}"
            );
        }

        Files.writeString(resultJson, result.toJson(), StandardCharsets.UTF_8);
        if (result.success()) {
            System.out.printf(Locale.ROOT, "Worker %s iteration %d succeeded in %dms%n",
                implementation, iterationIndex, result.elapsedMs());
            return;
        }
        System.err.printf(Locale.ROOT, "Worker %s iteration %d failed: %s%n",
            implementation, iterationIndex, result.errorMessage());
        System.exit(3);
    }

    private static WorkerResult runJniWorkerIteration(RunConfig config,
                                                      List<Path> arrowFiles,
                                                      Path iterationDir,
                                                      int iterationIndex) throws Exception {
        Path hfileDir = iterationDir.resolve(config.columnFamily());
        Path mergeTmpDir = iterationDir.resolve("merge-tmp");
        Files.createDirectories(hfileDir);
        Files.createDirectories(mergeTmpDir);

        long sdkMaxMemoryBytes = mbToBytes(config.jniSdkMaxMemoryMb());
        if (arrowFiles.size() == 1) {
            return runJniSingleFileWorkerIteration(config, arrowFiles.getFirst(), hfileDir, iterationIndex, sdkMaxMemoryBytes);
        }

        long start = System.nanoTime();
        String strategy = decideStrategy(arrowFiles, config.mergeThresholdMb());
        BatchConvertOptions options = buildJniBatchConvertOptions(config, arrowFiles, hfileDir, sdkMaxMemoryBytes,
            Math.toIntExact(config.jniSdkCompressionQueueDepth()));
        AdaptiveBatchConverter.Policy policy = AdaptiveBatchConverter.Policy.builder()
            .mergeThresholdMib(config.mergeThresholdMb())
            .triggerSizeMib(config.triggerSizeMb())
            .triggerCount(config.triggerCount())
            .triggerIntervalSeconds(config.triggerIntervalSeconds())
            .mergeTmpDir(mergeTmpDir)
            .build();
        BatchConvertResult batchResult = new AdaptiveBatchConverter().convertAll(options, policy);
        if (!batchResult.isFullSuccess()
            && config.jniSdkCompressionQueueDepth() > 0
            && hasUnsupportedCompressionQueueDepthFailure(batchResult)) {
            BatchConvertOptions fallbackOptions = buildJniBatchConvertOptions(config, arrowFiles, hfileDir, sdkMaxMemoryBytes, 0);
            batchResult = new AdaptiveBatchConverter().convertAll(fallbackOptions, policy);
        }

        long sdkBudgetBytes = 0L;
        long sdkPeakBytes = 0L;
        boolean sdkNumericSortFastPathUsed = false;
        for (ConvertResult result : batchResult.results.values()) {
            sdkBudgetBytes = Math.max(sdkBudgetBytes, result.memoryBudgetBytes);
            sdkPeakBytes = Math.max(sdkPeakBytes, result.trackedMemoryPeakBytes);
            sdkNumericSortFastPathUsed = sdkNumericSortFastPathUsed || result.numericSortFastPathUsed;
        }

        return new WorkerResult(
            iterationIndex,
            IMPL_JNI,
            batchResult.isFullSuccess(),
            batchResult.isFullSuccess() ? "" : "failed outputs: " + batchResult.failed.stream().map(path -> path.getFileName().toString()).sorted().toList(),
            strategy,
            nanosToMillis(System.nanoTime() - start),
            batchResult.totalHfileSizeBytes,
            batchResult.totalKvWritten,
            batchResult.results.size(),
            config.parallelism(),
            sdkBudgetBytes,
            sdkPeakBytes,
            config.jniSdkNumericSortFastPath(),
            sdkNumericSortFastPathUsed,
            batchConvertResultToJson(strategy, batchResult)
        );
    }

    private static WorkerResult runJniSingleFileWorkerIteration(RunConfig config,
                                                                Path arrowFile,
                                                                Path hfileDir,
                                                                int iterationIndex,
                                                                long sdkMaxMemoryBytes) throws Exception {
        String fileName = arrowFile.getFileName().toString();
        String hfileName = fileName.endsWith(".arrow") ? fileName.substring(0, fileName.length() - 6) + ".hfile" : fileName + ".hfile";
        Path hfilePath = hfileDir.resolve(hfileName);
        long start = System.nanoTime();
        ConvertResult result = new ArrowToHFileConverter(config.nativeLib()).convert(
            buildJniConvertOptions(config, arrowFile, hfilePath, sdkMaxMemoryBytes,
                Math.toIntExact(config.jniSdkCompressionQueueDepth()))
        );
        if (!result.isSuccess()
            && config.jniSdkCompressionQueueDepth() > 0
            && isUnsupportedCompressionQueueDepthError(result.errorMessage)) {
            result = new ArrowToHFileConverter(config.nativeLib()).convert(
                buildJniConvertOptions(config, arrowFile, hfilePath, sdkMaxMemoryBytes, 0)
            );
        }
        String detailJson = "{"
            + "\"source\":\"" + jsonEscape(arrowFile.toString()) + "\","
            + "\"error_code\":" + result.errorCode + ","
            + "\"error_message\":\"" + jsonEscape(result.errorMessage) + "\","
            + "\"arrow_rows_read\":" + result.arrowRowsRead + ","
            + "\"kv_written_count\":" + result.kvWrittenCount + ","
            + "\"memory_budget_bytes\":" + result.memoryBudgetBytes + ","
            + "\"tracked_memory_peak_bytes\":" + result.trackedMemoryPeakBytes + ","
            + "\"numeric_sort_fast_path_mode\":\"" + jsonEscape(result.numericSortFastPathMode) + "\","
            + "\"numeric_sort_fast_path_used\":" + result.numericSortFastPathUsed + ","
            + "\"hfile_size_bytes\":" + result.hfileSizeBytes + ","
            + "\"elapsed_ms\":" + result.elapsedMs
            + "}";
        return new WorkerResult(
            iterationIndex,
            IMPL_JNI,
            result.isSuccess(),
            result.isSuccess() ? "" : result.errorMessage,
            STRATEGY_DIRECT,
            nanosToMillis(System.nanoTime() - start),
            result.hfileSizeBytes,
            result.kvWrittenCount,
            result.isSuccess() ? 1 : 0,
            config.parallelism(),
            result.memoryBudgetBytes,
            result.trackedMemoryPeakBytes,
            result.numericSortFastPathMode,
            result.numericSortFastPathUsed,
            detailJson
        );
    }

    private static WorkerResult runJavaWorkerIteration(RunConfig config,
                                                       List<Path> arrowFiles,
                                                       Path iterationDir,
                                                       int iterationIndex) throws Exception {
        Path hfileDir = iterationDir.resolve(config.columnFamily());
        Files.createDirectories(hfileDir);
        ArrowToHFileJavaConverter converter = new ArrowToHFileJavaConverter();
        long start = System.nanoTime();
        long totalSize = 0L;
        long totalKvWritten = 0L;
        List<String> resultJsons = new ArrayList<>();

        for (Path arrowFile : arrowFiles) {
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
                    .defaultTimestampMs(config.defaultTimestampMs())
                    .build()
            );
            resultJsons.add(result.toJson());
            if (!result.isSuccess()) {
                return WorkerResult.failure(
                    IMPL_JAVA,
                    iterationIndex,
                    result.errorMessage,
                    STRATEGY_DIRECT,
                    nanosToMillis(System.nanoTime() - start),
                    totalSize,
                    totalKvWritten,
                    resultJsons.size(),
                    1,
                    0L,
                    0L,
                    "off",
                    false,
                    "[" + String.join(",", resultJsons) + "]"
                );
            }
            totalSize += result.hfileSizeBytes;
            totalKvWritten += result.kvWrittenCount;
        }

        return new WorkerResult(
            iterationIndex,
            IMPL_JAVA,
            true,
            "",
            STRATEGY_DIRECT,
            nanosToMillis(System.nanoTime() - start),
            totalSize,
            totalKvWritten,
            arrowFiles.size(),
            1,
            0L,
            0L,
            "off",
            false,
            "[" + String.join(",", resultJsons) + "]"
        );
    }

    private static ConvertOptions buildJniConvertOptions(RunConfig config,
                                                         Path arrowFile,
                                                         Path hfilePath,
                                                         long sdkMaxMemoryBytes,
                                                         int compressionQueueDepth) {
        return ConvertOptions.builder()
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
            .defaultTimestampMs(config.defaultTimestampMs())
            .maxMemoryBytes(sdkMaxMemoryBytes)
            .compressionThreads(Math.toIntExact(config.jniSdkCompressionThreads()))
            .compressionQueueDepth(compressionQueueDepth)
            .numericSortFastPath(config.jniSdkNumericSortFastPath())
            .nativeLibPath(config.nativeLib())
            .build();
    }

    private static BatchConvertOptions buildJniBatchConvertOptions(RunConfig config,
                                                                   List<Path> arrowFiles,
                                                                   Path hfileDir,
                                                                   long sdkMaxMemoryBytes,
                                                                   int compressionQueueDepth) {
        return BatchConvertOptions.builder()
            .arrowFiles(arrowFiles)
            .hfileDir(hfileDir)
            .tableName(config.tableName())
            .rowKeyRule(config.rowKeyRule())
            .columnFamily(config.columnFamily())
            .compression(config.compression())
            .dataBlockEncoding(config.encoding())
            .bloomType(config.bloom())
            .errorPolicy(config.errorPolicy())
            .blockSize(config.blockSize())
            .defaultTimestampMs(config.defaultTimestampMs())
            .maxMemoryBytes(sdkMaxMemoryBytes)
            .compressionThreads(Math.toIntExact(config.jniSdkCompressionThreads()))
            .compressionQueueDepth(compressionQueueDepth)
            .numericSortFastPath(config.jniSdkNumericSortFastPath())
            // Preserve JNI directory-conversion semantics: a worker may still
            // convert multiple Arrow files according to the configured parallelism.
            .parallelism(config.parallelism())
            .nativeLibPath(config.nativeLib())
            .build();
    }

    private static boolean hasUnsupportedCompressionQueueDepthFailure(BatchConvertResult batchResult) {
        for (ConvertResult result : batchResult.results.values()) {
            if (!result.isSuccess() && isUnsupportedCompressionQueueDepthError(result.errorMessage)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isUnsupportedCompressionQueueDepthError(String message) {
        return message != null && message.contains("Unsupported config key: compression_queue_depth");
    }

    private static List<String> buildWorkerJavaCommand(RunConfig config,
                                                       Path inputDir,
                                                       Path iterationDir,
                                                       Path resultJson,
                                                       String implementation,
                                                       int iterationIndex) throws Exception {
        Path self = Path.of(BulkLoadPerfRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.lang=ALL-UNNAMED");
        appendJvmLimits(command, config, implementation);
        if (self.toString().endsWith(".jar")) {
            command.add("-jar");
            command.add(self.toString());
        } else {
            command.add("-cp");
            command.add(System.getProperty("java.class.path"));
            command.add(BulkLoadPerfRunner.class.getName());
        }

        command.add("--worker-mode");
        command.add("--worker-implementation");
        command.add(implementation);
        command.add("--worker-input-dir");
        command.add(inputDir.toString());
        command.add("--worker-output-dir");
        command.add(iterationDir.toString());
        command.add("--worker-result-json");
        command.add(resultJson.toString());
        command.add("--worker-iteration-index");
        command.add(Integer.toString(iterationIndex));
        command.add("--table");
        command.add(config.tableName());
        command.add("--parallelism");
        command.add(Integer.toString(config.parallelism()));
        command.add("--merge-threshold");
        command.add(Integer.toString(config.mergeThresholdMb()));
        command.add("--trigger-size");
        command.add(Integer.toString(config.triggerSizeMb()));
        command.add("--trigger-count");
        command.add(Integer.toString(config.triggerCount()));
        command.add("--trigger-interval");
        command.add(Integer.toString(config.triggerIntervalSeconds()));
        command.add("--cf");
        command.add(config.columnFamily());
        command.add("--default-timestamp-ms");
        command.add(Long.toString(config.defaultTimestampMs()));
        command.add("--rule");
        command.add(config.rowKeyRule());
        command.add("--compression");
        command.add(config.compression());
        command.add("--encoding");
        command.add(config.encoding());
        command.add("--bloom");
        command.add(config.bloom());
        command.add("--error-policy");
        command.add(config.errorPolicy());
        command.add("--block-size");
        command.add(Integer.toString(config.blockSize()));
        if (!config.nativeLib().isBlank()) {
            command.add("--native-lib");
            command.add(config.nativeLib());
        }
        if (config.jniSdkMaxMemoryMb() > 0) {
            command.add("--jni-sdk-max-memory-mb");
            command.add(Long.toString(config.jniSdkMaxMemoryMb()));
        }
        if (config.jniSdkCompressionThreads() > 0) {
            command.add("--jni-sdk-compression-threads");
            command.add(Long.toString(config.jniSdkCompressionThreads()));
        }
        if (config.jniSdkCompressionQueueDepth() > 0) {
            command.add("--jni-sdk-compression-queue-depth");
            command.add(Long.toString(config.jniSdkCompressionQueueDepth()));
        }
        if (!config.jniSdkNumericSortFastPath().equals("auto")) {
            command.add("--jni-sdk-numeric-sort-fast-path");
            command.add(config.jniSdkNumericSortFastPath());
        }
        return command;
    }

    private static void appendJvmLimits(List<String> command, RunConfig config, String implementation) {
        long xmxMb = implementation.equals(IMPL_JNI) ? config.jniXmxMb() : config.javaXmxMb();
        long directMb = implementation.equals(IMPL_JNI) ? config.jniDirectMemoryMb() : config.javaDirectMemoryMb();
        if (xmxMb > 0) {
            command.add("-Xmx" + xmxMb + "m");
        }
        if (directMb > 0) {
            command.add("-XX:MaxDirectMemorySize=" + directMb + "m");
        }
    }

    private static ResourceLaunch startWorkerProcess(List<String> workerJavaCommand,
                                                     Path logPath,
                                                     RunConfig config) throws Exception {
        Files.createDirectories(logPath.getParent());
        LaunchCommand launchCommand = buildLaunchCommand(workerJavaCommand, config);
        ProcessBuilder processBuilder = new ProcessBuilder(launchCommand.command());
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(logPath.toFile());
        return new ResourceLaunch(processBuilder.start(), launchCommand.controlMode(), launchCommand.controlNote(), launchCommand.cgroupDir());
    }

    private static LaunchCommand buildLaunchCommand(List<String> workerJavaCommand, RunConfig config) throws Exception {
        boolean linux = isLinux();
        boolean wantsCpuPinning = !config.cpuSet().isBlank();
        boolean wantsMemoryLimit = config.processMemoryMb() > 0;
        if (!linux || (!wantsCpuPinning && !wantsMemoryLimit)) {
            String note = (!linux && (wantsCpuPinning || wantsMemoryLimit))
                ? "CPU/memory 硬限制仅在 Linux 上启用；当前进程未应用 OS 级硬限制。"
                : "";
            return new LaunchCommand(workerJavaCommand, "direct", note, null);
        }

        String execCommand = shellJoin(workerJavaCommand);
        String controlMode = "direct";
        List<String> noteParts = new ArrayList<>();
        Path cgroupDir = null;

        if (wantsCpuPinning) {
            if (isCommandAvailable("taskset")) {
                execCommand = "taskset -c " + shellQuote(config.cpuSet()) + " " + execCommand;
                controlMode = "taskset";
            } else {
                noteParts.add("taskset 不可用，未应用 CPU 绑核。");
            }
        }

        if (wantsMemoryLimit) {
            cgroupDir = tryCreateMemoryCgroup(mbToBytes(config.processMemoryMb()));
            if (cgroupDir != null) {
                String script = "echo $$ > " + shellQuote(cgroupDir.resolve("cgroup.procs").toString()) + "; exec " + execCommand;
                controlMode = controlMode.equals("taskset") ? "cgroup_v2+taskset" : "cgroup_v2";
                return new LaunchCommand(List.of("/bin/bash", "-lc", script), controlMode, String.join(" ", noteParts), cgroupDir);
            }
            if (isCommandAvailable("prlimit")) {
                execCommand = "prlimit --as=" + mbToBytes(config.processMemoryMb()) + " -- " + execCommand;
                controlMode = controlMode.equals("taskset") ? "prlimit+taskset" : "prlimit";
                noteParts.add("cgroup v2 不可写，已退化为 prlimit 地址空间限制。");
            } else {
                controlMode = controlMode.equals("taskset") ? "taskset-only" : "unbounded";
                noteParts.add("未找到可用的 cgroup v2/prlimit，未应用 OS 级内存硬限制。");
            }
        }

        return new LaunchCommand(List.of("/bin/bash", "-lc", "exec " + execCommand), controlMode, String.join(" ", noteParts), cgroupDir);
    }

    private static Path tryCreateMemoryCgroup(long limitBytes) {
        try {
            Path root = Path.of("/sys/fs/cgroup");
            if (!Files.isRegularFile(root.resolve("cgroup.controllers"))) {
                return null;
            }
            String relativePath = Files.readAllLines(Path.of("/proc/self/cgroup"), StandardCharsets.UTF_8).stream()
                .filter(line -> line.startsWith("0::"))
                .map(line -> line.substring(3))
                .findFirst()
                .orElse("/");
            if (relativePath.isBlank()) {
                relativePath = "/";
            }
            Path base = relativePath.equals("/") ? root : root.resolve(relativePath.substring(1));
            if (!Files.isDirectory(base) || !Files.isWritable(base)) {
                return null;
            }
            Path cgroupDir = base.resolve("hfilesdk-perf-" + UUID.randomUUID());
            Files.createDirectory(cgroupDir);
            Files.writeString(cgroupDir.resolve("memory.max"), Long.toString(limitBytes), StandardCharsets.UTF_8);
            return cgroupDir;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static ProcessMetrics waitForWorker(ResourceLaunch launch) throws Exception {
        Process process = launch.process();
        long pid = process.pid();
        long peakRssBytes = 0L;
        long userTicks = 0L;
        long sysTicks = 0L;
        long procfsCpuFallbackMs = 0L;

        while (process.isAlive()) {
            ProcfsSample sample = readProcfsSample(pid);
            if (sample != null) {
                peakRssBytes = Math.max(peakRssBytes, Math.max(sample.rssBytes(), sample.hwmBytes()));
                userTicks = sample.utimeTicks();
                sysTicks = sample.stimeTicks();
            } else {
                try {
                    procfsCpuFallbackMs = process.toHandle().info().totalCpuDuration()
                        .map(Duration::toMillis)
                        .orElse(procfsCpuFallbackMs);
                } catch (RuntimeException ignored) {
                }
            }
            Thread.sleep(PROCESS_SAMPLE_INTERVAL_MS);
        }

        int exitCode = process.waitFor();
        long cgroupPeakBytes = readCgroupPeakBytes(launch.cgroupDir());
        peakRssBytes = Math.max(peakRssBytes, cgroupPeakBytes);
        cleanupCgroup(launch.cgroupDir());

        long clkTck = readClockTicksPerSecond();
        long userCpuMs = userTicks > 0 ? ticksToMillis(userTicks, clkTck) : procfsCpuFallbackMs;
        long sysCpuMs = sysTicks > 0 ? ticksToMillis(sysTicks, clkTck) : 0L;
        return new ProcessMetrics(
            peakRssBytes,
            userCpuMs,
            sysCpuMs,
            exitCode,
            deriveExitSignal(exitCode)
        );
    }

    private static ProcfsSample readProcfsSample(long pid) {
        try {
            Path statusPath = Path.of("/proc", Long.toString(pid), "status");
            Path statPath = Path.of("/proc", Long.toString(pid), "stat");
            if (!Files.isRegularFile(statusPath) || !Files.isRegularFile(statPath)) {
                return null;
            }

            long rssBytes = 0L;
            long hwmBytes = 0L;
            for (String line : Files.readAllLines(statusPath, StandardCharsets.UTF_8)) {
                if (line.startsWith("VmRSS:")) {
                    rssBytes = parseProcKbLine(line);
                } else if (line.startsWith("VmHWM:")) {
                    hwmBytes = parseProcKbLine(line);
                }
            }

            String stat = Files.readString(statPath, StandardCharsets.UTF_8).trim();
            int split = stat.lastIndexOf(") ");
            if (split < 0) {
                return new ProcfsSample(rssBytes, hwmBytes, 0L, 0L);
            }
            String[] fields = stat.substring(split + 2).split(" ");
            long utimeTicks = fields.length > 11 ? Long.parseLong(fields[11]) : 0L;
            long stimeTicks = fields.length > 12 ? Long.parseLong(fields[12]) : 0L;
            return new ProcfsSample(rssBytes, hwmBytes, utimeTicks, stimeTicks);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static long readCgroupPeakBytes(Path cgroupDir) {
        if (cgroupDir == null) {
            return 0L;
        }
        try {
            Path peakPath = cgroupDir.resolve("memory.peak");
            if (!Files.isRegularFile(peakPath)) {
                return 0L;
            }
            String value = Files.readString(peakPath, StandardCharsets.UTF_8).trim();
            return value.equals("max") ? 0L : Long.parseLong(value);
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private static void cleanupCgroup(Path cgroupDir) {
        if (cgroupDir == null) {
            return;
        }
        try {
            Files.deleteIfExists(cgroupDir);
        } catch (Exception ignored) {
        }
    }

    private static long readClockTicksPerSecond() {
        try {
            Process process = new ProcessBuilder("getconf", "CLK_TCK").start();
            int exit = process.waitFor();
            if (exit != 0) {
                return FALLBACK_CLK_TCK;
            }
            String raw = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            return Long.parseLong(raw);
        } catch (Exception ignored) {
            return FALLBACK_CLK_TCK;
        }
    }

    private static boolean isCommandAvailable(String name) {
        String pathValue = System.getenv("PATH");
        if (pathValue == null || pathValue.isBlank()) {
            return false;
        }
        for (String rawDir : pathValue.split(java.io.File.pathSeparator)) {
            if (rawDir.isBlank()) {
                continue;
            }
            Path candidate = Path.of(rawDir, name);
            if (Files.isExecutable(candidate)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isLinux() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux");
    }

    private static String shellJoin(List<String> command) {
        return command.stream().map(BulkLoadPerfRunner::shellQuote).collect(Collectors.joining(" "));
    }

    private static String shellQuote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }

    private static long parseProcKbLine(String line) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length < 2) {
            return 0L;
        }
        return Long.parseLong(parts[1]) * 1024L;
    }

    private static long ticksToMillis(long ticks, long clkTck) {
        if (clkTck <= 0) {
            clkTck = FALLBACK_CLK_TCK;
        }
        return ticks * 1000L / clkTck;
    }

    private static int deriveExitSignal(int exitCode) {
        return exitCode >= 128 ? exitCode - 128 : 0;
    }

    private static List<Path> collectArrowFiles(Path inputDir) throws Exception {
        List<Path> arrowFiles = new ArrayList<>();
        try (var stream = Files.find(inputDir, 1, (path, attr) -> attr.isRegularFile() && path.toString().endsWith(".arrow"))) {
            stream.sorted().forEach(arrowFiles::add);
        }
        return arrowFiles;
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
            System.out.println("    max_peak_rss_mb   : " + formatMiB(implementation.maxProcessPeakRssBytes()));
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
            builder.append("\"memory_budget_bytes\":").append(result.memoryBudgetBytes).append(",");
            builder.append("\"tracked_memory_peak_bytes\":").append(result.trackedMemoryPeakBytes).append(",");
            builder.append("\"numeric_sort_fast_path_mode\":\"").append(jsonEscape(result.numericSortFastPathMode)).append("\",");
            builder.append("\"numeric_sort_fast_path_used\":").append(result.numericSortFastPathUsed).append(",");
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

    private static long parseNonNegativeLong(String raw, String optionName) {
        long value;
        try {
            value = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + optionName + " 必须为非负整数");
        }
        if (value < 0) {
            throw new IllegalArgumentException("--" + optionName + " 必须为非负整数");
        }
        return value;
    }

    private static String requireOption(CommandLine commandLine, String option) {
        String value = commandLine.getOptionValue(option);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("--" + option + " is required");
        }
        return value;
    }

    private static long nanosToMillis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static long mbToBytes(long mb) {
        return mb * 1024L * 1024L;
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

    private static long parseLongField(String json, String key, long defaultValue) {
        String tag = "\"" + key + "\":";
        int idx = json.indexOf(tag);
        if (idx < 0) {
            return defaultValue;
        }
        int start = idx + tag.length();
        while (start < json.length() && json.charAt(start) == ' ') {
            start++;
        }
        int end = start;
        while (end < json.length()) {
            char c = json.charAt(end);
            if (c == ',' || c == '}') {
                break;
            }
            end++;
        }
        String raw = json.substring(start, end).trim();
        if (raw.startsWith("\"") && raw.endsWith("\"")) {
            raw = raw.substring(1, raw.length() - 1);
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static boolean parseBooleanField(String json, String key, boolean defaultValue) {
        String tag = "\"" + key + "\":";
        int idx = json.indexOf(tag);
        if (idx < 0) {
            return defaultValue;
        }
        int start = idx + tag.length();
        while (start < json.length() && json.charAt(start) == ' ') {
            start++;
        }
        if (json.startsWith("true", start)) {
            return true;
        }
        if (json.startsWith("false", start)) {
            return false;
        }
        return defaultValue;
    }

    private static String parseStringField(String json, String key, String defaultValue) {
        String tag = "\"" + key + "\":\"";
        int idx = json.indexOf(tag);
        if (idx < 0) {
            return defaultValue;
        }
        int start = idx + tag.length();
        StringBuilder sb = new StringBuilder();
        boolean escape = false;
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (escape) {
                switch (c) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    default -> sb.append(c);
                }
                escape = false;
            } else if (c == '\\') {
                escape = true;
            } else if (c == '"') {
                break;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String parseTrailingJsonValue(String json, String key, String defaultValue) {
        String tag = "\"" + key + "\":";
        int idx = json.indexOf(tag);
        if (idx < 0) {
            return defaultValue;
        }
        int start = idx + tag.length();
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }
        if (start >= json.length()) {
            return defaultValue;
        }
        int end = json.lastIndexOf('}');
        if (end <= start) {
            return defaultValue;
        }
        return json.substring(start, end).trim();
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
        long defaultTimestampMs,
        String columnFamily,
        String rowKeyRule,
        String compression,
        String encoding,
        String bloom,
        String errorPolicy,
        int blockSize,
        boolean keepGeneratedFiles,
        String cpuSet,
        long processMemoryMb,
        long jniXmxMb,
        long jniDirectMemoryMb,
        long jniSdkMaxMemoryMb,
        long jniSdkCompressionThreads,
        long jniSdkCompressionQueueDepth,
        String jniSdkNumericSortFastPath,
        long javaXmxMb,
        long javaDirectMemoryMb
    ) {}

    private record ScenarioConfig(String scenarioId, String inputMode, int arrowFileCount, int targetSizeMb) {}

    private record GenerationStats(long rows, int batches, long arrowSizeBytes, List<Path> arrowFiles, long elapsedNanos) {}

    private record LaunchCommand(List<String> command, String controlMode, String controlNote, Path cgroupDir) {}

    private record ResourceLaunch(Process process, String controlMode, String controlNote, Path cgroupDir) {}

    private record ProcfsSample(long rssBytes, long hwmBytes, long utimeTicks, long stimeTicks) {}

    private record ProcessMetrics(
        long processPeakRssBytes,
        long processUserCpuMs,
        long processSysCpuMs,
        int processExitCode,
        int processExitSignal
    ) {}

    private record WorkerResult(
        int iterationIndex,
        String implementation,
        boolean success,
        String errorMessage,
        String strategy,
        long elapsedMs,
        long hfileSizeBytes,
        long kvWrittenCount,
        int hfileCount,
        int workerParallelism,
        long sdkMemoryBudgetBytes,
        long sdkTrackedMemoryPeakBytes,
        String sdkNumericSortFastPathMode,
        boolean sdkNumericSortFastPathUsed,
        String detailJson
    ) {
        private static WorkerResult failure(String implementation,
                                            int iterationIndex,
                                            String errorMessage,
                                            String strategy,
                                            long elapsedMs,
                                            long hfileSizeBytes,
                                            long kvWrittenCount,
                                            int hfileCount,
                                            int workerParallelism,
                                            long sdkMemoryBudgetBytes,
                                            long sdkTrackedMemoryPeakBytes,
                                            String sdkNumericSortFastPathMode,
                                            boolean sdkNumericSortFastPathUsed,
                                            String detailJson) {
            return new WorkerResult(
                iterationIndex,
                implementation,
                false,
                errorMessage == null ? "" : errorMessage,
                strategy,
                elapsedMs,
                hfileSizeBytes,
                kvWrittenCount,
                hfileCount,
                workerParallelism,
                sdkMemoryBudgetBytes,
                sdkTrackedMemoryPeakBytes,
                sdkNumericSortFastPathMode,
                sdkNumericSortFastPathUsed,
                detailJson
            );
        }

        private String toJson() {
            return "{"
                + "\"iteration_index\":" + iterationIndex + ","
                + "\"implementation\":\"" + jsonEscape(implementation) + "\","
                + "\"success\":" + success + ","
                + "\"error_message\":\"" + jsonEscape(errorMessage) + "\","
                + "\"strategy\":\"" + jsonEscape(strategy) + "\","
                + "\"elapsed_ms\":" + elapsedMs + ","
                + "\"hfile_size_bytes\":" + hfileSizeBytes + ","
                + "\"kv_written_count\":" + kvWrittenCount + ","
                + "\"hfile_count\":" + hfileCount + ","
                + "\"worker_parallelism\":" + workerParallelism + ","
                + "\"sdk_memory_budget_bytes\":" + sdkMemoryBudgetBytes + ","
                + "\"sdk_tracked_memory_peak_bytes\":" + sdkTrackedMemoryPeakBytes + ","
                + "\"sdk_numeric_sort_fast_path_mode\":\"" + jsonEscape(sdkNumericSortFastPathMode) + "\","
                + "\"sdk_numeric_sort_fast_path_used\":" + sdkNumericSortFastPathUsed + ","
                + "\"detail\":" + detailJson
                + "}";
        }

        private static WorkerResult fromJson(String json) {
            return new WorkerResult(
                (int) parseLongField(json, "iteration_index", 0),
                parseStringField(json, "implementation", ""),
                parseBooleanField(json, "success", false),
                parseStringField(json, "error_message", ""),
                parseStringField(json, "strategy", STRATEGY_DIRECT),
                parseLongField(json, "elapsed_ms", 0),
                parseLongField(json, "hfile_size_bytes", 0),
                parseLongField(json, "kv_written_count", 0),
                (int) parseLongField(json, "hfile_count", 0),
                (int) parseLongField(json, "worker_parallelism", 1),
                parseLongField(json, "sdk_memory_budget_bytes", 0),
                parseLongField(json, "sdk_tracked_memory_peak_bytes", 0),
                parseStringField(json, "sdk_numeric_sort_fast_path_mode", "auto"),
                parseBooleanField(json, "sdk_numeric_sort_fast_path_used", false),
                parseTrailingJsonValue(json, "detail", "{}")
            );
        }
    }

    private record IterationResult(
        int iterationIndex,
        boolean success,
        String errorMessage,
        String strategy,
        long elapsedMs,
        long hfileSizeBytes,
        long kvWrittenCount,
        int hfileCount,
        long processPeakRssBytes,
        long processUserCpuMs,
        long processSysCpuMs,
        int processExitCode,
        int processExitSignal,
        int workerParallelism,
        long sdkMemoryBudgetBytes,
        long sdkTrackedMemoryPeakBytes,
        String sdkNumericSortFastPathMode,
        boolean sdkNumericSortFastPathUsed,
        String processControlMode,
        String processControlNote,
        String detailJson
    ) {
        private String toJson() {
            return "{"
                + "\"iteration_index\":" + iterationIndex + ","
                + "\"success\":" + success + ","
                + "\"error_message\":\"" + jsonEscape(errorMessage) + "\","
                + "\"strategy\":\"" + jsonEscape(strategy) + "\","
                + "\"elapsed_ms\":" + elapsedMs + ","
                + "\"hfile_size_bytes\":" + hfileSizeBytes + ","
                + "\"kv_written_count\":" + kvWrittenCount + ","
                + "\"hfile_count\":" + hfileCount + ","
                + "\"process_peak_rss_bytes\":" + processPeakRssBytes + ","
                + "\"process_user_cpu_ms\":" + processUserCpuMs + ","
                + "\"process_sys_cpu_ms\":" + processSysCpuMs + ","
                + "\"process_exit_code\":" + processExitCode + ","
                + "\"process_exit_signal\":" + processExitSignal + ","
                + "\"worker_parallelism\":" + workerParallelism + ","
                + "\"sdk_memory_budget_bytes\":" + sdkMemoryBudgetBytes + ","
                + "\"sdk_tracked_memory_peak_bytes\":" + sdkTrackedMemoryPeakBytes + ","
                + "\"sdk_numeric_sort_fast_path_mode\":\"" + jsonEscape(sdkNumericSortFastPathMode) + "\","
                + "\"sdk_numeric_sort_fast_path_used\":" + sdkNumericSortFastPathUsed + ","
                + "\"process_control_mode\":\"" + jsonEscape(processControlMode) + "\","
                + "\"process_control_note\":\"" + jsonEscape(processControlNote) + "\","
                + "\"detail\":" + detailJson
                + "}";
        }
    }

    private record ImplementationSummary(String implementation, List<IterationResult> iterationResults) {
        private double averageMillis() {
            return iterationResults.stream().mapToLong(IterationResult::elapsedMs).average().orElse(0.0);
        }

        private long maxProcessPeakRssBytes() {
            return iterationResults.stream().mapToLong(IterationResult::processPeakRssBytes).max().orElse(0L);
        }

        private String iterationMillisJson() {
            return iterationResults.stream()
                .map(result -> Long.toString(result.elapsedMs()))
                .collect(Collectors.joining(",", "[", "]"));
        }

        private String toJson() {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            builder.append("\"implementation\":\"").append(jsonEscape(implementation)).append("\",");
            builder.append("\"average_ms\":").append(formatDouble(averageMillis())).append(",");
            builder.append("\"iteration_ms\":").append(iterationMillisJson()).append(",");
            builder.append("\"max_process_peak_rss_bytes\":").append(maxProcessPeakRssBytes()).append(",");
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
            builder.append("  \"cpu_set\": \"").append(jsonEscape(config.cpuSet())).append("\",\n");
            builder.append("  \"process_memory_mb\": ").append(config.processMemoryMb()).append(",\n");
            builder.append("  \"jni_xmx_mb\": ").append(config.jniXmxMb()).append(",\n");
            builder.append("  \"jni_direct_memory_mb\": ").append(config.jniDirectMemoryMb()).append(",\n");
            builder.append("  \"jni_sdk_max_memory_mb\": ").append(config.jniSdkMaxMemoryMb()).append(",\n");
            builder.append("  \"jni_sdk_compression_threads\": ").append(config.jniSdkCompressionThreads()).append(",\n");
            builder.append("  \"jni_sdk_compression_queue_depth\": ").append(config.jniSdkCompressionQueueDepth()).append(",\n");
            builder.append("  \"jni_sdk_numeric_sort_fast_path\": \"").append(jsonEscape(config.jniSdkNumericSortFastPath())).append("\",\n");
            builder.append("  \"java_xmx_mb\": ").append(config.javaXmxMb()).append(",\n");
            builder.append("  \"java_direct_memory_mb\": ").append(config.javaDirectMemoryMb()).append(",\n");
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
