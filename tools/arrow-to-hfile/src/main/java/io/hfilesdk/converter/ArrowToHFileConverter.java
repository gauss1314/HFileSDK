package io.hfilesdk.converter;

import com.hfile.HFileSDK;
import org.apache.commons.cli.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Arrow IPC Stream → HFile v3 converter.
 *
 * <p>Serves two roles simultaneously:
 *
 * <h2>1. 独立 CLI 工具</h2>
 * <pre>{@code
 * java -jar arrow-to-hfile-4.0.0.jar \
 *   --native-lib  /path/to/libhfilesdk.so \
 *   --arrow       /data/events.arrow \
 *   --hfile       /staging/cf/events.hfile \
 *   --table       events_table \
 *   --rule        "STARTTIME,0,false,10#IMSI,1,true,15#MSISDN,2,false,11" \
 *   [--cf         cf] \
 *   [--compression lz4] \
 *   [--encoding   FAST_DIFF] \
 *   [--block-size 65536] \
 *   [--error-policy skip_row]
 * }</pre>
 *
 * <p>Exit code: {@code 0} on success, non-zero on failure.
 *
 * <h2>2. 生产系统依赖模块</h2>
 * <pre>{@code
 * // 应用启动时（@PostConstruct 或 static 块中）：
 * ArrowToHFileConverter converter = ArrowToHFileConverter
 *     .withNativeLib("/opt/hfilesdk/libhfilesdk.so");
 *
 * // 每次转换：
 * ConvertResult result = converter.convert(
 *     ConvertOptions.builder()
 *         .arrowPath("/data/events.arrow")
 *         .hfilePath("/staging/cf/events.hfile")
 *         .tableName("events_table")
 *         .rowKeyRule("STARTTIME,0,false,10#IMSI,1,true,15")
 *         .columnFamily("cf")
 *         .compression("lz4")
 *         .build());
 *
 * if (!result.isSuccess()) {
 *     throw new ConvertException(result);
 * }
 * log.info("Converted: {}", result.summary());
 * }</pre>
 *
 * <h3>线程安全</h3>
 * {@link #convert} 和 {@link #convertOrThrow} 是线程安全的：每次调用都会
 * 创建一个新的 {@link HFileSDK} 实例（底层 C++ 对象各自独立）。
 * {@link NativeLibLoader#load} 有内置的幂等性保护，多次调用安全。
 *
 * <h3>Native Library 加载顺序</h3>
 * <ol>
 *   <li>{@link #withNativeLib(String)} 或 {@link ConvertOptions#nativeLibPath()} 中的显式路径</li>
 *   <li>{@code HFILESDK_NATIVE_LIB} 环境变量（绝对路径）</li>
 *   <li>{@code HFILESDK_NATIVE_DIR} 环境变量（目录，自动解析库文件名）</li>
 *   <li>{@code java.library.path}（通过 {@code -Djava.library.path=} 传入）</li>
 * </ol>
 */
public class ArrowToHFileConverter {

    /**
     * Explicit path to the native library, set at construction time.
     * {@code null} means fall through to env vars / java.library.path.
     */
    private final String nativeLibPath;

    // ── Construction ──────────────────────────────────────────────────────────

    /** Create a converter that relies on env vars or {@code java.library.path}. */
    public ArrowToHFileConverter() {
        this(null);
    }

    /**
     * Create a converter that loads the native library from the given path.
     *
     * @param nativeLibPath absolute path to {@code libhfilesdk.so} /
     *                      {@code libhfilesdk.dylib} / {@code hfilesdk.dll}.
     *                      May be {@code null} to rely on env vars /
     *                      {@code java.library.path}.
     */
    public ArrowToHFileConverter(String nativeLibPath) {
        this.nativeLibPath = nativeLibPath;
    }

    /**
     * Fluent factory for a converter with an explicit native library path.
     *
     * <pre>{@code
     * ArrowToHFileConverter converter =
     *     ArrowToHFileConverter.withNativeLib("/opt/hfilesdk/libhfilesdk.so");
     * }</pre>
     */
    public static ArrowToHFileConverter withNativeLib(String path) {
        return new ArrowToHFileConverter(path);
    }

    // ── Core API ──────────────────────────────────────────────────────────────

    /**
     * Convert an Arrow IPC Stream file to a HFile v3 file.
     *
     * <p>This method is thread-safe: it creates a new {@link HFileSDK} instance
     * per call and holds no shared mutable state.
     *
     * @param opts conversion parameters (non-null).
     * @return a {@link ConvertResult} describing the outcome.
     *         Check {@link ConvertResult#isSuccess()} before using the output file.
     */
    public ConvertResult convert(ConvertOptions opts) {
        // Determine effective native lib path (option-level overrides instance-level)
        String libPath = opts.nativeLibPath() != null && !opts.nativeLibPath().isBlank()
                         ? opts.nativeLibPath()
                         : this.nativeLibPath;

        try {
            // Load native library BEFORE HFileSDK class is referenced
            NativeLibLoader.load(libPath);
        } catch (NativeLibLoadException e) {
            return ConvertResult.ofError(ConvertResult.INTERNAL_ERROR, e.getMessage());
        }

        // Build and invoke SDK
        String configJson = opts.toConfigJson();
        HFileSDK sdk = new HFileSDK();

        if (!configJson.equals("{}")) {
            int cfgRc = sdk.configure(configJson);
            if (cfgRc != HFileSDK.OK) {
                String detail = sdk.getLastResult();
                return ConvertResult.fromJson(detail, cfgRc);
            }
        }

        int rc = sdk.convert(
            opts.arrowPath(),
            opts.hfilePath(),
            opts.tableName(),
            opts.rowKeyRule());

        return ConvertResult.fromJson(sdk.getLastResult(), rc);
    }

    /**
     * Convert and throw {@link ConvertException} on failure.
     *
     * <p>Convenience wrapper for code that wants to treat conversion errors
     * as exceptions rather than inspecting a result object.
     *
     * @throws ConvertException if {@link ConvertResult#isSuccess()} is false.
     */
    public ConvertResult convertOrThrow(ConvertOptions opts) {
        ConvertResult result = convert(opts);
        if (!result.isSuccess()) {
            throw new ConvertException(result);
        }
        return result;
    }

    // ── CLI entry point ───────────────────────────────────────────────────────

    /**
     * CLI entry point.  Run with {@code --help} for usage.
     *
     * <p>Exit codes:
     * <ul>
     *   <li>{@code 0} — success</li>
     *   <li>{@code 1} — bad arguments / help printed</li>
     *   <li>{@code 2} — native library load failure</li>
     *   <li>{@code error_code} — SDK error code (see {@link ConvertResult} constants)</li>
     * </ul>
     */
    public static void main(String[] args) {
        Options options = buildCliOptions();
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
            System.exit(0);
            return;
        }

        // ── Batch mode: --batch-dir takes priority over single-file mode ──────
        if (cmd.hasOption("batch-dir")) {
            runBatchMode(cmd);
            return;
        }

        // ── Validate required arguments ────────────────────────────────────
        String[] required = {"arrow", "hfile", "rule"};
        for (String r : required) {
            if (!cmd.hasOption(r)) {
                System.err.println("Error: --" + r + " is required.");
                printHelp(options);
                System.exit(1);
                return;
            }
        }

        String arrowPath   = cmd.getOptionValue("arrow");
        String hfilePath   = cmd.getOptionValue("hfile");
        String tableName   = cmd.getOptionValue("table", "");
        String rowKeyRule  = cmd.getOptionValue("rule");
        String nativeLib   = cmd.getOptionValue("native-lib");
        String cf          = cmd.getOptionValue("cf",            "cf");
        String compression = cmd.getOptionValue("compression",   "GZ");
        String encoding    = cmd.getOptionValue("encoding",      "FAST_DIFF");
        String bloomType   = cmd.getOptionValue("bloom",         "row");
        String fsyncPolicy = cmd.getOptionValue("fsync-policy",  "safe");
        String errorPolicy = cmd.getOptionValue("error-policy",  "skip_row");
        int    blockSize   = parseInt(cmd.getOptionValue("block-size", "65536"), 65536);
        int    compLevel   = parseInt(cmd.getOptionValue("compression-level", "1"), 1);
        long   maxMemoryBytes = parseLong(cmd.getOptionValue("max-memory-mb", "0"), 0) * 1024L * 1024L;

        // ── Validate input file ────────────────────────────────────────────
        Path arrowFilePath = Paths.get(arrowPath);
        if (!Files.exists(arrowFilePath)) {
            System.err.println("Error: Arrow file not found: " + arrowPath);
            System.exit(ConvertResult.ARROW_FILE_ERROR);
            return;
        }

        // ── Ensure output directory exists ─────────────────────────────────
        Path hfileOut = Paths.get(hfilePath);
        try {
            if (hfileOut.getParent() != null) {
                Files.createDirectories(hfileOut.getParent());
            }
        } catch (Exception e) {
            System.err.println("Error: cannot create output directory for: " + hfilePath);
            System.err.println("       " + e.getMessage());
            System.exit(ConvertResult.IO_ERROR);
            return;
        }

        // ── Load native library ────────────────────────────────────────────
        try {
            NativeLibLoader.load(nativeLib);
        } catch (NativeLibLoadException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println();
            System.err.println("Set the library path via one of:");
            System.err.println("  --native-lib /path/to/libhfilesdk.so");
            System.err.println("  export HFILESDK_NATIVE_LIB=/path/to/libhfilesdk.so");
            System.err.println("  export HFILESDK_NATIVE_DIR=/path/to/lib/dir/");
            System.err.println("  java -Djava.library.path=/path/to/lib/dir -jar ...");
            System.exit(2);
            return;
        }

        // ── Build options and run ──────────────────────────────────────────
        ConvertOptions opts;
        try {
            ConvertOptions.Builder optsBuilder = ConvertOptions.builder()
                .arrowPath(arrowPath)
                .hfilePath(hfilePath)
                .tableName(tableName)
                .rowKeyRule(rowKeyRule)
                .columnFamily(cf)
                .compression(compression)
                .compressionLevel(compLevel)
                .dataBlockEncoding(encoding)
                .bloomType(bloomType)
                .fsyncPolicy(fsyncPolicy)
                .errorPolicy(errorPolicy)
                .blockSize(blockSize)
                .maxMemoryBytes(maxMemoryBytes);

            // Column exclusion — exact names
            if (cmd.hasOption("exclude-cols")) {
                String raw = cmd.getOptionValue("exclude-cols");
                for (String col : raw.split(",")) {
                    String trimmed = col.trim();
                    if (!trimmed.isEmpty()) optsBuilder.excludedColumn(trimmed);
                }
            }
            // Column exclusion — prefixes
            if (cmd.hasOption("exclude-prefix")) {
                String raw = cmd.getOptionValue("exclude-prefix");
                for (String pfx : raw.split(",")) {
                    String trimmed = pfx.trim();
                    if (!trimmed.isEmpty()) optsBuilder.excludedColumnPrefix(trimmed);
                }
            }

            opts = optsBuilder.build();
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(ConvertResult.INVALID_ARGUMENT);
            return;
        }

        System.out.printf("Converting%n  arrow : %s%n  hfile : %s%n  table : %s%n  rule  : %s%n",
            arrowPath, hfilePath, tableName.isBlank() ? "(none)" : tableName, rowKeyRule);

        ArrowToHFileConverter converter = new ArrowToHFileConverter();
        ConvertResult result = converter.convert(opts);

        if (result.isSuccess()) {
            System.out.println("Result: " + result.summary());
            System.out.printf("Throughput: %.1f MB/s%n", result.throughputMbps());
            if (result.hasDuplicateKeys()) {
                System.err.printf(
                    "%nWARNING: %,d row key collision(s) detected.%n" +
                    "  Multiple Arrow rows mapped to the same HBase row key.%n" +
                    "  For each collision the first-in-sort-order row was kept;%n" +
                    "  data from subsequent rows with the same key was discarded.%n" +
                    "  Action: review --rule to ensure it produces unique keys%n" +
                    "          (e.g. include a timestamp or add a $RND$ segment).%n",
                    result.duplicateKeyCount);
            }
        } else {
            System.err.println("FAILED: " + result.summary());
            System.exit(result.errorCode);
        }
    }

    // ── Batch mode ────────────────────────────────────────────────────────────

    private static void runBatchMode(CommandLine cmd) {
        if (!cmd.hasOption("rule")) {
            System.err.println("Error: --rule is required in batch mode.");
            System.exit(1); return;
        }
        if (!cmd.hasOption("batch-hfile-dir")) {
            System.err.println("Error: --batch-hfile-dir is required in batch mode.");
            System.exit(1); return;
        }

        Path arrowDir  = Paths.get(cmd.getOptionValue("batch-dir"));
        Path hfileDir  = Paths.get(cmd.getOptionValue("batch-hfile-dir"));
        String rule    = cmd.getOptionValue("rule");
        String table   = cmd.getOptionValue("table", "");
        String nativeLib = cmd.getOptionValue("native-lib");
        String cf          = cmd.getOptionValue("cf",           "cf");
        String compression = cmd.getOptionValue("compression",  "GZ");
        String encoding    = cmd.getOptionValue("encoding",     "FAST_DIFF");
        String bloomType   = cmd.getOptionValue("bloom",        "row");
        String errorPolicy = cmd.getOptionValue("error-policy", "skip_row");
        int    blockSize   = parseInt(cmd.getOptionValue("block-size", "65536"), 65536);
        int    compLevel   = parseInt(cmd.getOptionValue("compression-level", "1"), 1);
        long   maxMemoryBytes = parseLong(cmd.getOptionValue("max-memory-mb", "0"), 0) * 1024L * 1024L;
        int    parallelism = parseInt(cmd.getOptionValue("parallelism",
            String.valueOf(Runtime.getRuntime().availableProcessors())),
            Runtime.getRuntime().availableProcessors());
        boolean skipExisting = cmd.hasOption("skip-existing");

        // ── Adaptive strategy parameters ──────────────────────────────────────
        long mergeThresholdMib      = parseLong(cmd.getOptionValue("merge-threshold", "100"), 100);
        long triggerSizeMib         = parseLong(cmd.getOptionValue("trigger-size",    "512"), 512);
        int  triggerCount           = parseInt (cmd.getOptionValue("trigger-count",   "500"), 500);
        long triggerIntervalSeconds = parseLong(cmd.getOptionValue("trigger-interval","180"), 180);
        Path mergeTmpDir = cmd.hasOption("merge-tmp-dir")
            ? Paths.get(cmd.getOptionValue("merge-tmp-dir")) : null;

        if (!Files.isDirectory(arrowDir)) {
            System.err.println("Error: --batch-dir is not a directory: " + arrowDir);
            System.exit(1); return;
        }

        BatchConvertOptions.Builder b = BatchConvertOptions.builder()
            .arrowDir(arrowDir)
            .hfileDir(hfileDir)
            .tableName(table)
            .rowKeyRule(rule)
            .columnFamily(cf)
            .compression(compression)
            .compressionLevel(compLevel)
            .dataBlockEncoding(encoding)
            .bloomType(bloomType)
            .errorPolicy(errorPolicy)
            .blockSize(blockSize)
            .maxMemoryBytes(maxMemoryBytes)
            .parallelism(parallelism)
            .skipExisting(skipExisting)
            .nativeLibPath(nativeLib);

        if (cmd.hasOption("exclude-cols")) {
            b.excludedColumns(java.util.Arrays.stream(
                cmd.getOptionValue("exclude-cols").split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList());
        }
        if (cmd.hasOption("exclude-prefix")) {
            b.excludedColumnPrefixes(java.util.Arrays.stream(
                cmd.getOptionValue("exclude-prefix").split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList());
        }

        BatchConvertOptions opts = b.build();

        AdaptiveBatchConverter.Policy policy = AdaptiveBatchConverter.Policy.builder()
            .mergeThresholdMib(mergeThresholdMib)
            .triggerSizeMib(triggerSizeMib)
            .triggerCount(triggerCount)
            .triggerIntervalSeconds(triggerIntervalSeconds)
            .mergeTmpDir(mergeTmpDir)
            .build();

        System.out.printf(
            "Batch mode (adaptive)%n" +
            "  arrow-dir       : %s%n  hfile-dir       : %s%n" +
            "  table           : %s%n  rule            : %s%n" +
            "  parallelism     : %d%n  merge-threshold : %dMiB%n" +
            "  trigger-size    : %dMiB%n  trigger-count   : %d%n" +
            "  trigger-interval: %ds%n",
            arrowDir, hfileDir,
            table.isBlank() ? "(none)" : table, rule,
            parallelism, mergeThresholdMib,
            triggerSizeMib, triggerCount, triggerIntervalSeconds);

        // Load native library before anything else
        try {
            NativeLibLoader.load(nativeLib);
        } catch (NativeLibLoadException e) {
            System.err.println("Error loading native lib: " + e.getMessage());
            System.exit(2); return;
        }

        try {
            BatchConvertResult batch = new AdaptiveBatchConverter().convertAll(opts, policy);
            if (batch.isFullSuccess()) {
                System.out.println("\nAll conversions completed successfully.");
                if (batch.totalElapsedMs > 0) {
                    System.out.printf("Overall throughput: %.1f MB/s%n",
                        batch.totalHfileSizeBytes / 1024.0 / 1024.0 /
                        (batch.totalElapsedMs / 1000.0));
                }
            } else {
                System.err.printf("%n%d batch(es) failed.%n", batch.failed.size());
                System.exit(3);
            }
        } catch (Exception e) {
            System.err.println("Batch error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    // ── CLI helpers ────────────────────────────────────────────────────────────

    private static Options buildCliOptions() {
        Options o = new Options();

        // ── 单文件模式（与批量模式互斥）──────────────────────────────────────
        o.addOption(Option.builder().longOpt("arrow")
            .desc("Path to the Arrow IPC Stream input file  [required for single-file mode]")
            .hasArg().argName("PATH").build());
        o.addOption(Option.builder().longOpt("hfile")
            .desc("Path for the output HFile v3 file        [required for single-file mode]")
            .hasArg().argName("PATH").build());

        // ── 批量模式 ──────────────────────────────────────────────────────────
        o.addOption(Option.builder().longOpt("batch-dir")
            .desc("Directory containing *.arrow files        [enables batch mode]")
            .hasArg().argName("DIR").build());
        o.addOption(Option.builder().longOpt("batch-hfile-dir")
            .desc("Output directory for HFile files in batch mode  [required with --batch-dir]")
            .hasArg().argName("DIR").build());
        o.addOption(Option.builder().longOpt("parallelism")
            .desc("Concurrent conversions in batch mode     [default: CPU count]")
            .hasArg().argName("N").build());
        o.addOption(Option.builder().longOpt("skip-existing")
            .desc("Skip Arrow files whose HFile output already exists").build());

        // ── 自适应策略参数 ─────────────────────────────────────────────────────
        o.addOption(Option.builder().longOpt("merge-threshold")
            .desc("Avg file size threshold in MiB. Below this → merge strategy. [default: 100]")
            .hasArg().argName("MiB").build());
        o.addOption(Option.builder().longOpt("trigger-size")
            .desc("Merge batch trigger: accumulated size in MiB.    [default: 512]")
            .hasArg().argName("MiB").build());
        o.addOption(Option.builder().longOpt("trigger-count")
            .desc("Merge batch trigger: accumulated file count.      [default: 500]")
            .hasArg().argName("N").build());
        o.addOption(Option.builder().longOpt("trigger-interval")
            .desc("Merge batch trigger: elapsed seconds (time fence). [default: 180]")
            .hasArg().argName("SEC").build());
        o.addOption(Option.builder().longOpt("merge-tmp-dir")
            .desc("Temp dir for merged Arrow files.  [default: system temp]")
            .hasArg().argName("DIR").build());

        o.addOption(Option.builder().longOpt("rule")
            .desc("Row Key rule expression                  [required]\n" +
                  "  Format: \"name,index,isReverse,padLen[,padMode][,padContent]#...\"\n" +
                  "  Example: \"REFID,0,false,15\"")
            .hasArg().argName("RULE").build());
        o.addOption(Option.builder().longOpt("table")
            .desc("HBase table name (for logging)           [default: (empty)]")
            .hasArg().argName("NAME").build());
        o.addOption(Option.builder().longOpt("cf")
            .desc("HBase column family name                 [default: cf]")
            .hasArg().argName("CF").build());
        o.addOption(Option.builder().longOpt("native-lib")
            .desc("Absolute path to libhfilesdk.so/.dll     [default: from env/library.path]")
            .hasArg().argName("PATH").build());
        o.addOption(Option.builder().longOpt("compression")
            .desc("Compression: none|lz4|zstd|snappy|GZ    [default: GZ, also accepts gzip]")
            .hasArg().argName("ALG").build());
        o.addOption(Option.builder().longOpt("compression-level")
            .desc("Compression level 1(fastest)-9(best ratio) [default: 1]")
            .hasArg().argName("N").build());
        o.addOption(Option.builder().longOpt("encoding")
            .desc("Encoding: NONE|PREFIX|DIFF|FAST_DIFF     [default: FAST_DIFF]")
            .hasArg().argName("ENC").build());
        o.addOption(Option.builder().longOpt("bloom")
            .desc("Bloom filter: none|row|rowcol            [default: row]")
            .hasArg().argName("TYPE").build());
        o.addOption(Option.builder().longOpt("block-size")
            .desc("Data block size in bytes                 [default: 65536]")
            .hasArg().argName("BYTES").build());
        o.addOption(Option.builder().longOpt("max-memory-mb")
            .desc("Soft SDK memory budget in MiB            [default: unlimited]")
            .hasArg().argName("MiB").build());
        o.addOption(Option.builder().longOpt("fsync-policy")
            .desc("Fsync: safe|fast|paranoid                [default: safe]")
            .hasArg().argName("POLICY").build());
        o.addOption(Option.builder().longOpt("error-policy")
            .desc("Row errors: strict|skip_row|skip_batch   [default: skip_row]")
            .hasArg().argName("POLICY").build());
        o.addOption(Option.builder().longOpt("exclude-cols")
            .desc("Comma-separated column names to exclude from HBase output.")
            .hasArg().argName("COL1,COL2,...").build());
        o.addOption(Option.builder().longOpt("exclude-prefix")
            .desc("Comma-separated column name prefixes to exclude.")
            .hasArg().argName("PFX1,PFX2,...").build());
        o.addOption(Option.builder().longOpt("help")
            .desc("Print this help message").build());

        return o;
    }

    private static void printHelp(Options options) {
        System.out.println();
        new HelpFormatter().printHelp(
            "java -jar arrow-to-hfile-4.0.0.jar",
            "\nConvert Arrow IPC Stream file(s) to HBase HFile v3.\n\n",
            options,
            "\nExamples:\n\n" +
            "  # Single-file conversion:\n" +
            "  java -jar arrow-to-hfile-4.0.0.jar \\\n" +
            "    --native-lib ./libhfilesdk.so \\\n" +
            "    --arrow /data/tdr_20550.arrow \\\n" +
            "    --hfile /staging/job_20550/cf/tdr_20550.hfile \\\n" +
            "    --rule  \"REFID,0,false,15\"\n\n" +
            "  # Batch conversion (all *.arrow in a directory, 4 threads):\n" +
            "  java -jar arrow-to-hfile-4.0.0.jar \\\n" +
            "    --native-lib ./libhfilesdk.so \\\n" +
            "    --batch-dir      /data/arrow/ \\\n" +
            "    --batch-hfile-dir /staging/job_20550/cf/ \\\n" +
            "    --rule       \"REFID,0,false,15\" \\\n" +
            "    --table      tdr_signal_stor_20550 \\\n" +
            "    --parallelism 4\n\n" +
            "  # Then bulk load all HFiles in one shot:\n" +
            "  hdfs dfs -mkdir -p /hbase/staging/job_20550/cf/\n" +
            "  hdfs dfs -put /staging/job_20550/cf/*.hfile /hbase/staging/job_20550/cf/\n" +
            "  hbase org.apache.hadoop.hbase.tool.BulkLoadHFilesTool \\\n" +
            "        /hbase/staging/job_20550 tdr_signal_stor_20550\n",
            true);
    }

    private static int  parseInt (String s, int  def) { try { return Integer.parseInt(s.trim());  } catch (Exception e) { return def; } }
    private static long parseLong (String s, long def) { try { return Long.parseLong(s.trim());    } catch (Exception e) { return def; } }
}
