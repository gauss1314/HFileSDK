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
        String compression = cmd.getOptionValue("compression",   "lz4");
        String encoding    = cmd.getOptionValue("encoding",      "FAST_DIFF");
        String bloomType   = cmd.getOptionValue("bloom",         "row");
        String fsyncPolicy = cmd.getOptionValue("fsync-policy",  "safe");
        String errorPolicy = cmd.getOptionValue("error-policy",  "skip_row");
        int    blockSize   = parseInt(cmd.getOptionValue("block-size", "65536"), 65536);

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
                .dataBlockEncoding(encoding)
                .bloomType(bloomType)
                .fsyncPolicy(fsyncPolicy)
                .errorPolicy(errorPolicy)
                .blockSize(blockSize);

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

    // ── CLI helpers ────────────────────────────────────────────────────────────

    private static Options buildCliOptions() {
        Options o = new Options();

        // Required
        o.addOption(Option.builder().longOpt("arrow")
            .desc("Path to the Arrow IPC Stream input file  [required]")
            .hasArg().argName("PATH").build());
        o.addOption(Option.builder().longOpt("hfile")
            .desc("Path for the output HFile v3 file        [required]")
            .hasArg().argName("PATH").build());
        o.addOption(Option.builder().longOpt("rule")
            .desc("Row Key rule expression                  [required]\n" +
                  "  Format: \"name,index,isReverse,padLen[,padMode][,padContent]#...\"\n" +
                  "  Example: \"STARTTIME,0,false,10#IMSI,1,true,15\"")
            .hasArg().argName("RULE").build());

        // Optional — common
        o.addOption(Option.builder().longOpt("table")
            .desc("HBase table name (for logging)           [default: (empty)]")
            .hasArg().argName("NAME").build());
        o.addOption(Option.builder().longOpt("cf")
            .desc("HBase column family name                 [default: cf]")
            .hasArg().argName("CF").build());
        o.addOption(Option.builder().longOpt("native-lib")
            .desc("Absolute path to libhfilesdk.so/.dll     [default: from env/library.path]")
            .hasArg().argName("PATH").build());

        // Optional — HFile tuning
        o.addOption(Option.builder().longOpt("compression")
            .desc("Compression: none|lz4|zstd|snappy|gzip  [default: lz4]")
            .hasArg().argName("ALG").build());
        o.addOption(Option.builder().longOpt("encoding")
            .desc("Encoding: NONE|PREFIX|DIFF|FAST_DIFF     [default: FAST_DIFF]")
            .hasArg().argName("ENC").build());
        o.addOption(Option.builder().longOpt("bloom")
            .desc("Bloom filter: none|row|rowcol            [default: row]")
            .hasArg().argName("TYPE").build());
        o.addOption(Option.builder().longOpt("block-size")
            .desc("Data block size in bytes                 [default: 65536]")
            .hasArg().argName("BYTES").build());
        o.addOption(Option.builder().longOpt("fsync-policy")
            .desc("Fsync: safe|fast|paranoid                [default: safe]")
            .hasArg().argName("POLICY").build());
        o.addOption(Option.builder().longOpt("error-policy")
            .desc("Row errors: strict|skip_row|skip_batch   [default: skip_row]")
            .hasArg().argName("POLICY").build());

        o.addOption(Option.builder().longOpt("help")
            .desc("Print this help message").build());

        // Column exclusion
        o.addOption(Option.builder().longOpt("exclude-cols")
            .desc("Comma-separated column names to exclude from HBase output.\n" +
                  "  Example: --exclude-cols _hoodie_commit_time,_hoodie_file_name\n" +
                  "  Does NOT affect row key column indices.")
            .hasArg().argName("COL1,COL2,...").build());
        o.addOption(Option.builder().longOpt("exclude-prefix")
            .desc("Comma-separated column name prefixes to exclude.\n" +
                  "  Example: --exclude-prefix _hoodie  (drops all 5 Hudi meta columns)\n" +
                  "  Does NOT affect row key column indices.")
            .hasArg().argName("PFX1,PFX2,...").build());

        return o;
    }

    private static void printHelp(Options options) {
        System.out.println();
        new HelpFormatter().printHelp(
            "java -jar arrow-to-hfile-4.0.0.jar",
            "\nConvert an Arrow IPC Stream file to HBase HFile v3 via HFileSDK.\n\n",
            options,
            "\nExamples:\n" +
            "  # Basic conversion:\n" +
            "  java -jar arrow-to-hfile-4.0.0.jar \\\n" +
            "    --native-lib /opt/hfilesdk/libhfilesdk.so \\\n" +
            "    --arrow /data/events.arrow \\\n" +
            "    --hfile /staging/cf/events.hfile \\\n" +
            "    --rule  \"STARTTIME,0,false,10#IMSI,1,true,15\"\n\n" +
            "  # Hudi / DeltaLake: drop _hoodie_* metadata columns:\n" +
            "  java -jar arrow-to-hfile-4.0.0.jar \\\n" +
            "    --native-lib /opt/hfilesdk/libhfilesdk.so \\\n" +
            "    --arrow  /data/hudi_events.arrow \\\n" +
            "    --hfile  /staging/cf/events.hfile \\\n" +
            "    --rule   \"STARTTIME,0,false,10#IMSI,1,true,15\" \\\n" +
            "    --exclude-prefix _hoodie\n\n" +
            "  Hudi original schema:  [0]_hoodie_commit_time ... [4]_hoodie_file_name [5]STARTTIME [6]IMSI\n" +
            "  After --exclude-prefix _hoodie: [0]STARTTIME [1]IMSI ...\n" +
            "  So --rule uses index 0 for STARTTIME, 1 for IMSI — no offset needed.\n",
            true);
    }

    private static int parseInt(String s, int defaultValue) {
        try { return Integer.parseInt(s); }
        catch (NumberFormatException e) { return defaultValue; }
    }
}
