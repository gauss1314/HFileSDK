package io.hfilesdk.converter;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Converts a batch of Arrow IPC Stream files to HBase HFile v3 files
 * using a fixed-size thread pool.
 *
 * <h2>核心设计决策：Java 多线程 + 每线程独立 HFileSDK 实例</h2>
 *
 * <h3>为什么不在 C++ 内部实现多线程批量接口？</h3>
 * <ol>
 *   <li>每次 {@code convert()} 调用的状态完全独立（sort_index、batches[]、
 *       HFileWriter 均在栈/堆上独立分配），天然线程安全。</li>
 *   <li>JNI 层的全局锁（{@code g_config_mutex}）只在 {@code configure()} 和
 *       实例查找时持有（微秒级），{@code convert()} 主体不持锁，并发调用无阻塞。</li>
 *   <li>Java 线程池提供更灵活的调度（可中途取消、动态调整并发度、监控进度），
 *       C++ 内部线程池则需要额外的 JNI 数组传递、错误聚合、进度回调接口。</li>
 *   <li>内存可控：每线程独立使用内存，峰值 = parallelism × 单文件内存，
 *       可通过调节并发度控制总内存压力。</li>
 * </ol>
 *
 * <h3>线程安全保证</h3>
 * <ul>
 *   <li>每个工作线程有自己的 {@link ArrowToHFileConverter} 实例（持有独立的
 *       {@code HFileSDK} C++ 对象），互不干扰。</li>
 *   <li>{@code NativeLibLoader.load()} 有幂等保护，多线程重复调用安全。</li>
 *   <li>输出 HFile 路径各不相同，不存在文件级竞争。</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * BatchConvertOptions opts = BatchConvertOptions.builder()
 *     .arrowDir(Path.of("/data/arrow/"))
 *     .hfileDir(Path.of("/staging/job_20550/cf/"))
 *     .tableName("tdr_signal_stor_20550")
 *     .rowKeyRule("REFID,0,false,15")
 *     .columnFamily("cf")
 *     .parallelism(4)
 *     .progressCallback((file, result) ->
 *         log.info("{}: {}", file.getFileName(), result.summary()))
 *     .build();
 *
 * BatchConvertResult batch = new BatchArrowToHFileConverter()
 *     .convertAll(opts);
 *
 * if (!batch.isFullSuccess()) {
 *     batch.failed.forEach(f -> log.error("FAILED: {}", f));
 *     throw new RuntimeException("Batch conversion incomplete");
 * }
 * log.info(batch.summary());
 * // Batch: 12 files  success=12  failed=0  kvs=4,521,200  hfiles=1240.5MB  elapsed=42310ms
 * }</pre>
 *
 * <h2>与 BulkLoad 配合的完整流程</h2>
 * <pre>{@code
 * // 1. 并行转换所有 Arrow 文件
 * BatchConvertResult batch = converter.convertAll(opts);
 *
 * // 2. 全部成功后上传 HDFS
 * //    hdfs dfs -mkdir -p /hbase/staging/job_20550/cf/
 * //    hdfs dfs -put /staging/job_20550/cf/*.hfile /hbase/staging/job_20550/cf/
 *
 * // 3. 一次 BulkLoad（HBase 会自动按 Region 分配所有 HFile）
 * //    hbase org.apache.hadoop.hbase.tool.BulkLoadHFilesTool
 * //          /hbase/staging/job_20550 tdr_signal_stor_20550
 * }</pre>
 */
public class BatchArrowToHFileConverter {

    /**
     * Convert all Arrow files described by {@code opts}.
     *
     * @param opts batch conversion configuration.
     * @return aggregated result for all files.
     * @throws IOException if the Arrow directory cannot be scanned.
     * @throws InterruptedException if the calling thread is interrupted.
     */
    public BatchConvertResult convertAll(BatchConvertOptions opts)
            throws IOException, InterruptedException {

        // ── 1. Collect Arrow files ────────────────────────────────────────────
        List<Path> arrowFiles = collectArrowFiles(opts);
        if (arrowFiles.isEmpty()) {
            System.out.printf("[BatchConverter] No Arrow files found%n");
            return new BatchConvertResult(Map.of(), List.of(), List.of(), 0L);
        }

        // Ensure output directory exists
        Files.createDirectories(opts.hfileDir);

        int parallelism = Math.min(opts.parallelism, arrowFiles.size());
        System.out.printf(
            "[BatchConverter] Starting batch: %d files, parallelism=%d, outputDir=%s%n",
            arrowFiles.size(), parallelism, opts.hfileDir);

        // ── 2. Load native lib once (idempotent) ──────────────────────────────
        // NativeLibLoader.load() is thread-safe and idempotent — safe to call
        // before spawning threads so workers never race on the first load.
        try {
            NativeLibLoader.load(opts.nativeLibPath);
        } catch (NativeLibLoadException e) {
            throw new IOException("Cannot load native library: " + e.getMessage(), e);
        }

        // ── 3. Submit tasks to thread pool ────────────────────────────────────
        // Each worker gets its own ArrowToHFileConverter (which holds its own
        // HFileSDK C++ object). No sharing of state between workers.
        ExecutorService pool = Executors.newFixedThreadPool(parallelism,
            r -> {
                Thread t = new Thread(r, "hfile-convert");
                t.setDaemon(true);
                return t;
            });

        Map<Path, Future<ConvertResult>> futures = new LinkedHashMap<>();
        AtomicInteger doneCount = new AtomicInteger(0);
        long startMs = System.currentTimeMillis();

        for (Path arrowFile : arrowFiles) {
            Path hfileOut = resolveHfilePath(arrowFile, opts.hfileDir);

            // Skip existing files if requested
            if (opts.skipExisting && Files.exists(hfileOut)) {
                System.out.printf("  [skip] %s (HFile exists)%n", arrowFile.getFileName());
                continue;
            }

            Future<ConvertResult> future = pool.submit(() -> {
                // Each worker creates its own converter instance —
                // this is the key to thread safety: no shared HFileSDK state.
                ArrowToHFileConverter converter = new ArrowToHFileConverter(opts.nativeLibPath);

                ConvertOptions fileOpts = ConvertOptions.builder()
                    .arrowPath(arrowFile.toAbsolutePath().toString())
                    .hfilePath(hfileOut.toAbsolutePath().toString())
                    .tableName(opts.tableName)
                    .rowKeyRule(opts.rowKeyRule)
                    .columnFamily(opts.columnFamily)
                    .compression(opts.compression)
                    .compressionLevel(opts.compressionLevel)
                    .dataBlockEncoding(opts.dataBlockEncoding)
                    .bloomType(opts.bloomType)
                    .errorPolicy(opts.errorPolicy)
                    .blockSize(opts.blockSize)
                    .maxMemoryBytes(opts.maxMemoryBytes)
                    .defaultTimestampMs(opts.defaultTimestampMs)
                    .excludedColumnPrefixes(opts.excludedColumnPrefixes)
                    .excludedColumns(opts.excludedColumns)
                    .build();

                ConvertResult result = converter.convert(fileOpts);

                int done = doneCount.incrementAndGet();
                System.out.printf("  [%d/%d] %s → %s  %s%n",
                    done, arrowFiles.size(),
                    arrowFile.getFileName(),
                    hfileOut.getFileName(),
                    result.isSuccess() ? result.summary() : "FAILED: " + result.errorMessage);

                if (opts.progressCallback != null) {
                    opts.progressCallback.accept(arrowFile, result);
                }

                return result;
            });

            futures.put(arrowFile, future);
        }

        pool.shutdown();
        // Wait for all tasks; use a generous timeout (10h) to handle large files
        boolean completed = pool.awaitTermination(10, TimeUnit.HOURS);
        if (!completed) {
            pool.shutdownNow();
            throw new InterruptedException("Batch conversion timed out after 10 hours");
        }

        // ── 4. Collect results ────────────────────────────────────────────────
        Map<Path, ConvertResult> results   = new LinkedHashMap<>();
        List<Path>               succeeded = new ArrayList<>();
        List<Path>               failed    = new ArrayList<>();

        for (Map.Entry<Path, Future<ConvertResult>> entry : futures.entrySet()) {
            Path arrowFile = entry.getKey();
            ConvertResult result;
            try {
                result = entry.getValue().get();
            } catch (ExecutionException e) {
                // Unexpected exception inside the worker — treat as internal error
                result = ConvertResult.ofError(ConvertResult.INTERNAL_ERROR,
                    "Worker exception: " + e.getCause().getMessage());
            }
            results.put(arrowFile, result);
            if (result.isSuccess()) succeeded.add(arrowFile);
            else                    failed.add(arrowFile);
        }

        long totalElapsedMs = System.currentTimeMillis() - startMs;
        BatchConvertResult batch = new BatchConvertResult(
            results, succeeded, failed, totalElapsedMs);

        System.out.println();
        System.out.println("[BatchConverter] " + batch.summary());
        if (!batch.isFullSuccess()) {
            System.out.println("[BatchConverter] Failed files:");
            failed.forEach(f -> {
                ConvertResult r = results.get(f);
                System.out.printf("  FAILED %s: %s%n",
                    f.getFileName(), r.errorMessage);
            });
        }

        return batch;
    }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Collect Arrow files from {@code opts.arrowDir} (*.arrow glob) or
     * from {@code opts.arrowFiles} (explicit list), sorted by name.
     */
    private static List<Path> collectArrowFiles(BatchConvertOptions opts)
            throws IOException {
        List<Path> files = new ArrayList<>();

        if (opts.arrowDir != null) {
            try (Stream<Path> stream = Files.find(opts.arrowDir, 1,
                    (p, attr) -> attr.isRegularFile() &&
                                 p.toString().endsWith(".arrow"))) {
                stream.sorted().forEach(files::add);
            }
        }

        for (Path p : opts.arrowFiles) {
            if (Files.isRegularFile(p)) files.add(p);
        }

        files.sort(Comparator.comparing(Path::getFileName));
        return files;
    }

    /**
     * Derive the HFile output path for a given Arrow input file.
     * Example: /data/arrow/events_001.arrow → /staging/job/cf/events_001.hfile
     */
    private static Path resolveHfilePath(Path arrowFile, Path hfileDir) {
        String name = arrowFile.getFileName().toString();
        if (name.endsWith(".arrow")) {
            name = name.substring(0, name.length() - ".arrow".length());
        }
        return hfileDir.resolve(name + ".hfile");
    }
}
