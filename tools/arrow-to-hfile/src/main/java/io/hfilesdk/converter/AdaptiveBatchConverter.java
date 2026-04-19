package io.hfilesdk.converter;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 自适应批量转换器。
 *
 * <p>根据单个 Arrow 文件的平均大小自动选择转换策略：
 *
 * <pre>
 * ┌──────────────────────────────────────────────────────────────┐
 * │  文件平均大小 &lt; mergeThresholdBytes（默认 100 MiB）？           │
 * │                                                              │
 * │  YES → 合并策略（小文件优化）                                  │
 * │    ① 按 triggerSize / triggerCount / triggerInterval 攒批     │
 * │    ② ArrowFileMerger 把一批小文件合并为一个大文件               │
 * │    ③ 1 次 JNI convert() 把合并文件转为 HFile                  │
 * │                                                              │
 * │  NO → 并行策略（大文件优化）                                    │
 * │    ① 每个 Arrow 文件独立转换                                   │
 * │    ② parallelism 个线程并发调用 JNI convert()                  │
 * └──────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h3>攒批触发条件（满足任一即触发）</h3>
 * <ol>
 *   <li>累积文件总大小 ≥ {@code triggerSizeBytes}（默认 512 MiB）</li>
 *   <li>累积文件数量  ≥ {@code triggerCount}（默认 500 个）</li>
 *   <li>距上次触发已过 {@code triggerIntervalMs}（默认 3 分钟，时效兜底）</li>
 * </ol>
 *
 * <h3>CLI 用法</h3>
 * <pre>{@code
 * # 自适应批量转换（自动选择策略）
 * java -jar arrow-to-hfile-4.0.0.jar \
 *   --native-lib ./libhfilesdk.so \
 *   --batch-dir       /data/arrow/ \
 *   --batch-hfile-dir /staging/job_20550/cf/ \
 *   --rule            "REFID,0,false,15" \
 *   --table           tdr_signal_stor_20550 \
 *   --merge-threshold 100        # MiB，文件均值小于此值触发合并策略（默认100）
 *   --trigger-size    512        # MiB，攒批大小阈值（默认512）
 *   --trigger-count   500        # 攒批数量阈值（默认500）
 *   --trigger-interval 180       # 秒，时效兜底（默认180）
 *   --merge-tmp-dir   /tmp/merged/
 * }</pre>
 *
 * <h3>编程 API 用法</h3>
 * <pre>{@code
 * AdaptiveBatchConverter converter = new AdaptiveBatchConverter();
 * BatchConvertResult result = converter.convertAll(
 *     BatchConvertOptions.builder()
 *         .arrowDir(Path.of("/data/arrow/"))
 *         .hfileDir(Path.of("/staging/cf/"))
 *         .rowKeyRule("REFID,0,false,15")
 *         .parallelism(4)
 *         .build(),
 *     AdaptiveBatchConverter.Policy.builder()
 *         .mergeThresholdMib(100)
 *         .triggerSizeMib(512)
 *         .triggerCount(500)
 *         .triggerIntervalSeconds(180)
 *         .mergeTmpDir(Path.of("/tmp/merged/"))
 *         .build());
 * }</pre>
 */
public class AdaptiveBatchConverter {

    // ── Nested Policy class ───────────────────────────────────────────────────

    /**
     * Strategy parameters for adaptive batch conversion.
     */
    public static final class Policy {

        /** File average size below this value triggers the MERGE strategy. Default: 100 MiB. */
        public final long mergeThresholdBytes;

        // ── Merge-batch trigger conditions ────────────────────────────────────
        /** Trigger merge when accumulated size ≥ this. Default: 512 MiB. */
        public final long triggerSizeBytes;
        /** Trigger merge when accumulated file count ≥ this. Default: 500. */
        public final int  triggerCount;
        /** Trigger merge when this many ms have passed since last trigger. Default: 3 min. */
        public final long triggerIntervalMs;

        /** Directory for temporary merged Arrow files. Default: system temp dir. */
        public final Path mergeTmpDir;

        private Policy(Builder b) {
            this.mergeThresholdBytes = b.mergeThresholdMib  * 1024L * 1024;
            this.triggerSizeBytes    = b.triggerSizeMib     * 1024L * 1024;
            this.triggerCount        = b.triggerCount;
            this.triggerIntervalMs   = b.triggerIntervalSeconds * 1000L;
            this.mergeTmpDir         = b.mergeTmpDir;
        }

        public static Builder builder() { return new Builder(); }

        public static final class Builder {
            private long mergeThresholdMib      = 100;
            private long triggerSizeMib         = 512;
            private int  triggerCount           = 500;
            private long triggerIntervalSeconds = 180;
            private Path mergeTmpDir;

            /** Average file size threshold in MiB. Files smaller → merge strategy. */
            public Builder mergeThresholdMib(long v)      { mergeThresholdMib = v;      return this; }
            /** Accumulated size trigger in MiB. */
            public Builder triggerSizeMib(long v)         { triggerSizeMib = v;         return this; }
            /** Accumulated file count trigger. */
            public Builder triggerCount(int v)            { triggerCount = v;            return this; }
            /** Time-based trigger in seconds. */
            public Builder triggerIntervalSeconds(long v) { triggerIntervalSeconds = v; return this; }
            /** Temp directory for merged Arrow files. */
            public Builder mergeTmpDir(Path v)            { mergeTmpDir = v;            return this; }

            public Policy build() { return new Policy(this); }
        }

        @Override public String toString() {
            return String.format(
                "Policy{mergeThreshold=%dMiB, triggerSize=%dMiB, " +
                "triggerCount=%d, triggerInterval=%ds, mergeTmpDir=%s}",
                mergeThresholdBytes / 1024 / 1024,
                triggerSizeBytes    / 1024 / 1024,
                triggerCount,
                triggerIntervalMs   / 1000,
                mergeTmpDir);
        }
    }

    // ── Core API ──────────────────────────────────────────────────────────────

    /**
     * Convert all Arrow files using the adaptive strategy.
     *
     * @param opts   batch conversion options (shared for both strategies)
     * @param policy adaptive strategy parameters
     */
    public BatchConvertResult convertAll(BatchConvertOptions opts, Policy policy)
            throws IOException, InterruptedException {

        // ── 1. Collect & measure input files ─────────────────────────────────
        List<Path> allFiles = collectAndSort(opts);
        if (allFiles.isEmpty()) {
            System.out.println("[AdaptiveBatch] No Arrow files found.");
            return emptyResult();
        }

        long totalBytes = ArrowFileMerger.totalBytes(allFiles);
        long avgBytes   = totalBytes / allFiles.size();
        long avgMib     = avgBytes / 1024 / 1024;
        boolean useMerge = avgBytes < policy.mergeThresholdBytes;

        System.out.printf(
            "[AdaptiveBatch] %d files  total=%.1fMiB  avg=%.1fMiB  " +
            "threshold=%dMiB  strategy=%s%n",
            allFiles.size(),
            totalBytes / 1024.0 / 1024.0,
            avgBytes   / 1024.0 / 1024.0,
            policy.mergeThresholdBytes / 1024 / 1024,
            useMerge ? "MERGE-THEN-CONVERT" : "PARALLEL-CONVERT");

        // ── 2. Dispatch to strategy ───────────────────────────────────────────
        if (useMerge) {
            return runMergeStrategy(allFiles, opts, policy);
        } else {
            return new BatchArrowToHFileConverter().convertAll(opts);
        }
    }

    // ── Merge strategy ────────────────────────────────────────────────────────

    private BatchConvertResult runMergeStrategy(
            List<Path> allFiles,
            BatchConvertOptions opts,
            Policy policy) throws IOException, InterruptedException {

        // Prepare merge temp directory
        Path tmpDir = policy.mergeTmpDir != null
            ? policy.mergeTmpDir
            : Path.of(System.getProperty("java.io.tmpdir"), "hfilesdk-merge");
        Files.createDirectories(tmpDir);
        Files.createDirectories(opts.hfileDir);

        // Load native library once before any conversion
        try {
            NativeLibLoader.load(opts.nativeLibPath);
        } catch (NativeLibLoadException e) {
            throw new IOException("Cannot load native library: " + e.getMessage(), e);
        }

        // ── Partition into batches ────────────────────────────────────────────
        List<List<Path>> batches = partitionIntoBatches(allFiles, policy);
        System.out.printf("[AdaptiveBatch] Partitioned into %d merge batch(es)%n",
            batches.size());

        // ── Process each batch concurrently ──────────────────────────────────
        // We can run multiple merge+convert pipelines in parallel if there are
        // multiple batches. Each batch: merge → 1 merged.arrow → convert → 1 HFile.
        int batchParallelism = Math.min(opts.parallelism, batches.size());
        ExecutorService pool = Executors.newFixedThreadPool(batchParallelism,
            r -> { Thread t = new Thread(r, "adaptive-merge"); t.setDaemon(true); return t; });

        Map<Path, Future<ConvertResult>> futures = new LinkedHashMap<>();
        AtomicLong batchSeq = new AtomicLong(0);
        long wallStart = System.currentTimeMillis();

        for (List<Path> batch : batches) {
            long seq = batchSeq.incrementAndGet();

            Future<ConvertResult> future = pool.submit(() -> {
                String tag = String.format("[Batch-%03d]", seq);
                long batchBytes = ArrowFileMerger.totalBytes(batch);

                System.out.printf("%s Merging %d files (%.1fMiB)...%n",
                    tag, batch.size(), batchBytes / 1024.0 / 1024.0);

                // ── A. Merge Arrow files ──────────────────────────────────────
                Path mergedArrow = tmpDir.resolve(
                    String.format("merged_%s_%03d.arrow",
                        opts.tableName.isBlank() ? "batch" : sanitize(opts.tableName),
                        seq));
                long mergeStart = System.currentTimeMillis();
                try {
                    ArrowFileMerger.merge(batch, mergedArrow);
                } catch (IOException e) {
                    return ConvertResult.ofError(ConvertResult.ARROW_FILE_ERROR,
                        tag + " merge failed: " + e.getMessage());
                }
                long mergeMs = System.currentTimeMillis() - mergeStart;
                long mergedSize = Files.size(mergedArrow);
                System.out.printf("%s Merge done: %.1fMiB in %dms → %s%n",
                    tag, mergedSize / 1024.0 / 1024.0, mergeMs, mergedArrow.getFileName());

                // ── B. Convert merged Arrow → HFile ───────────────────────────
                String hfileName = String.format("%s_%03d.hfile",
                    opts.tableName.isBlank() ? "merged" : sanitize(opts.tableName), seq);
                Path hfileOut = opts.hfileDir.resolve(hfileName);

                ArrowToHFileConverter converter = new ArrowToHFileConverter(opts.nativeLibPath);
                ConvertResult result = converter.convert(
                    ConvertOptions.builder()
                        .arrowPath(mergedArrow.toAbsolutePath().toString())
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
                        .compressionThreads(opts.compressionThreads)
                        .compressionQueueDepth(opts.compressionQueueDepth)
                        .numericSortFastPath(opts.numericSortFastPath)
                        .excludedColumnPrefixes(opts.excludedColumnPrefixes)
                        .excludedColumns(opts.excludedColumns)
                        .build());

                System.out.printf("%s Convert %s: %s%n",
                    tag, hfileOut.getFileName(),
                    result.isSuccess() ? result.summary() : "FAILED: " + result.errorMessage);

                // ── C. Delete temp merged file ─────────────────────────────────
                try { Files.deleteIfExists(mergedArrow); } catch (IOException ignored) {}

                if (opts.progressCallback != null) {
                    // Use mergedArrow path as the "file" identifier for the callback
                    opts.progressCallback.accept(mergedArrow, result);
                }
                return result;
            });

            // Key = the first Arrow file in this batch (unique identifier)
            futures.put(batch.get(0), future);
        }

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.HOURS);

        // ── Collect results ───────────────────────────────────────────────────
        Map<Path, ConvertResult> results   = new LinkedHashMap<>();
        List<Path>               succeeded = new ArrayList<>();
        List<Path>               failed    = new ArrayList<>();

        for (Map.Entry<Path, Future<ConvertResult>> e : futures.entrySet()) {
            ConvertResult r;
            try {
                r = e.getValue().get();
            } catch (ExecutionException ex) {
                r = ConvertResult.ofError(ConvertResult.INTERNAL_ERROR,
                    "Worker exception: " + ex.getCause().getMessage());
            }
            results.put(e.getKey(), r);
            if (r.isSuccess()) succeeded.add(e.getKey());
            else               failed.add(e.getKey());
        }

        long totalElapsedMs = System.currentTimeMillis() - wallStart;
        BatchConvertResult batch = new BatchConvertResult(
            results, succeeded, failed, totalElapsedMs);

        System.out.println();
        System.out.println("[AdaptiveBatch] " + batch.summary());
        return batch;
    }

    // ── Partition logic ───────────────────────────────────────────────────────

    /**
     * Partition {@code files} into batches according to the three trigger conditions.
     *
     * <p>Trigger conditions (any one satisfied → start new batch):
     * <ol>
     *   <li>Accumulated size ≥ triggerSizeBytes</li>
     *   <li>Accumulated file count ≥ triggerCount</li>
     *   <li>Elapsed time since batch start ≥ triggerIntervalMs
     *       (simulated as wall clock during partitioning; actual timer fires
     *       in streaming use cases — here we use file mtime as a proxy)</li>
     * </ol>
     */
    static List<List<Path>> partitionIntoBatches(List<Path> files, Policy policy) {
        List<List<Path>> batches     = new ArrayList<>();
        List<Path>       currentBatch = new ArrayList<>();
        long             batchSize    = 0;
        long             batchStartMs = getFileMtime(files.get(0));

        for (Path f : files) {
            long fileSize = getFileSize(f);
            long fileMtime = getFileMtime(f);
            boolean sizeTrigger     = (batchSize + fileSize) >= policy.triggerSizeBytes;
            boolean countTrigger    = currentBatch.size() >= policy.triggerCount;
            boolean intervalTrigger = (fileMtime - batchStartMs) >= policy.triggerIntervalMs;

            if (!currentBatch.isEmpty() && (sizeTrigger || countTrigger || intervalTrigger)) {
                batches.add(new ArrayList<>(currentBatch));
                currentBatch.clear();
                batchSize    = 0;
                batchStartMs = fileMtime;

                String reason = sizeTrigger ? "size" : countTrigger ? "count" : "interval";
                System.out.printf(
                    "[AdaptiveBatch] New batch triggered by %s (%d files in previous batch)%n",
                    reason, batches.get(batches.size() - 1).size());
            }

            currentBatch.add(f);
            batchSize += fileSize;
        }

        if (!currentBatch.isEmpty()) batches.add(currentBatch);
        return batches;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static List<Path> collectAndSort(BatchConvertOptions opts) throws IOException {
        List<Path> files = new ArrayList<>();
        if (opts.arrowDir != null) {
            try (var stream = java.nio.file.Files.find(opts.arrowDir, 1,
                    (p, a) -> a.isRegularFile() && p.toString().endsWith(".arrow"))) {
                stream.sorted().forEach(files::add);
            }
        }
        for (Path p : opts.arrowFiles) {
            if (java.nio.file.Files.isRegularFile(p)) files.add(p);
        }
        files.sort(Comparator.comparing(Path::getFileName));
        return files;
    }

    private static long getFileSize(Path f) {
        try { return Files.size(f); } catch (IOException e) { return 0L; }
    }

    private static long getFileMtime(Path f) {
        try { return Files.getLastModifiedTime(f).toMillis(); } catch (IOException e) { return 0L; }
    }

    private static String sanitize(String s) {
        return s.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    private static BatchConvertResult emptyResult() {
        return new BatchConvertResult(Map.of(), List.of(), List.of(), 0L);
    }
}
