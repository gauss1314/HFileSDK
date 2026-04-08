package io.hfilesdk.converter;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Aggregated result of a batch conversion job.
 */
public final class BatchConvertResult {

    /** Per-file outcomes, keyed by Arrow file path. */
    public final Map<Path, ConvertResult> results;

    /** Files that converted successfully. */
    public final List<Path> succeeded;

    /** Files that failed conversion. */
    public final List<Path> failed;

    /** Total elapsed wall-clock time in milliseconds. */
    public final long totalElapsedMs;

    /** Sum of all converted KVs. */
    public final long totalKvWritten;

    /** Sum of all output HFile sizes in bytes. */
    public final long totalHfileSizeBytes;

    public BatchConvertResult(
            Map<Path, ConvertResult> results,
            List<Path> succeeded,
            List<Path> failed,
            long totalElapsedMs) {
        this.results            = Map.copyOf(results);
        this.succeeded          = List.copyOf(succeeded);
        this.failed             = List.copyOf(failed);
        this.totalElapsedMs     = totalElapsedMs;
        this.totalKvWritten     = results.values().stream()
                                         .mapToLong(r -> r.kvWrittenCount).sum();
        this.totalHfileSizeBytes = results.values().stream()
                                          .mapToLong(r -> r.hfileSizeBytes).sum();
    }

    public boolean isFullSuccess() { return failed.isEmpty(); }

    public String summary() {
        return String.format(
            "Batch: %d files  success=%d  failed=%d  kvs=%,d  " +
            "hfiles=%.1fMB  elapsed=%dms",
            results.size(), succeeded.size(), failed.size(),
            totalKvWritten,
            totalHfileSizeBytes / 1024.0 / 1024.0,
            totalElapsedMs);
    }

    @Override public String toString() { return "BatchConvertResult{" + summary() + "}"; }
}
