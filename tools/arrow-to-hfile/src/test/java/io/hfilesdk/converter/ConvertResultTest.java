package io.hfilesdk.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

final class ConvertResultTest {

    @Test
    void fromJsonParsesMemoryFields() {
        ConvertResult result = ConvertResult.fromJson("""
            {
              "error_code":0,
              "error_message":"",
              "arrow_batches_read":1,
              "arrow_rows_read":2,
              "kv_written_count":4,
              "kv_skipped_count":0,
              "duplicate_key_count":0,
              "memory_budget_bytes":1048576,
              "tracked_memory_peak_bytes":65536,
              "hfile_size_bytes":2048,
              "elapsed_ms":12,
              "sort_ms":4,
              "write_ms":5
            }
            """, 0);

        assertEquals(1_048_576L, result.memoryBudgetBytes);
        assertEquals(65_536L, result.trackedMemoryPeakBytes);
        assertEquals(12L, result.elapsedMs);
    }
}
