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
              "numeric_sort_fast_path_mode":"on",
              "numeric_sort_fast_path_used":true,
              "hfile_size_bytes":2048,
              "elapsed_ms":12,
              "sort_ms":4,
              "write_ms":5,
              "data_block_encode_ms":1,
              "data_block_compress_ms":2,
              "data_block_write_ms":3,
              "leaf_index_write_ms":4,
              "bloom_chunk_write_ms":5,
              "load_on_open_write_ms":6,
              "data_block_count":7,
              "leaf_index_block_count":8,
              "bloom_chunk_flush_count":9,
              "load_on_open_block_count":10
            }
            """, 0);

        assertEquals(1_048_576L, result.memoryBudgetBytes);
        assertEquals(65_536L, result.trackedMemoryPeakBytes);
        assertEquals("on", result.numericSortFastPathMode);
        assertEquals(true, result.numericSortFastPathUsed);
        assertEquals(12L, result.elapsedMs);
        assertEquals(3L, result.dataBlockWriteMs);
        assertEquals(10L, result.loadOnOpenBlockCount);
    }
}
