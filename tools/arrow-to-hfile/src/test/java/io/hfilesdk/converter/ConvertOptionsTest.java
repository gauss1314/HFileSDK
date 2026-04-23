package io.hfilesdk.converter;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

final class ConvertOptionsTest {

    @Test
    void toConfigJsonIncludesCompressionPipelineFields() {
        ConvertOptions options = ConvertOptions.builder()
            .arrowPath("/tmp/input.arrow")
            .hfilePath("/tmp/output.hfile")
            .rowKeyRule("USER_ID,0,false,0")
            .compression("GZ")
            .compressionLevel(1)
            .compressionThreads(3)
            .compressionQueueDepth(6)
            .numericSortFastPath("on")
            .build();

        String json = options.toConfigJson();
        assertTrue(json.contains("\"compression_threads\":3"), json);
        assertTrue(json.contains("\"compression_queue_depth\":6"), json);
        assertTrue(json.contains("\"numeric_sort_fast_path\":\"on\""), json);
    }

    @Test
    void toConfigJsonOmitsDefaultTimestampUnlessPositive() {
        ConvertOptions defaultTimestamp = ConvertOptions.builder()
            .arrowPath("/tmp/input.arrow")
            .hfilePath("/tmp/output.hfile")
            .rowKeyRule("USER_ID,0,false,0")
            .build();
        assertFalse(defaultTimestamp.toConfigJson().contains("default_timestamp_ms"),
            defaultTimestamp.toConfigJson());

        ConvertOptions fixedTimestamp = ConvertOptions.builder()
            .arrowPath("/tmp/input.arrow")
            .hfilePath("/tmp/output.hfile")
            .rowKeyRule("USER_ID,0,false,0")
            .defaultTimestampMs(1_715_678_900_123L)
            .build();
        assertTrue(fixedTimestamp.toConfigJson().contains("\"default_timestamp_ms\":1715678900123"),
            fixedTimestamp.toConfigJson());
    }

    @Test
    void builderRejectsNegativeCompressionPipelineFields() {
        assertThrows(IllegalArgumentException.class, () -> ConvertOptions.builder()
            .arrowPath("/tmp/input.arrow")
            .hfilePath("/tmp/output.hfile")
            .rowKeyRule("USER_ID,0,false,0")
            .compressionThreads(-1)
            .build());

        assertThrows(IllegalArgumentException.class, () -> ConvertOptions.builder()
            .arrowPath("/tmp/input.arrow")
            .hfilePath("/tmp/output.hfile")
            .rowKeyRule("USER_ID,0,false,0")
            .compressionQueueDepth(-1)
            .build());

        assertThrows(IllegalArgumentException.class, () -> ConvertOptions.builder()
            .arrowPath("/tmp/input.arrow")
            .hfilePath("/tmp/output.hfile")
            .rowKeyRule("USER_ID,0,false,0")
            .numericSortFastPath("maybe")
            .build());
    }
}
