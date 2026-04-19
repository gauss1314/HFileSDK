package io.hfilesdk.converter;

import static org.junit.jupiter.api.Assertions.assertThrows;
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
