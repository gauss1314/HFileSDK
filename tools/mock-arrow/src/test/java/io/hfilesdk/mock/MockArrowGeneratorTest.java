package io.hfilesdk.mock;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class MockArrowGeneratorTest {

    @Test
    void generatedArrowRowsAreShuffledWithinBatch(@TempDir Path tempDir) throws Exception {
        Path arrowFile = tempDir.resolve("mock.arrow");
        MockArrowGenerator.generate(
            TableSchema.TDR_SIGNAL_STOR_20550,
            arrowFile,
            1,
            64,
            new RowGenerator(TableSchema.TDR_SIGNAL_STOR_20550, 42L),
            null
        );

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             InputStream inputStream = Files.newInputStream(arrowFile);
             ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator)) {
            assertTrue(reader.loadNextBatch());
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            BigIntVector refidVector = (BigIntVector) root.getVector("REFID");
            boolean hasDescendingPair = false;
            for (int index = 1; index < root.getRowCount(); index++) {
                if (refidVector.get(index - 1) > refidVector.get(index)) {
                    hasDescendingPair = true;
                    break;
                }
            }
            assertTrue(hasDescendingPair, "generated Arrow batch should not remain sorted by REFID");
        }
    }
}
