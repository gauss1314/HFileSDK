package io.hfilesdk.perf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BulkLoadPerfRunnerTest {

    @Test
    void workerModeWritesStructuredResult(@TempDir Path tempDir) throws Exception {
        Path inputDir = tempDir.resolve("input");
        Path outputDir = tempDir.resolve("output");
        Path resultJson = tempDir.resolve("worker-result.json");
        Files.createDirectories(inputDir);
        writeArrowStream(inputDir.resolve("worker.arrow"),
            List.of("user-000000000001", "user-000000000002"),
            List.of("value1", "value2"));

        BulkLoadPerfRunner.main(new String[] {
            "--worker-mode",
            "--worker-implementation", "arrow-to-hfile-java",
            "--worker-input-dir", inputDir.toString(),
            "--worker-output-dir", outputDir.toString(),
            "--worker-result-json", resultJson.toString(),
            "--worker-iteration-index", "1",
            "--table", "perf_table",
            "--rule", "USER_ID,0,false,0",
            "--cf", "cf",
            "--compression", "GZ",
            "--encoding", "NONE",
            "--bloom", "row",
            "--block-size", "65536"
        });

        String json = Files.readString(resultJson, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"implementation\":\"arrow-to-hfile-java\""));
        assertTrue(json.contains("\"success\":true"));
        assertTrue(json.contains("\"detail\":"));
    }

    @Test
    void parentModeReportContainsProcessMetrics(@TempDir Path tempDir) throws Exception {
        Path reportJson = tempDir.resolve("report.json");

        BulkLoadPerfRunner.main(new String[] {
            "--implementations", "arrow-to-hfile-java",
            "--scenario-filter", "single-001mb",
            "--work-dir", tempDir.toString(),
            "--report-json", reportJson.toString(),
            "--rule", "USER_ID,0,false,0",
            "--java-xmx-mb", "256",
            "--java-direct-memory-mb", "256"
        });

        String json = Files.readString(reportJson, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"process_peak_rss_bytes\":"));
        assertTrue(json.contains("\"process_user_cpu_ms\":"));
        assertTrue(json.contains("\"process_exit_code\":0"));
        assertTrue(json.contains("\"process_control_mode\":"));
        assertFalse(Files.exists(tempDir.resolve("single-001mb")));
    }

    @Test
    void defaultReportJsonIsWrittenToCurrentWorkingDirectory(@TempDir Path tempDir) throws Exception {
        Path workDir = tempDir.resolve("work");
        Path expectedReport = tempDir.resolve("perf-matrix-report.json");

        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.lang=ALL-UNNAMED");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(BulkLoadPerfRunner.class.getName());
        command.add("--implementations");
        command.add("arrow-to-hfile-java");
        command.add("--scenario-filter");
        command.add("single-001mb");
        command.add("--work-dir");
        command.add(workDir.toString());
        command.add("--rule");
        command.add("USER_ID,0,false,0");
        command.add("--java-xmx-mb");
        command.add("256");
        command.add("--java-direct-memory-mb");
        command.add("256");

        Process process = new ProcessBuilder(command)
            .directory(tempDir.toFile())
            .redirectErrorStream(true)
            .start();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        process.getInputStream().transferTo(output);
        int exitCode = process.waitFor();
        assertEquals(0, exitCode, output.toString(StandardCharsets.UTF_8));
        assertTrue(Files.isRegularFile(expectedReport));
        assertFalse(Files.exists(workDir.resolve("perf-matrix-report.json")));
    }

    @Test
    void jniWorkerDirectoryModePreservesConfiguredParallelism(@TempDir Path tempDir) throws Exception {
        Path nativeLib = findNativeLib();
        Assumptions.assumeTrue(nativeLib != null, "native lib not available for JNI worker test");

        Path inputDir = tempDir.resolve("input");
        Path outputDir = tempDir.resolve("output");
        Path resultJson = tempDir.resolve("worker-result.json");
        Files.createDirectories(inputDir);
        writeArrowStream(inputDir.resolve("part-000.arrow"), 4096, 1024);
        writeArrowStream(inputDir.resolve("part-001.arrow"), 4096, 1024);
        writeArrowStream(inputDir.resolve("part-002.arrow"), 4096, 1024);
        assertTrue(Files.size(inputDir.resolve("part-000.arrow")) > 1024 * 1024);
        assertTrue(Files.size(inputDir.resolve("part-001.arrow")) > 1024 * 1024);
        assertTrue(Files.size(inputDir.resolve("part-002.arrow")) > 1024 * 1024);

        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.lang=ALL-UNNAMED");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(BulkLoadPerfRunner.class.getName());
        command.add("--worker-mode");
        command.add("--worker-implementation");
        command.add("arrow-to-hfile");
        command.add("--worker-input-dir");
        command.add(inputDir.toString());
        command.add("--worker-output-dir");
        command.add(outputDir.toString());
        command.add("--worker-result-json");
        command.add(resultJson.toString());
        command.add("--worker-iteration-index");
        command.add("1");
        command.add("--native-lib");
        command.add(nativeLib.toString());
        command.add("--table");
        command.add("perf_table");
        command.add("--parallelism");
        command.add("3");
        command.add("--merge-threshold");
        command.add("1");
        command.add("--jni-sdk-max-memory-mb");
        command.add("64");
        command.add("--rule");
        command.add("USER_ID,0,false,0");
        command.add("--cf");
        command.add("cf");
        command.add("--compression");
        command.add("GZ");
        command.add("--encoding");
        command.add("NONE");
        command.add("--bloom");
        command.add("row");
        command.add("--block-size");
        command.add("65536");

        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        process.getInputStream().transferTo(output);
        int exitCode = process.waitFor();
        assertEquals(0, exitCode, output.toString(StandardCharsets.UTF_8));

        String json = Files.readString(resultJson, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"implementation\":\"arrow-to-hfile\""));
        assertTrue(json.contains("\"success\":true"), json);
        assertTrue(json.contains("\"strategy\":\"PARALLEL-CONVERT\""), json);
        assertTrue(json.contains("\"worker_parallelism\":3"), json);
    }

    private static void writeArrowStream(Path path,
                                         List<String> ids,
                                         List<String> values) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VarCharVector idVector = new VarCharVector("USER_ID", allocator);
             VarCharVector valueVector = new VarCharVector("VALUE", allocator)) {
            idVector.allocateNew(ids.size() * 32, ids.size());
            valueVector.allocateNew(values.size() * 32, values.size());
            for (int i = 0; i < ids.size(); ++i) {
                idVector.setSafe(i, ids.get(i).getBytes(StandardCharsets.UTF_8));
                valueVector.setSafe(i, values.get(i).getBytes(StandardCharsets.UTF_8));
            }
            idVector.setValueCount(ids.size());
            valueVector.setValueCount(values.size());
            try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(idVector, valueVector));
                 ArrowStreamWriter writer = new ArrowStreamWriter(
                     root, null, Channels.newChannel(Files.newOutputStream(path)))) {
                root.setRowCount(ids.size());
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }
    }

    private static void writeArrowStream(Path path, int rowCount, int payloadBytes) throws Exception {
        List<String> ids = new ArrayList<>(rowCount);
        List<String> values = new ArrayList<>(rowCount);
        String payload = "x".repeat(payloadBytes);
        for (int i = 0; i < rowCount; i++) {
            ids.add(String.format("user-%012d", i));
            values.add(payload + i);
        }
        writeArrowStream(path, ids, values);
    }

    private static Path findNativeLib() {
        String nativeDir = System.getenv("HFILESDK_NATIVE_DIR");
        if (nativeDir != null && !nativeDir.isBlank()) {
            Path resolved = findNativeLibInDir(Path.of(nativeDir));
            if (resolved != null) {
                return resolved;
            }
        }
        Path cwd = Path.of("").toAbsolutePath().normalize();
        for (Path candidate : List.of(cwd.resolve("../../build"), cwd.resolve("../build"), cwd.resolve("build"))) {
            Path resolved = findNativeLibInDir(candidate.normalize());
            if (resolved != null) {
                return resolved;
            }
        }
        return null;
    }

    private static Path findNativeLibInDir(Path dir) {
        for (String fileName : List.of("libhfilesdk.dylib", "libhfilesdk.so", "hfilesdk.dll")) {
            Path candidate = dir.resolve(fileName);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        return null;
    }
}
