package io.hfilesdk.mockarrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.nio.Buffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class MockArrowGenerator {

    private static final List<String> DEFAULT_REFIDS = List.of(
        "47820206294",
        "47820201464",
        "47820208445",
        "47820203845",
        "47820201769"
    );

    private static final String DEFAULT_COMMIT_TIME = "20260331103209001";
    private static final String DEFAULT_COMMIT_SEQNO = "20260331103209001_0_0";
    private static final String DEFAULT_FILE_NAME =
        "3399654c-5fde-4084-9037-ac355c7abc4a-0_0-0-0_20260331103209001.parquet";
    private static final long DEFAULT_TIME = 1774924319L;
    private static final int DEFAULT_BIT_MAP = 4;

    private record MockRow(
        String hoodieCommitTime,
        String hoodieCommitSeqno,
        String hoodieRecordKey,
        String hoodiePartitionPath,
        String hoodieFileName,
        String refid,
        long time,
        String sigstore,
        int bitMap
    ) {}

    public static void main(String[] args) throws Exception {
        if (ensureJavaNioOpen(args)) {
            return;
        }

        Options options = buildOptions();
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
            return;
        }

        if (!cmd.hasOption("output")) {
            System.err.println("Error: --output is required.");
            printHelp(options);
            System.exit(1);
            return;
        }

        Path output = Path.of(cmd.getOptionValue("output"));
        int rows = Integer.parseInt(cmd.getOptionValue("rows", String.valueOf(DEFAULT_REFIDS.size())));
        long time = Long.parseLong(cmd.getOptionValue("time", String.valueOf(DEFAULT_TIME)));
        int bitMap = Integer.parseInt(cmd.getOptionValue("bit-map", String.valueOf(DEFAULT_BIT_MAP)));
        String commitTime = cmd.getOptionValue("commit-time", DEFAULT_COMMIT_TIME);
        String commitSeqno = cmd.getOptionValue("commit-seqno", DEFAULT_COMMIT_SEQNO);
        String fileName = cmd.getOptionValue("file-name", DEFAULT_FILE_NAME);
        String partitionPath = cmd.getOptionValue("partition-path", "");

        List<String> refids = cmd.hasOption("refids")
            ? parseRefids(cmd.getOptionValue("refids"))
            : buildDefaultRefids(rows);

        List<MockRow> data = buildRows(refids, commitTime, commitSeqno, partitionPath, fileName, time, bitMap);

        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }

        writeArrowStream(output, data);

        System.out.printf("Generated Arrow IPC Stream: %s%n", output.toAbsolutePath());
        System.out.printf("Rows: %d%n", data.size());
        System.out.println("Schema: _hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,REFID,TIME,SIGSTORE,BIT_MAP");
        System.out.println("Suggested rowKeyRule: REFID,0,true,0,LEFT,,,");
        System.out.println("Suggested exclude-prefix: _hoodie");
    }

    private static Options buildOptions() {
        Options o = new Options();
        o.addOption(Option.builder().longOpt("output").hasArg().argName("PATH").desc("输出 Arrow IPC Stream 文件路径").build());
        o.addOption(Option.builder().longOpt("rows").hasArg().argName("N").desc("生成行数，未显式传 refids 时生效").build());
        o.addOption(Option.builder().longOpt("refids").hasArg().argName("CSV").desc("逗号分隔的 REFID 列表").build());
        o.addOption(Option.builder().longOpt("time").hasArg().argName("EPOCH").desc("TIME 列值，默认 1774924319").build());
        o.addOption(Option.builder().longOpt("bit-map").hasArg().argName("N").desc("BIT_MAP 列值，默认 4").build());
        o.addOption(Option.builder().longOpt("commit-time").hasArg().argName("STR").desc("固定 _hoodie_commit_time").build());
        o.addOption(Option.builder().longOpt("commit-seqno").hasArg().argName("STR").desc("固定 _hoodie_commit_seqno").build());
        o.addOption(Option.builder().longOpt("partition-path").hasArg().argName("STR").desc("固定 _hoodie_partition_path").build());
        o.addOption(Option.builder().longOpt("file-name").hasArg().argName("STR").desc("固定 _hoodie_file_name").build());
        o.addOption(Option.builder().longOpt("help").desc("显示帮助").build());
        return o;
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp("mock-arrow", options);
    }

    private static boolean ensureJavaNioOpen(String[] args) throws Exception {
        if (Buffer.class.getModule().isOpen("java.nio", MockArrowGenerator.class.getModule())) {
            return false;
        }
        Path self = Path.of(MockArrowGenerator.class.getProtectionDomain()
            .getCodeSource().getLocation().toURI());
        List<String> cmd = new ArrayList<>();
        cmd.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        cmd.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        if (self.toString().endsWith(".jar")) {
            cmd.add("-jar");
            cmd.add(self.toString());
        } else {
            cmd.add("-cp");
            cmd.add(System.getProperty("java.class.path"));
            cmd.add(MockArrowGenerator.class.getName());
        }
        for (String arg : args) {
            cmd.add(arg);
        }
        Process process = new ProcessBuilder(cmd)
            .inheritIO()
            .start();
        int code = process.waitFor();
        System.exit(code);
        return true;
    }

    private static List<String> parseRefids(String raw) {
        List<String> out = new ArrayList<>();
        for (String token : raw.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("refids must not be empty");
        }
        return out;
    }

    private static List<String> buildDefaultRefids(int rows) {
        if (rows <= DEFAULT_REFIDS.size()) {
            return DEFAULT_REFIDS.subList(0, rows);
        }
        List<String> out = new ArrayList<>(rows);
        out.addAll(DEFAULT_REFIDS);
        long next = 47820210000L;
        while (out.size() < rows) {
            out.add(Long.toString(next++));
        }
        return out;
    }

    private static List<MockRow> buildRows(List<String> refids,
                                           String commitTime,
                                           String commitSeqno,
                                           String partitionPath,
                                           String fileName,
                                           long time,
                                           int bitMap) {
        List<MockRow> rows = new ArrayList<>(refids.size());
        for (String refid : refids) {
            rows.add(new MockRow(
                commitTime,
                commitSeqno,
                "REFID:" + refid + ",TIME:" + time,
                partitionPath,
                fileName,
                refid,
                time,
                "dfx_hbase_sigstor-" + refid,
                bitMap
            ));
        }
        return rows;
    }

    private static void writeArrowStream(Path output, List<MockRow> rows) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VarCharVector commitTimeVector = new VarCharVector("_hoodie_commit_time", allocator);
             VarCharVector commitSeqnoVector = new VarCharVector("_hoodie_commit_seqno", allocator);
             VarCharVector recordKeyVector = new VarCharVector("_hoodie_record_key", allocator);
             VarCharVector partitionPathVector = new VarCharVector("_hoodie_partition_path", allocator);
             VarCharVector fileNameVector = new VarCharVector("_hoodie_file_name", allocator);
             VarCharVector refidVector = new VarCharVector("REFID", allocator);
             BigIntVector timeVector = new BigIntVector("TIME", allocator);
             VarCharVector sigstoreVector = new VarCharVector("SIGSTORE", allocator);
             IntVector bitMapVector = new IntVector("BIT_MAP", allocator)) {

            int estimatedTextBytes = Math.max(256, rows.size() * 64);
            commitTimeVector.allocateNew(estimatedTextBytes, rows.size());
            commitSeqnoVector.allocateNew(estimatedTextBytes, rows.size());
            recordKeyVector.allocateNew(estimatedTextBytes, rows.size());
            partitionPathVector.allocateNew(Math.max(64, rows.size() * 8), rows.size());
            fileNameVector.allocateNew(estimatedTextBytes, rows.size());
            refidVector.allocateNew(estimatedTextBytes, rows.size());
            timeVector.allocateNew(rows.size());
            sigstoreVector.allocateNew(estimatedTextBytes, rows.size());
            bitMapVector.allocateNew(rows.size());

            for (int i = 0; i < rows.size(); ++i) {
                MockRow row = rows.get(i);
                commitTimeVector.setSafe(i, utf8(row.hoodieCommitTime()));
                commitSeqnoVector.setSafe(i, utf8(row.hoodieCommitSeqno()));
                recordKeyVector.setSafe(i, utf8(row.hoodieRecordKey()));
                partitionPathVector.setSafe(i, utf8(row.hoodiePartitionPath()));
                fileNameVector.setSafe(i, utf8(row.hoodieFileName()));
                refidVector.setSafe(i, utf8(row.refid()));
                timeVector.setSafe(i, row.time());
                sigstoreVector.setSafe(i, utf8(row.sigstore()));
                bitMapVector.setSafe(i, row.bitMap());
            }

            commitTimeVector.setValueCount(rows.size());
            commitSeqnoVector.setValueCount(rows.size());
            recordKeyVector.setValueCount(rows.size());
            partitionPathVector.setValueCount(rows.size());
            fileNameVector.setValueCount(rows.size());
            refidVector.setValueCount(rows.size());
            timeVector.setValueCount(rows.size());
            sigstoreVector.setValueCount(rows.size());
            bitMapVector.setValueCount(rows.size());

            try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(
                     commitTimeVector,
                     commitSeqnoVector,
                     recordKeyVector,
                     partitionPathVector,
                     fileNameVector,
                     refidVector,
                     timeVector,
                     sigstoreVector,
                     bitMapVector));
                 ArrowStreamWriter writer = new ArrowStreamWriter(
                     root, null, Channels.newChannel(Files.newOutputStream(output)))) {
                root.setRowCount(rows.size());
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
