package io.hfilesdk.verify;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Verifies that C++ SDK-generated HFile v3 files can be correctly read
 * by HBase's native HFile reader.
 *
 * Usage:
 *   java -jar hfile-verify.jar --hfile /path/to/file.hfile
 *   java -jar hfile-verify.jar --hfile-dir /path/to/staging/cf/
 */
public class HFileVerifier {

    private static final class Expectations {
        Integer majorVersion;
        Long entryCount;
        String compression;
        String encoding;
        List<String> rows = List.of();
        List<String> families = List.of();
        List<String> qualifiers = List.of();
        List<String> values = List.of();
        List<String> types = List.of();
        List<Long> timestamps = List.of();
    }

    private int totalFiles   = 0;
    private int passedFiles  = 0;
    private int failedFiles  = 0;
    private long totalKVs    = 0;

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt("hfile")
            .hasArg().desc("Path to a single HFile").build());
        opts.addOption(Option.builder().longOpt("hfile-dir")
            .hasArg().desc("Directory containing HFile(s)").build());
        opts.addOption(Option.builder().longOpt("verbose")
            .desc("Print each KV").build());
        opts.addOption(Option.builder().longOpt("max-kvs")
            .hasArg().desc("Max KVs to scan per file (default: unlimited)").build());
        opts.addOption(Option.builder().longOpt("expect-major-version")
            .hasArg().desc("Expected HFile major version").build());
        opts.addOption(Option.builder().longOpt("expect-entry-count")
            .hasArg().desc("Expected HFile entry count").build());
        opts.addOption(Option.builder().longOpt("expect-compression")
            .hasArg().desc("Expected compression name").build());
        opts.addOption(Option.builder().longOpt("expect-encoding")
            .hasArg().desc("Expected data block encoding name").build());
        opts.addOption(Option.builder().longOpt("expect-rows")
            .hasArg().desc("Comma-separated expected row sequence").build());
        opts.addOption(Option.builder().longOpt("expect-families")
            .hasArg().desc("Comma-separated expected family sequence").build());
        opts.addOption(Option.builder().longOpt("expect-qualifiers")
            .hasArg().desc("Comma-separated expected qualifier sequence").build());
        opts.addOption(Option.builder().longOpt("expect-values")
            .hasArg().desc("Comma-separated expected UTF-8 value sequence").build());
        opts.addOption(Option.builder().longOpt("expect-types")
            .hasArg().desc("Comma-separated expected cell type sequence").build());
        opts.addOption(Option.builder().longOpt("expect-timestamps")
            .hasArg().desc("Comma-separated expected timestamp sequence").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(opts, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            new HelpFormatter().printHelp("hfile-verify", opts);
            System.exit(1);
            return;
        }

        boolean verbose = cmd.hasOption("verbose");
        long maxKVs = cmd.hasOption("max-kvs")
            ? Long.parseLong(cmd.getOptionValue("max-kvs"))
            : Long.MAX_VALUE;
        Expectations expectations = parseExpectations(cmd);

        List<File> files = new ArrayList<>();

        if (cmd.hasOption("hfile")) {
            files.add(new File(cmd.getOptionValue("hfile")));
        } else if (cmd.hasOption("hfile-dir")) {
            File dir = new File(cmd.getOptionValue("hfile-dir"));
            if (!dir.isDirectory()) {
                System.err.println("Not a directory: " + dir);
                System.exit(1);
            }
            // Recursively find HFile files
            collectHFiles(dir, files);
        } else {
            System.err.println("Must specify --hfile or --hfile-dir");
            new HelpFormatter().printHelp("hfile-verify", opts);
            System.exit(1);
        }

        if (files.isEmpty()) {
            System.err.println("No HFiles found.");
            System.exit(1);
        }

        HFileVerifier verifier = new HFileVerifier();
        for (File f : files) {
            verifier.verifyFile(f, verbose, maxKVs, expectations);
        }

        System.out.printf("\n=== Summary ===%n");
        System.out.printf("Files verified : %d%n", verifier.totalFiles);
        System.out.printf("Passed         : %d%n", verifier.passedFiles);
        System.out.printf("Failed         : %d%n", verifier.failedFiles);
        System.out.printf("Total KVs read : %,d%n", verifier.totalKVs);

        System.exit(verifier.failedFiles > 0 ? 1 : 0);
    }

    private static void collectHFiles(File dir, List<File> out) {
        File[] children = dir.listFiles();
        if (children == null) return;
        for (File f : children) {
            if (f.isDirectory()) {
                collectHFiles(f, out);
            } else if (!f.getName().startsWith(".")) {
                out.add(f);
            }
        }
    }

    private static Expectations parseExpectations(CommandLine cmd) {
        Expectations expectations = new Expectations();
        if (cmd.hasOption("expect-major-version")) {
            expectations.majorVersion = Integer.parseInt(cmd.getOptionValue("expect-major-version"));
        }
        if (cmd.hasOption("expect-entry-count")) {
            expectations.entryCount = Long.parseLong(cmd.getOptionValue("expect-entry-count"));
        }
        if (cmd.hasOption("expect-compression")) {
            expectations.compression = cmd.getOptionValue("expect-compression");
        }
        if (cmd.hasOption("expect-encoding")) {
            expectations.encoding = cmd.getOptionValue("expect-encoding");
        }
        if (cmd.hasOption("expect-rows")) {
            expectations.rows = parseCsv(cmd.getOptionValue("expect-rows"));
        }
        if (cmd.hasOption("expect-families")) {
            expectations.families = parseCsv(cmd.getOptionValue("expect-families"));
        }
        if (cmd.hasOption("expect-qualifiers")) {
            expectations.qualifiers = parseCsv(cmd.getOptionValue("expect-qualifiers"));
        }
        if (cmd.hasOption("expect-values")) {
            expectations.values = parseCsv(cmd.getOptionValue("expect-values"));
        }
        if (cmd.hasOption("expect-types")) {
            expectations.types = parseCsv(cmd.getOptionValue("expect-types"));
        }
        if (cmd.hasOption("expect-timestamps")) {
            expectations.timestamps = parseCsv(cmd.getOptionValue("expect-timestamps")).stream()
                .map(Long::parseLong)
                .toList();
        }
        return expectations;
    }

    private static List<String> parseCsv(String raw) {
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? List.of() : Arrays.asList(trimmed.split(",", -1));
    }

    private void verifyFile(File file, boolean verbose, long maxKVs, Expectations expectations) {
        ++totalFiles;
        System.out.printf("Verifying: %s%n", file.getAbsolutePath());

        Configuration conf = HBaseConfiguration.create();
        try {
            Path path = new Path(file.getAbsolutePath());
            FileSystem fs = FileSystem.getLocal(conf);

            CacheConfig cacheConf = new CacheConfig(conf);
            Reader reader = HFile.createReader(fs, path, cacheConf, true, conf);

            // ── Validate trailer / metadata ────────────────────────────────
            HFileContext ctx = reader.getFileContext();
            System.out.printf("  Major version   : %d%n",
                reader.getTrailer().getMajorVersion());
            System.out.printf("  Minor version   : %d%n",
                reader.getTrailer().getMinorVersion());
            System.out.printf("  Entry count     : %,d%n",
                reader.getEntries());
            System.out.printf("  Data blocks     : %d%n",
                reader.getTrailer().getDataIndexCount());
            System.out.printf("  Compression     : %s%n",
                ctx.getCompression());
            System.out.printf("  Encoding        : %s%n",
                ctx.getDataBlockEncoding());

            // Validate version
            if (reader.getTrailer().getMajorVersion() != 3) {
                throw new RuntimeException(
                    "Expected HFile v3, got v" +
                    reader.getTrailer().getMajorVersion());
            }
            if (expectations.majorVersion != null &&
                reader.getTrailer().getMajorVersion() != expectations.majorVersion) {
                throw new RuntimeException(
                    "Expected major version " + expectations.majorVersion +
                    ", got " + reader.getTrailer().getMajorVersion());
            }
            if (expectations.entryCount != null &&
                reader.getEntries() != expectations.entryCount) {
                throw new RuntimeException(
                    "Expected entry count " + expectations.entryCount +
                    ", got " + reader.getEntries());
            }
            if (expectations.compression != null &&
                !ctx.getCompression().name().equalsIgnoreCase(expectations.compression)) {
                throw new RuntimeException(
                    "Expected compression " + expectations.compression +
                    ", got " + ctx.getCompression().name());
            }
            if (expectations.encoding != null &&
                !ctx.getDataBlockEncoding().name().equalsIgnoreCase(expectations.encoding)) {
                throw new RuntimeException(
                    "Expected encoding " + expectations.encoding +
                    ", got " + ctx.getDataBlockEncoding().name());
            }

            // ── Validate FileInfo mandatory fields ─────────────────────────
            validateFileInfo(reader);

            // ── Scan all KVs and check sort order ─────────────────────────
            HFileScanner scanner = reader.getScanner(conf, false, false);
            boolean seekResult = scanner.seekTo();
            if (!seekResult) {
                throw new RuntimeException("Failed to seek to first cell");
            }

            Cell prevCell = null;
            long kvCount  = 0;
            boolean sortOk = true;
            List<String> actualRows = new ArrayList<>();
            List<String> actualFamilies = new ArrayList<>();
            List<String> actualQualifiers = new ArrayList<>();
            List<String> actualValues = new ArrayList<>();
            List<String> actualTypes = new ArrayList<>();
            List<Long> actualTimestamps = new ArrayList<>();

            do {
                Cell cell = scanner.getCell();
                if (cell == null) break;

                if (prevCell != null) {
                    int cmp = org.apache.hadoop.hbase.CellComparator.getInstance().compare(prevCell, cell);
                    if (cmp >= 0) {
                        System.err.printf(
                            "  SORT ERROR at KV %d: %s >= %s%n",
                            kvCount,
                            Bytes.toStringBinary(CellUtil.cloneRow(prevCell)),
                            Bytes.toStringBinary(CellUtil.cloneRow(cell)));
                        sortOk = false;
                    }
                }
                actualRows.add(Bytes.toString(CellUtil.cloneRow(cell)));
                actualFamilies.add(Bytes.toString(CellUtil.cloneFamily(cell)));
                actualQualifiers.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
                actualValues.add(Bytes.toString(CellUtil.cloneValue(cell)));
                actualTypes.add(cell.getType().toString());
                actualTimestamps.add(cell.getTimestamp());

                if (verbose && kvCount < 20) {
                    System.out.printf(
                        "  KV[%d]: row=%s  col=%s:%s  ts=%d  type=%s  value=%s%n",
                        kvCount,
                        Bytes.toStringBinary(CellUtil.cloneRow(cell)),
                        Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
                        Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
                        cell.getTimestamp(),
                        cell.getType(),
                        Bytes.toStringBinary(CellUtil.cloneValue(cell)));
                }

                prevCell = cell;
                ++kvCount;
                ++totalKVs;
                if (kvCount >= maxKVs) break;
            } while (scanner.next());

            reader.close();

            if (!sortOk) throw new RuntimeException("Sort order violation detected");
            if (!expectations.rows.isEmpty() && !actualRows.equals(expectations.rows)) {
                throw new RuntimeException(
                    "Row sequence mismatch: expected=" + expectations.rows +
                    ", actual=" + actualRows);
            }
            if (!expectations.families.isEmpty() && !actualFamilies.equals(expectations.families)) {
                throw new RuntimeException(
                    "Family sequence mismatch: expected=" + expectations.families +
                    ", actual=" + actualFamilies);
            }
            if (!expectations.qualifiers.isEmpty() && !actualQualifiers.equals(expectations.qualifiers)) {
                throw new RuntimeException(
                    "Qualifier sequence mismatch: expected=" + expectations.qualifiers +
                    ", actual=" + actualQualifiers);
            }
            if (!expectations.values.isEmpty() && !actualValues.equals(expectations.values)) {
                throw new RuntimeException(
                    "Value sequence mismatch: expected=" + expectations.values +
                    ", actual=" + actualValues);
            }
            if (!expectations.types.isEmpty() && !actualTypes.equals(expectations.types)) {
                throw new RuntimeException(
                    "Type sequence mismatch: expected=" + expectations.types +
                    ", actual=" + actualTypes);
            }
            if (!expectations.timestamps.isEmpty() && !actualTimestamps.equals(expectations.timestamps)) {
                throw new RuntimeException(
                    "Timestamp sequence mismatch: expected=" + expectations.timestamps +
                    ", actual=" + actualTimestamps);
            }

            System.out.printf("  KVs scanned     : %,d%n", kvCount);
            System.out.println("  Result          : PASS");
            ++passedFiles;

        } catch (Exception e) {
            System.err.printf("  Result          : FAIL — %s%n", e.getMessage());
            if (verbose) e.printStackTrace();
            ++failedFiles;
        }
    }

    private void validateFileInfo(Reader reader) throws Exception {
        String[] required = {
            "hfile.LASTKEY",
            "hfile.AVG_KEY_LEN",
            "hfile.AVG_VALUE_LEN",
            "hfile.MAX_TAGS_LEN",
            "hfile.COMPARATOR",
            "DATA_BLOCK_ENCODING",
            "hfile.KEY_VALUE_VERSION",
            "hfile.MAX_MEMSTORE_TS_KEY",
            "hfile.CREATE_TIME_TS",
            "hfile.LEN_OF_BIGGEST_CELL",
        };

        HFileInfo fileInfo = ((HFileReaderImpl) reader).getHFileInfo();
        for (String key : required) {
            byte[] value = fileInfo.get(Bytes.toBytes(key));
            if (value == null) {
                throw new RuntimeException("Missing FileInfo key: " + key);
            }
        }

        String comparator = reader.getTrailer().getComparatorClassName();
        if (comparator == null || comparator.isEmpty()) {
            throw new RuntimeException("Missing comparator class name in trailer");
        }
        if (!comparator.contains("CellComparator") &&
            !comparator.contains("MetaCellComparator")) {
            throw new RuntimeException(
                "Unexpected comparator: " + comparator);
        }
        System.out.printf("  Comparator      : %s%n", comparator);
    }
}
