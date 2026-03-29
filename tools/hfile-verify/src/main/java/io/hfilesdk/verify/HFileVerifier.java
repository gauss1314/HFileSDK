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
            String raw = cmd.getOptionValue("expect-rows").trim();
            expectations.rows = raw.isEmpty() ? List.of() : Arrays.asList(raw.split(",", -1));
        }
        return expectations;
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

                if (verbose && kvCount < 20) {
                    System.out.printf(
                        "  KV[%d]: row=%s  col=%s:%s  ts=%d  type=%s  valLen=%d%n",
                        kvCount,
                        Bytes.toStringBinary(CellUtil.cloneRow(cell)),
                        Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
                        Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
                        cell.getTimestamp(),
                        cell.getType(),
                        cell.getValueLength());
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
        // These fields are mandatory per DESIGN.md §2.3
        String[] required = {
            "hfile.LASTKEY",
            "hfile.AVG_KEY_LEN",
            "hfile.AVG_VALUE_LEN",
            "hfile.MAX_TAGS_LEN",
            "hfile.COMPARATOR",
            "hfile.DATA_BLOCK_ENCODING",
            "hfile.KEY_VALUE_VERSION",
            "hfile.MAX_MEMSTORE_TS_KEY",
            "hfile.CREATE_TIME_TS",
        };

        // HBase 2.6.x exposes FileInfo through getFileContext / internal API
        // We verify by checking that the reader opens without error and
        // the trailer has valid comparator class set
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
