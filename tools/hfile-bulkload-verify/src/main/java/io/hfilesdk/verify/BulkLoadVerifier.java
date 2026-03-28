package io.hfilesdk.verify;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.*;

/**
 * Verifies data integrity after Bulk Load by scanning the HBase table
 * and comparing against the original source data.
 *
 * Workflow:
 *   1. C++ SDK writes HFiles
 *   2. BulkLoadHFilesTool loads them into HBase
 *   3. This tool scans the table and confirms all expected KVs are present
 *
 * Usage:
 *   java -jar hfile-bulkload-verify.jar \
 *     --zookeeper localhost:2181 \
 *     --table my_table \
 *     --expected-file /path/to/expected_kvs.tsv \
 *     [--row-count 1000000]
 */
public class BulkLoadVerifier {

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt("zookeeper")
            .hasArg().required().desc("ZooKeeper quorum (host:port)").build());
        opts.addOption(Option.builder().longOpt("table")
            .hasArg().required().desc("HBase table name").build());
        opts.addOption(Option.builder().longOpt("expected-file")
            .hasArg().desc("TSV file: row\\tcf\\tqualifier\\tts\\tvalue").build());
        opts.addOption(Option.builder().longOpt("row-count")
            .hasArg().desc("Expected total row count (quick check)").build());
        opts.addOption(Option.builder().longOpt("sample-rate")
            .hasArg().desc("Fraction of rows to verify deeply (0.0-1.0, default 0.01)").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);

        String zk       = cmd.getOptionValue("zookeeper");
        String tableName = cmd.getOptionValue("table");
        double sampleRate = cmd.hasOption("sample-rate")
            ? Double.parseDouble(cmd.getOptionValue("sample-rate")) : 0.01;

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zk);

        System.out.printf("Connecting to HBase (zk=%s, table=%s)%n", zk, tableName);

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(tableName))) {

            // ── Row count check ────────────────────────────────────────────
            if (cmd.hasOption("row-count")) {
                long expected = Long.parseLong(cmd.getOptionValue("row-count"));
                long actual   = countRows(table);
                System.out.printf("Row count: expected=%,d  actual=%,d%n",
                    expected, actual);
                if (actual < expected) {
                    System.err.printf("FAIL: missing %,d rows%n", expected - actual);
                    System.exit(1);
                }
                System.out.println("Row count: PASS");
            }

            // ── Deep KV verification from file ────────────────────────────
            if (cmd.hasOption("expected-file")) {
                verifyFromFile(table,
                               cmd.getOptionValue("expected-file"),
                               sampleRate);
            }

            System.out.println("Bulk load verification: PASS");
        }
    }

    private static long countRows(Table table) throws IOException {
        Scan scan = new Scan();
        scan.setFilter(new org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter());
        scan.setCaching(10000);
        long count = 0;
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result r : scanner) { ++count; }
        }
        return count;
    }

    private static void verifyFromFile(Table table, String filePath,
                                        double sampleRate) throws IOException {
        long checked = 0, missing = 0, mismatched = 0;
        Random rng = new Random(42);

        System.out.printf("Verifying KVs from %s (sample rate=%.1f%%)%n",
            filePath, sampleRate * 100);

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (rng.nextDouble() > sampleRate) continue;

                // TSV format: row\tcf\tqualifier\tts\tvalue
                String[] parts = line.split("\t", 5);
                if (parts.length < 5) continue;

                byte[] row       = Bytes.fromHex(parts[0]);
                byte[] cf        = Bytes.toBytes(parts[1]);
                byte[] qualifier = Bytes.toBytes(parts[2]);
                long   ts        = Long.parseLong(parts[3]);
                byte[] value     = Bytes.fromHex(parts[4]);

                Get get = new Get(row);
                get.addColumn(cf, qualifier);
                get.setTimeStamp(ts);

                Result result = table.get(get);
                if (result.isEmpty()) {
                    ++missing;
                    if (missing <= 10) {
                        System.err.printf("  MISSING: row=%s  cf=%s  q=%s  ts=%d%n",
                            Bytes.toStringBinary(row),
                            Bytes.toString(cf),
                            Bytes.toString(qualifier), ts);
                    }
                } else {
                    byte[] actual = result.getValue(cf, qualifier);
                    if (!Arrays.equals(actual, value)) {
                        ++mismatched;
                        if (mismatched <= 5) {
                            System.err.printf("  MISMATCH: row=%s  expected=%s  got=%s%n",
                                Bytes.toStringBinary(row),
                                Bytes.toHex(value),
                                actual != null ? Bytes.toHex(actual) : "null");
                        }
                    }
                }
                ++checked;
            }
        }

        System.out.printf("Checked: %,d  Missing: %,d  Mismatched: %,d%n",
            checked, missing, mismatched);

        if (missing > 0 || mismatched > 0) {
            System.err.println("FAIL: data integrity errors detected");
            System.exit(1);
        }
        System.out.println("Deep verification: PASS");
    }
}
