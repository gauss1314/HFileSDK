package io.hfilesdk.mock;

import java.util.Random;

/**
 * Generates realistic per-row field values for each supported table schema.
 *
 * <p>Each instance is stateful (maintains auto-increment counters, random seed)
 * and produces one logical row at a time via {@link #nextRow()}.
 *
 * <h3>Design goals</h3>
 * <ul>
 *   <li>REFID / STARTTIME are unique and monotonically increasing per row</li>
 *   <li>String fields have realistic, variable-length values so that the
 *       generated file size is predictable and configurable</li>
 *   <li>All numeric ranges match the sample data provided in the spec</li>
 * </ul>
 */
public class RowGenerator {

    // ── Sample anchor values from the spec ───────────────────────────────────
    // tdr_signal_stor_20550 samples:
    //   REFID:   183971587293920  (15-digit, incremented by 10–500 per row)
    //   TIME:    1775537735       (epoch-s, stable across a batch, drifts slowly)
    //   BIT_MAP: 12              (constant in samples)
    //   no:      "0"             (0-9 cycling)
    private static final long REFID_START    = 183971587293920L;
    private static final long TIME_BASE      = 1775537735L;

    // tdr_mock anchors
    private static final long STARTTIME_BASE = 1_700_000_000_000L; // epoch-ms Jan 2024
    private static final long IMSI_BASE      = 460001234560000L;   // 15-digit IMSI

    private final TableSchema schema;
    private final Random      rng;

    // ── Per-row state ─────────────────────────────────────────────────────────
    private long refid;
    private long starttime;
    private long imsiBase;
    private int  rowIndex;  // global counter used to derive cycling fields

    public RowGenerator(TableSchema schema, long seed) {
        this.schema    = schema;
        this.rng       = new Random(seed);
        this.refid     = REFID_START;
        this.starttime = STARTTIME_BASE;
        this.imsiBase  = IMSI_BASE;
        this.rowIndex  = 0;
    }

    /**
     * Generate the next logical row as an Object array.
     * The array length equals the number of columns in the schema.
     * Types:
     *   {@code Long} for bigint columns
     *   {@code String} for string/utf8 columns
     */
    public Object[] nextRow() {
        Object[] row = switch (schema) {
            case TDR_SIGNAL_STOR_20550 -> nextRow20550();
            case TDR_MOCK -> nextRowMock();
        };
        rowIndex++;
        return row;
    }

    // ─── tdr_signal_stor_20550 ────────────────────────────────────────────────

    private Object[] nextRow20550() {
        // REFID: unique, monotonically increasing; increment by 10-500 to simulate
        // real data gaps (IDs are not gapless in production)
        long currentRefid = refid;
        refid += 10 + rng.nextInt(490);

        // TIME: epoch-s, drifts +0 or +1 every few rows
        long time = TIME_BASE + (rowIndex / 3);

        // SIGSTORE: semicolon-terminated CSV, variable length (50-300 chars)
        String sigstore = buildSigstore(time);

        // BIT_MAP: mostly 12, occasionally other small values
        long bitmap = (rowIndex % 20 == 0) ? rng.nextInt(64) : 12L;

        // no: single digit "0"-"9", cycles 0→9
        String no = String.valueOf(rowIndex % 10);

        return new Object[]{ currentRefid, time, sigstore, bitmap, no };
    }

    /**
     * Build a realistic SIGSTORE value.
     * Format from spec: "cs,{svc},{mcc},{mnc},{imsi},0,{rat},...,{rsrp},{snr},{ber},; "
     * Multiple records can be concatenated (separated by ";") for wider rows.
     *
     * The number of records is varied (1-3) to produce variable-length values.
     */
    private String buildSigstore(long time) {
        int numRecords = 1 + (rowIndex % 3);  // 1, 2, or 3 signal records
        StringBuilder sb = new StringBuilder();
        for (int r = 0; r < numRecords; r++) {
            long imsi = IMSI_BASE + rng.nextInt(1_000_000);
            int  svc  = 12111;
            int  mcc  = (rowIndex % 5 == 0) ? 120 : 100;
            int  mnc  = (rowIndex % 5 == 0) ? 121 : 100;
            int  rat  = rng.nextInt(10);
            int  cell = 34 + rng.nextInt(10);
            int  rsrp = rng.nextInt(120);
            int  snr  = rng.nextInt(40);
            int  ber  = 10;
            int  bytes= 11 + rng.nextInt(500);
            sb.append(String.format(
                "cs,%d,%d,%d,%d,0,%d,,,%d,%d,,%d,%d,,,%d,%d,%d,",
                svc, mcc, mnc, imsi, rat, time, time, cell, rsrp, ber, snr, bytes));
        }
        sb.append("; ");
        return sb.toString();
    }

    // ─── tdr_mock ────────────────────────────────────────────────

    private Object[] nextRowMock() {
        // STARTTIME: epoch-ms, increments by 1-10 ms per row
        long currentStarttime = starttime;
        starttime += 1 + rng.nextInt(10);

        // IMSI: 15-digit, unique per row
        long imsi = imsiBase + rowIndex;
        String imsiStr = String.format("%015d", imsi);

        // MSISDN: 11-digit (starts with "138")
        String msisdn = String.format("138%08d", rng.nextInt(100_000_000));

        // DURATION: session duration 1-3600 seconds
        long duration = 1 + rng.nextInt(3600);

        // BYTES_UP / BYTES_DW: bytes transferred (0 to 100MB)
        long bytesUp = rng.nextLong(100_000_000L);
        long bytesDw = rng.nextLong(500_000_000L);

        // CELL_ID: 6-char hex
        String cellId = String.format("%06X", rng.nextInt(0xFFFFFF));

        // RAT_TYPE: radio access technology
        String[] rats = { "LTE", "NR", "UMTS" };
        String ratType = rats[rowIndex % rats.length];

        return new Object[]{ currentStarttime, imsiStr, msisdn, duration,
                             bytesUp, bytesDw, cellId, ratType };
    }

    /** Estimate the approximate serialised byte size of one row (for size budgeting). */
    public int estimateRowBytes() {
        return switch (schema) {
            // REFID(8) + TIME(8) + SIGSTORE(avg 120B) + BITMAP(8) + NO(1) + overhead(20)
            case TDR_SIGNAL_STOR_20550 -> 165;
            // STARTTIME(8) + IMSI(15) + MSISDN(11) + DURATION(8)*3 + CELL(6) + RAT(3) + overhead(30)
            case TDR_MOCK -> 105;
        };
    }
}
