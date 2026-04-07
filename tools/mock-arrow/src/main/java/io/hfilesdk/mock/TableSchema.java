package io.hfilesdk.mock;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.*;

import java.util.List;

/**
 * Defines supported table schemas for the mock Arrow generator.
 *
 * <p>Each schema knows:
 * <ul>
 *   <li>The HBase table name</li>
 *   <li>The Arrow schema (field names + types)</li>
 *   <li>The rowKeyRule string consumed by HFileSDK's converter</li>
 *   <li>How to allocate / populate a batch of vectors</li>
 * </ul>
 *
 * <h3>Schemas</h3>
 * <ol>
 *   <li>{@link #TDR_MOCK} — original telecom CDR table</li>
 *   <li>{@link #TDR_SIGNAL_STOR_20550} — new signal-store table (default)</li>
 * </ol>
 */
public enum TableSchema {

    // ─── tdr_mock ────────────────────────────────────────────────
    // Original telecom CDR-like table.
    // Columns (post-hoodie-exclusion, indices 0-based):
    //   0: STARTTIME (bigint) — call/session start epoch-ms
    //   1: IMSI      (string) — 15-digit IMSI
    //   2: MSISDN    (string) — 11-digit MSISDN
    //   3: DURATION  (bigint) — session duration in seconds
    //   4: BYTES_UP  (bigint) — uplink bytes
    //   5: BYTES_DW  (bigint) — downlink bytes
    //   6: CELL_ID   (string) — 6-char hex cell identifier
    //   7: RAT_TYPE  (string) — radio access type: "LTE"/"NR"/"UMTS"
    //
    // Row key: STARTTIME (10-digit, left-padded) + IMSI (reversed 15-char)
    //          rule: "STARTTIME,0,false,10#IMSI,1,true,15"
    TDR_MOCK(
        "tdr_mock",
        "STARTTIME,0,false,10#IMSI,1,true,15",
        List.of(
            new Field("STARTTIME", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("IMSI",      FieldType.nullable(new ArrowType.Utf8()),        null),
            new Field("MSISDN",    FieldType.nullable(new ArrowType.Utf8()),        null),
            new Field("DURATION",  FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("BYTES_UP",  FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("BYTES_DW",  FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("CELL_ID",   FieldType.nullable(new ArrowType.Utf8()),        null),
            new Field("RAT_TYPE",  FieldType.nullable(new ArrowType.Utf8()),        null)
        )
    ),

    // ─── tdr_signal_stor_20550 ────────────────────────────────────────────────
    // New signal-store table.
    // Sample data:
    //   183971587293920 1775537735 cs,12111,100,100,...; 12 0
    //   REFID           TIME       SIGSTORE              BIT_MAP no
    //
    // Columns (0-based):
    //   0: REFID     (bigint) — unique 15-digit auto-increment key
    //   1: TIME      (bigint) — epoch seconds (10-digit)
    //   2: SIGSTORE  (string) — semicolon-terminated CSV signal record
    //   3: BIT_MAP   (bigint) — bitmap indicator (12 in samples)
    //   4: no        (string) — single digit "0"-"9"
    //
    // Row key: REFID (15 digits, left-padded to 15)
    //          rule: "REFID,0,false,15"
    TDR_SIGNAL_STOR_20550(
        "tdr_signal_stor_20550",
        "REFID,0,false,15",
        List.of(
            new Field("REFID",    FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("TIME",     FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("SIGSTORE", FieldType.nullable(new ArrowType.Utf8()),        null),
            new Field("BIT_MAP",  FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("no",       FieldType.nullable(new ArrowType.Utf8()),        null)
        )
    );

    // ─────────────────────────────────────────────────────────────────────────

    public final String tableName;
    public final String rowKeyRule;
    public final Schema arrowSchema;

    TableSchema(String tableName, String rowKeyRule, List<Field> fields) {
        this.tableName  = tableName;
        this.rowKeyRule = rowKeyRule;
        this.arrowSchema = new Schema(fields);
    }

    /** Look up a schema by table name (case-insensitive). */
    public static TableSchema forTable(String name) {
        for (TableSchema s : values()) {
            if (s.tableName.equalsIgnoreCase(name)) return s;
        }
        throw new IllegalArgumentException(
            "Unknown table: '" + name + "'. Available: " +
            String.join(", ", java.util.Arrays.stream(values())
                .map(s -> s.tableName).toList()));
    }

    /** Allocate an empty VectorSchemaRoot for this schema. */
    public VectorSchemaRoot allocateRoot(BufferAllocator allocator) {
        return VectorSchemaRoot.create(arrowSchema, allocator);
    }
}
