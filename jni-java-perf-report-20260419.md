# JNI/C++ vs Pure Java Arrow->HFile Performance Report

Date: 2026-04-19

## Scope

- Dataset: `/tmp/hfile-rootcause-single-500.arrow`
- Row key rule: `REFID,0,false,15`
- Column family: `cf`
- Compression: `GZ`
- Compression level: `1`
- Encoding: `NONE`
- Bloom: `row`
- Timestamp: `1715678900123`
- Execution mode: serial, JNI/C++ first and pure Java second

## Code Changes In This Round

1. `Pass1` sort changed from `std::stable_sort(row_key)` to `std::sort(row_key, batch_idx, row_idx)`.
   - Keeps duplicate-row-key relative order identical to the old scan order.
   - Reduces sort overhead without changing HFile byte layout.
2. Precomputed sorted qualifier order once per schema and reused it across all `RecordBatch` instances.
   - Removes repeated per-batch qualifier sorting work.
   - Keeps qualifier order unchanged.

## Validation

### Native / JNI regression

- `ctest --output-on-failure -R 'test_hfile_writer|test_arrow_converter|test_compressor'`
- Result: `100% tests passed`

### Java/C++ consistency

- `HFileConsistencyTest`
- Result: passed

### Byte-level equality

- JNI MD5: `05e91ed8c17f2adaec5bb0a9b684c626`
- Java MD5: `05e91ed8c17f2adaec5bb0a9b684c626`
- Conclusion: byte-identical

### HBase reader verification

Both files passed `tools/hfile-verify`:

- Major version: `3`
- Compression: `GZ`
- Encoding: `NONE`
- Comparator: `org.apache.hadoop.hbase.InnerStoreCellComparator`
- Bloom meta: `CompoundBloomFilter`
- Entry count: `15,093,140`
- Data blocks: `18,750`
- Index blocks: `9`
- Layout bytes:
  - data=`236,712,223`
  - index=`299,470`
  - bloom=`3,668,107`
  - meta=`0`
  - fileinfo=`336`
  - trailer=`4,096`

## Performance Results

### JNI/C++

- elapsed: `7939 ms`
- sort: `800 ms`
- write: `7138 ms`
- compress: `5600 ms`
- data block write: `99 ms`
- leaf index write: `7 ms`
- bloom chunk write: `66 ms`
- kvs: `15,093,140`
- hfile size: `240,684,232 bytes`

### Pure Java

- elapsed: `10425 ms`
- sort: `1626 ms`
- write: `8772 ms`
- kvs: `15,093,140`
- hfile size: `240,684,232 bytes`

### Comparison

- End-to-end speedup: `1.31x` (`10425 / 7939`)
- Sort stage speedup: `2.03x` (`1626 / 800`)
- Write stage speedup: `1.23x` (`8772 / 7138`)

## Interpretation

This round confirms two things:

1. The new `Pass1` optimizations are effective.
   - JNI/C++ sort time is now materially lower than pure Java.
2. For large single-file scenarios, total time is increasingly dominated by `Pass2` block writing, especially GZ compression.
   - JNI/C++ write time is still better than Java, but the gap is much smaller than the sort gap.
   - In the JNI/C++ run, compression alone consumed `5600 ms`, already about `70%` of total elapsed time.

This is the main reason why the larger the file gets, the smaller the overall lead becomes:

- fixed and semi-fixed overheads like row-key build and sort do not scale as fast as compression work
- once compression dominates, end-to-end speed converges toward "whose gzip path is better"
- therefore, further wins now depend more on the gzip/write hot path than on `Pass1`

## Note

This run was executed on macOS development environment. Process peak RSS was not captured in this report because the macOS sandbox blocked the resource query path used by `/usr/bin/time -l`. The Linux-side fairness plan based on isolated worker subprocesses plus `/proc` and cgroup metrics remains the authoritative process-level measurement approach.
