# JNI/C++ vs Pure Java Arrow->HFile Performance Report

Date: 2026-04-19

## Scope

- Correctness baseline:
  - `HFILESDK_NATIVE_DIR=/Users/gauss/workspace/github_project/HFileSDK/build mvn -q -f tools/hfile-bulkload-perf/pom.xml -Dtest=io.hfilesdk.perf.HFileConsistencyTest test`
  - Result: passed
- Performance runner:
  - `tools/hfile-bulkload-perf`
  - Structured report: [jni-java-perf-report-20260419-formal.json](/Users/gauss/workspace/github_project/HFileSDK/jni-java-perf-report-20260419-formal.json)
- Compression: `GZ`
- Compression level: `1`
- Data block encoding: `NONE`
- Bloom: `row`
- JNI/C++ tuning:
  - `--jni-sdk-compression-threads 4`
  - `--jni-sdk-compression-queue-depth 8`
- JVM budget:
  - JNI worker: `-Xmx1024m`, `-XX:MaxDirectMemorySize=1024m`
  - Java worker: `-Xmx1024m`, `-XX:MaxDirectMemorySize=1024m`
- JNI SDK soft budget: `2048 MiB`

## Code Changes Included In This Run

1. Reused gzip/deflater state per writer instead of per block.
2. Reused precomputed raw CRC32 when writing gzip trailer for `Encoding::None` data blocks.
3. Added configurable C++ data-block compression pipeline:
   - `compression_threads`
   - `compression_queue_depth`
4. Fixed async compression path so bloom chunk flushing remains byte-identical to the synchronous writer, including multi-bloom-chunk scenarios.

## Correctness Conclusion

- JNI/C++ and pure Java still pass the existing consistency validation.
- The regression suite now also covers synchronous vs asynchronous C++ compression output equality under multi-bloom-chunk scenarios.
- Conclusion: the current JNI/C++ optimization round did not trade away HFile format correctness.

## Performance Results

| Scenario | JNI/C++ avg ms | Pure Java avg ms | Speedup |
|---|---:|---:|---:|
| `single-001mb` | `81.33` | `529.67` | `6.51x` |
| `single-010mb` | `96.00` | `863.00` | `8.99x` |
| `single-100mb` | `565.33` | `2577.67` | `4.56x` |
| `single-500mb` | `2693.00` | `10361.00` | `3.85x` |
| `directory-100x001mb` | `773.33` | `3484.67` | `4.51x` |
| `directory-100x010mb` | `5004.00` | `19724.67` | `3.94x` |

## Formal Conclusion

This round achieved the target.

- JNI/C++ is now faster than pure Java by more than `3x` in all six standard benchmark scenarios.
- The slowest relative lead is the largest single-file case, `single-500mb`, where JNI/C++ still maintains `3.85x`.
- The largest gain appears in the smaller single-file cases, where the new gzip and pipeline optimizations reduce the fixed overhead very effectively.

In short:

- correctness: passed
- performance target: passed
- target status: `JNI/C++ >= 3x pure Java` across the current formal matrix

## Notes

- This run was executed on the current macOS development machine, not the Linux fairness environment.
- Because `/proc` and cgroup metrics are Linux-specific, the process-level RSS metrics in this run are not authoritative.
- For a final production-style fairness statement, Linux isolated-worker reruns with CPU pinning and process memory limits are still recommended.
