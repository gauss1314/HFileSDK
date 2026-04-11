# HFile Bulkload Perf Report

Date: 2026-04-11

## Scope

- Input generator: `mock-arrow`
- Table schema: `tdr_signal_stor_20550`
- Input ordering: unsorted within Arrow batches
- Row key rule: `REFID,0,false,15`
- Implementations compared:
  - `arrow-to-hfile` (JNI / C++)
  - `arrow-to-hfile-java` (pure Java)
- Iterations per scenario: `3`
- Intermediate Arrow/HFile artifacts: auto-cleaned after run

## Average Elapsed Time

| Scenario | Generated Arrow Size | JNI Avg (ms) | Java Avg (ms) | Faster |
| --- | ---: | ---: | ---: | --- |
| `single-001mb` | 1.12 MiB | 76.33 | 463.33 | JNI 6.07x |
| `single-010mb` | 11.23 MiB | 398.67 | 906.67 | JNI 2.27x |
| `single-100mb` | 112.26 MiB | 3755.33 | 4546.67 | JNI 1.21x |
| `single-500mb` | 561.32 MiB | 19302.00 | 21427.33 | JNI 1.11x |
| `directory-100x001mb` | 112.37 MiB | 3914.67 | 5611.33 | JNI 1.43x |
| `directory-100x010mb` | 1122.74 MiB | 21197.33 | 43649.33 | JNI 2.06x |

## Validation Notes

- JNI is faster in all 6 scenarios under the current unsorted `tdr_signal_stor_20550` workload.
- The previous large single-file reversal is gone; under unsorted business-like input, `single-500mb` is now also JNI-leading.
- The generated Arrow size is slightly above the nominal target because `mock-arrow` stops on batch boundaries.
- The temp work directory `/tmp/hfilesdk-bulkload-perf-tdr20550-run` was removed after verification, and no Arrow/HFile artifacts were retained.

## Artifacts

- JSON report: `perf-matrix-report-tdr20550-unsorted-20260411.json`
