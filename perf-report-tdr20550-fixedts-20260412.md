# JNI/C++ vs 纯 Java Arrow->HFile 性能验证报告

测试时间：2026-04-12

## 测试前提

- 数据模型：`tdr_signal_stor_20550`
- 场景矩阵：6 个固定场景，2 个实现，每场景 3 轮
- 压缩：`GZ`
- 压缩级别：`1`
- Data Block Encoding：`NONE`
- Bloom：`ROW`
- 统一固定写入时间戳：`1715678900123`
- 工作目录：`/tmp/hfilesdk-bulkload-perf-tdr20550-fixedts-20260412`
- 结果报告：`/Users/gauss/workspace/github_project/HFileSDK/perf-matrix-report-tdr20550-fixedts-20260412.json`

执行参数：

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib /Users/gauss/workspace/github_project/HFileSDK/build/libhfilesdk.dylib \
  --report-json /Users/gauss/workspace/github_project/HFileSDK/perf-matrix-report-tdr20550-fixedts-20260412.json \
  --work-dir /tmp/hfilesdk-bulkload-perf-tdr20550-fixedts-20260412 \
  --cpu-set 0-3 \
  --process-memory-mb 4096 \
  --jni-xmx-mb 512 \
  --jni-direct-memory-mb 512 \
  --jni-sdk-max-memory-mb 1024 \
  --java-xmx-mb 512 \
  --java-direct-memory-mb 512
```

## 一致性校验

在正式重跑完整矩阵前，已用 `single-001mb` 场景做过同源 Arrow 输入的一致性验证：

- JNI/C++ 输出 HFile 与纯 Java 输出 HFile 已做到字节级完全一致
- `cmp` 结果：`IDENTICAL`
- 两边 `hfile-verify` 均为 `PASS`

这意味着下面的性能结果建立在“功能一致、输出一致”的前提上。

## 环境说明

- 机器：`Darwin arm64`
- perf runner 进程控制状态：全部为 `direct`
- `process_peak_rss_bytes` / `process_user_cpu_ms` / `process_sys_cpu_ms` 在本次 macOS 环境下均为 `0`

说明：

- 当前实现的 `/proc` 采样、`taskset`、`cgroup/prlimit` 进程级硬限制只在 Linux 上生效
- 本报告可用于吞吐对比
- 若需要正式 CPU/RSS 公平对比，请在 Linux 上用同一命令再跑一轮

## 场景结果

| 场景 | JNI/C++ 平均 ms | 纯 Java 平均 ms | 结论 |
| --- | ---: | ---: | --- |
| `single-001mb` | `65.33` | `551.00` | JNI/C++ 快 `8.43x` |
| `single-010mb` | `319.00` | `741.33` | JNI/C++ 快 `2.32x` |
| `single-100mb` | `2843.00` | `2541.33` | 纯 Java 快 `1.12x` |
| `single-500mb` | `13686.67` | `9739.33` | 纯 Java 快 `1.40x` |
| `directory-100x001mb` | `3010.67` | `3339.67` | JNI/C++ 快 `1.11x` |
| `directory-100x010mb` | `15149.67` | `20008.33` | JNI/C++ 快 `1.32x` |

## 总结

- JNI/C++ 在 `4/6` 个场景中更快
- 纯 Java 在“大单文件”场景更快：`single-100mb`、`single-500mb`
- JNI/C++ 在“小单文件 + 目录批量”场景更快
- 6 个场景平均耗时求和：
  - JNI/C++：`35074.34 ms`
  - 纯 Java：`36920.99 ms`
  - 整体上 JNI/C++ 约快 `1.05x`

## 内存观察

- JNI/C++ 最大 `sdk_tracked_memory_peak_bytes`：`728430520` bytes
- 约等于 `694.69 MiB`
- 这是 C++ SDK 内部可归因峰值内存，不是整个进程 RSS

## 清理确认

- 完整矩阵工作目录 `/tmp/hfilesdk-bulkload-perf-tdr20550-fixedts-20260412` 已自动清理，目录为空
- 中途一致性校验保留的 Arrow/HFile 临时样本目录已删除
