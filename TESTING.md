# HFileSDK 测试说明

## 当前状态

- 当前已纳入 `ctest` 的测试目标覆盖主路径与回归场景
- 测试源码包含 C++ 单元/集成测试与 Java JNI 集成测试
- 覆盖范围包含：编码、压缩、元数据、Writer、Arrow 转换、基础 I/O、故障注入、Java JNI 端到端、HBase Reader 黑盒校验

## 运行方式

```bash
bash scripts/test.sh
```

如需只做构建，可执行：

```bash
bash scripts/build.sh
```

如需向 `ctest` 透传筛选条件：

```bash
bash scripts/test.sh -- -R test_arrow_converter
```

Windows + MSYS2 Clang64 下对应入口：

```bash
bash scripts/build.sh
bash scripts/test.sh
```

## 覆盖率报表

当前仓库已接入基于 Clang 工具链的 `llvm-cov` 覆盖率流程；在 macOS、Linux 以及 Windows + MSYS2 Clang 环境下，只要 `llvm-cov` / `llvm-profdata` 可用，都可以走同一套覆盖率目标。
在 Windows + MSYS2 Clang64 环境下，请直接执行 `bash scripts/coverage.sh`；脚本会在配置前检查 `cmake`、`clang/clang++`、`llvm-cov`、`llvm-profdata`，并提示 `MSYSTEM=CLANG64` 等推荐前置条件。

```bash
cmake -B build-coverage \
  -DCMAKE_BUILD_TYPE=Release \
  -DHFILE_ENABLE_COVERAGE=ON
cmake --build build-coverage -j8
cmake --build build-coverage --target hfile_coverage
```

或直接使用本地脚本：

```bash
bash scripts/coverage.sh
```

Windows + MSYS2 Clang64 下对应入口：

```bash
bash scripts/coverage.sh
```

生成结果：

- 文本摘要：`build-coverage/coverage/summary.txt`
- HTML 报表：`build-coverage/coverage/html/index.html`
- CI 产物目录：`build-coverage/artifacts/`
- CI HTML 产物目录：`build-coverage/artifacts/coverage-html/`
- CI 摘要产物：`build-coverage/artifacts/coverage-summary.txt`

说明：

- `hfile_coverage` 会自动运行 `ctest`、收集 `.profraw`、合并为 `.profdata`，并生成文本与 HTML 报表
- `hfile_coverage_ci` 与 `hfile_coverage` 共享同一套生成逻辑，但额外约定 `build-coverage/artifacts/` 作为 CI 上传目录
- 覆盖率模式会关闭默认 Release 优化，并额外注入 `-O0 -g -fprofile-instr-generate -fcoverage-mapping`
- 当前覆盖率流程面向 Clang 工具链；macOS/Linux 可直接使用，Windows 需要通过 MSYS2 Clang 环境提供 `bash`、`cmake`、`clang/clang++`、`llvm-cov`、`llvm-profdata`

## 覆盖矩阵

### 编码与序列化

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| CRC32C | 已知值、增量计算、大缓冲区、chunk checksum | `test_crc32c` |
| KV 编码 | KV 序列化、tags 长度、时间戳大端序、`compare_keys`、VarInt 边界 | `test_kv_encoding`、`test_round3_bugs` |
| NoneEncoder | 空块、单 KV、满块、reset、工厂创建 | `test_block_builder` |

### 元数据与格式

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| Bloom Filter | 空 Bloom、插入、chunk 边界、元数据输出 | `test_bloom_filter` |
| Block Index | 空索引、单层/双层索引、offset 与 first key | `test_index_writer` |
| FileInfo | 必填字段、编码名称、比较器字符串 | `test_file_info` |
| Trailer | 版本尾部、ProtoBuf 反序列化 | `test_trailer` |

### 压缩

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| Compressor | None/LZ4/Zstd/Snappy 等往返、空输入、容量估算 | `test_compressor` |

### Writer 主链路

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| HFileWriter 基础路径 | 空文件、单 KV、多 KV、多 block、压缩、错误 CF、排序校验、double finish | `test_hfile_writer`、`test_round3_bugs` |
| AutoSort 与资源治理 | 内部乱序排序、内存预算、tags/MVCC 开关、`append(span...)` | `test_hfile_writer` |
| 输入边界 | `ROW_KEY_TOO_LONG`、`VALUE_TOO_LARGE`、负时间戳、磁盘阈值拒绝 | `test_hfile_writer`、`test_production_features` |
| Fsync / ErrorPolicy | Safe/Fast、Strict/SkipRow、错误回调、最大错误数 | `test_production_features` |

### Arrow 与转换编排

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| convert() 输入校验 | 空路径、缺失 Arrow 文件、损坏 stream、非法 row key rule、内存超限 | `test_arrow_converter` |
| convert() 结果路径 | 重复 row key、重复 qualifier、LargeString、空 row key 全过滤、进度回调 | `test_arrow_converter` |

### RowKey 规则与分区

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| RowKeyBuilder | 规则编译、分隔、pad、reverse、随机段、越界列、排序稳定性 | `test_row_key_builder` |
| RegionPartitioner | 无 split、单/多 split、无序 split、二进制 key、空 key | `test_region_partitioner`、`test_round3_bugs` |

### I/O 与可靠性

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| BufferedFileWriter | 缓冲写入、flush、close 幂等、工厂打开文件 | `test_io_writers` |
| AtomicFileWriter | 提交、回滚、析构清理 | `test_production_features` |
| Chaos | `kill-during-write`、`disk-full-sim` | `hfile_chaos_kill`、`hfile_chaos_disk_full` |

### 观测与辅助能力

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| MemoryBudget | reserve/release、RAII、峰值、无限制 | `test_production_features` |
| MetricsRegistry | counter/gauge/histogram、snapshot、线程安全、report callback | `test_production_features` |
| JNI JSON | 严格 JSON 解析、重复 key、非法语法、转义 | `test_jni_json_utils` |

### Java JNI

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| HFileSDK Java JNI | `configure()` 非法 JSON、空路径错误、非法 row key rule、Java 生成 Arrow 文件并经 JNI 完成真实转换；测试源码位于 `tools/arrow-to-hfile/src/test/java/com/hfile/HFileSDKIntegrationTest.java` | `test_java_jni_integration` |
| HBase Reader 黑盒校验 | Java/JNI 生成固定 fixture，再用 HBase 原生 Reader 校验 major version、entry count、compression、encoding，以及 row/family/qualifier/value/type 顺序 | `test_java_hbase_reader_verify` |

## 当前边界

- 当前文档中的“全覆盖”指当前 macOS 生效构建配置下，主要功能模块、关键异常路径和重要资源边界均已有自动化回归保护
- 当前已接入 `llvm-cov` 报表流程，但这里仍不表示 100% 行覆盖率或分支覆盖率
- `io_uring` 与 `HDFS` 后端属于条件编译路径，当前 macOS 构建未启用，因此未进入当前 `ctest` 矩阵
- Java/JNI 端到端与 HBase Reader 黑盒校验已纳入自动化测试，但当前仍不包含 Java 侧 HBase 集群集成链路

## Linux 性能测试（更偏生产口径）

性能对比建议优先在 Linux 上采集。`tools/hfile-bulkload-perf` 的官方对比口径就是 Linux，同机、同核集、同进程内存上限下对比 JNI 实现 `arrow-to-hfile` 与纯 Java 实现 `arrow-to-hfile-java`。

当前版本的参数边界：

- `--encoding` 只建议使用 `NONE`
- `--compression` 只建议使用 `GZ` 或 `NONE`；`gzip` 作为兼容别名保留
- 更贴近生产的默认口径建议使用 `--compression GZ --encoding NONE`
- `--table` 目前建议使用 `tdr_signal_stor_20550`
  - `tdr_mock` 更适合作为轻量 smoke/perf 冒烟

运行建议：

- 对 JNI 与纯 Java 使用同一组 `--cpu-set` 和 `--process-memory-mb`
- 将 `--report-json`、当前 commit SHA、`lscpu`、`free -h`、磁盘类型与挂载点一起归档
- 如需排查异常波动，可追加 `--keep-generated-files` 保留 Arrow/HFile/worker 日志
- 若机器是 NUMA 架构，建议将 `--cpu-set` 固定在同一 NUMA 节点内
- 如果出现 `Unsupported config key: max_memory_bytes`、`compression_threads` 或 `compression_queue_depth`，不要只看源码是否已更新，要确认当前进程实际加载的 `libhfilesdk.so` 是否包含这些 key；建议先按下面的 clean rebuild 步骤重建，再跑 smoke 命令验证。

### 性能测试前 clean rebuild

性能测试前建议清理 native build、coverage build、所有 Java `target`，以及本地 Maven 仓库里的 `io.hfilesdk` 旧包，确保 perf jar、`arrow-to-hfile` 依赖和 native SDK 来自同一份最新源码。

```bash
rm -rf build build-coverage \
  tools/mock-arrow/target \
  tools/arrow-to-hfile/target \
  tools/arrow-to-hfile-java/target \
  tools/hfile-bulkload-perf/target \
  ~/.m2/repository/io/hfilesdk/mock-arrow \
  ~/.m2/repository/io/hfilesdk/arrow-to-hfile \
  ~/.m2/repository/io/hfilesdk/arrow-to-hfile-java \
  ~/.m2/repository/io/hfilesdk/hfile-bulkload-perf

bash scripts/test.sh -- -E hfile_chaos_kill

export HFILESDK_NATIVE_DIR="$PWD/build"
export HFILESDK_BUILD_DIR="$PWD/build"
if [[ "$(uname -s)" == "Darwin" ]]; then
  export HFILESDK_NATIVE_LIB="$PWD/build/libhfilesdk.dylib"
else
  export HFILESDK_NATIVE_LIB="$PWD/build/libhfilesdk.so"
fi

mvn -q -f tools/mock-arrow/pom.xml clean install
mvn -q -f tools/arrow-to-hfile/pom.xml clean install
mvn -q -f tools/arrow-to-hfile-java/pom.xml clean install
mvn -q -f tools/hfile-bulkload-perf/pom.xml clean package
```

构建完成后，Linux 下 `--native-lib` 使用 `$PWD/build/libhfilesdk.so`；macOS 下使用 `$PWD/build/libhfilesdk.dylib`。

如果需要先做跨平台 smoke，macOS 和 Linux 都可以用下面这个不绑定 CPU、不设置进程内存上限的小场景验证 jar 与 native 库是否匹配：

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib "$HFILESDK_NATIVE_LIB" \
  --table tdr_signal_stor_20550 \
  --scenario-filter single-001mb \
  --compression GZ \
  --encoding NONE \
  --bloom row \
  --error-policy skip_row \
  --block-size 65536 \
  --parallelism 1 \
  --jni-xmx-mb 1024 \
  --jni-direct-memory-mb 1024 \
  --jni-sdk-max-memory-mb 1024 \
  --jni-sdk-compression-threads 2 \
  --jni-sdk-compression-queue-depth 4 \
  --jni-sdk-numeric-sort-fast-path auto \
  --java-xmx-mb 4096 \
  --java-direct-memory-mb 1024 \
  --work-dir /tmp/hfilesdk-perf-smoke \
  --report-json /tmp/hfilesdk-perf-smoke/report.json
```

如果 smoke 仍报 `Unsupported config key`，先在同一个 shell 中确认实际传入的 native 库包含这些配置名：

```bash
strings "$HFILESDK_NATIVE_LIB" | grep -E 'max_memory_bytes|compression_threads|compression_queue_depth'
```

### Linux x86 最高性能调参

下面参数是“性能优先”口径，不是最保守的资源隔离口径。建议在独占机器或独占 NUMA 节点上执行，输入和输出目录优先放在本地 NVMe / SSD，避免和 HDFS 客户端、本机其他任务或远端网络盘争抢资源。

关键参数含义：

- `--jni-sdk-compression-threads`：每个 JNI 转换任务内部的 GZip 数据块后台压缩线程数。`0` 表示同步压缩，通常不是 GZip 最高性能。
- `--jni-sdk-compression-queue-depth`：每个转换任务允许排队的未写出压缩块数量。`0` 表示 SDK 自动使用 `max(2, compression_threads * 2)`。
- `--jni-sdk-max-memory-mb`：C++ SDK 内部 soft budget。纯性能压测且机器内存足够时可设 `0` 表示不限制，避免预算检查和误限流；生产演练建议设为进程内存上限扣除 JVM heap/direct 后的 `60%~70%`。
- `--process-memory-mb`：worker 进程 OS 级硬限制。性能压测时不要贴满物理内存，至少给 Linux page cache、文件系统和系统守护进程预留 `20%~30%`。

起步配置表：

| 绑定 CPU 核数 | 单文件延迟：`parallelism` | 单文件延迟：压缩线程 | 单文件延迟：队列 | 目录吞吐：`parallelism` | 目录吞吐：每任务压缩线程 | 目录吞吐：队列 |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 1 | 4 | 0 或 8 | 2 | 2 | 0 |
| 16 | 1 | 8 | 0 或 16 | 4 | 2 | 0 |
| 32 | 1 | 8~12 | 0 或 `threads*3` | 6~8 | 3 | 0 |
| 64 | 1 | 12~16 | 0 或 `threads*3` | 8~12 | 3~4 | 0 |

调参规则：

- 单文件场景只有一个 Arrow 文件，`--parallelism` 固定为 `1`，主要增加 `--jni-sdk-compression-threads`；通常从物理核数的 `1/2` 起步，超过 `8~16` 后收益会逐步变小。
- 目录场景会同时跑多个转换任务，总线程数大致是 `parallelism * (1 + compression_threads)`；建议先让这个值不超过绑定核数的 `75%~90%`，再逐步上调。
- `compression_queue_depth=0` 是推荐起点，因为 SDK 会自动设置成 `2 * compression_threads`；如果看到压缩线程有空闲、磁盘很快且平均耗时仍随队列增加下降，可以试 `threads*3` 或 `threads*4`。
- `--jni-sdk-max-memory-mb 0` 通常是纯性能压测最快口径；如果需要避免 OOM kill，则设为 `process_memory_mb - jni_xmx_mb - jni_direct_memory_mb - 4096` 的 `60%~70%`，并确认 `report.json` 里的 `sdk_tracked_memory_peak_bytes` 不贴近预算。
- `--java-xmx-mb` 和 `--java-direct-memory-mb` 不影响 JNI C++ hot path，但会影响纯 Java 对比实现是否被堆或 direct memory 限制；为了公平，Java 侧也要给足内存。
- `--compression NONE` 下压缩线程和队列不会启用；只有 `--compression GZ` 或兼容别名 `gzip` 时这些参数才影响性能。
- 每次只改一个维度，至少保留三轮默认迭代结果。优先比较 `average_ms`、`iteration_ms` 稳定性、`process_peak_rss_bytes` 和 `sdk_tracked_memory_peak_bytes`。

16 核 / 32 GiB 进程限制的最高性能起步命令：

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib "$HFILESDK_NATIVE_LIB" \
  --table tdr_signal_stor_20550 \
  --scenario-filter single-500mb \
  --compression GZ \
  --encoding NONE \
  --bloom row \
  --error-policy skip_row \
  --block-size 65536 \
  --cpu-set 0-15 \
  --process-memory-mb 32768 \
  --parallelism 1 \
  --jni-xmx-mb 1024 \
  --jni-direct-memory-mb 1024 \
  --jni-sdk-max-memory-mb 0 \
  --jni-sdk-compression-threads 8 \
  --jni-sdk-compression-queue-depth 0 \
  --jni-sdk-numeric-sort-fast-path auto \
  --java-xmx-mb 8192 \
  --java-direct-memory-mb 2048 \
  --work-dir /data/tmp/hfilesdk-perf-single-fast \
  --report-json /data/tmp/hfilesdk-perf-single-fast/report.json
```

32 核机器可优先做这一组单文件 sweep，选择 `average_ms` 最低且 `iteration_ms` 波动最小的组合：

```bash
for t in 8 12 16; do
  java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
    --native-lib "$HFILESDK_NATIVE_LIB" \
    --table tdr_signal_stor_20550 \
    --scenario-filter single-500mb \
    --compression GZ \
    --encoding NONE \
    --bloom row \
    --error-policy skip_row \
    --block-size 65536 \
    --cpu-set 0-31 \
    --process-memory-mb 65536 \
    --parallelism 1 \
    --jni-xmx-mb 1024 \
    --jni-direct-memory-mb 1024 \
    --jni-sdk-max-memory-mb 0 \
    --jni-sdk-compression-threads "$t" \
    --jni-sdk-compression-queue-depth 0 \
    --jni-sdk-numeric-sort-fast-path auto \
    --java-xmx-mb 16384 \
    --java-direct-memory-mb 4096 \
    --work-dir "/data/tmp/hfilesdk-perf-single-t${t}" \
    --report-json "/data/tmp/hfilesdk-perf-single-t${t}/report.json"
done
```

### 单文件延迟基线

适合观察单个大 Arrow 文件转单个 HFile 的延迟表现，建议关注 `single-100mb` 和 `single-500mb` 两个场景。

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib "$HFILESDK_NATIVE_LIB" \
  --table tdr_signal_stor_20550 \
  --scenario-filter single-500mb \
  --compression GZ \
  --encoding NONE \
  --bloom row \
  --error-policy skip_row \
  --block-size 65536 \
  --cpu-set 0-7 \
  --process-memory-mb 16384 \
  --parallelism 1 \
  --jni-xmx-mb 1024 \
  --jni-direct-memory-mb 1024 \
  --java-xmx-mb 4096 \
  --java-direct-memory-mb 1024 \
  --work-dir /data/tmp/hfilesdk-perf-single \
  --report-json /data/tmp/hfilesdk-perf-single/report.json
```

建议说明：

- `--parallelism 1`：单文件场景下避免把目录批处理并行度混入延迟口径
- 该基线命令刻意不传 `--jni-sdk-*` advanced key，先验证 jar 与 native SDK 的基础链路；最高性能测试请使用上方“Linux x86 最高性能调参”里的命令。
- `--java-xmx-mb` 建议显式大于 2 GiB，否则大文件场景更容易把结果变成 JVM 堆配置对比，而不是实现对比
- 性能基线默认不固定 `default_timestamp_ms`，由实现使用当前时间；只有需要 JNI/Java HFile 字节级一致性对比时，才额外传 `--timestamp-ms <millis>`

### 目录吞吐基线

适合观察大量 Arrow 文件批量转换时的整体吞吐，建议关注 `directory-100x010mb` 场景。

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib "$HFILESDK_NATIVE_LIB" \
  --table tdr_signal_stor_20550 \
  --scenario-filter directory-100x010mb \
  --compression GZ \
  --encoding NONE \
  --bloom row \
  --error-policy skip_row \
  --block-size 65536 \
  --cpu-set 0-7 \
  --process-memory-mb 16384 \
  --parallelism 4 \
  --jni-xmx-mb 1024 \
  --jni-direct-memory-mb 1024 \
  --jni-sdk-max-memory-mb 8192 \
  --jni-sdk-compression-threads 2 \
  --jni-sdk-compression-queue-depth 8 \
  --jni-sdk-numeric-sort-fast-path auto \
  --java-xmx-mb 4096 \
  --java-direct-memory-mb 1024 \
  --work-dir /data/tmp/hfilesdk-perf-directory \
  --report-json /data/tmp/hfilesdk-perf-directory/report.json
```

建议说明：

- `--parallelism 4`：目录场景更偏吞吐，通常不建议直接打满所有核，保留一部分给压缩线程和文件系统更稳
- `--jni-sdk-compression-threads 2`：与目录并行度叠加时更容易避免线程过量竞争
- 若机器核数明显更多，可按经验把 `--cpu-set` 扩大到专属核集，再将 `--parallelism` 调到 `4~8` 区间观察吞吐拐点

16 核 / 32 GiB 进程限制的目录吞吐性能优先命令：

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib "$HFILESDK_NATIVE_LIB" \
  --table tdr_signal_stor_20550 \
  --scenario-filter directory-100x010mb \
  --compression GZ \
  --encoding NONE \
  --bloom row \
  --error-policy skip_row \
  --block-size 65536 \
  --cpu-set 0-15 \
  --process-memory-mb 32768 \
  --parallelism 4 \
  --jni-xmx-mb 1024 \
  --jni-direct-memory-mb 1024 \
  --jni-sdk-max-memory-mb 0 \
  --jni-sdk-compression-threads 2 \
  --jni-sdk-compression-queue-depth 0 \
  --jni-sdk-numeric-sort-fast-path auto \
  --java-xmx-mb 8192 \
  --java-direct-memory-mb 2048 \
  --work-dir /data/tmp/hfilesdk-perf-directory-fast \
  --report-json /data/tmp/hfilesdk-perf-directory-fast/report.json
```

32 核机器的目录吞吐 sweep 建议先固定 `compression_queue_depth=0`，扫描 `parallelism` 和压缩线程：

```bash
for p in 4 6 8; do
  for t in 2 3; do
    java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
      --native-lib "$HFILESDK_NATIVE_LIB" \
      --table tdr_signal_stor_20550 \
      --scenario-filter directory-100x010mb \
      --compression GZ \
      --encoding NONE \
      --bloom row \
      --error-policy skip_row \
      --block-size 65536 \
      --cpu-set 0-31 \
      --process-memory-mb 65536 \
      --parallelism "$p" \
      --jni-xmx-mb 1024 \
      --jni-direct-memory-mb 1024 \
      --jni-sdk-max-memory-mb 0 \
      --jni-sdk-compression-threads "$t" \
      --jni-sdk-compression-queue-depth 0 \
      --jni-sdk-numeric-sort-fast-path auto \
      --java-xmx-mb 16384 \
      --java-direct-memory-mb 4096 \
      --work-dir "/data/tmp/hfilesdk-perf-dir-p${p}-t${t}" \
      --report-json "/data/tmp/hfilesdk-perf-dir-p${p}-t${t}/report.json"
  done
done
```

### 全矩阵回归

在版本发布前或优化项对比前，建议跑完整矩阵而不是只跑单一场景：

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib "$HFILESDK_NATIVE_LIB" \
  --table tdr_signal_stor_20550 \
  --compression GZ \
  --encoding NONE \
  --bloom row \
  --error-policy skip_row \
  --block-size 65536 \
  --cpu-set 0-7 \
  --process-memory-mb 16384 \
  --parallelism 4 \
  --jni-xmx-mb 1024 \
  --jni-direct-memory-mb 1024 \
  --jni-sdk-max-memory-mb 8192 \
  --jni-sdk-compression-threads 2 \
  --jni-sdk-compression-queue-depth 8 \
  --jni-sdk-numeric-sort-fast-path auto \
  --java-xmx-mb 4096 \
  --java-direct-memory-mb 1024 \
  --work-dir /data/tmp/hfilesdk-perf-full \
  --report-json /data/tmp/hfilesdk-perf-full/report.json
```

如果是在集群机或需要先 `source` / `kinit`，可以复用包装脚本：

```bash
bash scripts/hfile-bulkload-perf-runner.sh \
  --env-script /opt/client/bigdata_env \
  --principal ossuser \
  --keytab /opt/client/keytab/ossuser.keytab \
  --native-lib "$HFILESDK_NATIVE_LIB" \
  --work-dir /data/tmp/hfilesdk-perf-full \
  -- \
  --table tdr_signal_stor_20550 \
  --compression GZ \
  --encoding NONE \
  --cpu-set 0-7 \
  --process-memory-mb 16384 \
  --parallelism 4 \
  --jni-xmx-mb 1024 \
  --jni-direct-memory-mb 1024 \
  --jni-sdk-max-memory-mb 8192 \
  --jni-sdk-compression-threads 2 \
  --jni-sdk-compression-queue-depth 8 \
  --jni-sdk-numeric-sort-fast-path auto \
  --java-xmx-mb 4096 \
  --java-direct-memory-mb 1024 \
  --report-json /data/tmp/hfilesdk-perf-full/report.json
```

### 建议一起记录的信息

- `git rev-parse HEAD`
- `lscpu`
- `free -h`
- `uname -a`
- 磁盘介质类型与挂载目录
- perf runner 输出的 `report.json`
- 是否开启 `--keep-generated-files`

## 建议的后续增强

- 在 Linux CI 中增加 `io_uring` 与 `HDFS` 条件矩阵
- 将 `hfile_coverage` 纳入 CI 并保存 HTML 报表产物
- 在 Linux CI 中补一组真实 HBase Scan / BulkLoad 集成验证
