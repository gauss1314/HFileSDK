# hfile-bulkload-perf

统一的端到端转换性能入口。它会生成固定场景矩阵的 mock Arrow 数据，并对同一批输入分别执行：

- `tools/arrow-to-hfile`
- `tools/arrow-to-hfile-java`

每个用例固定执行 3 次，输出每轮耗时与平均耗时，便于横向对比两种实现。

生成的 Arrow 输入默认会在批内打乱行顺序，以模拟生产环境中的乱序源文件；这样 JNI 与纯 Java 实现都会在相同的乱序输入上完成内部排序后再写出 HFile。

从当前版本开始，perf runner 会将每个 `implementation + iteration` 放到独立 worker 进程中执行，再由父进程汇总报告。这样才能对 JNI 实现和纯 Java 实现做进程级 CPU / 内存对比，而不是把两者混在同一个 JVM 内部。

需要区分两层串行/并行语义：

- 父进程层面：不同 implementation 与不同 iteration 仍然串行调度，避免 JNI 与纯 Java 在同一轮里互相抢占资源
- worker 层面：保留各实现自己的目录转换能力；例如 JNI 目录场景仍可使用 `--parallelism` 对多个 Arrow 文件并行生成多个 HFile

## 固定场景

- 单文件：`1MB`、`10MB`、`100MB`、`500MB`
- 目录：`100 x 1MB`
- 目录：`100 x 10MB`

## 构建

```bash
mvn -q -f tools/arrow-to-hfile/pom.xml -DskipTests install
mvn -q -f tools/arrow-to-hfile-java/pom.xml install
mvn -q -f tools/hfile-bulkload-perf/pom.xml test
mvn -q -f tools/hfile-bulkload-perf/pom.xml package
```

## 直接运行

同时执行 JNI 与纯 Java 两种实现：

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib /Users/gauss/workspace/github_project/HFileSDK/build/libhfilesdk.dylib \
  --cpu-set 0-3 \
  --process-memory-mb 4096 \
  --jni-xmx-mb 512 \
  --jni-direct-memory-mb 512 \
  --jni-sdk-max-memory-mb 1024 \
  --jni-sdk-compression-threads 4 \
  --jni-sdk-compression-queue-depth 8 \
  --jni-sdk-numeric-sort-fast-path auto \
  --java-xmx-mb 512 \
  --java-direct-memory-mb 512 \
  --table perf_table \
  --work-dir /tmp/hfilesdk-bulkload-perf
```

只跑纯 Java 实现，并过滤到一个小场景：

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --implementations arrow-to-hfile-java \
  --scenario-filter single-001mb \
  --work-dir /tmp/hfilesdk-bulkload-perf-java-only
```

## 集群脚本入口

如果需要沿用集群上的 `source` / `kinit` 操作流程，可以使用：

```bash
bash scripts/hfile-bulkload-perf-runner.sh \
  --env-script /opt/client/bigdata_env \
  --principal ossuser \
  --keytab /opt/client/keytab/ossuser.keytab \
  --native-lib ./release/libhfilesdk.so \
  -- \
  --table perf_table
```

## 关键参数

- `--native-lib`：JNI 实现所需 `libhfilesdk` 动态库路径
- `--table`：结果中使用的表名标签，默认 `tdr_signal_stor_20550`
- `--work-dir`：场景输入、输出与临时中间目录，默认 `/tmp/hfilesdk-bulkload-perf`
- `--report-json`：最终结构化报告路径；未显式指定时默认写到启动命令的当前目录 `./perf-matrix-report.json`
- `--implementations`：实现列表，默认同时执行两种实现
- `--scenario-filter`：只运行匹配场景 ID 的子集
- `--iterations`：固定为 `3`
- `--parallelism`：JNI 目录场景的 worker 内并行度；JNI 与纯 Java 两个 implementation 本身仍由父进程串行执行
- `--merge-threshold`：JNI 小文件合并阈值
- `--trigger-size` / `--trigger-count` / `--trigger-interval`：JNI 小文件合并策略参数
- `--payload-bytes`：每行 `PAYLOAD` 的字节数
- `--batch-rows`：每个 Arrow RecordBatch 行数
- `--rule`：rowKeyRule，默认 `USER_ID,0,false,0`
- `--cpu-set`：Linux 上通过 `taskset` 绑定 worker CPU，确保两种实现使用同一组核
- `--process-memory-mb`：worker 进程总内存硬限制；优先 cgroup v2，失败时退化为 `prlimit`
- `--jni-xmx-mb` / `--jni-direct-memory-mb`：JNI worker JVM 的 heap / direct memory
- `--jni-sdk-max-memory-mb`：JNI 内部 C++ SDK soft budget
- `--jni-sdk-compression-threads`：JNI 内部 C++ SDK 数据块压缩后台线程数；`0` 表示关闭流水线
- `--jni-sdk-compression-queue-depth`：JNI 内部 C++ SDK 压缩流水线 in-flight block 上限；`0` 表示自动
- `--jni-sdk-numeric-sort-fast-path`：JNI 内部数值 rowkey 排序快路径开关，`auto` / `on` / `off`
  说明：该能力按 rowkey 首段形态启用，不绑定任何表名；首段为零左补齐数值列时，复合 rowkey 也可以受益
- `--java-xmx-mb` / `--java-direct-memory-mb`：纯 Java worker JVM 的 heap / direct memory
- `--keep-generated-files`：保留输入 Arrow、每轮 HFile 输出和 worker 日志；默认在场景完成后自动清理这些中间文件

公平性说明：

- 官方对比口径以 Linux 为准，优先使用相同 `--cpu-set` 和 `--process-memory-mb`
- 纯 Java 实现不能只设置 `-Xmx`，还应同时设置 `--java-direct-memory-mb`
- JNI 报告中的 `sdk_memory_budget_bytes` / `sdk_tracked_memory_peak_bytes` 是 SDK 内部可归因内存，不是整个 worker RSS
- `--jni-sdk-numeric-sort-fast-path=on` 只适用于满足快路径约束的 rowkey 规则；不满足时会主动失败，便于避免“静默误配”

## 输出结构

最终 JSON 报告按以下维度组织：

- `scenario_id`
- `input_mode`
- `arrow_file_count`
- `target_size_mb`
- `implementations`
- `iteration_ms`
- `average_ms`

每个实现的每轮结果还会记录：

- `strategy`
- `elapsed_ms`
- `hfile_size_bytes`
- `kv_written_count`
- `process_peak_rss_bytes`
- `process_user_cpu_ms`
- `process_sys_cpu_ms`
- `process_exit_code`
- `process_exit_signal`
- `worker_parallelism`
- `sdk_memory_budget_bytes`
- `sdk_tracked_memory_peak_bytes`
- `sdk_numeric_sort_fast_path_mode`
- `sdk_numeric_sort_fast_path_used`
- `process_control_mode`
- `process_control_note`
- `detail`
