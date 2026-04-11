# hfile-bulkload-perf

统一的端到端转换性能入口。它会生成固定场景矩阵的 mock Arrow 数据，并对同一批输入分别执行：

- `tools/arrow-to-hfile`
- `tools/arrow-to-hfile-java`

每个用例固定执行 3 次，输出每轮耗时与平均耗时，便于横向对比两种实现。

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
- `--table`：结果中使用的表名标签，默认 `hfilesdk_perf`
- `--work-dir`：场景输入、输出与中间报告目录
- `--report-json`：最终结构化报告路径
- `--implementations`：实现列表，默认同时执行两种实现
- `--scenario-filter`：只运行匹配场景 ID 的子集
- `--iterations`：固定为 `3`
- `--parallelism`：JNI 大文件并行转换线程数
- `--merge-threshold`：JNI 小文件合并阈值
- `--trigger-size` / `--trigger-count` / `--trigger-interval`：JNI 小文件合并策略参数
- `--payload-bytes`：每行 `PAYLOAD` 的字节数
- `--batch-rows`：每个 Arrow RecordBatch 行数
- `--rule`：rowKeyRule，默认 `USER_ID,0,false,0`
- `--keep-generated-files`：保留输入 Arrow 与每轮 HFile 输出

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
- `detail`
