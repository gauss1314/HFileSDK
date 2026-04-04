# hfile-bulkload-perf

用于单表场景的端到端性能测试：

1. 构造接近目标大小的 Arrow IPC Stream 文件
2. 通过 JNI 调用 HFileSDK 转成 HFile
3. 执行 `hbase org.apache.hadoop.hbase.tool.BulkLoadHFilesTool <staging_dir> <table>`
4. 记录各阶段耗时并计算吞吐量
5. 可选调用 `hfile-bulkload-verify` 做 BulkLoad 后行数校验

默认假设：

- 一个 Arrow 文件只对应一个 HBase 表
- 目标 Arrow 文件大小约为 1GB
- BulkLoad staging 目录默认为 `/tmp/hbase_bulkload`

## 构建

```bash
mvn -q -f tools/hfile-bulkload-perf/pom.xml package
```

## 运行

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib /Users/gauss/workspace/github_project/HFileSDK/build/libhfilesdk.dylib \
  --table perf_single_table \
  --work-dir /tmp/hfilesdk-bulkload-perf \
  --bulkload-dir /tmp/hbase_bulkload \
  --iterations 3 \
  --target-size-mb 1024 \
  --cf cf
```

默认会执行：

```bash
hbase org.apache.hadoop.hbase.tool.BulkLoadHFilesTool /tmp/hbase_bulkload perf_single_table
```

## BulkLoad 后校验

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib /Users/gauss/workspace/github_project/HFileSDK/build/libhfilesdk.dylib \
  --table perf_single_table \
  --bulkload-dir /tmp/hbase_bulkload \
  --verify-bulkload \
  --zookeeper localhost:2181
```

默认会尝试调用：

```bash
java -jar tools/hfile-bulkload-verify/target/hfile-bulkload-verify-1.0.0.jar \
  --zookeeper localhost:2181 \
  --table perf_single_table \
  --row-count <generated_rows>
```

## 仅验证生成与 JNI 转换

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib /Users/gauss/workspace/github_project/HFileSDK/build/libhfilesdk.dylib \
  --table perf_single_table \
  --target-size-mb 64 \
  --skip-bulkload
```

## 关键参数

- `--table`：目标 HBase 表名
- `--native-lib`：`libhfilesdk` 动态库绝对路径
- `--work-dir`：Arrow 文件与报告输出目录
- `--bulkload-dir`：BulkLoad staging 目录
- `--target-size-mb`：目标 Arrow 文件大小，默认 `1024`
- `--iterations`：完整端到端执行轮数，默认 `1`
- `--payload-bytes`：每行 `PAYLOAD` 列的字节数，默认 `768`
- `--batch-rows`：每个 Arrow RecordBatch 的行数，默认 `8192`
- `--rule`：rowKeyRule，默认 `USER_ID,0,false,0#long(),1,false,0`
- `--skip-bulkload`：跳过最终 BulkLoad，仅做前两步
- `--verify-bulkload`：BulkLoad 成功后执行行数校验
- `--verify-jar`：显式指定 `hfile-bulkload-verify` 的 jar 路径
- `--zookeeper`：校验阶段使用的 ZooKeeper 地址
- `--keep-generated-files`：保留 Arrow 文件与 staging HFile

## 输出

程序会打印：

- Arrow 文件大小
- HFile 文件大小
- Arrow 生成耗时与吞吐
- JNI 转换耗时与吞吐
- BulkLoad 耗时与吞吐
- Verify 耗时
- 端到端总耗时与吞吐
- 多轮运行的平均/最小/最大吞吐统计
- 成功/失败状态与错误信息

同时会输出 JSON 报告，默认路径为：

```text
/tmp/hfilesdk-bulkload-perf/<table>-perf-report.json
```
