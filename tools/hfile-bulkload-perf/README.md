# hfile-bulkload-perf

用于单表场景的端到端性能测试：

1. 构造一组 Arrow IPC Stream 文件
2. 复用 `tools/arrow-to-hfile` 的最新自适应批量转换逻辑
3. 当单个 Arrow 文件平均大小大于等于 `100MB` 时，走 Java 多线程 + 多次 JNI 调用
4. 当单个 Arrow 文件平均大小小于 `100MB` 时，先合并 Arrow 再转换为 HFile
5. 创建 HDFS staging 目录并上传 HFile
6. 执行 `hbase org.apache.hadoop.hbase.tool.BulkLoadHFilesTool <hdfs_staging_dir> <table>`
7. 记录各阶段耗时并计算吞吐量
8. 可选调用 `hfile-bulkload-verify` 做 BulkLoad 后行数校验

默认假设：

- 一批 Arrow 文件都属于同一个 HBase 表
- `target-size-mb` 表示单个 Arrow 文件目标大小
- BulkLoad staging 目录默认为 `/tmp/hbase_bulkload`
- HDFS staging 根目录默认 `/hbase/staging/<table>`

## 构建

```bash
mvn -q -f tools/arrow-to-hfile/pom.xml -DskipTests install
mvn -q -f tools/hfile-bulkload-perf/pom.xml -DskipTests package
```

## 集群脚本入口

如果你需要按集群上的实际操作流程执行，可以直接使用仓库脚本：

```bash
bash scripts/hfile-bulkload-perf-runner.sh \
  --env-script /opt/client/bigdata_env \
  --principal ossuser \
  --keytab /opt/client/keytab/ossuser.keytab \
  --native-lib ./release/libhfilesdk.so \
  --table tdr_signal_stor_20550 \
  --hdfs-staging-dir /hbase/staging/job_20550 \
  -- \
  --cf cf \
  --arrow-file-count 8 \
  --target-size-mb 256 \
  --parallelism 4
```

这个脚本会顺序执行：

- `source /opt/client/bigdata_env`
- `kinit -kt <keytab> <principal>`
- 调用 `hfile-bulkload-perf` fat jar
- 由 perf 工具内部完成 Arrow 生成、Arrow→HFile、自适应分流、HDFS staging 创建、HFile 上传与 BulkLoad

## 大文件场景

单个 Arrow 文件大于等于 `100MB` 时，转换阶段会自动选择并行策略：

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib /Users/gauss/workspace/github_project/HFileSDK/build/libhfilesdk.dylib \
  --table perf_single_table \
  --work-dir /tmp/hfilesdk-bulkload-perf \
  --bulkload-dir /tmp/hbase_bulkload \
  --hdfs-staging-dir /hbase/staging/perf_single_table \
  --iterations 1 \
  --arrow-file-count 4 \
  --target-size-mb 256 \
  --parallelism 4 \
  --cf cf
```

## 小文件场景

单个 Arrow 文件小于 `100MB` 时，转换阶段会自动先合并再转换：

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib /Users/gauss/workspace/github_project/HFileSDK/build/libhfilesdk.dylib \
  --table perf_small_files \
  --work-dir /tmp/hfilesdk-bulkload-perf-small \
  --bulkload-dir /tmp/hbase_bulkload_small \
  --hdfs-staging-dir /hbase/staging/perf_small_files \
  --arrow-file-count 16 \
  --target-size-mb 32 \
  --merge-threshold 100 \
  --trigger-size 512 \
  --trigger-count 500 \
  --trigger-interval 180 \
  --cf cf
```

两种场景最终都会执行与手工流程一致的三步：

```bash
hdfs dfs -mkdir -p /hbase/staging/perf_single_table/cf
hdfs dfs -put -f /tmp/hbase_bulkload/cf/*.hfile /hbase/staging/perf_single_table/cf/
hbase org.apache.hadoop.hbase.tool.BulkLoadHFilesTool /hbase/staging/perf_single_table perf_single_table
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
  --arrow-file-count 8 \
  --target-size-mb 64 \
  --skip-bulkload
```

## 关键参数

- `--table`：目标 HBase 表名
- `--native-lib`：`libhfilesdk` 动态库绝对路径
- `--work-dir`：Arrow 文件与报告输出目录
- `--bulkload-dir`：BulkLoad staging 目录
- `--hdfs-staging-dir`：HDFS 侧 BulkLoad staging 根目录
- `--arrow-file-count`：生成的 Arrow 文件数量，默认 `1`
- `--target-size-mb`：每个 Arrow 文件目标大小，默认 `1024`
- `--parallelism`：大文件并行转换线程数，默认 CPU 核数
- `--merge-threshold`：小文件/大文件分流阈值，默认 `100`
- `--trigger-size`：小文件合并策略的攒批大小阈值，默认 `512`
- `--trigger-count`：小文件合并策略的攒批文件数阈值，默认 `500`
- `--trigger-interval`：小文件合并策略的时间阈值，默认 `180`
- `--iterations`：完整端到端执行轮数，默认 `1`
- `--hdfs-bin`：HDFS 命令路径，默认 `hdfs`
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

- Arrow 目录、HFile 目录
- Arrow 文件数量、生成出的 HFile 数量
- 实际采用的转换策略
- Arrow 总大小
- HFile 总大小
- Arrow 生成耗时与吞吐
- 自适应转换耗时与吞吐
- HDFS staging 准备耗时
- HDFS 上传耗时
- BulkLoad 耗时与吞吐
- Verify 耗时
- 端到端总耗时与吞吐
- 多轮运行的平均/最小/最大吞吐统计
- 成功/失败状态与错误信息

同时会输出 JSON 报告，默认路径为：

```text
/tmp/hfilesdk-bulkload-perf/<table>-perf-report.json
```
