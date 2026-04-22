# HFileSDK 测试说明

## 当前状态

- 当前已纳入 `ctest` 的测试目标共 **25** 个
- 测试源码文件共 **22** 个（C++ 21 个 + Java JNI 集成测试 1 个）
- 覆盖范围包含：编码、压缩、元数据、Writer、Arrow 转换、BulkLoad、基础 I/O、故障注入、Java JNI 端到端、HBase Reader 黑盒校验

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
| WideTable 转换 | 正常映射、缺失 row key、null row key | `test_arrow_converter` |
| TallTable 转换 | 正常映射、缺列 | `test_arrow_converter` |
| RawKV 转换 | 正常预编码 key、损坏 key 拒绝 | `test_arrow_converter`、`test_bulk_load_writer_behavior` |
| 标量序列化 | Int64/Float32 大端序、Bool 字节、String 透传、时间戳单位归一化 | `test_arrow_converter` |
| convert() 输入校验 | 空路径、缺失 Arrow 文件、损坏 stream、非法 row key rule、内存超限 | `test_arrow_converter` |
| convert() 结果路径 | 重复 row key、重复 qualifier、LargeString、空 row key 全过滤、进度回调 | `test_arrow_converter` |

### RowKey 规则与分区

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| RowKeyBuilder | 规则编译、分隔、pad、reverse、随机段、越界列、排序稳定性 | `test_row_key_builder` |
| RegionPartitioner | 无 split、单/多 split、无序 split、二进制 key、空 key | `test_region_partitioner`、`test_round3_bugs` |
| CFGrouper | 列族注册、排序输出、family 校验、列表重建 | `test_cf_grouper` |

### BulkLoad

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| BulkLoadWriter 正常路径 | `SkipBatch`、`max_open_files` 滚动、TallTable 多 CF、RawKV、多批次统计 | `test_bulk_load_writer_behavior` |
| BulkLoadWriter 异常路径 | Strict 未知 CF、SkipBatch 批内乱序、Builder 必填参数校验 | `test_bulk_load_writer_behavior` |
| 结果与进度语义 | `BulkLoadResult`、`partial_success()`、进度回调异常收敛 | `test_production_features` |

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

## 建议的后续增强

- 在 Linux CI 中增加 `io_uring` 与 `HDFS` 条件矩阵
- 将 `hfile_coverage` 纳入 CI 并保存 HTML 报表产物
- 在 Linux CI 中补一组真实 HBase Scan / BulkLoad 集成验证
