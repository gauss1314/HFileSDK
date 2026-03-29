# HFileSDK 测试说明

## 当前状态

- 当前已纳入 `ctest` 的测试目标共 **23** 个
- 测试源码文件共 **21** 个
- 覆盖范围包含：编码、压缩、元数据、Writer、Arrow 转换、BulkLoad、基础 I/O、故障注入

## 运行方式

```bash
cmake --build build -j8
cd build && ctest --output-on-failure
```

## 覆盖矩阵

### 编码与序列化

| 模块 | 已覆盖场景 | 对应用例 |
|---|---|---|
| CRC32C | 已知值、增量计算、大缓冲区、chunk checksum | `test_crc32c` |
| KV 编码 | KV 序列化、tags 长度、时间戳大端序、`compare_keys`、VarInt 边界 | `test_kv_encoding`、`test_round3_bugs` |
| NoneEncoder | 空块、单 KV、满块、reset、工厂创建 | `test_block_builder` |
| PrefixEncoder | 空块、共享前缀、首 key、reset | `test_prefix_encoder` |
| DiffEncoder | 空块、单 KV、压缩收益、首 key、reset、大 key 回退 | `test_diff_encoder` |
| FastDiffEncoder | 空块、单/多 KV、压缩收益、首 key、reset、tags、大 key回退 | `test_fast_diff_encoder` |

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

## 当前边界

- 当前文档中的“全覆盖”指当前 macOS 生效构建配置下，主要功能模块、关键异常路径和重要资源边界均已有自动化回归保护
- 当前仓库尚未接入覆盖率统计工具，因此这里不表示 100% 行覆盖率或分支覆盖率
- `io_uring` 与 `HDFS` 后端属于条件编译路径，当前 macOS 构建未启用，因此未进入当前 `ctest` 矩阵
- Java/JNI 端到端调用链尚未单独纳入自动化测试；当前覆盖重点在 C++ JNI 配置解析层

## 建议的后续增强

- 接入 `llvm-cov` 或 `lcov` 输出覆盖率报表
- 在 Linux CI 中增加 `io_uring` 与 `HDFS` 条件矩阵
- 为 Java 层补充 JNI 端到端集成测试
