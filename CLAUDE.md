# CLAUDE.md

本文件为 Claude Code 在 HFileSDK 项目中工作时提供指引。

## 项目简介

HFileSDK 是一个 C++20 SDK，读取磁盘上的 Arrow IPC Stream 格式文件，转换为 HFile v3 格式文件，用于 Bulk Load 到 HBase 2.6.1。SDK 编译为共享库（`.so`），由 Java 进程通过 JNI 调用。本项目替换现有的 Java 写入链路以提升性能。性能目标：写入吞吐量 ≥ Java HBase HFile.Writer 的 3 倍。**本 SDK 上生产环境，对可靠性和容错要求苛刻。**

完整设计文档：`@DESIGN.md`

---

## 不可违反的约束

以下任何一条被违反都会导致数据无法被 HBase 加载：

- 每个 HFile 只能包含**一个** Column Family。多 CF 文件会被 BulkLoadHFilesTool 拒绝。
- HFile 版本**必须是 v3**。每个 Cell 在 Value 之后必须包含 `tags_length`（2字节，通常为0）和 MVCC/MemstoreTS（变长 VarInt，通常为0）——即使为空也必须存在。
- Trailer 必须用 ProtoBuf 序列化（`proto/hfile_trailer.proto`），后跟 `pb偏移量(4B) + 主版本号3(4B) + 次版本号3(4B)`。
- FileInfo 必须包含**全部**必填字段：`LASTKEY`、`AVG_KEY_LEN`、`AVG_VALUE_LEN`、`COMPARATOR`、`MAX_TAGS_LEN`、`KEY_VALUE_VERSION`、`MAX_MEMSTORE_TS_KEY`、`CREATE_TIME_TS`、`DATA_BLOCK_ENCODING`、`LEN_OF_BIGGEST_CELL`。
- Bulk Load 输出目录结构：`<staging_dir>/<cf_name>/<hfile>`。
- 每个 HFile 内的 KV 对必须有序：Row(升序) → Family(升序) → Qualifier(升序) → Timestamp(降序) → Type(降序)。

---

## 生产可靠性规则

- **崩溃安全**：HFile 先写入临时文件（`<dir>/.tmp/<uuid>_<n>.tmp`），完成后 fsync → rename 到最终路径（`AtomicFileWriter`）。任何时刻断电，最终路径上要么有完整文件要么没有文件。
- **输入不可信**：所有 JNI 传入参数和 Arrow 文件内容在入口处校验（空 key、超长 key、超大 value、负 timestamp）。非法输入返回错误码，**绝不能导致 JVM 崩溃（C++ 异常不能穿透 JNI 边界）**。
- **内存可控**：`convert()` 对 Arrow 文件执行两遍扫描并保留全量 batch；`HFileWriter` 的 AutoSort 会在 `finish()` 前缓存全部 KV。`MemoryBudget` 可设置内存上限（默认无限制）。
- **可观测**：关键操作输出结构化 stderr 日志（INFO/WARN/ERROR，无外部日志框架依赖）。每次 `convert()` 调用通过 `getLastResult()` 返回详细指标 JSON（KV 数、字节数、耗时、跳过行数、sort_ms、write_ms 等）。
- **析构安全**：`HFileWriterImpl` 析构时检测 `opened_ && !finished_`，自动删除残留的损坏文件。

---

## 技术栈

- **语言**：C++20（concepts、`std::span`、`std::expected`）
- **构建**：CMake 3.20+，源外构建目录为 `build/`，输出 `libhfilesdk.so`
- **依赖**：Apache Arrow C++ **16.0.0**、protobuf 3.21+、zstd、lz4、snappy、spdlog 1.12+、JDK 21（仅需 `jni.h`）、Google Test、Google Benchmark、jemalloc（可选）
- **Java 工具**：Java **21**（`tools/`、`bench/java/`、`java/` 均使用 Java 21）
- **编译参数（Release）**：`-O3 -march=native -flto`
- **平台**：Linux x86-64 完整支持；macOS 完整支持（无 io_uring）；Windows + Clang-cl 核心功能支持（无 io_uring/HDFS）

---

## 常用命令

```bash
# 配置（需设置 JAVA_HOME）
cmake -B build -DCMAKE_BUILD_TYPE=Release -DJAVA_HOME=$JAVA_HOME \
    -DHFILE_ENABLE_BENCHMARKS=ON

# 编译
cmake --build build -j$(nproc)

# 运行全部测试
cd build && ctest --output-on-failure

# 生成 llvm-cov 覆盖率报表
cmake -B build-coverage -DCMAKE_BUILD_TYPE=Release -DHFILE_ENABLE_COVERAGE=ON
cmake --build build-coverage -j$(nproc)
cmake --build build-coverage --target hfile_coverage

# 或直接使用本地脚本
bash scripts/coverage.sh

# 运行单个测试
./build/test_prefix_encoder
./build/test_row_key_builder

# 运行基准测试
./build/bench/micro/bm_kv_encode --benchmark_format=json
./build/bench/macro/bm_e2e_write --benchmark_format=json

# 对比 Java 基线
cd bench/java && mvn package -q
bash scripts/bench-runner.sh

# 格式化代码
find include src test bench -name '*.cc' -o -name '*.h' | xargs clang-format -i

# 验证生成的 HFile（需要 Java + HBase jar）
java -jar tools/hfile-verify/target/hfile-verify-*.jar --hfile-dir /path/to/output
```

---

## 架构概览

```
include/hfile/          C++ 公开头文件
  types.h               KeyValue / Encoding / Compression / BE 辅助函数 / VarInt
  status.h              Status 错误处理
  writer_options.h      WriterOptions（含 FsyncPolicy / ErrorPolicy / 生产参数）
  writer.h              HFileWriter + Builder
  bulk_load_writer.h    BulkLoadWriter + Builder + ProgressInfo + BulkLoadResult
  region_partitioner.h  RegionPartitioner 接口 + 工厂方法

src/
  jni/                  JNI 接口层（异常捕获 + 参数转换）
  convert/              核心转换编排（Arrow IPC 文件 → HFile 文件，含 AutoSort）
  arrow/                Arrow IPC Stream 读取 + RowKeyRule 规则引擎 + 列值序列化
  block/                数据块编码器（None / Prefix / Diff / FastDiff）
  index/                2 级块索引写入器（entries ≤ 128 单级，超过则生成中间块）
  bloom/                复合布隆过滤器写入器
  codec/                压缩封装（LZ4 / ZSTD / Snappy / GZip / None）
  io/                   BufferedFileWriter（跨平台 FILE*）
                          AtomicFileWriter（写 .tmp → fsync → rename）
                          IoUringWriter（Linux only，双缓冲）
                          HdfsWriter（Linux/macOS only，libhdfs3）
  meta/                 FileInfo 构建器 + ProtoBuf Trailer 构建器
  checksum/             硬件加速 CRC32C（SSE4.2 + 标量回退）
  memory/               ArenaAllocator / BlockPool / MemoryBudget
  metrics/              MetricsRegistry（Counter / Gauge / Histogram / ScopedTimer）
  partition/            RegionPartitioner 实现 + CFGrouper
  writer.cc             HFileWriterImpl 核心逻辑
  bulk_load_writer.cc   BulkLoadWriterImpl（ThreadPool 并行 finish，ProgressCallback）

proto/                  hfile_trailer.proto（HFile v3 Trailer）
java/                   Java JNI 桥接（HFileSDK.java，Java 21）
bench/
  micro/                Google Benchmark 微基准（5 个）
  macro/                端到端基准（1 个）
  java/                 Java HFile.Writer 基线 benchmark（Java 21）
test/                   测试（21 个文件，已全部纳入 ctest）
tools/
  hfile-verify/         Java HBase 原生 Reader 验证工具（Java 21）
  hfile-bulkload-verify/ Bulk Load 后数据完整性验证（Java 21）
  hfile-chaos/          故障注入测试（掉电/磁盘满模拟）
  hfile-report/         Python HTML 对比报告生成器
```

---

## 核心数据流

```
Java 进程调用: HFileSDK.convert(arrowPath, hfilePath, tableName, rowKeyRule)
  → JNI 层: 参数校验 + 异常捕获（C++ 异常绝不穿透 JNI 边界）
    → 编译 rowKeyRule 为 RowKeyBuilder
    → 【第一遍】逐 Batch 读取 Arrow IPC Stream 文件
        → 每行: 列值 "|" 拼接为 rowValue 字符串
        → RowKeyBuilder.build(fields) → row key
        → 收集 SortEntry{rowKey, batchIdx, rowIdx}，所有 batch 保留在内存
    → stable_sort（row key 字典序升序）
    → 【第二遍】按排序顺序逐行读取
        → 每行所有列值序列化为 Big-Endian bytes
        → qualifier 字母序排列 → HBase KV 对
        → 编码到 DataBlock → Block 满时压缩 + CRC → AtomicFileWriter
    → 写入 Bloom Filter / Index / FileInfo / Trailer
    → close 临时文件 → rename 到最终路径
  → 返回错误码（0=成功），getLastResult() 返回 JSON 指标
```

**关键：`rowValue` 参数已在 v4.0 删除。** SDK 从 Arrow 列值内部构建管道符分隔字符串，调用方无需传入。

---

## RowKeyRule 规则格式

规则字符串由 `#` 分隔的段组成，每段：`name,index,isReverse,padLen[,padMode][,padContent]`

| 字段 | 说明 | 默认 |
|------|------|------|
| `name` | 列名标签（信息性，不参与逻辑） | 必填 |
| `index` | Arrow Schema 中的列索引（0-based） | 必填 |
| `isReverse` | `true` = 先填充后反转 | 必填 |
| `padLen` | 目标宽度；0=不填充；值超长时不截断 | 必填 |
| `padMode` | `LEFT`（默认）或 `RIGHT` | `LEFT` |
| `padContent` | 填充字符 | `0` |

特殊名 `$RND$`：生成 `padLen` 位随机数字，每位 **0–8**（镜像 `UniverseHbaseBeanUtil.SEED=9`）。

处理顺序：取字段值 → 填充（LEFT/RIGHT）→ 反转（与 Java `setValue()` 完全一致）。

示例：`"STARTTIME,0,false,10#IMSI,1,true,15#MSISDN,2,false,11,RIGHT,#$RND$,3,false,4"`

---

## 编码规范

- **命名空间**：所有代码位于 `hfile::` 下，内部实现在 `hfile::internal::` 下
- **命名风格**：函数/变量/文件 `snake_case`，类/类型 `PascalCase`，constexpr 值加 `k` 前缀
- **头文件**：每个 `.cc` 对应一个 `.h`，公开的放 `include/hfile/`，内部放同级目录，使用 `#pragma once`
- **错误处理**：返回 `hfile::Status`；热路径禁止抛异常；使用 `HFILE_RETURN_IF_ERROR(expr)` 传播错误
- **JNI 安全**：JNI 层必须用 `try/catch(...)` 捕获所有 C++ 异常转为错误码，**绝不允许异常穿透到 JVM**
- **内存管理**：写入热路径零堆分配；`BlockPool` 管理定长缓冲；`MemoryBudget` 追踪内存使用
- **字节序**：HFile 中所有多字节整数均为 Big-Endian，使用 `write_be64/32/16()`，**禁止使用** `htons`/`htonl`
- **SIMD**：必须用 `#ifdef __SSE4_2__` / `#ifdef __AVX2__` 门控，始终提供标量回退实现
- **文件写入**：HFile 输出必须经过 `AtomicFileWriter`（写 `.tmp` → fsync → rename）
- **日志**：结构化 stderr 日志（无外部依赖），ERROR=数据/I/O 错误，WARN=行跳过/资源告警，INFO=转换开始/完成/统计
- **测试**：每个编码器、规则引擎、构建器都必须有 `test_*.cc`；**每个异常处理分支也必须有测试覆盖**

---

## 性能红线

- `DataBlockEncoder::append()` 和 `BlockBuilder::add()` 中**禁止堆分配**
- Arrow 的 string/binary 值必须通过 `std::span` 引用，**禁止拷贝**
- CRC32C 必须使用 `_mm_crc32_u64`（SSE4.2），3 路并行流水线
- Block 缓冲区必须 64 字节对齐（`alignas(64)`）
- 热路径分支优先使用 `[[likely]]`/`[[unlikely]]`

---

## 验证流程

每次修改编码、FileInfo 或 Trailer 后，必须执行：

1. 编译并运行单元测试：`ctest`
2. 用测试工具生成一个测试 HFile
3. 用 hfile-verify 验证：`java -jar hfile-verify.jar --hfile <path>`
4. 运行故障注入测试：`hfile-chaos --mode kill-during-write --verify-no-corrupt-files`
5. 如有 HDFS 环境，执行完整 Bulk Load 流程并用 hfile-bulkload-verify 验证

### 当前测试矩阵

- 完整测试说明见 `@TESTING.md`
- 当前已纳入 `ctest` 的目标数为 **25**
- 当前已支持 `llvm-cov` 覆盖率报表输出
- `build-coverage/artifacts/coverage-html/` 约定为 CI HTML 产物目录
- 自动化覆盖已包含：
  - 编码与序列化：CRC32C、KV 编码、None/Prefix/Diff/FastDiff、VarInt 边界
  - 格式与元数据：Bloom、Index、FileInfo、Trailer
  - Writer：AutoSort、内存预算、磁盘阈值、tags/MVCC、排序校验、错误策略
  - Arrow/convert：WideTable、TallTable、RawKV、坏 stream、非法 row key rule、进度回调
  - BulkLoad：`SkipBatch`、`Strict`、`max_open_files`、多 CF、多批次统计、Builder 校验
  - I/O 与可靠性：`BufferedFileWriter`、`AtomicFileWriter`、`hfile-chaos`
  - Java JNI：`configure()` 非法 JSON、空路径、非法 row key rule、Java 生成 Arrow 并经 JNI 完成转换
  - HBase Reader：JNI 生成固定 HFile 后，用 `hfile-verify` 黑盒校验版本、entry count、编码、压缩与 row 顺序

---

## 实现阶段（当前状态）

| Phase | 内容 | 状态 |
|-------|------|------|
| 1 | 基础框架 + HFile v3 格式（PB Trailer、FileInfo 全字段、Tags+MVCC） | ✅ 完成 |
| 2 | 核心写入器（NONE 编码、无压缩、单 CF 约束、AtomicFileWriter） | ✅ 完成 |
| 3 | Arrow IPC Stream 读取 + RowKeyRule 规则引擎 + JNI 接口 | ✅ 完成 |
| 4 | 编码（FAST_DIFF/DIFF/PREFIX）+ 压缩（LZ4/ZSTD/Snappy）+ Bloom Filter | ✅ 完成 |
| 5 | 生产可靠性（输入校验、FsyncPolicy、ErrorPolicy、MemoryBudget、磁盘检查、进度回调） | ✅ 完成 |
| 6 | 性能优化（SSE4.2 CRC、SIMD 前缀、512B 栈缓冲、双缓冲 IoUring；Pipeline 待基准数据）| ⚠️ 核心完成 |
| 7 | 基准测试工具 + Java 基线 benchmark + 验证工具 + 文档 | ✅ 完成 |

### 有意不实现的功能

- `RegionPartitioner::from_hbase()` — 在线查询 ZooKeeper/Meta 会将集群延迟耦合到写入热路径，且引入重依赖（详见 `region_partitioner.h`）
- PGO — SDK 热路径是线性代码，PGO 典型收益 5-10%，维护成本不成比例

### 待实现功能

- ⏳ **双缓冲 Encode/Compress/IO Pipeline**：待 `bench/java/` 基线数据驱动后决定（见 DESIGN.md §11）
- ⏳ **AutoSort 外排序**：当前全量 batch 保留内存；超大文件（> 可用 RAM）需外排序

---

## 已修复 Bug 记录

| # | 位置 | 问题 | 严重度 |
|---|------|------|-------|
| B-1 | `src/checksum/crc32c.cc` | 三路 CRC 合并算法错误 → 产生错误校验值 | 数据损坏 |
| B-2 | `src/index/block_index_writer.cc` | `inline_threshold_` 从不判断 → 大文件根索引无限膨胀 | 性能 |
| B-3 | `src/block/prefix_encoder.h` / `diff_encoder.h` | `append()` 热路径每次堆分配 | 性能 |
| B-4 | `src/bulk_load_writer.cc` | `set_parallelism(n)` 被忽略，`finish()` 完全串行 | 功能缺失 |
| B-5 | `src/io/iouring_writer.cc` | 单缓冲：in-flight SQE 被覆盖 | 数据损坏 |
| B-6 | `src/codec/compressor.cc` | Snappy 未验证输出缓冲区大小 → 堆溢出 | 安全 |
| B-7 | `src/memory/block_pool.h` | 无 double-release 检测 → 数据竞争 | 并发安全 |
| B-8 | `src/partition/region_partitioner.cc` | `region_for()` 热路径堆分配；空 key 无验证 | 性能+行为 |
| B-9 | `include/hfile/types.h` `decode_varint64` | shift≥64 → C++ UB + 越界读 | 安全 |
| B-10 | `src/writer.cc` `finish()` | `finished_=true` 设在 I/O 前；失败不清理残留文件 | 资源管理 |
| B-11 | `src/block/fast_diff_encoder.h` | 固定 4096B 栈缓冲，大 key（>4083B）溢出 | 栈溢出 |

已确认**不是 Bug**：`compare_keys` 空 span（`memcmp(p,q,0)` 是 C11 定义行为）。

---

## 上下文压缩指令

压缩对话时，务必保留：当前实现阶段、已修改文件列表、测试命令、开发过程中发现的所有 HFile 格式兼容性问题，以及已知的异常处理边界 case。
