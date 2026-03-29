# HFileSDK 详细设计文档 v4.0

> C++ 高性能 HFile Writer SDK — 适配 HBase 2.6.1 Bulk Load

**版本**: v4.0 | **日期**: 2026-03-28 | **基于实现**: 全部模块已完成并通过测试

---

## 修订说明

| 版本 | 变更摘要 |
|------|---------|
| v1.0 | 初始设计草稿 |
| v2.0 | 修复 6 个致命设计缺陷（Bulk Load 全流程、单 CF 约束、Region 分裂、HDFS I/O、Cell Tags、PB Trailer、FileInfo 完整字段） |
| v3.0 | 与实际代码对齐：更新所有已实现模块的精确描述，记录 10 个经代码审查发现并修复的 Bug，更新平台支持矩阵 |
| **v4.0** | **架构升级**：JNI 接口层、Arrow IPC Stream 文件读取、RowKeyRule 规则引擎、SDK 内部 AutoSort（两遍扫描）；删除 rowValue 参数 |

---

## 0. v4.0 关键变更

| 变更 | 说明 |
|------|------|
| **JNI 接口层新增** | SDK 编译为 `libhfilesdk.so`，Java 进程通过 JNI 调用 |
| **rowValue 参数删除** | rowValue 由 SDK 从 Arrow 列值内部生成，不再由调用方传入 |
| **RowKeyRule 规则引擎** | 实现 `RowKeyBuilder`，镜像 `UniverseHbaseBeanUtil` 逻辑 |
| **AutoSort 实现** | 两遍扫描：第一遍建 sort index，内存排序，第二遍按序写入 |
| **Java 版本升级** | 所有 Java 工具/桥接改为 Java 21 |
| **sort_mode 默认值** | 改为 `AutoSort`（输入端不保证有序） |

---

## 1. 项目概述

### 1.1 背景与目标

HFileSDK 是一个 C++20 库，将 Apache Arrow 格式的内存数据高性能写入 HFile v3 文件，通过 HBase 2.6.1 的 `BulkLoadHFilesTool` 批量导入 HBase。

核心数据流：
```
Arrow RecordBatch → HFile v3 → 本地/HDFS → BulkLoadHFilesTool → HBase RegionServer
```

**性能目标**：在相同硬件和压缩配置下，写入吞吐量 ≥ Java `HFile.Writer` 的 **3 倍**。

### 1.2 HBase 2.6.1 硬性约束（不可违反）

以下任何一条违反都会导致文件无法被 HBase 加载，SDK 在代码层面全部强制执行：

| # | 约束 | 执行位置 |
|---|------|----------|
| C-1 | 每个 HFile **只含一个** Column Family | `HFileWriterImpl::append()` 拒绝错误 CF |
| C-2 | HFile 版本必须是 **v3**（major=3, minor=3） | `TrailerBuilder` 硬编码 |
| C-3 | 每个 Cell 必须有 `tags_length`（2B BE）和 `mvcc`（VarInt），即使为零 | 所有 `*_encoder.h` |
| C-4 | Trailer 必须用 **ProtoBuf** 序列化，尾部固定 `[pb_offset(4B)][3][3]` | `TrailerBuilder::finish()` |
| C-5 | FileInfo 必须包含全部 **10 个字段**（见 §2.3） | `FileInfoBuilder` |
| C-6 | Bulk Load 目录：`<output_dir>/<cf>/<hfile>` | `BulkLoadWriterImpl` |
| C-7 | KV 严格有序：Row↑→Family↑→Qualifier↑→Timestamp↓→Type↓ | `compare_keys()` + 写入验证 |
| C-8 | 所有多字节整数为 **Big-Endian** | `write_be64/32/16()` 辅助函数 |

---

## 2. HFile v3 格式规范（已实现）

### 2.1 文件总体结构

```
┌─────────────────────────────────────────┐
│  Scanned Block Section                  │
│  Data Block 0  (DATABLK2 + 33B header)  │
│  Data Block 1                           │
│  ...                                    │
├─────────────────────────────────────────┤
│  Non-scanned Block Section              │
│  Bloom Chunk Block 0  (BLMFBLK2)        │
│  Bloom Chunk Block 1                    │
│  ...                                    │
│  Bloom Meta Block     (BLMFMET2)        │
├─────────────────────────────────────────┤
│  Load-on-open Section                   │
│  [Intermediate Index Blocks] (IDXINTE2) │ ← 仅大文件（entries > 128）
│  Root Index Block    (IDXROOT2)         │
│  FileInfo Block      (FILEINF2)         │
├─────────────────────────────────────────┤
│  Trailer                                │
│  [ProtoBuf FileTrailerProto bytes]      │
│  [pb_start_offset : uint32 BE]          │ ← pb大小 + 12
│  [major_version   : uint32 BE = 3]      │
│  [minor_version   : uint32 BE = 3]      │
└─────────────────────────────────────────┘
```

### 2.2 Block Header（33 字节，所有 Block 类型通用）

```
[Block Type Magic : 8B]    DATABLK2 / IDXROOT2 / IDXINTE2 / BLMFBLK2 / BLMFMET2 / FILEINF2
[Compressed Size  : 4B BE] 压缩后数据大小（不含 Header）
[Uncompressed Size: 4B BE] 压缩前数据大小
[Prev Block Offset: 8B BE] 前一个 Block 的文件偏移（第一个 Block 为 -1）
[Checksum Type    : 1B]    CRC32C = 2
[Bytes Per Checksum:4B BE] 每 512 字节一个 CRC32C
[On-disk Data Size: 4B BE] 磁盘上的数据大小（含校验和）
```

### 2.3 Cell（KeyValue）编码格式（HFile v3）

```
[keyLen       : 4B BE]     Key 的总字节数
[valueLen     : 4B BE]     Value 的字节数
[rowLen       : 2B BE]     Row Key 字节数
[row          : variable]  Row Key 原始字节
[famLen       : 1B]        Column Family 字节数
[family       : variable]  Column Family 原始字节
[qualifier    : variable]  Column Qualifier 原始字节
[timestamp    : 8B BE]     毫秒级 Unix 时间戳
[keyType      : 1B]        Put=4, Delete=8, DeleteColumn=12, DeleteFamily=14
[value        : variable]  实际值数据
[tagsLen      : 2B BE]  ★ HFile v3 必须存在，无 Tag 时为 0
[tags         : variable]  tags 序列（通常为空）
[mvcc         : VarInt]  ★ HFile v3 必须存在，Bulk Load 场景为 0
```

★ 两个字段即使为零也**必须写入**，这是 HBase 2.6.1 反序列化 v3 格式的硬性要求。

#### VarInt 编码说明

`mvcc`（MemstoreTS）使用 base-128 变长整数编码：
- 每字节低 7 位是数据，最高位是续位标志（1=还有后续字节）
- `uint64_t` 最多 10 字节（ceil(64/7) = 10）
- `decode_varint64()` 实现了 10 字节上限保护，超出则返回 -1（防止恶意输入触发 C++ UB）

### 2.4 FileInfo 必填字段（10 个，全部已实现）

| FileInfo Key | 值类型 | 说明 |
|-------------|-------|------|
| `hfile.LASTKEY` | bytes | 文件中最后一个 Cell 的 Key 字节 |
| `hfile.AVG_KEY_LEN` | int32 BE | 所有 Key 的平均长度 |
| `hfile.AVG_VALUE_LEN` | int32 BE | 所有 Value 的平均长度 |
| `hfile.MAX_TAGS_LEN` | int32 BE | Cell Tags 的最大长度（v3 必须设置）|
| `hfile.KEY_VALUE_VERSION` | int32 BE | = 1，表示包含 MemstoreTS |
| `hfile.MAX_MEMSTORE_TS_KEY` | int64 BE | Bulk Load 场景为 0 |
| `hfile.COMPARATOR` | UTF-8 | `org.apache.hadoop.hbase.CellComparatorImpl` |
| `hfile.DATA_BLOCK_ENCODING` | UTF-8 | `NONE`/`PREFIX`/`DIFF`/`FAST_DIFF` |
| `hfile.CREATE_TIME_TS` | int64 BE | 文件创建时间戳（毫秒） |
| `hfile.LEN_OF_BIGGEST_CELL` | int64 BE | 最大 Cell 的字节数 |

### 2.5 Trailer ProtoBuf 定义（`proto/hfile_trailer.proto`）

```protobuf
message FileTrailerProto {
  optional uint64 file_info_offset             = 1;
  optional uint64 load_on_open_data_offset     = 2;
  optional uint64 uncompressed_data_index_size = 3;
  optional uint64 total_uncompressed_bytes     = 4;
  optional uint32 data_index_count             = 5;
  optional uint32 meta_index_count             = 6;
  optional uint64 entry_count                  = 7;
  optional uint32 num_data_index_levels        = 8;
  optional uint64 first_data_block_offset      = 9;
  optional uint64 last_data_block_offset       = 10;
  optional string comparator_class_name        = 11;
  optional uint32 compression_codec            = 12;
  optional bytes  encryption_key               = 13;
}
```

### 2.6 块索引结构（2 级，已实现）

`BlockIndexWriter` 根据数据块数量自动选择索引级别：

**1 级（entries ≤ 128）**：
```
Root Index Block (IDXROOT2)
  count(4B BE) + [offset(8B) + dataSize(4B) + keyLen(4B) + key] × N
```

**2 级（entries > 128）**：
```
Intermediate Index Block 0 (IDXINTE2)  → 指向 Data Block 0..127
Intermediate Index Block 1 (IDXINTE2)  → 指向 Data Block 128..255
...
Root Index Block (IDXROOT2)            → 指向各 Intermediate Block
```

阈值 `max_entries_per_block` 默认 128，构造时可调整。

### 2.7 Bloom Filter 结构（Compound，已实现）

- 类型：Row（默认）或 RowCol
- 哈希算法：Murmur3，5 个哈希函数
- 默认误报率：1%
- 分块存储：每个 Data Block 对应一个 Bloom Chunk（`BLMFBLK2`）
- 元数据块：`BLMFMET2`，包含 chunk 目录和参数

### 2.8 数据块编码（4 种，全部已实现）

| 编码 | 压缩原理 | 热路径实现细节 |
|------|---------|--------------|
| `NONE` | 无压缩，顺序拼接 | 最快写路径 |
| `PREFIX` | 共享 Key 前缀压缩 | 512B 栈缓冲 + SSE4.2 SIMD 前缀扫描 |
| `DIFF` | PREFIX + 时间戳/类型差分 | 同上 |
| `FAST_DIFF` | 优化的 DIFF，减少分支预测失败 | 同上，生产推荐 |

所有编码器的 `append()` 热路径：
- Key 序列化使用 **512B 栈缓冲**（`kStackBuf = 512`），超长 Key 才回退到堆分配
- 前缀比较使用 **SSE4.2 `_mm_cmpeq_epi8`**（16B/次），标量回退

### 2.9 压缩算法（5 种，全部已实现）

| 算法 | 压缩比 | 压缩速度 | 解压速度 | 推荐场景 |
|------|-------|---------|---------|---------|
| None | 1:1 | N/A | N/A | 最快写入，CPU 敏感 |
| LZ4 | ~2.5:1 | 极快 | 极快 | **默认推荐，综合最佳** |
| ZSTD | ~3.5:1 | 快 | 快 | 存储空间敏感 |
| Snappy | ~2:1 | 极快 | 极快 | 低延迟场景 |
| GZip | ~4:1 | 慢 | 中 | 归档存储 |

Snappy 解压特别说明：已验证 `GetUncompressedLength()` 后才调用 `RawUncompress()`，防止输出缓冲区溢出。

---

## 3. HBase Bulk Load 全流程

### 3.1 端到端流程

```
Step 1: Prepare  — 获取 Region Split Points（离线手动提供或在线查询）
Step 2: Transform — Arrow → 按 CF+Region 分组 → 写入 HFile v3
Step 3: Stage    — HFile 文件放置到 HDFS（或本地临时目录）
Step 4: Load     — hbase org.apache.hadoop.hbase.tool.BulkLoadHFilesTool <dir> <table>
```

### 3.2 目录结构规范

```
/staging_dir/
  ├── cf1/
  │   ├── hfile_region_0000.hfile
  │   ├── hfile_region_0001.hfile
  │   └── ...
  └── cf2/
      ├── hfile_region_0000.hfile
      └── ...
```

命名格式：`hfile_region_%04d.hfile`，由 `BulkLoadWriterImpl` 自动生成。

### 3.3 Region 分区策略

提供两种工厂方法：

```cpp
// 离线模式：调用方提供 Split Points（已排序，非空）
auto p = hfile::RegionPartitioner::from_splits(split_points);

// 单 Region 模式：不分裂，由 BulkLoad 工具处理
auto p = hfile::RegionPartitioner::none();
```

`region_for(row_key)` 实现细节：
- 使用 `std::upper_bound` + 自定义 lambda 比较器，直接比较 `std::span` 和 `std::vector`
- **零堆分配**（已从原来的临时 vector 构造改为无分配比较）
- 空 row_key 返回 0（HBase 不允许空 key，此处容错处理）

在线查询模式（`from_hbase()`）**有意不提供**——详见 §11 Phase 4 设计决策。

---

## 4. Arrow 数据映射与 RowKey 生成

### 4.1 数据流（v4.0）

```
Arrow IPC Stream 文件（磁盘）
        │
        │ RecordBatchStreamReader（流式逐批读取）
        ▼
RecordBatch（内存，一批数千～数万行）
        │
        │ 第一遍：每行列值 → "|" 拼接 rowValue 字符串
        │         → RowKeyBuilder.build() → row key 字符串
        │         收集 SortEntry{rowKey, batchIdx, rowIdx}
        ▼
SortEntry 列表（全量 row key 索引）
        │ stable_sort（行字典序 ASC）
        ▼
排好序的 SortEntry 列表
        │
        │ 第二遍：按排序顺序，读对应 batch 的 row，
        │         列值序列化为 Big-Endian bytes → KV → 写入 HFile
        ▼
HFile v3（原子写入：.tmp → fsync → rename）
```

### 4.2 RowKeyRule 规则引擎（v4.0 新增）

#### 规则格式

规则字符串由 `#` 分隔的段组成，每段格式：

```
colName,index,isReverse,padLen[,padMode][,padContent]
```

| 字段 | 说明 | 默认值 |
|------|------|--------|
| `colName` | 列名标签（仅用于文档，不参与逻辑）| 必填 |
| `index` | 对应 Arrow Schema 中的列索引（0-based）| 必填 |
| `isReverse` | `true` 则**先填充后反转**；`false` 则只填充 | 必填 |
| `padLen` | 目标宽度；0 = 不填充；值超长时不截断 | 必填 |
| `padMode` | `LEFT`（默认，前缀补 `padContent`）或 `RIGHT` | `LEFT` |
| `padContent` | 填充字符 | `0` |

**特殊名称 `$RND$`**：不读取列值，生成 `padLen` 位随机数字（每位 0–8，与 `UniverseHbaseBeanUtil.SEED=9` 对应）。

#### 示例（来自设计文档）

```
rowKeyRule = "STARTTIME,0,false,10#IMSI,1,true,15#MSISDN,2,false,11,RIGHT,#$RND$,3,false,4"
rowValue   = "20240301123000|460001234567890|13800138000|120|1024000|2048000"
```

| 段 | 规则 | 原值 | 处理 | 结果 |
|----|------|------|------|------|
| 1 | `STARTTIME,0,false,10` | `20240301123000`（14位）| 14 > 10，无需填充 | `20240301123000` |
| 2 | `IMSI,1,true,15` | `460001234567890`（15位）| 长度==15，不填充，反转 | `098765432100064` |
| 3 | `MSISDN,2,false,11,RIGHT` | `13800138000`（11位）| 11==11，不填充 | `13800138000` |
| 4 | `$RND$,3,false,4` | — | 生成4位随机数（0–8）| `0732`（示例）|

最终 RowKey = `20240301123000` + `098765432100064` + `13800138000` + `0732`

#### 与 rowValue 的关系（v4.0 重要变更）

v3.0 及之前：`rowValue` 由 Java 调用方构建后传入 JNI。  
**v4.0 开始**：`rowValue` 由 SDK 内部从 Arrow 列值自动生成：

```
Arrow 行第 i 列的字符串值  →  pipe 拼接  →  rowValue
例：列[0]="20240301123000", 列[1]="460001234567890", 列[2]="13800138000"
    → rowValue = "20240301123000|460001234567890|13800138000"
```

这样 JNI 接口只需要 `arrowPath`、`hfilePath`、`tableName`、`rowKeyRule`，**rowValue 参数已删除**。

#### 实现：`src/arrow/row_key_builder.h/.cc`

```cpp
// 编译规则（每次 JNI 调用开始时执行一次）
auto [rkb, s] = RowKeyBuilder::compile(row_key_rule);

// 第一遍扫描：每行生成 row key
for (int64_t r = 0; r < batch.num_rows(); ++r) {
    std::string row_val = build_pipe_value(batch, r, max_col_idx);
    auto fields = split_row_value(row_val);
    std::string rk = rkb.build(fields);
    sort_index.push_back({rk, batch_idx, r});
}
```

### 4.3 AutoSort（v4.0 实现）

| 模式 | 描述 | 状态 |
|------|------|------|
| `PreSortedTrusted` | 调用方保证有序，不验证 | ✅ |
| `PreSortedVerified` | 流式验证每个 KV | ✅ |
| **`AutoSort`**（**默认**）| **`convert()` 两遍扫描排序；`HFileWriter` 内存缓冲并在 `finish()` 排序** | ✅ **已实现** |

**两遍扫描实现**（`src/convert/converter.cc`）：

1. **第一遍**：流式读取所有 RecordBatch，为每行生成 row key，收集 `SortEntry{rowKey, batchIdx, rowIdx}`，所有 batch 保留在内存。
2. **排序**：`std::stable_sort` 按 row key 字典序升序。
3. **第二遍**：按排序顺序，从内存中的 batch 读取对应行，列值序列化后写入 HFile（qualifier 按字母序排列，保证 HBase 排序正确性）。

**内存占用**：所有 batch 保留在内存，同时存储 sort index（每条 ~40 字节 + row key 长度）。对于 1GB Arrow 文件约需 1–3 GB 内存。如果内存紧张可考虑外排序（当前未实现）。

### 4.4 三种映射模式（保留，供 BulkLoadWriter 使用）

- **WideTable**：每行 → N 个 KV，需 `__row_key__` 列
- **TallTable**：每行 → 1 个 KV，需 `row_key/cf/qualifier/timestamp/value` 列
- **RawKV**：预编码的 `key`+`value` 两列

这三种模式用于 `BulkLoadWriter::write_batch()`。JNI 转换器（`converter.cc`）使用不同路径：直接按 RowKeyRule 生成 key，所有列值作为 qualifier。

### 4.5 Arrow 类型序列化规则

| Arrow 类型 | rowValue 字符串化 | HBase Value 字节 |
|-----------|-----------------|-----------------|
| Int8/16/32/64 | `std::to_string()` | Big-Endian 固定字节 |
| UInt8/16/32/64 | `std::to_string()` | Big-Endian 固定字节 |
| Float/Double | `std::to_string()` | IEEE 754 Big-Endian |
| Boolean | `"1"` / `"0"` | 1B: 0x01/0x00 |
| String/LargeString | 原始 UTF-8 | UTF-8 字节（零拷贝） |
| Binary | 不支持作 row key 字段 | 原始字节 |
| Timestamp | `std::to_string(ms)` | Int64 BE（毫秒） |

---

## 5. 系统架构

### 5.1 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│  API 层         HFileWriter / BulkLoadWriter / Builder      │
├─────────────────────────────────────────────────────────────┤
│  Partition 层   RegionPartitioner / CFGrouper               │
├─────────────────────────────────────────────────────────────┤
│  Transform 层   ArrowToKVConverter（3 种映射模式）            │
├─────────────────────────────────────────────────────────────┤
│  Encoding 层    DataBlockEncoder（None/Prefix/Diff/FastDiff）│
│                 BlockIndexWriter（1级/2级自动切换）            │
│                 CompoundBloomFilterWriter                    │
│                 FileInfoBuilder / TrailerBuilder             │
├─────────────────────────────────────────────────────────────┤
│  Codec 层       Compressor（LZ4/ZSTD/Snappy/GZip/None）     │
│                 CRC32C（SSE4.2 + 标量回退）                   │
├─────────────────────────────────────────────────────────────┤
│  I/O 层         BufferedFileWriter（跨平台，FILE*）           │
│                 IoUringWriter（Linux only，双缓冲）            │
│                 HdfsWriter（Linux/macOS only，libhdfs3）      │
├─────────────────────────────────────────────────────────────┤
│  Memory 层      ArenaAllocator / BlockPool                  │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 模块文件对照表

| 模块 | 头文件 | 实现文件 | 说明 |
|------|-------|---------|------|
| 公开 API | `include/hfile/*.h` | — | 用户直接使用的接口 |
| HFileWriter | `src/writer.cc` | — | 核心写入器（含 `HFileWriterImpl`） |
| BulkLoadWriter | `src/bulk_load_writer.cc` | — | 含内置 ThreadPool |
| DataBlockEncoder | `src/block/` | `block_builder.cc` | 工厂 + 4 种实现 |
| BlockIndexWriter | `src/index/block_index_writer.h` | `.cc` | 2 级索引 |
| BloomFilter | `src/bloom/compound_bloom_filter_writer.h` | `.cc` | 分块 Bloom |
| Compressor | `src/codec/compressor.h` | `.cc` | 5 种压缩 |
| CRC32C | `src/checksum/crc32c.h` | `.cc` | 串行 SSE4.2 + 标量 |
| FileInfoBuilder | `src/meta/file_info_builder.h` | `.cc` | 10 个必填字段 |
| TrailerBuilder | `src/meta/trailer_builder.h` | `.cc` | ProtoBuf 序列化 |
| ArenaAllocator | `src/memory/arena_allocator.h` | `.cc` | bump-pointer 分配 |
| BlockPool | `src/memory/block_pool.h` | `.cc` | 定长缓冲区池 |
| BufferedFileWriter | `src/io/buffered_writer.h` | `.cc` | 跨平台 FILE* |
| IoUringWriter | `src/io/iouring_writer.h` | `.cc` | 双缓冲，Linux only |
| HdfsWriter | `src/io/hdfs_writer.h` | `.cc` | libhdfs3，Linux/macOS |
| RegionPartitioner | `include/hfile/region_partitioner.h` | `src/partition/region_partitioner.cc` | 手动分裂 |
| CFGrouper | `src/partition/cf_grouper.h` | `.cc` | CF 注册与验证 |
| ArrowToKVConverter | `src/arrow/arrow_to_kv_converter.h` | `.cc` | 3 种映射模式 |

### 5.3 JNI 接口层（v4.0 新增）

#### Java 侧接口（`java/src/main/java/com/hfile/HFileSDK.java`）

```java
package com.hfile;

public class HFileSDK {
    static { System.loadLibrary("hfilesdk"); }

    /**
     * 将 Arrow IPC Stream 文件转换为 HFile v3 文件。
     * SDK 内部完成 AutoSort（两遍扫描），输入无需预排序。
     *
     * @param arrowPath   Arrow IPC Stream 格式文件路径（本地磁盘）
     * @param hfilePath   输出 HFile 文件路径（原子写入：.tmp → rename）
     * @param tableName   HBase 表名（用于元信息）
     * @param rowKeyRule  Row Key 规则表达式（见 §4.2）
     * @return 0=成功，非0=错误码（见 §5.3.1）
     */
    public native int convert(String arrowPath, String hfilePath,
                               String tableName, String rowKeyRule);

    /** 获取上次 convert() 调用的 JSON 结果（含耗时、KV数等）。 */
    public native String getLastResult();

    /** 设置全局配置（在第一次 convert() 前调用）。 */
    public native int configure(String configJson);
}
```

**注意**：相比早期设计，**`rowValue` 参数已删除**。rowValue 由 SDK 从 Arrow 列值内部构建。

#### C++ 侧 JNI 实现（`src/jni/hfile_jni.cc`）

```cpp
JNIEXPORT jint JNICALL
Java_com_hfile_HFileSDK_convert(JNIEnv* env, jobject,
    jstring j_arrow_path, jstring j_hfile_path,
    jstring j_table_name, jstring j_row_key_rule)
{
    try {
        ConvertOptions opts;
        opts.arrow_path   = jstring_to_string(env, j_arrow_path);
        opts.hfile_path   = jstring_to_string(env, j_hfile_path);
        opts.table_name   = jstring_to_string(env, j_table_name);
        opts.row_key_rule = jstring_to_string(env, j_row_key_rule);
        // writer_opts from global configure() state

        ConvertResult r = hfile::convert(opts);
        // store result for getLastResult()
        return r.error_code;
    } catch (const std::exception& e) {
        LOG_ERROR("JNI convert: {}", e.what());
        return ErrorCode::INTERNAL_ERROR;
    } catch (...) {
        return ErrorCode::INTERNAL_ERROR;
    }
}
```

**关键保证**：所有 C++ 异常均被 JNI 层捕获，永远不会穿透到 JVM（会导致 JVM 崩溃）。

#### 5.3.1 错误码表

| 错误码 | 名称 | 说明 |
|-------|------|------|
| 0 | OK | 成功 |
| 1 | INVALID_ARGUMENT | 参数为空或文件不存在 |
| 2 | ARROW_FILE_ERROR | Arrow 文件无法打开或格式错误 |
| 3 | SCHEMA_MISMATCH | Arrow Schema 与 rowKeyRule 不匹配 |
| 4 | INVALID_ROW_KEY_RULE | rowKeyRule 语法错误 |
| 5 | SORT_VIOLATION | 排序验证失败 |
| 10 | IO_ERROR | 文件 I/O 错误 |
| 11 | DISK_EXHAUSTED | 磁盘空间不足 |
| 12 | MEMORY_EXHAUSTED | 内存超出配置上限 |
| 20 | INTERNAL_ERROR | C++ 内部未预期错误（已被 JNI 捕获） |

#### 5.3.2 getLastResult() JSON 格式

```json
{
  "error_code":          0,
  "error_message":       "",
  "arrow_batches_read":  156,
  "arrow_rows_read":     1523400,
  "kv_written_count":    1523377,
  "kv_skipped_count":    23,
  "hfile_size_bytes":    845000000,
  "elapsed_ms":          1230,
  "sort_ms":             450,
  "write_ms":            520
}
```

```cpp
auto [bulk, s] = hfile::BulkLoadWriter::builder()
    .set_table_name("my_table")
    .set_column_families({"cf1", "cf2"})
    .set_output_dir("/tmp/staging/my_table")
    .set_partitioner(RegionPartitioner::from_splits(splits))
    .set_compression(hfile::Compression::LZ4)
    .set_block_size(64 * 1024)
    .set_data_block_encoding(hfile::Encoding::FastDiff)
    .set_bloom_type(hfile::BloomType::Row)
    .set_parallelism(4)     // finish() 时 4 个线程并行关闭各 HFile
    .build();

for (auto& batch : record_batches)
    bulk->write_batch(batch, MappingMode::WideTable);

auto [result, s2] = bulk->finish();
// result.staging_dir = "/tmp/staging/my_table"
// result.files = ["cf1/hfile_region_0000.hfile", "cf2/hfile_region_0000.hfile", ...]
// result.total_entries = ...
```

### 5.4 HFileWriter API

```cpp
auto [writer, s] = hfile::HFileWriter::builder()
    .set_path("/tmp/staging/cf1/hfile_0000.hfile")
    .set_column_family("cf1")
    .set_compression(hfile::Compression::LZ4)
    .set_data_block_encoding(hfile::Encoding::FastDiff)
    .set_bloom_type(hfile::BloomType::Row)
    .set_sort_mode(WriterOptions::SortMode::PreSortedVerified)
    .build();

writer->append(kv);         // 按序追加 KeyValue
writer->finish();           // 写入 Index/Bloom/FileInfo/Trailer，关闭文件
// 若 finish() 未调用或失败，析构函数自动删除残留的损坏文件
```

### 5.5 内存管理设计

```
写入热路径的内存分配策略：

BlockPool（预分配）
  ├── Compress Buffer Pool  → 存储压缩后数据
  └── I/O Buffer Pool       → 存储待写入数据

ArenaAllocator（bump-pointer）
  └── 索引构建期临时内存

栈分配（stack buffer）
  └── Key 序列化缓冲区（512B，覆盖 >99% 的实际 Key 大小）

设计目标：write() 热路径零 malloc/free
```

### 5.6 Pipeline 设计（当前状态）

**当前实现**：Encode → Compress → Write 三阶段**串行**执行。

**设计目标**（未实现）：双缓冲三线程流水线：
```
线程 1 (Encode):   [编码 Block N+1] [编码 Block N+2] ...
线程 2 (Compress): [压缩 Block N]   [压缩 Block N+1] ...
线程 3 (I/O):      [写入 Block N-1] [写入 Block N]   ...
```
实现方案：`HFileWriterImpl` 中增加 `std::thread` + 双 `BlockPool` 缓冲区 + `std::condition_variable` 协调。此功能是达到 3× 性能目标的关键优化之一。

---

## 6. 性能优化

### 6.1 已实现的优化

| 优化项 | 实现位置 | 预期收益 |
|-------|---------|---------|
| 零热路径堆分配 | `ArenaAllocator` + `BlockPool` + 栈缓冲 | 极高 |
| SSE4.2 硬件 CRC32C | `src/checksum/crc32c.cc` | 高（~10× vs 软件） |
| SIMD 前缀扫描（16B/次） | `prefix_encoder.h`, `diff_encoder.h`, `fast_diff_encoder.h` | 中高 |
| 64 字节对齐 Block 缓冲区 | `alignas(64)` | 中 |
| `[[likely]]/[[unlikely]]` 分支提示 | 热路径分支判断 | 中 |
| Arrow String/Binary 零拷贝 | `std::span` 引用原始 Arrow 缓冲区 | 高 |
| BulkLoad 多文件并行 finish | `ThreadPool` in `BulkLoadWriterImpl` | 中高 |
| IoUringWriter 双缓冲 | `buf_[2]` + `cur_` 轮换 | 中 |

### 6.2 未实现的优化

| 优化项 | 预期收益 | 优先级 |
|-------|---------|-------|
| 双缓冲 Encode/Compress/IO Pipeline | 极高（待基准数据驱动）| 见 §11 决策说明 |
| AVX2 前缀扫描（32B/次，当前为 16B） | 低 | P3 |

### 6.3 与 Java 版本差距分析

| Java 瓶颈 | 已对应的 C++ 优化 | 备注 |
|---------|---------------|------|
| 大量小对象分配 | Arena + BlockPool + 栈缓冲 | ✅ 已实现 |
| byte[] 反复拷贝 | `std::span` 零拷贝 | ✅ 已实现 |
| JNI 压缩开销 | 直接链接压缩库 | ✅ 已实现 |
| CRC32C 软件实现 | SSE4.2 硬件指令 | ✅ 已实现 |
| GC 停顿 | C++ 无 GC | ✅ 天然优势 |
| 串行 I/O | io_uring 双缓冲 | ✅ 已实现（可选） |
| 三阶段串行执行 | Pipeline（待基准数据驱动后决定是否实现）| ⏳ 待数据 |

---

## 7. I/O 层

### 7.1 BufferedFileWriter（跨平台，主要实现）

基于标准 C `FILE*`，在所有平台上可用。

```cpp
// 内部实现：fopen("wb") + setvbuf(_IONBF) + 自管理 4MB 应用层缓冲
// flush(): fwrite + fflush + fsync（Linux/macOS）/ no-op（Windows）
// 析构: 自动 drain + fclose
```

平台行为差异：
- **Linux/macOS**：`fsync(fileno(file_))` 保证数据持久化
- **Windows**：`fflush()` 后 Windows 文件缓存管理持久化（`fsync` 为 no-op）

### 7.2 IoUringWriter（Linux only，双缓冲）

```
buf_[0] ←→ buf_[1]  (cur_ 指向当前活跃缓冲)

submit_active() 协议：
  1. 等待上一个 in-flight SQE 完成（确保 inactive buffer 安全覆盖）
  2. 提交 buf_[cur_] 的异步写 SQE
  3. cur_ ^= 1 切换到另一个缓冲
```

关键保证：in-flight SQE 始终指向 **inactive** 缓冲，active 缓冲不被 pending SQE 引用，消除数据损坏风险。

### 7.3 HdfsWriter（Linux/macOS only，可选）

依赖 `libhdfs3`，直写 HDFS，无需 JVM。内置 8MB 应用层缓冲，减少 HDFS RPC 次数。Windows 不支持（CMake 自动禁用）。

---

## 8. 平台支持

| 平台 | 编译器 | 支持级别 | io_uring | HDFS |
|------|--------|---------|---------|------|
| Linux x86-64 | GCC 12+ | ✅ 完整 | ✅ | ✅ |
| Linux x86-64 | Clang 16+ | ✅ 完整 | ✅ | ✅ |
| macOS x86-64/ARM | Apple Clang 15+ | ✅ 完整 | ❌ | ✅ |
| Windows x86-64 | Clang 17+ (clang-cl) | ✅ 核心功能 | ❌ | ❌ |
| Windows x86-64 | MSVC 2022 | ⚠️ 需验证 | ❌ | ❌ |

**Windows 注意事项**：
- CMake 自动检测平台，Windows 上 `io_uring` 和 `HDFS` 选项自动关闭
- `BufferedFileWriter` 使用标准 C `FILE*`，Windows 完全支持
- SSE4.2 intrinsics 在 Clang-cl 中可用
- `__builtin_ctz`、`__builtin_expect` 在 Clang-cl 中可用
- Debug 构建需定义 `_CRT_SECURE_NO_WARNINGS` 和 `NOMINMAX`（CMake 已处理）

---

## 9. 工程结构

```
HFileSDK/
├── CMakeLists.txt           平台感知构建（Linux/macOS/Windows + GCC/Clang/MSVC）
├── CLAUDE.md                Claude Code 开发指引
├── DESIGN.md                本文件
├── README.md                用户文档（含 Windows 构建步骤）
├── proto/
│   └── hfile_trailer.proto  FileTrailerProto ProtoBuf 定义
├── include/hfile/           公开 API（用户 include 路径）
│   ├── hfile.h              一站式 include
│   ├── types.h              KeyValue / Encoding / Compression / BE 辅助函数 / VarInt
│   ├── status.h             Status 错误处理
│   ├── writer_options.h     WriterOptions 配置
│   ├── writer.h             HFileWriter + Builder
│   ├── bulk_load_writer.h   BulkLoadWriter + Builder + MappingMode
│   └── region_partitioner.h RegionPartitioner 接口
├── src/
│   ├── writer.cc            HFileWriterImpl + HFileWriter 公开 API
│   ├── bulk_load_writer.cc  BulkLoadWriterImpl + ThreadPool + 公开 API
│   ├── checksum/            CRC32C
│   ├── memory/              ArenaAllocator + BlockPool
│   ├── block/               4 种数据块编码器 + 工厂
│   ├── codec/               5 种压缩算法
│   ├── bloom/               Compound Bloom Filter
│   ├── index/               2 级 Block Index
│   ├── meta/                FileInfoBuilder + TrailerBuilder
│   ├── io/                  BufferedFileWriter / IoUringWriter / HdfsWriter
│   ├── partition/           RegionPartitioner 实现 + CFGrouper
│   └── arrow/               ArrowToKVConverter（3 种映射模式）
├── test/                    21 个测试文件（已全部纳入 ctest）
├── bench/
│   ├── micro/               5 个 Google Benchmark 微基准
│   └── macro/               1 个端到端基准
├── tools/
│   ├── hfile-verify/        Java HBase 原生 Reader 验证工具（pom.xml 已完整）
│   ├── hfile-bulkload-verify/ Java Bulk Load 后数据完整性验证（pom.xml 待补充）
│   └── hfile-report/        Python HTML 对比报告生成器
└── scripts/
    └── bench-runner.sh      全流程自动化基准测试脚本
```

### 依赖版本要求

| 依赖 | 最低版本 | 用途 | 平台 |
|-----|---------|------|------|
| Apache Arrow C++ | 15.0+ | 核心数据格式 | 全平台 |
| protobuf | 3.21+ | Trailer 序列化 | 全平台 |
| lz4 | 1.9+ | LZ4 压缩 | 全平台 |
| zstd | 1.5+ | ZSTD 压缩 | 全平台 |
| snappy | 1.1+ | Snappy 压缩 | 全平台 |
| zlib | 1.2+ | GZip 压缩 | 全平台 |
| liburing | 2.3+ | io_uring 支持 | Linux only |
| libhdfs3 | 2.3+ | HDFS 直写 | Linux/macOS only |
| Google Test | 1.14+ | 单元测试 | 全平台 |
| Google Benchmark | 1.8+ | 性能测试 | 全平台 |
| jemalloc | 5.3+ | 可选内存分配器 | Linux/macOS |

---

## 10. 质量保证

### 10.1 测试覆盖

完整测试覆盖矩阵见 `TESTING.md`；此处保留设计层摘要。

| 测试层次 | 覆盖范围 | 工具 | 数量 |
|---------|---------|------|------|
| 单元/回归测试 | 编码、压缩、元数据、Writer、Arrow、BulkLoad、I/O、CFGrouper 与历史缺陷回归 | Google Test + 自定义框架 | 21 文件 |
| `ctest` 目标 | 单元、集成、自定义测试与 chaos 故障注入入口 | GTest + 自定义测试 + `hfile-chaos` | 23 个 |
| 独立集成测试 | `convert()`、BulkLoad、多批次统计、RawKV/TallTable 等跨模块交互 | 同上 | 已纳入 `ctest` |
| 格式验证 | HFile 文件可被 HBase 原生 Reader 读取 | `hfile-verify` (Java) | 手动 |
| Bulk Load 验证 | 完整链路 + HBase Scan 数据完整性 | `hfile-bulkload-verify` (Java) | 手动 |
| 内存安全 | AddressSanitizer + UndefinedBehaviorSanitizer | Clang Sanitizers | 构建时选项 |
| 性能回归 | 关键路径基准 | Google Benchmark | 6 个基准 |

当前自动化测试矩阵已覆盖：

- 编码与序列化：CRC32C、KV 编码、None/Prefix/Diff/FastDiff、VarInt 边界
- 格式与元数据：Bloom、Index、FileInfo、Trailer
- Writer 主链路：AutoSort、内存预算、磁盘阈值、tags/MVCC、排序校验、错误策略
- Arrow 与转换编排：WideTable / TallTable / RawKV、坏 stream、非法 row key rule、空 row key 过滤、进度回调
- BulkLoad：`SkipBatch`、`Strict`、`max_open_files`、多 CF、多批次统计、Builder 校验
- I/O 与可靠性：`BufferedFileWriter`、`AtomicFileWriter`、`hfile-chaos` 掉电/磁盘满模拟

### 10.2 调试辅助

**`HFILE_DEBUG_POOL` 宏**（Debug 构建自动开启）：
- 检测 `BlockPool::release()` 的 double-release（同一指针 release 两次）
- 检测 foreign-pointer release（释放不属于该 pool 的指针）
- Release 构建：零开销（`#ifdef` 完全移除）

**`HFileWriterImpl` 析构行为**：
- `opened_ && !finished_`：自动 `close() + filesystem::remove(path_)` 清理损坏文件
- `finished_`：仅在 `writer_->close()` 成功后设置，保证 finish 失败可检测

### 10.3 构建与验证流程

```bash
# Debug 构建（含 ASan/UBSan + BlockPool 调试）
cmake -B build-debug -DCMAKE_BUILD_TYPE=Debug \
    -DHFILE_ENABLE_ASAN=ON -DHFILE_ENABLE_UBSAN=ON
cmake --build build-debug -j$(nproc)
cd build-debug && ctest --output-on-failure

# Release 构建
cmake -B build -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-O3 -march=native" \
    -DHFILE_ENABLE_BENCHMARKS=ON
cmake --build build -j$(nproc)

# HFile 格式验证（需要 Java + HBase jar）
cd tools/hfile-verify && mvn package -q
java -jar target/hfile-verify-1.0.0-jar-with-dependencies.jar \
    --hfile-dir /path/to/staging/cf/ --verbose

# 完整基准测试流水线
bash scripts/bench-runner.sh
```

---

## 11. 实现路线图

### 完成状态

| Phase | 内容 | 状态 |
|-------|------|------|
| Phase 1 | 基础框架 + HFile v3 格式（PB Trailer、FileInfo 全字段、Tags+MVCC） | ✅ **完成** |
| Phase 2 | 核心写入器（NONE 编码、无压缩、单 CF 约束） | ✅ **完成** |
| Phase 3 | 编码（FAST_DIFF/DIFF/PREFIX）+ 压缩（LZ4/ZSTD/Snappy）+ Bloom Filter | ✅ **完成** |
| Phase 4 | Region 分裂（手动模式）+ BulkLoadWriter + HDFS I/O | ✅ **完成** |
| Phase 5 | Arrow 集成（WideTable/TallTable/RawKV）+ RowKeyRule 引擎 + JNI 层 + AutoSort | ✅ **完成** |
| Phase 6 | 性能优化（SSE4.2 CRC、SIMD 前缀、栈缓冲、双缓冲 IoUring） | ⚠️ **核心完成**，Pipeline 待基准数据驱动 |
| Phase 7 | 基准工具 + 验证工具 + 文档 | ✅ **完成** |

### Phase 4 设计决策：不提供 `from_hbase()` 在线模式

在线查询 ZooKeeper/Meta 表**有意不提供**，理由：

1. **职责边界**：查询集群拓扑是写入作业启动前的数据准备工作，不属于写入 SDK 职责。将其混入写入热路径会把 ZooKeeper 延迟耦合到吞吐量，破坏 SDK 的核心保证。
2. **依赖负担**：C++ ZooKeeper/HBase 客户端（libhbase、JNI、Thrift）是与 SDK 定位不符的重依赖，大多数嵌入环境不需要。
3. **可用性**：写入路径可以在 HBase 集群临时不可用时继续工作；在线查询则会在此时阻塞或失败。

**推荐工作流**：写入作业启动前通过以下方式获取 split points，传入 `from_splits()`：
```
hbase shell>  get_splits 'my_table'
REST API:     GET /my_table/regions
Java Admin:   admin.getRegions(...).stream().map(r -> r.getStartKey())
```
若 split points 未知，使用 `none()` 模式，由 `BulkLoadHFilesTool` 在加载时处理分裂（速度稍慢但始终正确）。

### Phase 6 设计决策：Pipeline 和 PGO

**双缓冲 Encode/Compress/IO Pipeline**：
- 当前三阶段串行执行。理论上流水线可提升 30–50% 吞吐量，但实际收益高度依赖瓶颈位置。
- **决策准则**：先运行 `bm_e2e_write --benchmark_format=json` + `perf stat`。若 I/O 等待时间占比 < 50%（说明编码/压缩是瓶颈），或与 Java 基线差距 < 3×，再实现 Pipeline。若 I/O 已是瓶颈（NVMe 打满），Pipeline 帮助有限。
- 实现方案参见 §5.6。

**PGO（Profile-Guided Optimization）**：**有意不实现**。
- SDK 热路径是线性紧凑代码（无复杂多态分发、无密集分支），PGO 的典型收益场景不适用此处，预期收益 5–10%。
- 维护成本不成比例：需要稳定 CI 硬件、代表性 training workload、profile 数据版本管理，每次代码结构调整都需重新生成。

### 剩余工作（按优先级）

**P2 — 数据驱动后决定**
- [ ] **双缓冲 Pipeline**：先运行 `bench/java/` 拿到对比数据，若差距 < 3× 再实现
- [ ] **AutoSort 外排序**：当前 AutoSort 将全量 batch 保留在内存；超大文件（> RAM）需外排序实现

---

## 12. 性能测试方法

### 12.1 测试数据集

| 数据集 | Row Key 特征 | Value 大小 | 总量 | 场景 |
|-------|------------|-----------|------|------|
| DS-1 小 KV | 16B 随机 UUID | 100B | 10GB | 日志/事件 |
| DS-2 大 Value | 16B 顺序 | 10KB | 10GB | JSON 文档 |
| DS-3 宽表 | 8B 整型 | 50B × 20 列 | 10GB | 用户画像 |
| DS-4 倾斜 | Zipf 分布 | 10B–1KB | 10GB | 真实负载 |

### 12.2 基准测试目标

| 指标 | Java HBase 基线 | C++ SDK 目标 | 倍数 |
|-----|--------------|------------|------|
| 写入吞吐量 | ~150–200 MB/s | ≥600 MB/s | ≥3× |
| KV QPS | ~500K–800K/s | ≥2.4M/s | ≥3× |
| 内存分配次数 | 极高 | 接近零（热路径）| >10× 减少 |

### 12.3 对比方法论

1. **数据一致性**：C++ 和 Java 写入完全相同数据集（相同随机种子）
2. **配置一致性**：相同 Block 大小、压缩算法、Bloom Filter
3. **资源一致性**：相同 CPU 核心绑定（`taskset -c 0-3`）
4. **多轮测试**：每配置 ≥10 轮，取中位数和 P95
5. **预热**：Java ≥5 轮，C++ ≥2 轮
6. **结果验证**：两端生成的 HFile 都通过 `hfile-verify` 验证后才对比

---

## 13. Bug 修复记录

本节记录所有经代码审查发现并已修复的 Bug，避免回归。

### 第一轮（2026-03）

| Bug ID | 严重度 | 位置 | 问题描述 | 修复方案 |
|--------|--------|------|---------|---------|
| B-01 | 🔴 数据损坏 | `src/checksum/crc32c.cc` | 三路 CRC 合并算法错误：把 CRC 值当作普通数据再次喂给 `_mm_crc32_u64`，产生错误校验值 | 去掉错误的三路合并，改为正确的串行 8 字节 `_mm_crc32_u64` |
| B-02 | 🟡 性能 | `src/index/block_index_writer.cc` | `inline_threshold_` 从不被检查，任何规模文件都只生成单级根索引 | 实现真正的 2 级索引：entries>128 时生成 `IDXINTE2` 中间块 |
| B-03 | 🟡 性能 | `src/block/prefix_encoder.h` `diff_encoder.h` | `append()` 热路径每次 `std::vector<uint8_t>(kv.key_length())` 触发 malloc | 512B 栈缓冲 + 超长 Key 才 fallback 到堆 |
| B-04 | 🟠 功能缺失 | `src/bulk_load_writer.cc` | `set_parallelism(n)` 存储但被忽略，`finish()` 串行 | 内置 `ThreadPool`（50行），`finish()` 并发调用各 writer |
| B-05 | 🔴 数据损坏 | `src/io/iouring_writer.cc` | 单缓冲：`submit_pending()` 提交 SQE 后立即置 `buf_used_=0`，in-flight 写入被下次 `write()` 覆盖 | 双缓冲轮换：`buf_[2]`+`cur_`，提交前等上一个 SQE 完成 |

### 第二轮（2026-03）

| Bug ID | 严重度 | 位置 | 问题描述 | 修复方案 |
|--------|--------|------|---------|---------|
| B-06 | 🔴 安全 | `src/codec/compressor.cc:112` | `snappy::RawUncompress` 不验证输出缓冲区大小，`output_size` 不足时堆溢出 | 先 `GetUncompressedLength()` 验证，不足则返回 `InvalidArg` |
| B-07 | 🟠 并发安全 | `src/memory/block_pool.h` | 无 double-release 和 foreign-pointer 检测，同指针 release 两次导致两线程同时 acquire 同一块内存 | `HFILE_DEBUG_POOL` 宏：`owned_` + `in_use_` 两个 set 检测，Debug 构建自动开启 |
| B-08 | 🟡 性能+行为 | `src/partition/region_partitioner.cc` | `region_for()` 每次构造临时 `std::vector`（热路径堆分配）；空 key 无显式处理 | 自定义 `upper_bound` lambda 直接比较 `std::span` 和 `std::vector`，零分配 |
| B-09 | 🔴 安全 | `include/hfile/types.h:decode_varint64` | 超过 10 个续位字节时 `shift≥64` 触发 C++ UB，并越界读缓冲区 | `kMaxBytes=10` 循环上限，第 10 字节仍有续位则截止返回 -1 |
| B-10 | 🟠 资源管理 | `src/writer.cc:finish()` | `finished_=true` 设在 I/O 开始前；失败后残留损坏文件；析构不清理 | `finished_` 仅在 `close()` 成功后设置；析构函数检测 `opened_&&!finished_` 自动删除残留文件 |

**已确认不是 Bug 的条目**：
- `compare_keys` 空 span：`memcmp(p,q,0)` 是 C11 §7.1.4 定义行为，无论指针值如何都返回 0，空 span 合法
- 日志缺失：SDK 定位为嵌入式库，不应强依赖日志框架，由应用层决定记录方式

---

## 附录 A：Block Type Magic 常量

| Block Type | Magic（ASCII） | 用途 |
|-----------|--------------|------|
| DATA | `DATABLK2` | 数据 Block |
| LEAF_INDEX | `IDXLEAF2` | 叶子索引 Block |
| ROOT_INDEX | `IDXROOT2` | 根索引 Block |
| INTERMEDIATE_INDEX | `IDXINTE2` | 中间索引 Block |
| META | `METABLKc` | 元数据 Block |
| FILE_INFO | `FILEINF2` | 文件信息 Block |
| BLOOM_CHUNK | `BLMFBLK2` | Bloom Filter Chunk |
| BLOOM_META | `BLMFMET2` | Bloom Filter 元数据 |

## 附录 B：关键性能公式

- **写入吞吐量**：`MB/s = Total_Data_Size / Total_Write_Time`
- **KV QPS**：`QPS = Total_KV_Count / Total_Write_Time`
- **压缩比**：`Ratio = Uncompressed_Size / Compressed_File_Size`
- **写放大系数（WAF）**：`WAF = File_Size / Raw_KV_Data_Size`（含索引/Bloom/Header 开销）
- **性能提升倍数**：`Speedup = Throughput_CPP / Throughput_Java`（必须在相同数据集/配置/硬件下测量）

## 附录 C：HBase Key 比较规则

```
compare(a, b):
  1. Row:       lexicographic ASC
  2. Family:    lexicographic ASC
  3. Qualifier: lexicographic ASC
  4. Timestamp: numeric DESC  (大时间戳排在前面)
  5. KeyType:   numeric DESC  (Put=4 < Delete=8, Delete 排在前面)
```

实现在 `include/hfile/types.h:compare_keys()`，使用 `std::memcmp` 实现字节级词典比较，正确处理空 span（count=0 时 memcmp 返回 0，符合 C11 标准）。
