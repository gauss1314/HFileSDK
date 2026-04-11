# 03 — HFile v3 二进制格式详解

本文档描述 SDK 实际生成的 HFile v3 文件的二进制布局，每个字段都标注了对应的代码位置。当 HBase 拒绝加载某个 HFile 时，这份文档是排查的起点。

---

## 1. 文件总体结构

一个 HFile v3 文件由四个区域顺序组成：

```
偏移量 0
┌───────────────────────────────────────┐
│  Scanned Block Section                │  ← Data Blocks（顺序排列）
│  [DataBlock 0][DataBlock 1]...        │
├───────────────────────────────────────┤
│  Non-scanned Block Section            │  ← Bloom Filter 数据块（BLMFBLK2）
│  [BloomChunk 0][BloomChunk 1]...      │
├───────────────────────────────────────┤
│  Load-on-open Section                 │  ← HBase 打开文件时加载到内存
│  [IntermediateIndex...] (可选)         │
│  [RootDataIndex]                      │
│  [MetaRootIndex]                      │
│  [FileInfo]                           │
│  [BloomMeta]                          │
├───────────────────────────────────────┤
│  Trailer (固定 4096 字节)             │  ← 文件末尾
└───────────────────────────────────────┘
```

HBase 读取 HFile 时**从尾部开始**：先读最后 4096 字节（Trailer），从 Trailer 获取各区域的偏移量，再跳转读取。

---

## 2. Block Header（33 字节）

所有 Block（Data、Index、FileInfo、Bloom 等）共享相同的 33 字节 Header 格式。

```
偏移  大小  字段                    代码位置
─────────────────────────────────────────────────────
 0    8B   Magic                   types.h: kDataBlockMagic 等
 8    4B   onDiskSizeWithoutHeader writer.cc: on_disk_size_without_header
                                    （含压缩数据 + 校验和）
12    4B   uncompressedSize        writer.cc: uncompressed_size
16    8B   prevBlockOffset         writer.cc: prev_block_offset_
24    1B   checksumType            types.h: kChecksumTypeCRC32C = 2
25    4B   bytesPerChecksum        types.h: kBytesPerChecksum = 512
29    4B   onDiskDataSizeWithHeader writer.cc: on_disk_data_with_header
                                    （Header + 压缩数据，不含校验和）
─────────────────────────────────────────────────────
33 字节  kBlockHeaderSize
```

紧跟 Header 之后是**压缩后的数据**（当前强制 NONE 编码，所以等于原始数据），再之后是**校验和数组**。

校验和计算方式（`crc32c.cc: compute_hfile_checksums`）：把 `Header + 压缩数据` 视为连续字节流，每 512 字节（`bytesPerChecksum`）计算一个 CRC32C，每个校验和 4 字节 Big-Endian。最后一个 chunk 可能不足 512 字节。

```
[33B Header][压缩数据...][CRC_0(4B)][CRC_1(4B)]...[CRC_N(4B)]
```

Magic 字符串标识了 Block 类型：

| Magic | 常量 | 用途 |
|-------|------|------|
| `DATABLK*` | `kDataBlockMagic` | NONE 编码的数据块 |
| `DATABLKE` | `kEncodedDataBlockMagic` | 非 NONE 编码的数据块 |
| `IDXROOT2` | `kRootIndexMagic` | 根索引块 |
| `IDXINTE2` | `kIntermedIdxMagic` | 中间索引块 |
| `FILEINF2` | `kFileInfoMagic` | FileInfo 块 |
| `BLMFBLK2` | `kBloomChunkMagic` | Bloom Filter 数据块 |
| `BLMFMET2` | `kBloomMetaMagic` | Bloom Filter 元数据块 |
| `TRABLK"$` | `kTrailerBlockMagic` | Trailer 块 |

---

## 3. KeyValue 编码格式（NONE Encoding）

每个 KeyValue 在 Data Block 中的二进制布局（`data_block_encoder.h: serialize_kv`）：

```
偏移  大小  字段                    代码
─────────────────────────────────────────────────────
 0    4B   keyLength               write_be32(p, kv.key_length())
 4    4B   valueLength             write_be32(p, kv.value.size())
                                   ─── key 区域开始 ───
 8    2B   rowLength               write_be16(p, kv.row.size())
10    变长  rowKey                  memcpy(p, kv.row.data(), ...)
 ?    1B   familyLength            *p++ = kv.family.size()
 ?    变长  family                  memcpy(p, kv.family.data(), ...)
 ?    变长  qualifier               memcpy(p, kv.qualifier.data(), ...)
 ?    8B   timestamp               write_be64(p, kv.timestamp)
 ?    1B   keyType                 *p++ = kv.key_type (Put=4)
                                   ─── key 区域结束 ───
 ?    变长  value                   memcpy(p, kv.value.data(), ...)
                                   ─── v3 扩展字段 ───
 ?    2B   tagsLength              write_be16(p, kv.tags.size())
 ?    变长  tags                    通常为空（Bulk Load）
 ?    变长  memstoreTS              encode_writable_vint(p, kv.memstore_ts)
                                    通常为 0 → 1 字节
─────────────────────────────────────────────────────
```

**key_length 计算公式**（`KeyValue::key_length()`）：

```
key_length = 2(rowLen) + row.size + 1(familyLen) + family.size + qualifier.size + 8(ts) + 1(type)
```

**HFile v3 的关键区别**：Value 之后必须有 `tagsLength`（2 字节）和 `memstoreTS`（WritableVInt）。即使都为 0，这两个字段也**必须存在**。缺少它们会导致 HBase 2.6.1 反序列化失败。

### 3.1 WritableVInt 编码

`memstoreTS` 使用 Hadoop 的 WritableVInt 编码（`types.h: encode_writable_vint`），与标准 varint 不同：

- 值在 -112..127 之间：1 字节直接存储
- 否则：1 字节前缀（编码符号和长度）+ N 字节 Big-Endian 值

Bulk Load 场景下 memstoreTS = 0，编码为单字节 `0x00`。

### 3.2 serialize_key

`serialize_key` 函数（`data_block_encoder.h`）只序列化 key 部分（不含 keyLength/valueLength/value/tags/mvcc），用于索引条目的 key：

```
rowLen(2B) + row + familyLen(1B) + family + qualifier + timestamp(8B) + keyType(1B)
```

---

## 4. Data Block 结构

每个 Data Block 由 Block Header + KV 序列 + 校验和组成。当 NONE Encoding 时，KV 序列就是 `serialize_kv()` 的输出简单拼接：

```
[Block Header 33B]
[KV_0][KV_1][KV_2]...[KV_N]   ← NoneEncoder::buffer_
[CRC32C_0][CRC32C_1]...
```

Block 大小控制（`none_encoder.h: append`）：当 `buffer_.size() + kv_size > block_size_` 时，`append()` 返回 false，触发 `flush_data_block()`。block_size 默认 64KB。

`[!]` 第一个 KV 即使超过 block_size 也会被接受（`!buffer_.empty()` 条件），不会导致死循环。

---

## 5. Block Index 结构

索引将 Data Block 的 first key 映射到文件偏移量，供 HBase 做二分查找。

### 5.1 索引条目格式

每个条目（`block_index_writer.cc: write_entry`）：

```
offset(8B BE) + dataSize(4B BE) + keyLen(WritableVInt) + key(变长)
```

### 5.2 单级索引 vs 多级索引

当条目数 ≤ `max_per_block_`（默认 128）时，所有条目直接放在 Root Index Block 中：

```
Root Index Block (IDXROOT2):
  [Block Header 33B]
  [entryCount(4B BE)]
  [entry_0][entry_1]...[entry_N]
  [checksums]
```

当条目数超过阈值时，构建两级索引：

```
Intermediate Block 0 (IDXINTE2): [Header][count][entries 0..127]
Intermediate Block 1 (IDXINTE2): [Header][count][entries 128..255]
...
Root Index Block (IDXROOT2): [Header][count][指向 Intermediate Blocks 的条目]
```

Root 级条目的 offset 指向 Intermediate Block，Intermediate 级条目的 offset 指向 Data Block。

`[!]` `BlockIndexWriter` 的 `max_per_block_` 在 `writer.cc` 中被设为 `std::numeric_limits<size_t>::max()`，实际上**禁用了多级索引**。这是因为中间索引块的压缩处理还不完善。所有条目都放在 Root Index Block 中。

---

## 6. Bloom Filter 结构

SDK 使用分块 Compound Bloom Filter（`compound_bloom_filter_writer.h`）。

### 6.1 数据块（BLMFBLK2）

每个 Data Block 对应一个 Bloom chunk。当 chunk 中的 key 数量达到 `chunk_keys_` 时封存。每个 chunk 是一个位数组，通过 Murmur3 哈希设置位。

写入位置在 Non-scanned Block Section（所有 Data Block 之后、Load-on-open Section 之前）。

### 6.2 元数据块（BLMFMET2）

存储 Bloom Filter 的全局参数：版本、hash 函数数量、hash 类型、总 key 数、位数组总大小、每个 chunk 的 key 数等。

位置在 Load-on-open Section 的 FileInfo 块之后。

### 6.3 Meta Root Index

Load-on-open Section 包含一个 Meta Root Index Block（IDXROOT2），其中只有一个条目，指向 BLMFMET2 块的偏移量。key 是固定字符串 `"GENERAL_BLOOM_META"`。

---

## 7. FileInfo 块

FileInfo 是一个 key-value map，序列化为 ProtoBuf（`hfile_file_info.proto: FileInfoProto`）。

`file_info_builder.h: FileInfoBuilder` 构建此块。必填字段及其编码：

| FileInfo Key | 值格式 | 构建方法 |
|---|---|---|
| `hfile.LASTKEY` | 原始 key 字节 | `set_last_key()` — 最后一个 KV 的 serialize_key 输出 |
| `hfile.AVG_KEY_LEN` | 4B BE uint32 | `set_avg_key_len()` — total_key_bytes / entry_count |
| `hfile.AVG_VALUE_LEN` | 4B BE uint32 | `set_avg_value_len()` — total_value_bytes / entry_count |
| `hfile.MAX_TAGS_LEN` | 4B BE uint32 | `set_max_tags_len()` — 通常为 0 |
| `KEY_VALUE_VERSION` | 4B BE uint32 = 1 | `set_key_value_version(1)` — 表示含 MemstoreTS |
| `MAX_MEMSTORE_TS_KEY` | 8B BE uint64 = 0 | `set_max_memstore_ts(0)` — Bulk Load 场景 |
| `hfile.COMPARATOR` | UTF-8 字符串 | `set_comparator(kCellComparator)` |
| `DATA_BLOCK_ENCODING` | UTF-8: "NONE" | `set_data_block_encoding(Encoding::None)` |
| `hfile.CREATE_TIME_TS` | 8B BE int64 | `set_create_time()` — 当前毫秒时间戳 |
| `hfile.LEN_OF_BIGGEST_CELL` | 8B BE uint64 | `set_len_of_biggest_cell()` |

如果启用了 Bloom Filter，还会包含 `BLOOM_FILTER_TYPE`（"ROW"/"ROWCOL"）和 `LAST_BLOOM_KEY`。

`validate_required_fields()` 在 `finish()` 之前检查所有必填字段是否已设置。

FileInfo 序列化为 ProtoBuf 后，再包装成标准 Block（加 FILEINF2 Header + 校验和）。

实际的序列化方式（`file_info_builder.h: finish`）：遍历内部的 `std::map<std::string, std::vector<uint8_t>>`，按 key 字典序排列，写入 ProtoBuf 的 `FileInfoProto.map_entry` 重复字段。

---

## 8. Trailer（文件尾部，固定 4096 字节）

Trailer 是 HFile 的"入口"——HBase 总是先读文件最后 4096 字节来解析 Trailer。

### 8.1 布局

```
偏移（相对 Trailer 起始） 大小  字段
──────────────────────────────────────────
0                       8B   Magic: TRABLK"$
8                       变长  varint(pb_length) — ProtoBuf 消息长度
?                       变长  FileTrailerProto 序列化字节
?                       变长  Zero Padding（填充到 4092 字节）
4092                    4B   Materialized Version
──────────────────────────────────────────
共 4096 字节            kTrailerFixedSize
```

### 8.2 ProtoBuf Trailer 字段

```protobuf
message FileTrailerProto {
  optional uint64 file_info_offset              = 1;  // FileInfo 块偏移
  optional uint64 load_on_open_data_offset      = 2;  // Load-on-open 区域起始
  optional uint64 uncompressed_data_index_size  = 3;  // 索引未压缩总大小
  optional uint64 total_uncompressed_bytes      = 4;  // 所有数据块未压缩总大小
  optional uint32 data_index_count              = 5;  // Data Block 数量
  optional uint32 meta_index_count              = 6;  // Meta Index 条目数（0 或 1）
  optional uint64 entry_count                   = 7;  // KV 总数
  optional uint32 num_data_index_levels         = 8;  // 索引层数（1 或 2）
  optional uint64 first_data_block_offset       = 9;  // 第一个 Data Block 偏移
  optional uint64 last_data_block_offset        = 10; // 最后一个 Data Block 偏移
  optional string comparator_class_name         = 11; // CellComparatorImpl
  optional uint32 compression_codec             = 12; // 压缩算法 ID
}
```

### 8.3 Materialized Version

最后 4 字节是版本号的特殊编码（`trailer_builder.h: finish`）：

```
materialized_version = (minor << 24) | (major & 0x00FFFFFF)
                     = (3 << 24) | 3
                     = 0x03000003
```

以 Big-Endian 写入。HBase 读取时先检查这 4 字节确认文件版本。

---

## 9. Load-on-open Section 偏移量计算

这是 `writer.cc: finish()` 中最复杂的部分。各块的偏移量互相依赖，代码通过**预构建+重新构建**解决：

```
load_on_open_offset = 当前写入位置（Bloom 数据块之后）

1. 先构建 intermediate + root index → 知道大小
2. root_block_offset = load_on_open_offset + intermediate 大小
3. meta_root_offset = root_block_offset + root_block 大小

关键：Meta Root Index 中需要存 bloom_meta_offset，
      但 bloom_meta_offset = file_info_offset + file_info 大小
      而 file_info_offset = meta_root_offset + meta_root 大小
      
解决：先构建一个 probe（临时 meta_root + 临时 file_info），
      计算出 bloom_meta_offset，再用正确值重新构建。
```

这段代码约 60 行，是整个 writer 中最不直观的部分。如果 HBase 报告 Bloom Filter 损坏或索引错误，首先检查这里的偏移量计算。

---

## 10. 当前已知的格式限制

| 限制 | 说明 | 代码位置 |
|------|------|----------|
| 编码强制 NONE | Prefix/Diff/FastDiff 还不兼容 HBase | `writer.cc: open()` |
| 压缩强制 NONE | 压缩也被禁用 | `writer.cc: open()` |
| 索引单级 | max_per_block 设为 MAX，禁用多级 | `writer.cc: index_writer_` |
| 无 Encryption | encryption_key 字段未使用 | `trailer_builder.h` |
| Tags 始终为空 | Bulk Load 不需要 ACL/Visibility | `converter.cc` |

这些限制意味着当前生成的 HFile 文件**比可能的大**（无压缩+无前缀编码），但格式**完全正确**，HBase 2.6.1 可以正常读取。
