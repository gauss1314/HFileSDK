# 07 — writer.cc 深度剖析

`src/writer.cc` 是整个 SDK 中最核心的单文件（903 行），它负责将有序的 KeyValue 流组装成一个完整的、可被 HBase 2.6.1 读取的 HFile v3 文件。所有其他模块——编码器、压缩器、Bloom Filter、索引、FileInfo、Trailer——最终都在这里汇聚。

本文档逐层拆解 writer.cc 的设计，从公开 API 到内部实现，从单个 KV 的追加到整个文件的收尾。

---

## 1. 架构：公开接口与内部实现的分离

writer.cc 使用经典的 **Pimpl（Pointer to Implementation）** 模式：

```
┌────────────────────────────────────┐
│  HFileWriter (公开类)               │  include/hfile/writer.h
│    - append() / finish() / ...     │  只转发调用
│    - std::unique_ptr<Impl> impl_   │
└──────────────┬─────────────────────┘
               │ 持有
               ▼
┌────────────────────────────────────┐
│  HFileWriterImpl (内部类)           │  writer.cc 内部定义
│    - 全部状态 + 全部逻辑            │  不暴露给外部
│    - ~700 行核心实现                │
└────────────────────────────────────┘
```

这样做的好处是 `include/hfile/writer.h` 不需要包含任何内部头文件（encoder、compressor、bloom 等），调用方只需要链接 `libhfilesdk.so`，编译速度和 ABI 稳定性都有保障。

### 1.1 Builder 模式

`HFileWriter` 不允许直接构造，必须通过 Builder：

```cpp
auto [writer, status] = HFileWriter::builder()
    .set_path("/staging/cf/events.hfile")
    .set_column_family("cf")
    .set_compression(Compression::GZip)
    .set_block_size(65536)
    .set_bloom_type(BloomType::Row)
    .set_sort_mode(WriterOptions::SortMode::PreSortedVerified)
    .build();
```

`build()` 内部做三件事：

1. 校验必填参数（path 和 column_family 非空）
2. `new HFileWriterImpl(path, opts)` + `impl->open()`
3. 任何异常被 `try/catch(...)` 捕获，返回 Status 而非抛出

Builder 的每个 setter 返回 `Builder&`，支持链式调用。所有可选配置都有合理默认值（见 `WriterOptions`）。

### 1.2 两个 append 重载

```cpp
// 重载 1：接收 KeyValue 结构体
Status append(const KeyValue& kv);

// 重载 2：接收散列参数，内部构造 KeyValue 再转发
Status append(std::span<const uint8_t> row,
              std::span<const uint8_t> family,
              std::span<const uint8_t> qualifier,
              int64_t timestamp,
              std::span<const uint8_t> value,
              KeyType key_type = KeyType::Put,
              std::span<const uint8_t> tags = {},
              uint64_t memstore_ts = 0);
```

重载 2 是便捷接口——在栈上构造一个临时 `KeyValue`（零分配，只赋 span 指针），然后转发到重载 1。converter.cc 使用的就是这个接口。

---

## 2. open()：初始化所有子系统

`HFileWriterImpl::open()` 是 writer 的启动流程，按以下顺序初始化：

### 2.1 参数校验与目录创建

```cpp
if (opts_.bytes_per_checksum == 0)
    return Status::InvalidArg("bytes_per_checksum must be > 0");
fs::create_directories(path_.parent_path());
```

自动创建输出目录的所有缺失层级，免得调用方手动 `mkdir -p`。

### 2.2 I/O 后端选择

根据 `FsyncPolicy` 选择不同的 I/O 后端：

```cpp
if (opts_.fsync_policy == FsyncPolicy::Safe) {
    atomic_writer_ = make_unique<io::AtomicFileWriter>(path_);
    writer_ = atomic_writer_.get();     // 非拥有指针
} else {
    plain_writer_ = io::BlockWriter::open_file(path_);
    writer_ = plain_writer_.get();      // 非拥有指针
}
```

`writer_` 是一个裸指针，别名指向实际的 I/O 后端。后续所有写入操作都通过 `writer_->write()` 进行，不关心底层是 atomic 还是 plain。这是**策略模式**的应用。

`[!]` 两个 unique_ptr（`atomic_writer_` 和 `plain_writer_`）最多只有一个非空。`writer_` 永远指向那个非空的。析构时 unique_ptr 自动释放，`writer_` 不需要管理生命周期。

### 2.3 编码/压缩的强制降级

当前版本中，builder 会拒绝非 `NONE` 的数据块编码，并且只接受 `NONE` 或 `GZ` 压缩。也就是说，writer 的压缩路径只剩 `NoneCompressor` 和 `GZipCompressor` 两种实现，编码路径只剩 `NoneEncoder`。

### 2.4 子系统创建

```cpp
encoder_    = DataBlockEncoder::create(opts_.data_block_encoding, opts_.block_size);
compressor_ = Compressor::create(opts_.compression);
bloom_      = make_unique<CompoundBloomFilterWriter>(opts_.bloom_type, opts_.bloom_error_rate);
```

注意 `index_writer_` 不在这里创建——它是成员直接初始化的：

```cpp
index::BlockIndexWriter index_writer_{std::numeric_limits<size_t>::max()};
```

`max_entries_per_block` 设为 `SIZE_MAX`，这实际上**禁用了多级索引**。所有索引条目都会进入 Root Index Block，不会产生 Intermediate Index Block。代码注释解释了原因：中间索引块没有经过压缩管道，在非 NONE 压缩下可能破坏 HBase 的预期。

### 2.5 压缩缓冲区预分配

```cpp
compress_buf_.resize(compressor_->max_compressed_size(opts_.block_size + 65536));
```

一次性分配足以容纳最大压缩输出的缓冲区。`+ 65536` 的余量是因为 encoder 可能输出略超过 block_size 的数据（第一个 KV 不受 block_size 限制）。这个缓冲区在整个 writer 生命周期中**复用**，不再重分配。

如果启用了 MemoryBudget，这个固定缓冲区也计入配额。

---

## 3. append()：KV 进入 writer 的三道关卡

### 3.1 第一关：输入校验（validate_kv）

```
kv.row 为空?           → ROW_KEY_EMPTY
kv.row.size() > 32KB?  → ROW_KEY_TOO_LONG
kv.value.size() > 10MB? → VALUE_TOO_LARGE
kv.timestamp < 0?       → NEGATIVE_TIMESTAMP
```

校验失败后的处理取决于 `ErrorPolicy`：

| 策略 | 行为 | 返回值 |
|------|------|--------|
| Strict | 立即终止 | 原始 Status 错误 |
| SkipRow | 跳过这行，继续 | `Status::OK()` |
| SkipBatch | 信号给调用方跳过当前 batch | 特殊 SKIP_BATCH 错误 |

SkipRow 模式下还有一个**累积上限**：`max_error_count`（默认 1000）。超过后即使是 SkipRow 也会终止，防止产出一个几乎全是空洞的 HFile。

### 3.2 第二关：Column Family 检查

```cpp
if (kv_family != opts_.column_family)
    return Status::InvalidArg("KeyValue family != configured family");
```

HBase BulkLoadHFilesTool 要求每个 HFile 只包含一个 Column Family 的数据。这个检查在 writer 层硬性执行，无论 ErrorPolicy 如何设置——CF 不匹配永远是致命错误。

### 3.3 第三关：排序模式分流

```
AutoSort?
  → sanitize_owned_kv() → buffer_auto_sorted_kv()
  → 排序在 finish() 时进行

PreSorted (Trusted 或 Verified)?
  → sanitize_owned_kv() → append_materialized_kv()
  → 直接写入（Verified 模式会校验排序）
```

`sanitize_owned_kv()` 做两件事：

1. 将 `KeyValue`（span 视图）深拷贝为 `OwnedKeyValue`（vector 持有）
2. 根据 `include_tags` 和 `include_mvcc` 选项，决定是否拷贝 tags 和 memstore_ts

**为什么需要深拷贝？** 因为调用方传入的 `KeyValue` 的 span 可能指向临时缓冲区（如 converter.cc 中的 `GroupedCell::value`），append 返回后缓冲区可能被覆写。AutoSort 模式更需要拷贝——KV 在 finish 时才真正写入磁盘。

---

## 4. append_materialized_kv()：写入的真正执行者

这个方法是 KV 被物理编码并最终写入磁盘的核心路径（第 655-694 行）。

### 4.1 排序校验

```cpp
if (verify_sort && written_entry_count_ > 0) {
    int cmp = compare_keys(kv, last_kv_.as_view());
    if (cmp <= 0)
        return Status::InvalidArg("SORT_ORDER_VIOLATION: KV out of order");
}
```

`compare_keys` 实现完整的 HBase 5 级排序比较：Row ASC → Family ASC → Qualifier ASC → Timestamp DESC → Type DESC。

`cmp <= 0` 意味着**不允许重复 key**。如果两个 KV 的全部 5 个维度都相同，也被视为排序违规。在 converter.cc 中，这种情况理论上不会发生，因为 `append_grouped_row_cells()` 已经在同一 row key 内去重了 qualifier。

### 4.2 编码器追加

```cpp
if (!encoder_->append(kv)) {
    HFILE_RETURN_IF_ERROR(flush_data_block());  // Block 满了，先刷盘
    if (!encoder_->append(kv))
        return Status::Internal("Failed to append KV even after flush");
}
```

这个二次重试模式保证了即使 Block 满也能正确处理：

1. 第一次 `append` 失败 = Block 满
2. `flush_data_block()` 把当前 Block 写入磁盘并重置 encoder
3. 第二次 `append` 到空 Block（此时一定能成功，除非 KV 本身有问题）
4. 如果第二次还失败——这是一个不可恢复的内部错误

### 4.3 Bloom Filter 喂数据

```cpp
bloom_->add(kv.row);
if (opts_.bloom_type == BloomType::RowCol)
    bloom_->add_row_col(kv.row, kv.qualifier);
```

对于 `Row` 类型 Bloom，只喂 row key。对于 `RowCol` 类型，喂 row+qualifier 的拼接。

注意这里有一个细节：`Row` 类型时 `add()` 被调用一次；`RowCol` 类型时 `add()` 和 `add_row_col()` **都被调用**。但实际上 `add_row_col()` 内部在 RowCol 模式下不会再额外调用 `add(row)`——它用 row+qualifier 的拼接调用 `add(combined)`。所以 RowCol 模式下每个 KV 总共对 bloom filter 做了**两次** add：一次 row 级别，一次 row+col 级别。

`last_bloom_key_` 跟踪最后一个被 bloom filter 记录的 key，用于 FileInfo 中的 `LAST_BLOOM_KEY` 字段。

### 4.4 统计更新

```cpp
if (record_stats) record_cell_stats(kv);  // key/value 字节数、最大 cell、tags 等
if (written_entry_count_ == 0) save_first_key(kv);  // 文件级 first key
save_last_key(kv, verify_sort);  // 文件级 last key + 排序基准
++written_entry_count_;
```

`record_cell_stats()` 累积以下统计信息：

| 统计量 | 用途 | 使用位置 |
|--------|------|---------|
| `total_key_bytes_` | 计算 AVG_KEY_LEN | FileInfo |
| `total_value_bytes_` | 计算 AVG_VALUE_LEN | FileInfo |
| `max_tags_len_` | MAX_TAGS_LEN | FileInfo |
| `max_memstore_ts_` | MAX_MEMSTORE_TS_KEY | FileInfo |
| `max_cell_size_` | LEN_OF_BIGGEST_CELL | FileInfo |

这些统计在 `finish()` 中被 `build_file_info_block()` 消费。少了任何一个都可能导致 HBase 拒绝加载。

### 4.5 entry_count 与 written_entry_count 的区别

```cpp
++written_entry_count_;
if (increment_entry_count) ++entry_count_;
```

两个计数器的用途不同：

- `entry_count_`：报告给调用方的"有多少个 KV"。在 AutoSort 路径中，`append()` 阶段递增（入队时），`finish()` 排序写入阶段不递增。
- `written_entry_count_`：实际写入磁盘的 KV 数。用于排序校验（"这是第一个 KV 吗？"）和 first_key 保存。

Trailer 中的 `entry_count` 字段使用 `entry_count_`。

---

## 5. flush_data_block()：Block 落盘的全过程

当 encoder 内部缓冲区超过 `block_size`（默认 64KB），或 `finish()` 时缓冲区非空，都会触发 flush。

### 5.1 编码完成

```cpp
auto raw = encoder_->finish_block();
if (raw.empty()) { encoder_->reset(); return Status::OK(); }
```

`finish_block()` 返回编码后的原始字节 span。对于 NoneEncoder，这就是所有 `serialize_kv()` 的拼接。

### 5.2 编码 ID 前缀（非 NONE 编码时）

```cpp
if (opts_.data_block_encoding != Encoding::None) {
    encoded_block_with_id.resize(2 + raw.size());
    write_be16(encoded_block_with_id.data(), hbase_data_block_encoding_id(...));
    memcpy(encoded_block_with_id.data() + 2, raw.data(), raw.size());
    raw = {encoded_block_with_id.data(), encoded_block_with_id.size()};
}
```

HBase 的 encoded data block 在数据前要求 2 字节的 encoding type ID。当前由于强制 NONE 编码，这段代码是死路径。

### 5.3 Bloom chunk 封存

```cpp
bloom_->finish_chunk();
```

每个 Data Block flush 时同步封存当前的 Bloom Filter chunk。这保证了 Bloom chunk 与 Data Block 的 1:1 对应关系。

### 5.4 压缩

```cpp
size_t comp_len = compressor_->compress(raw, compress_buf_.data(), compress_buf_.size());
if (comp_len == 0 && opts_.compression != Compression::None)
    return Status::Internal("Compression failed");

std::span<const uint8_t> compressed{compress_buf_.data(), comp_len};
if (opts_.compression == Compression::None) compressed = raw;
```

NoneCompressor 的 `compress()` 返回 0（不做任何事），然后 `compressed` 直接指向 raw。零拷贝。

### 5.5 write_data_block()：物理写入

这个方法构建 Block 的完整磁盘表示：

```
[33B Header] + [compressed_data] + [CRC32C checksum array]
```

**Header 33 字节的详细构建**：

```cpp
uint8_t hdr[kBlockHeaderSize];
memcpy(p, magic.data(), 8);                    // [0:8]   Magic
write_be32(p, on_disk_size_without_header);     // [8:12]  含压缩数据+校验和，不含 Header
write_be32(p, uncompressed_size);               // [12:16] 未压缩大小
write_be64(p, prev_block_offset_);              // [16:24] 前一个 Block 的偏移
*p++ = kChecksumTypeCRC32C;                     // [24]    校验和类型=2
write_be32(p, opts_.bytes_per_checksum);        // [25:29] 每个校验和覆盖的字节数
write_be32(p, on_disk_data_with_header);        // [29:33] Header + 压缩数据（不含校验和）
```

**CRC32C 校验和计算**：

```cpp
checksummed_block = hdr + compressed_data;  // 拼接 header 和数据
compute_hfile_checksums(checksummed_block, len, bytes_per_checksum, checksum_buf);
```

校验和范围是 `Header + 压缩数据`（**不含校验和自身**）。每 512 字节一个 CRC32C，最后一个 chunk 可能不足 512 字节。

**索引注册**：

```cpp
index_writer_.add_entry(first_key, block_offset, data_size);
```

每个 Data Block 在索引中记录一个条目：`(first_key, 在文件中的偏移, Block 总大小)`。

**三次 write 调用**：

```cpp
writer_->write({hdr, kBlockHeaderSize});
writer_->write(compressed_data);
writer_->write({checksum_buf.data(), checksum_buf.size()});
```

Header、数据、校验和分三次写入，利用 `BufferedFileWriter` 的 4MB 缓冲区合并为单次 fwrite。

### 5.6 Paranoid 模式的周期 fsync

```cpp
if (opts_.fsync_policy == FsyncPolicy::Paranoid &&
    opts_.fsync_block_interval > 0 &&
    (data_block_count_ % opts_.fsync_block_interval) == 0) {
    writer_->flush();
}
```

只在 Paranoid 模式下，每 N 个 Block 执行一次 flush（触发 fflush → fsync）。Safe 模式不需要，因为 AtomicFileWriter 的 commit 会做最终 fsync。

---

## 6. build_raw_block()：通用 Block 构建器

Data Block 之外的所有 Block（Index、FileInfo、Bloom Meta、Bloom Chunk）都通过 `build_raw_block()` 构建。它与 `write_data_block()` 的区别：

| 维度 | write_data_block | build_raw_block |
|------|-----------------|----------------|
| 输出 | 直接写磁盘 | 返回 `vector<uint8_t>` |
| 压缩 | 用预分配的 compress_buf_ | 内部临时分配 |
| 索引 | 注册到 index_writer_ | 不注册 |
| prev_block_offset | 更新成员变量 | 使用参数传入的值 |
| 使用场景 | Data Block | Index/FileInfo/Bloom/Meta |

返回 vector 而非直接写磁盘的原因是：finish() 中需要先计算各 Block 的大小以确定偏移量，然后才能写入。这涉及一个偏移量依赖链（见下节）。

**压缩回退**：`build_raw_block` 会尝试压缩；如果压缩后反而更大（`compressed_len == 0`，对小 payload 可能发生），自动回退到不压缩。

---

## 7. finish()：文件收尾的精密编排

`finish()` 是 writer.cc 中最复杂的方法（约 150 行），按严格顺序完成以下步骤。

### 7.1 AutoSort 排序与写入

```cpp
if (sort_mode == AutoSort && !auto_sorted_kvs_.empty()) {
    std::stable_sort(auto_sorted_kvs_.begin(), auto_sorted_kvs_.end(),
                     [](const OwnedKeyValue& a, const OwnedKeyValue& b) {
                         return compare_keys(a.as_view(), b.as_view()) < 0;
                     });
    for (const auto& kv : auto_sorted_kvs_) {
        append_materialized_kv(kv.as_view(), false, false, true);
    }
}
```

AutoSort 是 writer 的"保底"排序——如果调用方没有预排序，writer 在 finish 时内存排序。`converter.cc` 不使用这个路径，它会先完成排序，再以预排序模式写入 HFile。

`stable_sort` 保证相同 key 的 KV 保持原始插入顺序。

### 7.2 Flush 最后的 Data Block

```cpp
if (!encoder_->empty()) {
    flush_data_block();
}
bloom_->finish_chunk();  // 封存最后一个 Bloom chunk
```

encoder 中可能还有不满一个 block_size 的数据。在关闭文件之前必须 flush 出去。

### 7.3 Non-scanned Block Section：Bloom 数据块

```cpp
if (bloom_result.enabled) {
    bloom_result.bloom_data_offset = writer_->position();
    bloom_->finish_data_blocks(bloom_chunk_buf, writer_->position());
    writer_->write(bloom_chunk_buf);
}
```

所有 Bloom chunk 被序列化为 BLMFBLK2 Block 连续写入。每个 chunk 对应一个 Data Block，包含该 Data Block 中所有 key 的 Bloom Filter 位数组。

### 7.4 Load-on-open Section：偏移量依赖链

这是 finish() 中最复杂的部分。HBase 在打开 HFile 时会一次性读取这个区域的所有块到内存。这些块必须按特定顺序排列，且彼此之间的偏移量引用必须正确。

**依赖链**：

```
Meta Root Index 的 payload → 需要 bloom_meta_offset
bloom_meta_offset = file_info_offset + sizeof(FileInfo Block)
file_info_offset = meta_root_offset + sizeof(Meta Root Block)
meta_root_offset = root_block_offset + sizeof(Root Index Block)
root_block_offset = load_on_open_offset + sizeof(Intermediate Index Blocks)
```

问题：每一层的 offset 都依赖下一层的大小，但大小又依赖 offset（因为 Header 中有 `prev_block_offset`）。

**代码的解法**：两轮构建。

**第一轮（probe）**：用临时值构建 Meta Root Block 和 FileInfo Block，只为了**测量大小**：

```cpp
auto meta_root_probe = build_raw_block(kRootIndexMagic, meta_root_payload, root_block_offset);
const int64_t file_info_offset = meta_root_offset + meta_root_probe.size();

std::vector<uint8_t> file_info_block_probe;
build_file_info_block(meta_root_offset, bloom_result, &file_info_block_probe);
const int64_t bloom_meta_offset = file_info_offset + file_info_block_probe.size();
```

**第二轮（final）**：用正确的 `bloom_meta_offset` 填充 Meta Root payload，然后重新构建所有块：

```cpp
// 写入正确的 bloom_meta_offset
write_be64(mp, bloom_meta_offset);
write_be32(mp, bloom_meta_block.size());

// 重新构建（这次大小与 probe 一致）
auto meta_root_block = build_raw_block(kRootIndexMagic, meta_root_payload, root_block_offset);
build_file_info_block(meta_root_offset, bloom_result, &file_info_block);
```

**关键假设**：第二轮构建的 Block 大小与第一轮 probe 完全相同。这成立是因为 payload 大小不变，只是内容不同。Header 大小固定 33 字节，校验和数量由 payload 大小决定，所以总大小不变。

`[!]` 如果未来修改了 build_raw_block 的逻辑（比如压缩后大小变化），这个假设可能被打破，需要改用迭代收敛。

### 7.5 Meta Root Index：指向 Bloom Meta 的桥梁

```
Meta Root Index Block (IDXROOT2):
  [Header 33B]
  [entryCount = 1 (4B BE)]        ← 只有一个条目
  [bloom_meta_offset (8B BE)]      ← 指向 BLMFMET2 块
  [bloom_meta_block_size (4B BE)]  ← BLMFMET2 块的数据大小
  [key = "GENERAL_BLOOM_META"]     ← WritableVInt 长度 + 字符串
  [checksums]
```

如果 Bloom Filter 未启用，Meta Root Index 的 payload 只有 4 字节（`entryCount = 0`）。

### 7.6 FileInfo Block：必填字段组装

```cpp
meta::FileInfoBuilder fib;
fib.set_last_key(last_key_);
fib.set_avg_key_len(total_key_bytes_ / entry_count_);
fib.set_avg_value_len(total_value_bytes_ / entry_count_);
fib.set_max_tags_len(max_tags_len_);
fib.set_key_value_version(1);
fib.set_max_memstore_ts(has_mvcc_cells_ ? max_memstore_ts_ : 0);
fib.set_comparator(opts_.comparator);
fib.set_data_block_encoding(opts_.data_block_encoding);
fib.set_create_time();
fib.set_len_of_biggest_cell(max_cell_size_);
fib.validate_required_fields();
```

`validate_required_fields()` 检查所有 HBase 要求的字段是否已设置。缺少任何一个都会导致 HBase 读取失败。

### 7.7 Trailer：固定 4KB 尾部

```cpp
Status write_trailer(file_info_offset, load_on_open_offset, idx_result, bloom_result) {
    TrailerBuilder tb;
    tb.set_file_info_offset(file_info_offset);
    tb.set_load_on_open_offset(load_on_open_offset);
    tb.set_entry_count(entry_count_);
    tb.set_data_index_count(data_block_count_);
    ...
    tb.finish(trailer_bytes);
    writer_->write(trailer_bytes);
}
```

Trailer 固定 4096 字节。内部是 ProtoBuf 序列化的 `FileTrailerProto`，前后有 magic、varint 长度前缀、零填充和版本号。

### 7.8 提交与状态标记

```cpp
if (atomic_writer_) {
    atomic_writer_->commit();      // fsync + rename
} else {
    plain_writer_->flush();
    plain_writer_->close();
}

finished_ = true;  // 只在全部 I/O 成功后设置
```

`finished_` 标志**仅在最后设置**——这是安全设计的核心。如果 commit/close 中途失败，`finished_` 保持 false，析构函数会删除部分文件。

---

## 8. 析构函数：最后的安全网

```cpp
~HFileWriterImpl() {
    if (opened_ && !finished_) {
        if (atomic_writer_) {
            atomic_writer_->abort();   // close + delete temp file
        } else if (plain_writer_) {
            plain_writer_->close();
            std::filesystem::remove(path_, ec);
        }
        log::warn("Partial HFile deleted: " + path_);
    }
}
```

三种场景会触发这个清理：

1. `append()` 过程中发生错误，调用方没有调 `finish()` 就析构了 writer
2. `finish()` 内部失败（比如 write_trailer 时磁盘满）
3. C++ 异常导致栈展开，writer 被提前析构

在所有场景中，最终路径上不会留下损坏的 HFile。这与 AtomicFileWriter 的 temp → rename 策略互补——atomic 模式下损坏文件在 `.tmp/` 目录中，plain 模式下被 `remove()` 删除。

---

## 9. 状态变量全景

writer.cc 中 `HFileWriterImpl` 维护了大量状态变量。以下按用途分组：

### 9.1 配置与 I/O

| 变量 | 类型 | 说明 |
|------|------|------|
| `path_` | string | 输出文件路径 |
| `opts_` | WriterOptions | 全部配置（被 open 修改过） |
| `writer_` | BlockWriter* | 非拥有 I/O 指针 |
| `atomic_writer_` | unique_ptr | Safe 模式 I/O（拥有） |
| `plain_writer_` | unique_ptr | Fast/Paranoid I/O（拥有） |

### 9.2 编码/压缩/索引/Bloom

| 变量 | 类型 | 说明 |
|------|------|------|
| `encoder_` | unique_ptr\<DataBlockEncoder> | 当前 Block 编码器 |
| `compressor_` | unique_ptr\<Compressor> | 压缩器 |
| `bloom_` | unique_ptr\<CompoundBloomFilterWriter> | Bloom Filter 写入器 |
| `index_writer_` | BlockIndexWriter | 索引写入器（直接成员） |
| `compress_buf_` | vector\<uint8_t> | 压缩输出缓冲区（复用） |

### 9.3 Key 追踪

| 变量 | 类型 | 说明 |
|------|------|------|
| `first_key_` | vector\<uint8_t> | 文件中第一个 KV 的序列化 key |
| `last_key_` | vector\<uint8_t> | 最后一个 KV 的序列化 key → FileInfo LASTKEY |
| `last_kv_` | OwnedKeyValue | 最后一个 KV 的完整拷贝 → 排序校验基准 |
| `last_bloom_key_` | vector\<uint8_t> | 最后一个 bloom key → FileInfo LAST_BLOOM_KEY |

### 9.4 统计量

| 变量 | 写入 Trailer/FileInfo 的哪个字段 |
|------|------|
| `entry_count_` | Trailer.entry_count |
| `total_key_bytes_` | FileInfo.AVG_KEY_LEN (除以 entry_count) |
| `total_value_bytes_` | FileInfo.AVG_VALUE_LEN |
| `total_uncompressed_bytes_` | Trailer.total_uncompressed_bytes |
| `data_block_count_` | Trailer.data_index_count |
| `max_tags_len_` | FileInfo.MAX_TAGS_LEN |
| `max_memstore_ts_` | FileInfo.MAX_MEMSTORE_TS_KEY |
| `max_cell_size_` | FileInfo.LEN_OF_BIGGEST_CELL |
| `first_data_block_offset_` | Trailer.first_data_block_offset |
| `last_data_block_offset_` | Trailer.last_data_block_offset |

### 9.5 Block 链表

| 变量 | 说明 |
|------|------|
| `prev_block_offset_` | 上一个写入的 Block 的文件偏移量，填入下一个 Block Header 的 prevBlockOffset 字段 |

这形成一个**反向链表**：每个 Block 的 Header 中记录了前一个 Block 的偏移，HBase 可以从任意 Block 反向遍历。初始值为 -1（第一个 Block 没有前驱，-1 被 cast 为 `0xFFFFFFFFFFFFFFFF`）。

### 9.6 生命周期标记

| 变量 | 说明 |
|------|------|
| `opened_` | open() 成功后为 true |
| `finished_` | finish() 全部 I/O 成功后为 true |

这两个标记控制析构函数的行为——只有 `opened_ && !finished_` 时才需要清理。

---

## 10. 性能瓶颈与优化空间

当前 writer.cc 的主要性能特征：

**已有的优化**：
- compress_buf_ 全生命周期复用，flush_data_block 零额外分配
- NoneEncoder 的 append 是纯内存拷贝（serialize_kv 到连续 buffer）
- BlockWriter 有 4MB 应用层写缓冲，小 write 合并为大 fwrite

**当前瓶颈**（按影响排序）：
1. **无压缩**：输出文件比必要的大 2-3 倍，I/O 量直接影响吞吐
2. **无编码**：NONE 编码不做前缀压缩，Data Block 比 FastDiff 大 30-50%
3. **write_data_block 中的临时分配**：`checksummed_block` 每次 flush 都 resize+拷贝
4. **build_raw_block 的 vector 返回**：每个非 Data Block 都创建临时 vector
5. **sanitize_owned_kv 深拷贝**：每个 KV 的 row/family/qualifier/value 都被拷贝一次

启用压缩和 FastDiff 编码后，瓶颈 1 和 2 会消失。瓶颈 3 可以通过预分配 checksummed_block 缓冲区解决。瓶颈 5 在 PreSortedTrusted 模式下可以避免（直接用 span，不拷贝），但需要调用方保证 span 引用的缓冲区在 flush 前有效。
