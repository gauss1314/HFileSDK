# 02 — Arrow → HFile 转换流水线

本文档追踪一次 `convert()` 调用的完整数据流，从 Java 发起 JNI 调用，到最终 HFile 文件落盘。这是理解 SDK 最核心的一份文档。

---

## 1. 全景：一次 convert 调用经历了什么

```
Java: sdk.convert("/data/events.arrow", "/staging/cf/events.hfile", "events", "COL_A,0,false,10#COL_B,1,true,15")
  │
  ▼
┌─────────────────────── JNI 层（hfile_jni.cc）────────────────────────┐
│ 1. 提取 JNI 字符串为 std::string                                      │
│ 2. 从 InstanceState 加载 WriterOptions + 列排除配置                    │
│ 3. 构建 ConvertOptions，调用 hfile::convert(opts)                     │
│ 4. 捕获所有 C++ 异常 → 转为错误码返回                                  │
└───────────────────────────┬──────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────── 转换编排层（converter.cc）────────────────────────┐
│                                                                      │
│ ① 输入校验（路径存在、规则非空）                                        │
│ ② 编译 rowKeyRule → RowKeyBuilder                                    │
│ ③ 计算列排除索引（excluded_columns / excluded_column_prefixes）        │
│ ④ Pass 1：逐批读 Arrow → 构建 row key → 收集 SortEntry               │
│ ⑤ 排序：std::stable_sort 按 row_key 字典序                           │
│ ⑥ 打开 HFileWriter（PreSortedVerified 模式）                         │
│ ⑦ Pass 2：按排序顺序遍历，聚合同 row_key 的列 → 写入 HFileWriter      │
│ ⑧ HFileWriter::finish() → 写 Index/Bloom/FileInfo/Trailer           │
│ ⑨ 返回 ConvertResult                                                 │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. JNI 入口层详解

### 2.1 三个 JNI 方法

`hfile_jni.cc` 导出三个 `extern "C"` 函数：

| Java 方法 | C++ 入口 | 职责 |
|-----------|----------|------|
| `convert(arrowPath, hfilePath, tableName, rowKeyRule)` | `Java_com_hfile_HFileSDK_convert` | 执行转换 |
| `getLastResult()` | `Java_com_hfile_HFileSDK_getLastResult` | 返回上次结果 JSON |
| `configure(configJson)` | `Java_com_hfile_HFileSDK_configure` | 设置全局配置 |

`[!]` 注意：DESIGN.md v4.0 中 `convert()` 有 5 个参数（含 `rowValue`），但实际代码中 `rowValue` 已被移除，只有 4 个参数。Row value 现在从 Arrow 列自动提取。

### 2.2 实例状态管理

每个 Java `HFileSDK` 对象在 C++ 侧对应一个 `InstanceState`，存储在全局 `g_instance_states` 向量中。通过 `WeakGlobalRef` 追踪 Java 对象——当 Java 对象被 GC 回收后，`cleanup_instance_states_locked()` 会自动清理对应的 C++ 状态。

`InstanceState` 包含：

- `writer_opts`：通过 `configure()` 设置的 WriterOptions
- `last_result` / `last_result_json`：上次 `convert()` 的结果
- `excluded_columns` / `excluded_column_prefixes`：列排除设置

所有对 `g_instance_states` 的访问都通过 `g_config_mutex` 保护。但 `convert()` 执行期间不持锁——它在入口处拍快照（`get_instance_snapshot`），之后用快照工作。

### 2.3 异常隔离

JNI 层的核心安全保证：**C++ 异常绝不穿透到 JVM**。

```cpp
try {
    // ... 全部转换逻辑 ...
    return result.error_code;
} catch (const std::exception& e) {
    // 记录日志，返回 INTERNAL_ERROR
} catch (...) {
    // 兜底：捕获一切未知异常
}
```

所有三个 JNI 函数都遵循这个模式。`getLastResult()` 甚至在 catch 中返回 `"{}"`，确保 Java 侧永远能得到有效的 jstring。

---

## 3. 转换编排：converter.cc 的 convert() 函数

这是整个 SDK 最核心的 ~350 行代码。

### 3.1 输入校验

```cpp
if (opts.arrow_path.empty()) → INVALID_ARGUMENT
if (opts.hfile_path.empty()) → INVALID_ARGUMENT
if (!std::filesystem::exists(opts.arrow_path)) → ARROW_FILE_ERROR
if (opts.row_key_rule.empty()) → INVALID_ROW_KEY_RULE
```

### 3.2 编译 rowKeyRule

```cpp
auto [rkb, rk_status] = arrow_convert::RowKeyBuilder::compile(opts.row_key_rule);
```

`compile()` 将规则字符串（如 `"COL_A,0,false,10#COL_B,1,true,15"`）解析为 `RowKeySegment` 数组。解析失败立即返回 `INVALID_ROW_KEY_RULE`。

同时提取 `max_col_index`——规则引用的最大列索引，用于后续 Schema 校验。

### 3.3 列排除处理

如果 `configure()` 设置了 `excluded_columns` 或 `excluded_column_prefixes`（典型场景：过滤 Hudi 元数据列如 `_hoodie_commit_time`），SDK 会：

1. 额外打开一次 Arrow 文件仅读取 Schema
2. 调用 `build_removal_indices()` 匹配列名/前缀，生成**降序**索引列表
3. 在 Pass 1 中，每个 batch 先调用 `apply_column_removal()` 物理删除这些列

降序很重要：`RemoveColumn(5)` 后 `RemoveColumn(3)` 仍然正确，反过来就会错位。

`[!]` 关键设计决策：rowKeyRule 中的列索引引用的是**排除后**的 Schema。例如原始 Schema 有 `[_hoodie_x, _hoodie_y, STARTTIME, IMSI]`，排除 `_hoodie` 前缀后变为 `[STARTTIME, IMSI]`，规则中 `STARTTIME` 应该用索引 0。

### 3.4 Pass 1：构建排序索引

```cpp
Status build_sort_index(arrow_path, rkb, max_col_idx, removal_indices,
                        sort_index_out, batches_out, budget, result)
```

这个函数做了以下事情：

**（a）打开 Arrow IPC Stream 文件**

```cpp
auto file = arrow::io::ReadableFile::Open(arrow_path);
auto reader = arrow::ipc::RecordBatchStreamReader::Open(file);
```

**（b）校验 Schema**

- 检查 `max_col_index` 不超过过滤后的列数
- 检查每个被 rowKeyRule 引用的列的 Arrow 类型是否受支持（String/Int/Float/Bool/Timestamp）
- 不支持的类型（如 List、Struct、Map）返回 `SCHEMA_MISMATCH`

**（c）逐批处理**

```
while reader->ReadNext(&batch):
    if excluded columns → apply_column_removal(batch)
    for each row in batch:
        对 rowKeyRule 引用的每一列：scalar_to_string(列值) → 字符串
        rkb.build_checked(fields) → row_key 字符串
        sort_index.push_back({row_key, batch_idx, row_idx})
    batches.push_back(batch)  // 保留 batch 供 Pass 2 使用
```

每个 batch 处理完后的引用会被保留（`batches_out`），不会释放——因为 Pass 2 需要随机访问。

如果配置了 `MemoryBudget`，每次 Arrow batch 加载和 SortEntry 创建都会调用 `budget->reserve()`，超限则返回 `MEMORY_EXHAUSTED`。

**（d）空 row key 处理**

`build_checked()` 可能因为列值为 null 而生成空 row key。这些行被标记为 `kv_skipped_count++`，在排序前通过 `std::remove_if` 过滤掉。

### 3.5 排序

```cpp
std::stable_sort(sort_index.begin(), sort_index.end(),
                 [](const SortEntry& a, const SortEntry& b) {
                     return a.row_key < b.row_key;
                 });
```

这里用的是**字典序**（`std::string::operator<`），等价于 HBase 的 Row Key 字节序比较。

`[!]` 这里只按 Row Key 排序。同一个 Row Key 下的多个列（qualifier）在 Pass 2 的 `append_grouped_row_cells()` 中再排序。

### 3.6 打开 HFileWriter

```cpp
auto [writer, ws] = HFileWriter::builder()
    .set_path(opts.hfile_path)
    .set_column_family(wo.column_family)
    .set_sort_mode(WriterOptions::SortMode::PreSortedVerified)
    ...
    .build();
```

关键：排序模式是 `PreSortedVerified`。converter 已经在外部完成了排序，但 writer 仍然会**逐 KV 验证**排序正确性。如果发现乱序，立即返回 `SORT_ORDER_VIOLATION`。

`[!]` 当前实现中，`open()` 方法会**强制覆盖**用户请求的编码和压缩为 NONE：

```cpp
if (opts_.data_block_encoding != Encoding::None) {
    log::warn("...falling back to NONE...");
    opts_.data_block_encoding = Encoding::None;
}
if (opts_.compression != Compression::None) {
    log::warn("...falling back to NONE...");
    opts_.compression = Compression::None;
}
```

原因是 Prefix/Diff/FastDiff 编码器的输出还不能保证与 HBase 解码器字节级兼容。这意味着目前实际生成的 HFile **始终使用 NONE 编码 + 无压缩**。

### 3.7 Pass 2：按排序顺序写入

这是最复杂的部分。核心逻辑：

```
for i in sort_index:
    收集所有 row_key 相同的连续 SortEntry → 一个 "row group"
    对 group 中的每一行、每一列：
        scalar_to_bytes(列值) → value 字节
        生成 GroupedCell{qualifier=列名, value=字节}
    调用 append_grouped_row_cells() 写入 HFileWriter
```

**Row Key 冲突处理**

当多个 Arrow 源行映射到同一个 HBase Row Key 时（`source_rows > 1`），SDK 会：

1. 发出一条 `DUPLICATE_KEY` 的 WARN 日志（每个冲突 key 只一条，不是每列一条）
2. 在 `append_grouped_row_cells()` 中，对同一个 qualifier 只保留第一次出现的值
3. `duplicate_key_count` 计数器递增

**qualifier 排序与去重**

`append_grouped_row_cells()` 内部先对所有 cells 按 qualifier 排序（`std::sort`），然后去重。这确保了 HFile 内 KV 的排序满足 HBase 要求：Row ASC → Family(固定) → Qualifier ASC → Timestamp DESC。

**时间戳**

如果 `default_timestamp > 0`，使用它；否则使用 `std::chrono::system_clock::now()` 的毫秒数。同一个 row group 内所有 KV 使用相同的时间戳。

### 3.8 完成与收尾

```cpp
writer->finish();
```

`finish()` 触发 `HFileWriterImpl::finish()`，这是 HFile 物理格式写入的核心（详见 03_HFILE_V3_FORMAT.md）。

完成后，读取输出文件大小，计算总耗时，记录日志并返回 `ConvertResult`。

---

## 4. HFileWriter 内部写入流程

`writer.cc` 中 `HFileWriterImpl` 的 `append()` → `finish()` 路径。

### 4.1 append() 流程

```
append(kv)
  │
  ├─ validate_kv()：空 key? key 太长? value 太大? 负时间戳?
  │   ├─ 失败 + Strict → 返回错误
  │   ├─ 失败 + SkipRow → 记录 warning，吞掉错误，返回 OK
  │   └─ 失败 + SkipBatch → 返回特殊错误码
  │
  ├─ Column Family 检查：kv.family 必须等于 opts.column_family
  │
  ├─ AutoSort 模式? → buffer_auto_sorted_kv()（converter 不走这条路）
  │
  └─ append_materialized_kv(kv)
      ├─ PreSortedVerified? → compare_keys(kv, last_kv) 校验排序
      ├─ maybe_check_disk_space()
      ├─ encoder_->append(kv)
      │   └─ 返回 false（Block 满）→ flush_data_block() → 再 append
      ├─ bloom_->add(kv.row)
      ├─ 更新 first_key / last_key / 统计
      └─ ++written_entry_count_
```

### 4.2 flush_data_block() 流程

当 DataBlockEncoder 报告 Block 已满（超过 `block_size`，默认 64KB）时触发：

```
flush_data_block()
  │
  ├─ raw = encoder_->finish_block()          // 获取原始编码字节
  ├─ bloom_->finish_chunk()                  // 封存当前 Bloom chunk
  ├─ compressed = compressor_->compress(raw) // 压缩（当前强制为 NONE）
  ├─ write_data_block(raw.size(), compressed, first_key, offset)
  │   ├─ 构建 33 字节 Block Header
  │   ├─ 计算 CRC32C 校验和（每 512 字节一个）
  │   ├─ 写入：Header + 压缩数据 + 校验和
  │   └─ index_writer_.add_entry(first_key, offset, size)
  ├─ total_uncompressed_bytes_ += raw.size()
  ├─ ++data_block_count_
  └─ encoder_->reset()
```

### 4.3 finish() 流程

这是 HFile 文件格式组装的核心，按严格顺序写入：

```
finish()
  │
  ├─ AutoSort? → sort → append all
  ├─ flush 最后的 DataBlock
  ├─ bloom_->finish_chunk()                 // 封存最后一个 Bloom chunk
  │
  ├─── Non-scanned Block Section ───
  │  └─ bloom_->finish_data_blocks()        // 写 BLMFBLK2 块
  │
  ├─── Load-on-open Section ────────
  │  ├─ Intermediate Index Blocks           // IDXINTE2（大文件才有）
  │  ├─ Root Data Index Block               // IDXROOT2
  │  ├─ Meta Root Index Block               // IDXROOT2（指向 Bloom Meta）
  │  ├─ FileInfo Block                      // FILEINF2
  │  └─ Bloom Meta Block                    // BLMFMET2
  │
  ├─── Trailer ─────────────────────
  │  └─ write_trailer()                     // ProtoBuf + 4KB 固定大小
  │
  ├─ AtomicFileWriter::commit()             // fsync + rename
  └─ finished_ = true
```

`[!]` Bloom Meta Block 的偏移量计算涉及一个"鸡生蛋"问题：FileInfo 需要 bloom_meta_offset，但 bloom_meta_offset 依赖于 FileInfo 的大小。代码通过**两次构建**解决：先用临时值构建 meta_root_block 计算大小，再用正确偏移量重新构建。

---

## 5. I/O 后端：AtomicFileWriter

### 5.1 崩溃安全设计

当 `FsyncPolicy::Safe`（默认）时，writer 使用 `AtomicFileWriter`：

```
写入阶段: /staging/cf/.tmp/<uuid>_events.hfile.tmp
         ↑ 所有 write() 调用写入这个临时文件

commit():
  1. inner_->flush()    — 应用层缓冲刷到 FILE*
  2. fflush(file)       — C 库缓冲刷到 OS
  3. fsync(fileno(file)) — OS 缓冲刷到磁盘
  4. fclose(file)
  5. fsync(.tmp 目录)    — 确保临时文件目录项持久化
  6. rename(.tmp → 最终路径) — POSIX 原子操作
  7. fsync(最终路径父目录)  — 确保 rename 目录项持久化
```

**安全保证**：在任何时刻断电，最终路径上要么有完整文件（rename 已完成），要么没有文件（rename 未执行）。不会出现写了一半的损坏 HFile。

### 5.2 析构函数清理

`HFileWriterImpl` 的析构函数会检查 `opened_ && !finished_`：如果 writer 被打开但 finish 没成功，自动删除部分写入的文件。这防止了异常路径中临时文件泄漏。

---

## 6. 内存模型

### 6.1 两遍扫描的内存代价

Pass 1 将**所有 batch 保留在内存中**（`batches_out`），因为 Pass 2 需要随机访问。这意味着整个 Arrow 文件的数据都在内存里。

同时，`sort_index` 存储每行的 `{row_key, batch_idx, row_idx}`，其中 `row_key` 是完整的 Row Key 字符串副本。

对于 1GB 的 Arrow 文件，内存峰值大约是：Arrow 数据 + sort_index ≈ 1.5-2x Arrow 文件大小。

`[!]` 这与 DESIGN.md §4.4 描述的"流式处理"有显著差异。DESIGN.md 说"内存中只保留1个 Batch"，但实际实现在排序场景下必须保留所有 batch。只有 `PRESORTED_TRUSTED` 模式（当前未在 convert() 中使用）才能真正实现流式处理。

### 6.2 MemoryBudget 控制

如果 `WriterOptions::max_memory_bytes > 0`，`MemoryBudget` 会追踪内存使用：

- Pass 1 中每加载一个 batch、每创建一个 SortEntry 都调用 `budget->reserve()`
- 超限时返回 `MEMORY_EXHAUSTED` 错误
- 这是一个**软限制**——它追踪的是 SDK 自己 reserve 的量，不拦截 Arrow 库内部的分配

---

## 7. 错误处理总结

| 阶段 | 典型错误 | 处理 | 返回码 |
|------|---------|------|--------|
| JNI 字符串提取 | null/encoding 错误 | 返回错误 | INVALID_ARGUMENT |
| 文件打开 | 不存在/权限 | 返回错误 | ARROW_FILE_ERROR |
| 规则编译 | 语法错误 | 返回错误 | INVALID_ROW_KEY_RULE |
| Schema 校验 | 列索引越界/类型不支持 | 返回错误 | SCHEMA_MISMATCH |
| Row Key 构建 | 值为 null → 空 key | 跳过行 | 继续（kv_skipped++) |
| 内存超限 | budget->reserve 失败 | 返回错误 | MEMORY_EXHAUSTED |
| 排序验证 | KV 乱序 | 终止写入 | SORT_VIOLATION |
| 值序列化 | 类型转换失败 | 跳过 cell | 继续（kv_skipped++) |
| 磁盘空间 | statvfs 检测不足 | 返回错误 | DISK_EXHAUSTED |
| C++ 异常 | 任何未预期异常 | JNI catch | INTERNAL_ERROR |
