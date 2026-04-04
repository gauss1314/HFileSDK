# 05 — 数据块编码器

本文档覆盖 `src/block/` 目录下的四种 DataBlockEncoder 实现。虽然当前 SDK 强制使用 NONE 编码（见 03_HFILE_V3_FORMAT.md §10），但其他三种编码器已经实现，理解它们对未来启用编码和性能优化至关重要。

---

## 1. 编码器体系

### 1.1 抽象基类

`DataBlockEncoder`（`data_block_encoder.h`）定义了统一接口：

```cpp
class DataBlockEncoder {
    virtual bool append(const KeyValue& kv) = 0;     // 追加 KV，满了返回 false
    virtual std::span<const uint8_t> finish_block();  // 返回编码后字节
    virtual void reset();                              // 重置准备下一个 Block
    virtual std::span<const uint8_t> first_key();     // 当前 Block 的第一个 key
    virtual size_t current_size();                     // 当前编码大小
    virtual uint32_t num_kvs();                        // KV 数量
};
```

工厂方法 `DataBlockEncoder::create(Encoding, block_size)` 在 `block_builder.cc` 中，根据编码类型创建对应实现。

### 1.2 共享工具函数

所有编码器共用两个内联序列化函数（`data_block_encoder.h`）：

- `serialize_kv(kv, dst)` — 完整 KV 序列化（含 keyLen/valueLen/tags/mvcc），NONE 编码直接使用
- `serialize_key(kv, dst)` — 仅 key 部分，用于索引和前缀计算

公共前缀计算函数 `prefix_len()`（定义在各编码器 `.h` 中）：

```cpp
static size_t prefix_len(const uint8_t* a, size_t a_len,
                         const uint8_t* b, size_t b_len) {
    size_t limit = std::min(a_len, b_len);
    #ifdef __SSE4_2__
    // SSE4.2 pcmpistri 加速：一次比较 16 字节
    #else
    // 逐字节比较
    #endif
}
```

SSE4.2 路径使用 `_mm_cmpistri` 指令一次比较 16 字节，将前缀计算从 O(n) 优化到 O(n/16)。所有非 NONE 编码器都依赖这个函数。

---

## 2. NONE Encoder

**文件**：`none_encoder.h`（62 行，纯头文件实现）

**原理**：最简单的编码——KV 原样拼接，没有任何压缩。

### 2.1 append() 流程

```cpp
bool append(const KeyValue& kv) {
    size_t kv_size = kv.encoded_size();
    if (!buffer_.empty() && buffer_.size() + kv_size > block_size_)
        return false;  // Block 满

    buffer_.resize(old_size + kv_size);
    serialize_kv(kv, buffer_.data() + old_size);

    if (num_kvs_ == 0) {
        // 保存 first key（供索引使用）
        first_key_buf_ = serialize_key(kv);
    }
    ++num_kvs_;
    return true;
}
```

**Block 大小控制**：当 buffer 非空且加上新 KV 会超过 `block_size_` 时返回 false。第一个 KV 永远被接受（`!buffer_.empty()` 条件），这避免了单个超大 KV 导致无限循环。

### 2.2 磁盘格式

```
[KV_0 完整序列化][KV_1 完整序列化]...[KV_N 完整序列化]
```

每个 KV 的格式见 03_HFILE_V3_FORMAT.md §3。

---

## 3. PREFIX Encoder

**文件**：`prefix_encoder.h`（138 行，纯头文件实现）

**原理**：利用相邻 Key 的公共前缀。只存储前缀长度 + 后缀，通常能减少 30-50% 的 Block 大小。

### 3.1 每个 KV 的磁盘格式

```
[keySharedLen(2B BE)]     — 与前一个 key 的公共前缀长度
[keyUnsharedLen(2B BE)]   — key 的后缀长度
[valueLen(4B BE)]         — value 长度
[unsharedKeyBytes]        — key 后缀字节
[valueBytes]              — value 字节
[tagsLen(2B BE)]          — tags 长度
[tagsBytes]               — tags 字节
[mvcc(WritableVInt)]      — memstoreTS
```

第一个 KV 的 `keySharedLen = 0`，`keyUnsharedLen = 完整 key 长度`。

### 3.2 核心实现

```cpp
bool append(const KeyValue& kv) {
    // 1. 序列化当前 key（栈上 512B 缓冲区，避免堆分配）
    uint8_t key_stack[512];
    serialize_key(kv, key_stack);

    // 2. 计算公共前缀
    size_t shared = prefix_len(prev_key_, cur_key);
    size_t unshared = key_len - shared;

    // 3. 估算大小，检查 Block 是否满
    size_t est = 2 + 2 + 4 + unshared + value.size + 2 + tags.size + 10;
    if (!buffer_.empty() && buffer_.size() + est > block_size_)
        return false;

    // 4. 写入编码后的字节
    write_be16(p, shared);
    write_be16(p, unshared);
    write_be32(p, value_len);
    memcpy(p, cur_key + shared, unshared);
    memcpy(p, value...);
    // ... tags + mvcc

    // 5. 更新 prev_key
    prev_key_.assign(cur_key, cur_key + key_len);
}
```

**性能关键**：key 序列化使用 512 字节栈缓冲区（`__builtin_expect(key_len <= 512, true)`），只有超大 key 才会退化到堆分配。这避免了每次 append 调用的 malloc/free。

### 3.3 写入后裁剪

`append()` 先估算最大可能大小分配空间，写入后 `buffer_.resize(实际写入位置)` 裁掉多余部分。这比精确预计算大小更快（避免了提前扫描 varint 长度）。

---

## 4. DIFF Encoder

**文件**：`diff_encoder.h`（168 行，纯头文件实现）

**原理**：在 PREFIX 基础上进一步差分编码 timestamp 和 key type。一个 flags 字节记录哪些字段与前一个 KV 不同。

### 4.1 Flags 字节

```
bit 0 (0x01): timestamp 与前一个不同 → 后面跟完整 8B timestamp
bit 1 (0x02): key type 与前一个相同 → 不输出 type 字节
bit 2 (0x04): value 长度与前一个相同 → 不输出 vlen 字段
```

### 4.2 每个 KV 的磁盘格式

```
[flags(1B)]
[keySharedLen(2B BE)]
[keyUnsharedLen(2B BE)]
[valueLen(4B BE)]              — 仅当 !(flags & 0x04)
[timestamp(8B BE)]             — 仅当 (flags & 0x01)
[keyType(1B)]                  — 仅当 !(flags & 0x02)
[unsharedKeyBytes]
[valueBytes]
[tagsLen(2B BE)]
[tagsBytes]
[mvcc(WritableVInt)]
```

### 4.3 收益分析

对于 Bulk Load 场景（所有 KV 都是 Put、timestamp 通常相同、value 长度变化大），DIFF 编码的主要收益来自：

- key type 相同（bit 1），省 1 字节/KV
- timestamp 相同（bit 0 为 0），省 8 字节/KV

对百万级 KV 来说，节省 ~9MB。

---

## 5. FAST_DIFF Encoder

**文件**：`fast_diff_encoder.h`（209 行，纯头文件实现）

**原理**：优化的 DIFF 编码。额外支持 timestamp 差分编码（存 delta 而非完整值），减少解码时的分支预测失败。

### 5.1 Flags 字节（与 DIFF 不同）

```
bit 0 (0x01): kFlagTimestampNew   — 用于指示非 same_type
bit 1 (0x02): kFlagSameType       — key type 相同
bit 2 (0x04): kFlagSameValueLen   — value 长度相同
bit 3 (0x08): kFlagUseTimeDelta   — 使用 timestamp delta（而非完整值）
```

### 5.2 Timestamp Delta

当连续 KV 的 timestamp 差值在 `[-128, 127]` 范围内且非零时，启用 delta 编码。存储 8 字节 delta 而非 8 字节完整 timestamp。

`[!]` 这里实际上没有节省字节（都是 8B），但 delta 编码提高了后续压缩算法的效率（小数值的高位全是 0/1，压缩比更好）。

### 5.3 磁盘格式

```
[flags(1B)]
[keySharedLen(2B BE)]
[keyUnsharedLen(2B BE)]
[valueLen(4B BE)]              — 仅当 !(flags & 0x04)
[timestamp 或 delta(8B BE)]    — delta 仅当 (flags & 0x08)
[keyType(1B)]                  — 仅当 !(flags & 0x02)
[unsharedKeyBytes]
[valueBytes]
[tagsLen(2B BE)]
[tagsBytes]
[mvcc(WritableVInt)]
```

---

## 6. 编码器对比

| 维度 | NONE | PREFIX | DIFF | FAST_DIFF |
|------|------|--------|------|-----------|
| Block 大小 | 最大 | 减少 30-50% | 减少 35-55% | 减少 35-55% |
| 编码速度 | 最快 | 快 | 中 | 中 |
| 实现复杂度 | 62 行 | 138 行 | 168 行 | 209 行 |
| 栈分配优化 | 不需要 | 512B 栈缓冲 | 512B 栈缓冲 | 512B 栈缓冲 |
| SSE4.2 加速 | 不需要 | prefix_len | prefix_len | prefix_len |
| HBase 兼容性 | ✅ | ❌ 未验证 | ❌ 未验证 | ❌ 未验证 |

`[!]` 当前只有 NONE 编码器被实际使用。其他三种编码器已经实现完毕，但由于与 HBase 解码器的字节级兼容性尚未通过端到端验证（`hfile-verify` 工具），被 `writer.cc: open()` 强制降级为 NONE。

启用非 NONE 编码的前提条件：

1. 用 `hfile-verify` 工具验证每种编码生成的 HFile 能被 HBase 正确读取
2. 验证 `DataBlock` 使用 `DATABLKE` magic 而非 `DATABLK*`
3. 验证 Block 开头需要 2 字节 encoding ID（`hbase_data_block_encoding_id()`）
4. 确保 Prefix/Diff/FastDiff 的中间状态（prev_key, prev_timestamp 等）在 Block 边界正确重置

---

## 7. 编码器在写入流程中的位置

```
writer.cc: append_materialized_kv(kv)
  │
  ├── encoder_->append(kv)        ← 编码器将 KV 编码到内部缓冲区
  │   └── 返回 false              ← Block 满
  │
  ├── flush_data_block()
  │   ├── raw = encoder_->finish_block()   ← 获取编码后的原始字节
  │   ├── if encoding != NONE:
  │   │     prepend 2B encoding_id         ← HBase 需要知道编码类型
  │   ├── compressed = compressor->compress(raw)
  │   ├── write_data_block(raw.size, compressed, first_key, offset)
  │   └── encoder_->reset()                ← 重置编码器状态
  │
  └── encoder_->append(kv)        ← 重试追加到新 Block
```

`first_key()` 返回的是当前 Block 第一个 KV 的完整 key 字节（不是编码后的），用于索引。所有编码器都在第一次 `append()` 时保存这个值。
