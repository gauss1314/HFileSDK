# 05 — 数据块编码器

当前 SDK 只保留 `NONE` 数据块编码。

## 1. 当前实现

- `src/block/data_block_encoder.h` 定义统一接口，以及 `serialize_kv()` / `serialize_key()` 两个共享工具函数。
- `src/block/none_encoder.h` / `src/block/none_encoder.cc` 实现唯一的 on-disk 编码器 `NoneEncoder`。
- `src/block/block_builder.cc` 中的 `DataBlockEncoder::create()` 固定返回 `NoneEncoder`。

## 2. NONE 编码格式

`NONE` 编码不做前缀压缩或差分编码。每个 KeyValue 都按 HFile v3 原始格式直接顺序写入当前 Data Block：

```text
[keyLen(4)][valueLen(4)][rowLen(2)][row][familyLen(1)][family][qualifier]
[timestamp(8)][type(1)][value][tagsLen(2)][tags][memstoreTS?]
```

对应实现位于 `serialize_kv()` 和 `NoneEncoder::append()`。

## 3. 约束

- 配置层只支持 `data_block_encoding = NONE`
- `FileInfo` 中的 `DATA_BLOCK_ENCODING` 固定写入 `NONE`
- writer 不再生成 encoded block magic 或编码 ID 前缀

后续关于块编码的维护范围只剩 `NONE` 路径和其相关测试。
