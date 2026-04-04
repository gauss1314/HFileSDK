# 01 — 源码地图

本文档帮助你回答一个问题：**某个功能的代码在哪里？**

---

## 1. 目录总览

```
src/
├── jni/                   ← JNI 入口层（Java↔C++ 桥接）
│   ├── hfile_jni.cc          convert/configure/getLastResult 的 JNI 实现
│   ├── jni_utils.h           jstring↔std::string 安全转换
│   └── json_utils.h          手写 JSON 解析器（configure 用）
│
├── convert/               ← 转换编排层（Arrow 文件 → HFile 文件的完整流程）
│   ├── converter.cc          核心：两遍扫描 + 排序 + 写入编排
│   ├── converter.h           convert() 函数声明
│   └── convert_options.h     ConvertOptions / ConvertResult / ErrorCode
│
├── arrow/                 ← Arrow 数据处理层
│   ├── row_key_builder.cc    rowKeyRule 编译器与执行引擎
│   ├── row_key_builder.h     RowKeySegment / RowKeyBuilder
│   ├── arrow_to_kv_converter.cc  Arrow 类型序列化（Wide/Tall/RawKV 三种模式）
│   └── arrow_to_kv_converter.h   ArrowToKVConverter / WideTableConfig
│
├── block/                 ← 数据块编码器
│   ├── data_block_encoder.h  抽象基类 + serialize_kv/serialize_key 内联函数
│   ├── none_encoder.h        NONE 编码：KV 原样拼接
│   ├── prefix_encoder.h      PREFIX 编码：公共前缀压缩
│   ├── diff_encoder.h        DIFF 编码：时间戳/类型差分
│   ├── fast_diff_encoder.h   FAST_DIFF 编码：优化的差分编码
│   └── block_builder.cc      工厂方法 DataBlockEncoder::create()
│
├── bloom/                 ← Bloom Filter
│   └── compound_bloom_filter_writer.h  分块 Bloom Filter（Murmur3 哈希）
│
├── index/                 ← 块索引
│   ├── block_index_writer.h  多级索引写入器
│   └── block_index_writer.cc 索引条目序列化 + 中间索引块构建
│
├── codec/                 ← 压缩编解码
│   ├── compressor.h          Compressor 抽象 + 工厂
│   └── compressor.cc         LZ4/ZSTD/Snappy/GZip 封装
│
├── io/                    ← 文件 I/O 后端
│   ├── buffered_writer.h     BlockWriter 抽象 + BufferedFileWriter（FILE* 封装）
│   ├── buffered_writer.cc    4MB 应用层写缓冲
│   ├── atomic_file_writer.h  崩溃安全写入（.tmp → fsync → rename）
│   ├── atomic_file_writer.cc UUID 临时路径 + commit/abort
│   ├── hdfs_writer.h/cc      HDFS 直写（条件编译 HFILE_HAS_HDFS）
│   └── iouring_writer.h/cc   io_uring 双缓冲（条件编译 HFILE_HAS_IO_URING）
│
├── meta/                  ← HFile 元数据
│   ├── file_info_builder.h   FileInfo 块构建（必填字段校验）
│   ├── trailer_builder.h     ProtoBuf Trailer 序列化（固定 4KB 尾部）
│   └── *.cc                  头文件实现
│
├── checksum/              ← CRC32C 校验
│   ├── crc32c.h              SSE4.2 硬件加速 + 标量回退
│   └── crc32c.cc             compute_hfile_checksums（每 512B 一个校验和）
│
├── memory/                ← 内存管理
│   ├── arena_allocator.h     Bump-pointer 分配器（64B 对齐）
│   ├── block_pool.h          固定大小缓冲池（线程安全，Debug 检测双释放）
│   └── memory_budget.h       原子内存配额追踪（reserve/release）
│
├── metrics/               ← 指标收集
│   └── metrics_registry.h    Counter / Gauge / Histogram（无外部依赖）
│
├── partition/             ← Region 分区路由
│   ├── cf_grouper.h          Column Family 路由（BulkLoadWriter 用）
│   ├── region_partitioner.cc 手动分裂点 + 单 Region 模式
│   └── cf_grouper.cc
│
├── writer.cc              ← HFile 单文件写入器（核心，约 900 行）
└── bulk_load_writer.cc    ← 批量写入器（多 CF + 多 Region 路由）
```

公开头文件在 `include/hfile/` 下，内部头文件与对应 `.cc` 同目录。

---

## 2. 核心数据结构

### KeyValue（`include/hfile/types.h`）

整个 SDK 围绕这个结构运转。它是一个**非拥有视图**（所有字段都是 `std::span`），零拷贝引用 Arrow 缓冲区中的原始数据：

```
KeyValue {
    row:          std::span<const uint8_t>   // HBase Row Key
    family:       std::span<const uint8_t>   // Column Family（单文件内固定）
    qualifier:    std::span<const uint8_t>   // 列名（来自 Arrow Schema field name）
    timestamp:    int64_t                    // 毫秒时间戳
    key_type:     KeyType                    // Put=4, Delete=8 等
    value:        std::span<const uint8_t>   // 列值（Big-Endian 序列化后）
    tags:         std::span<const uint8_t>   // v3 Tags（Bulk Load 通常为空）
    memstore_ts:  uint64_t                   // MVCC 版本号（Bulk Load = 0）
}
```

有一个**拥有副本** `OwnedKeyValue`（所有字段是 `std::vector<uint8_t>`），用于 AutoSort 模式下的排序缓冲区。

### HBase Key 排序规则（`compare_keys`）

HFile 内 KV 必须严格有序。排序规则是：

```
Row 升序 → Family 升序 → Qualifier 升序 → Timestamp 降序 → KeyType 降序
```

`compare_keys()` 函数实现了这个 5 级比较，是正确性的关键。

### ConvertOptions / ConvertResult（`src/convert/convert_options.h`）

`ConvertOptions` 是一次 JNI 调用的全部输入参数包。`ConvertResult` 是返回给 Java 的结果，包含错误码、KV 计数、耗时等统计信息。

`ErrorCode` 命名空间定义了所有错误码（0=成功，1-20=各类错误），与 Java 侧的 `HFileSDK.OK/INVALID_ARGUMENT/...` 常量一一对应。

### WriterOptions（`include/hfile/writer_options.h`）

控制 HFile 写入行为的完整配置：压缩算法、Block 大小、编码类型、Bloom 类型、排序模式、FsyncPolicy、ErrorPolicy、内存上限等。所有生产环境相关的配置都在这里。

---

## 3. 四条关键代码路径

### 路径 A：JNI convert() 调用（最常用）

```
Java: HFileSDK.convert(arrowPath, hfilePath, tableName, rowKeyRule)
  → hfile_jni.cc: Java_com_hfile_HFileSDK_convert()
    → 提取 JNI 字符串，构建 ConvertOptions
    → 加载 InstanceSnapshot（configure 设置的 WriterOptions + 列排除）
    → converter.cc: hfile::convert(opts)
      → 打开 Arrow IPC Stream，编译 rowKeyRule
      → Pass 1: 逐批读取，构建 SortEntry(row_key, batch_idx, row_idx)
      → std::stable_sort 按 row_key 字典序排序
      → Pass 2: 按排序顺序遍历，每个 row_key 聚合所有列，写入 HFileWriter
      → HFileWriter::finish()
        → flush 最后 DataBlock → 写 Bloom 数据块
        → 写 Load-on-open Section（Index + FileInfo + Bloom Meta）
        → 写 Trailer（ProtoBuf 序列化，固定 4KB）
        → AtomicFileWriter::commit()（fsync + rename）
  → 返回错误码给 Java
```

### 路径 B：HFileWriter 直接使用（测试/基准测试）

```
HFileWriter::builder()
  .set_path(path)
  .set_column_family("cf")
  .set_compression(...)
  .build()
  → HFileWriterImpl::open()
    → 创建 I/O 后端（AtomicFileWriter 或 plain BlockWriter）
    → 创建 DataBlockEncoder + Compressor + BloomFilter

writer->append(kv)
  → validate_kv() 输入校验
  → Column Family 检查
  → AutoSort? → buffer_auto_sorted_kv()
  → PreSorted? → append_materialized_kv()
    → 排序验证 → 编码到 DataBlock → Block 满? → flush_data_block()
    → Bloom add → 统计更新

writer->finish()
  → AutoSort: stable_sort + 逐个 append
  → flush 最后 Block → Bloom finish → Index finish
  → 写 Load-on-open Section → 写 Trailer → commit
```

### 路径 C：BulkLoadWriter 批量写入

```
BulkLoadWriter::builder()
  .set_table_name(...)
  .set_column_families({...})
  .set_partitioner(...)
  .build()

writer->write_batch(batch, MappingMode::WideTable)
  → ArrowToKVConverter::convert_wide_table()
  → 对每个 KV: CFGrouper 路由到 (cf, region) → 找到或创建 HFileWriter → append

writer->finish()
  → 逐个 HFileWriter::finish()
  → 返回 BulkLoadResult
```

### 路径 D：configure() 配置

```
Java: HFileSDK.configure(configJson)
  → hfile_jni.cc: Java_com_hfile_HFileSDK_configure()
    → json_utils.h: parse_json_config() 手工解析 JSON
    → 校验 key 白名单、值合法性
    → 更新 InstanceState 中的 WriterOptions + 列排除设置
```

---

## 4. 模块依赖关系

从上到下，上层依赖下层：

```
┌──────────────────────────────────────────┐
│  jni/hfile_jni.cc                        │  JNI 入口
├──────────────────────────────────────────┤
│  convert/converter.cc                    │  转换编排
├──────────────────────────────────────────┤
│  arrow/row_key_builder     arrow/arrow_to_kv_converter  │  Arrow 处理
├──────────────────────────────────────────┤
│  writer.cc  /  bulk_load_writer.cc       │  HFile 写入
├──────────┬──────────┬──────────┬─────────┤
│ block/   │ bloom/   │ index/   │ meta/   │  HFile 组件
├──────────┼──────────┴──────────┴─────────┤
│ codec/   │ checksum/                     │  压缩 / 校验
├──────────┴───────────────────────────────┤
│ io/  (BufferedWriter / AtomicFileWriter) │  文件 I/O
├──────────────────────────────────────────┤
│ memory/  (Arena / BlockPool / Budget)    │  内存管理
├──────────────────────────────────────────┤
│ include/hfile/types.h                    │  基础类型 + 字节序工具
└──────────────────────────────────────────┘
```

关键依赖细节：

- `converter.cc` 直接使用 Arrow C++ API（`arrow::ipc::RecordBatchStreamReader`），不经过 `arrow_to_kv_converter.cc`。后者的 WideTable/TallTable/RawKV 模式只被 `bulk_load_writer.cc` 使用。
- `writer.cc` 是 HFile 物理格式的唯一出口，所有 Block/Bloom/Index/Meta 组件都在这里被组装。
- `hfile_jni.cc` 持有全局 `InstanceState` 数组，通过 WeakGlobalRef 跟踪 Java 对象生命周期。

---

## 5. 文件大小与复杂度速查

| 文件 | 行数 | 复杂度 | 说明 |
|------|------|--------|------|
| `writer.cc` | ~900 | 高 | HFile 物理格式组装，最复杂的单文件 |
| `converter.cc` | ~700 | 高 | 两遍扫描 + 排序 + Arrow 类型转换 |
| `hfile_jni.cc` | ~300 | 中 | JNI 桥接 + configure 解析 |
| `bulk_load_writer.cc` | ~635 | 中 | 多文件编排 |
| `row_key_builder.cc` | ~330 | 中 | 规则解析 + base64/hash 编码 |
| `arrow_to_kv_converter.cc` | ~300 | 中 | 三种映射模式 |
| `block_index_writer.cc` | ~170 | 中 | 多级索引构建 |
| `compound_bloom_filter_writer.h` | ~290 | 中 | Murmur3 + 分块 Bloom |
| `crc32c.cc` | ~117 | 低 | SSE4.2 + 标量回退 |
| `atomic_file_writer.cc` | ~164 | 低 | fsync + rename |

---

## 6. 调试入口建议

**HFile 被 HBase 拒绝？**
从 `writer.cc` 的 `finish()` 方法开始，检查 FileInfo 必填字段（`build_file_info_block`）和 Trailer 序列化（`write_trailer`）。

**Row Key 生成错误？**
在 `converter.cc` 的 `build_sort_index()` 中加日志，打印 `rkb.build_checked()` 的输入字段和输出 row_key。

**KV 排序不对？**
检查 `converter.cc` 中 `std::stable_sort` 的比较函数，以及 `append_grouped_row_cells()` 中 qualifier 的排序和去重逻辑。

**内存超限？**
`memory_budget.h` 的 `reserve()` 返回错误时会带 "MemoryBudget:" 前缀，在 `converter.cc` 的 `map_pass1_status_to_error_code()` 中被捕获。

**JNI 崩溃？**
所有 C++ 异常都在 `hfile_jni.cc` 的 `try/catch(...)` 中被拦截。如果 JVM 还是崩了，说明有异常逃逸——检查是否有新增的 extern "C" 函数没有加 try/catch。
