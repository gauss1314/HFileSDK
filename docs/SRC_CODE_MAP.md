# src 代码地图

本文档面向对 C++ 不熟悉、但希望快速理解 `src/` 目录实现方案的读者。目标不是逐行解释所有代码，而是回答下面几个问题：

- `src/` 里每个子目录是干什么的
- 数据是如何从 Arrow 变成 HFile 的
- 单文件写入、Bulk Load、JNI 三条链路分别怎么走
- 哪些文件是主入口，哪些文件是支撑模块
- 如果要调试 Bug，应该先看哪里

---

## 1. 先建立整体心智模型

可以把 `src/` 看成 4 层：

1. **入口层**
   - 单 HFile 写入：`src/writer.cc`
   - 多 HFile Bulk Load：`src/bulk_load_writer.cc`
   - Arrow 文件转 HFile：`src/convert/converter.cc`
   - Java 调 C++：`src/jni/hfile_jni.cc`

2. **映射层**
   - Arrow `RecordBatch` 变成 HBase `KeyValue`
   - RowKey 规则编译与执行
   - 主要在 `src/arrow/` 与 `src/convert/`

3. **HFile 物理组织层**
   - Data Block 编码
   - Bloom Filter
   - Block Index
   - FileInfo / Trailer
   - 主要在 `src/block/`、`src/bloom/`、`src/index/`、`src/meta/`

4. **底层支撑层**
   - 压缩
   - Checksum
   - 文件写入后端
   - 内存预算
   - 指标
   - 主要在 `src/codec/`、`src/checksum/`、`src/io/`、`src/memory/`、`src/metrics/`

一句话总结：

- **`writer.cc` 决定 HFile 怎么写出来**
- **`converter.cc` 决定 Arrow 怎么变成要写入的 KeyValue**
- **`bulk_load_writer.cc` 决定多 Region / 多列族时如何组织多个 HFile**

---

## 2. 目录级地图

### 2.1 `src/` 根目录

- `writer.cc`
  - 单个 HFile 文件的核心写入实现
  - 是整个仓库最重要的 C++ 文件
- `bulk_load_writer.cc`
  - 批量导入场景下的多文件输出编排器
  - 自己不定义 HFile 格式，而是调度多个 `HFileWriter`

### 2.2 `src/arrow/`

- `arrow_to_kv_converter.*`
  - 把 Arrow 的行列数据转换为 HBase 的 `KeyValue`
  - 支持 Wide Table / Tall Table / RawKV 三种模式
- `row_key_builder.*`
  - 解析并执行 `rowKeyRule`
  - 负责把若干字段拼成最终 row key

### 2.3 `src/block/`

- 数据块编码器层
- 提供：
  - `None`
  - `Prefix`
  - `Diff`
  - `FastDiff`
- 这些编码器的目标是：把一批有序 `KeyValue` 编码成 HFile Data Block 的 payload

### 2.4 `src/bloom/`

- 负责 Bloom Filter 的构造与序列化
- 这里实现的是 HBase 兼容的 **Compound Bloom Filter**

### 2.5 `src/index/`

- 负责 HFile 的 Data Block Index / Root Index
- 让读取方可以通过 row key 找到对应 block

### 2.6 `src/meta/`

- 负责 FileInfo 与 Trailer
- 这是 HFile 文件尾部最关键的元数据区域

### 2.7 `src/io/`

- 抽象底层写入目标
- 当前主要有：
  - 本地缓冲写入
  - 原子写入（临时文件 + rename）
  - HDFS
  - io_uring

### 2.8 `src/codec/`

- 对压缩算法做统一封装
- 支持 `None / LZ4 / Zstd / Snappy / GZip`

### 2.9 `src/checksum/`

- 负责 CRC32C
- 用于 HFile block 的 chunk checksum

### 2.10 `src/partition/`

- 只服务 Bulk Load
- 负责：
  - 列族合法性检查
  - row key 到 Region 的映射

### 2.11 `src/convert/`

- 负责 **Arrow IPC Stream 文件 → HFile 文件**
- 是工具链 `arrow-to-hfile` 和 JNI `convert()` 的真正核心

### 2.12 `src/jni/`

- Java 与 C++ 的桥接层
- 暴露 native 方法给 `com.hfile.HFileSDK`

### 2.13 `src/memory/`

- 负责内存预算与一些潜在的内存优化基础设施

### 2.14 `src/metrics/`

- 轻量指标系统
- 当前接入较浅，更偏基础设施储备

---

## 3. 你需要先认识的几个核心数据对象

如果你对 C++ 不熟悉，可以先只盯住这些对象：

### 3.1 `KeyValue`

定义在公开头文件 `include/hfile/types.h` 中。

可以把它理解成 HBase 单元格：

- row
- family
- qualifier
- timestamp
- type
- value
- tags
- memstore ts

后面不管是 Arrow 转换、Bulk Load，还是最终写 block，本质上都在围绕 `KeyValue` 做事情。

### 3.2 `WriterOptions`

定义在 `include/hfile/writer_options.h`。

它描述写 HFile 的各种参数，例如：

- block size
- 压缩算法
- 编码方式
- Bloom 类型
- fsync 策略
- 内存上限

### 3.3 `ConvertOptions`

定义在 `src/convert/convert_options.h`。

这是 `Arrow IPC Stream -> HFile` 的专用参数对象，包括：

- Arrow 输入路径
- HFile 输出路径
- rowKeyRule
- 列族
- 列排除规则
- 写入选项

### 3.4 `ConvertResult`

也是 `convert_options.h` 中定义的结果对象。

用于承载：

- 写了多少行
- 生成多少 KV
- 跳过多少条
- 是否有重复 row key
- 最终错误码与错误消息

---

## 4. 最重要的 3 条业务主链路

---

## 4.1 单 HFile 写入链路

### 入口

- 公开 API：`HFileWriter`
- 实现文件：`src/writer.cc`

### 你可以把它理解成

> 给我一串已经准备好的 `KeyValue`，我把它们写成一个符合 HBase/HFile v3 规范的文件。

### 主流程

#### 第 1 步：构建 writer

调用方通过 Builder 配好：

- 输出路径
- 列族
- block size
- compression
- encoding
- bloom
- fsync 策略

然后 `build()`。

#### 第 2 步：`open()`

`writer.cc` 里的 `open()` 会初始化：

- 底层文件写入后端
- DataBlockEncoder
- Compressor
- Bloom Writer
- MemoryBudget

这一步结束后，writer 进入可追加状态。

#### 第 3 步：`append()`

每收到一个 `KeyValue`：

1. 做参数与顺序校验
2. 校验列族是否匹配
3. 根据 `SortMode` 决定：
   - 直接写
   - 或先缓存，finish 时再排序

实际落到内部是 `append_materialized_kv()`。

#### 第 4 步：Data Block 编码

内部 encoder 持续把 KV 编码进当前 block。

一旦当前 block 接近上限，就触发 `flush_data_block()`：

1. encoder 导出 block payload
2. compressor 压缩
3. 生成 block header
4. 计算 checksum
5. 写入文件
6. 把该 block 的首 key / offset / size 记入 index writer

#### 第 5 步：`finish()`

这是 HFile 成型的关键阶段。

它会按顺序写：

1. 最后一个 data block
2. Bloom data blocks（如果开启）
3. Data block index
4. Meta root index
5. FileInfo
6. Bloom meta block（如果开启）
7. Trailer
8. commit / close

### 为什么 `writer.cc` 最重要

因为 HFile 的物理格式几乎都在这里被“串起来”。

你可以把其他模块看成：

- block 编码器：负责 data block payload
- bloom：负责 bloom payload
- index：负责索引 payload
- meta：负责 file info / trailer payload

而真正把这些 payload 按 HFile 规定顺序组装到一个文件里的，是 `writer.cc`。

---

## 4.2 Arrow 文件转 HFile 链路

### 入口

- `src/convert/converter.cc`
- 暴露函数：`convert(const ConvertOptions&)`

### 你可以把它理解成

> 给我一个 Arrow IPC Stream 文件，我帮你把它变成一个排序正确、列结构符合规则的 HFile。

### 为什么这个流程是“两遍扫描”

因为 HFile 中写入的 KV 必须按 HBase key 顺序组织。

而输入 Arrow 文件不一定天然有序，所以系统采取两阶段：

1. 第一遍只负责“生成 row key + 收集索引 + 排序”
2. 第二遍再按排序后的顺序真正写 HFile

### 主流程

#### 第 1 步：参数校验

校验：

- Arrow 文件是否存在
- HFile 输出路径是否合法
- rowKeyRule 是否可编译
- 列排除配置是否合法

#### 第 2 步：编译 rowKeyRule

调用 `RowKeyBuilder::compile()`。

rowKeyRule 类似：

```text
REFID,0,true,0,LEFT,,,
```

它会被编译成若干 segment，每个 segment 描述：

- 从哪一列取值
- 是否 reverse
- pad 长度
- pad 方向
- 是否要做 `long()/short()/hash()` 编码

#### 第 3 步：列排除

在 `converter.cc` 里会先算出需要删除的列：

- 按列名排除
- 按前缀排除

这样 `_hoodie_*` 一类元数据列就能在输出 HFile 时去掉。

#### 第 4 步：第一遍扫描 `build_sort_index()`

这一步做的事很多：

1. 打开 Arrow IPC Stream reader
2. 读取 schema
3. 对 rowKeyRule 中引用到的列做类型校验
4. 逐个 batch 读取
5. 对每一行：
   - 提取 rowKeyRule 需要的字段
   - 生成 row key
   - 记录 `(row_key, batch_idx, row_idx)` 到 `SortEntry`

最后得到：

- 全部 `RecordBatch` 缓存
- 全部 `SortEntry`

#### 第 5 步：排序

对 `SortEntry` 做稳定排序。

排序键是 row key。

这样就得到最终写 HFile 的顺序。

#### 第 6 步：第二遍回放写入

按排序后的 `SortEntry` 顺序回放：

1. 找到对应 batch 和行
2. 把这一行的列变成若干 cell
3. qualifier/value 组装成 `GroupedCell`
4. 同一 row key 的 cell 按 qualifier 排序
5. 调 `HFileWriter::append()` 写入

#### 第 7 步：处理重复 row key

如果多个 Arrow 行生成了同一个 row key：

- 只保留第一组 cell
- 其他计入 `duplicate_key_count`

这是为了保证最终 HFile 的 key 顺序和唯一性策略稳定。

### 这个链路里最重要的辅助模块

- `src/arrow/row_key_builder.*`
- `src/arrow/arrow_to_kv_converter.*`
- `src/writer.cc`

---

## 4.3 Bulk Load 链路

### 入口

- `src/bulk_load_writer.cc`

### 你可以把它理解成

> 给我一批 Arrow 数据，我不只写一个 HFile，而是根据 row key 所属 Region、列族等规则，把它们拆到多个目标 HFile 中。

### 主流程

#### 第 1 步：Arrow batch 转 KV

`write_batch()` 会先使用 `ArrowToKVConverter` 把 Arrow `RecordBatch` 转成 `KeyValue`。

#### 第 2 步：按 `(cf, region)` 路由

每条 KV 会：

1. 校验列族
2. 用 `RegionPartitioner` 计算它属于哪个 Region
3. 形成 `(column family, region)` 的组合键

#### 第 3 步：定位或创建目标 writer

对于每个 `(cf, region)`：

- 如果已有打开的 `HFileWriter`，直接复用
- 如果没有，就新建一个

#### 第 4 步：控制打开文件数量

如果当前打开的 writer 太多：

- 选择最久未使用的 writer
- 先 `finish()`
- 再释放
- 然后为新的 `(cf, region)` 打开 writer

这是一种典型的 LRU writer 管理策略。

#### 第 5 步：最终 `finish()`

关闭所有活跃 writer，并返回：

- 成功输出的文件列表
- 失败文件列表
- 耗时与统计信息

### Bulk Load 与单文件 convert 的区别

- `convert()`：始终只输出一个 HFile
- `BulkLoadWriter`：输出很多 HFile，按 Region / CF 划分

---

## 5. `src/arrow/`：Arrow 到 KV/RowKey 的映射层

---

## 5.1 `row_key_builder.*`

这是理解 `rowKeyRule` 的关键。

### 作用

把用户提供的规则字符串编译成可执行结构，再按每一行字段生成 row key。

### 主要职责

#### `compile()`

做“规则编译”：

- 解析 segment
- 识别段类型：
  - 普通列引用
  - 编码列
  - 随机列
  - 填充列
- 解析 pad/reverse
- 校验非法配置

#### `build_checked()`

做“规则执行”：

- 逐段取值
- 必要时做编码
- 应用 pad/reverse
- 拼出最终字符串

### 为什么这个模块重要

因为最终 HFile 的排序和去重都依赖 row key。

换句话说，`rowKeyRule` 一旦理解错，后面所有行为都会偏。

### 对 C++ 初学者的阅读建议

先看：

1. `RowKeySegment` 这个结构表达了什么
2. `compile()` 如何把文本规则变成 segment 列表
3. `build_checked()` 如何遍历这些 segment

---

## 5.2 `arrow_to_kv_converter.*`

### 作用

把 Arrow 里的行列值转换成 HBase `KeyValue`。

### 三种模式

#### WideTable

特点：

- 一行 Arrow 对应多个 HBase cell
- 通常有一个 row key 列
- 其余列都展开成 qualifier/value

#### TallTable

特点：

- 一行 Arrow 就是一条完整 KV
- Arrow 里显式有：
  - row_key
  - cf
  - qualifier
  - timestamp
  - value

#### RawKV

特点：

- 输入列里已经带了 HBase 原始 key bytes
- 这里负责把它再拆回 row/family/qualifier/timestamp/type

### 关键函数

#### `serialize_scalar_checked()`

它把 Arrow 单元格转换成字节。

支持：

- 字符串
- 二进制
- 数值
- timestamp

这是整个 Arrow 转换层最底层的“类型桥接点”。

### 阅读建议

如果你想理解“为什么某一列会变成某个 qualifier/value”，就从这里看起。

---

## 6. `src/block/`：Data Block 编码层

HFile 并不是简单地把 KeyValue 一条条裸写进去，而是先聚合成 block，再编码。

### 统一抽象：`DataBlockEncoder`

它定义了所有编码器共同的接口：

- `append()`
- `finish_block()`
- `reset()`
- `first_key()`
- `current_size()`
- `num_kvs()`

### 四种编码器

#### `NoneEncoder`

- 不做高级编码
- 最容易理解
- 最适合作为阅读起点

#### `PrefixEncoder`

- 对相邻 key 的公共前缀做压缩

#### `DiffEncoder`

- 在 Prefix 基础上继续压缩差分信息

#### `FastDiffEncoder`

- 面向性能优化的差分编码器
- 当前更接近生产默认选择

### 推荐阅读顺序

1. `none_encoder.h`
2. `prefix_encoder.h`
3. `diff_encoder.h`
4. `fast_diff_encoder.h`
5. `block_builder.cc` 看工厂如何选择

---

## 7. `src/bloom/`：Bloom Filter

### 作用

帮助读取方快速判断某个 row / row+col 是否“可能存在”。

### 为什么叫 Compound Bloom

不是一个大 Bloom bitset 直接落盘，而是：

- 分 chunk 构建
- 每个 chunk 形成一个独立 block
- 再用 meta block 描述这些 chunk

### 核心职责

- `add()` / `add_row_col()`
  - 在写入每个 cell 时灌入 Bloom
- `finish_chunk()`
  - 封口当前 chunk
- `finish_data_blocks()`
  - 把所有 Bloom data blocks 写到文件的 non-scanned section
- `finish_meta_block()`
  - 生成 Bloom meta block

### 为什么这里容易出兼容性问题

因为它涉及：

- chunk block header
- meta block 布局
- meta root index entry 编码

任何长度字段或偏移字段有误，HBase Reader 读 load-on-open 区时都可能直接报错。

---

## 8. `src/index/`：Block Index

### 作用

记录每个 data block：

- 首 key
- offset
- block size

这样读取方可以定位 block。

### `BlockIndexWriter`

职责包括：

- 收集 data block 条目
- 决定是否需要 intermediate index
- 产出 root index payload

### 你需要记住

HFile 读取失败时，如果报：

- data index
- meta index
- load-on-open

这类错误通常都和 `index/`、`bloom/`、`meta/`、`writer.cc` 的组装逻辑有关。

---

## 9. `src/meta/`：FileInfo 与 Trailer

### 9.1 `FileInfoBuilder`

负责构建 HFile FileInfo 字典，保存：

- `LASTKEY`
- 平均 key 长度
- 平均 value 长度
- comparator
- block encoding
- 其他关键元字段

### 9.2 `TrailerBuilder`

负责生成 HFile 最后固定长度的 trailer。

Trailer 会告诉读取方：

- 版本
- load-on-open offset
- file info offset
- meta index count
- data index count
- comparator
- 其他汇总信息

### 为什么 Trailer 很关键

因为读取方通常先从文件尾部反推整个文件结构。

如果 trailer 偏了，整个文件都会被当作损坏。

---

## 10. `src/io/`：底层 I/O 后端

---

## 10.1 `BufferedFileWriter`

### 作用

最常用的本地文件写入实现。

### 特点

- 先写应用层 buffer
- 再落到 `FILE*`
- 最后 `fflush + fsync`

### 使用场景

- 本地磁盘普通写入
- 默认后端

---

## 10.2 `AtomicFileWriter`

### 作用

实现安全提交：

1. 先写到临时文件
2. 确保内容落盘
3. rename 到目标文件
4. 再刷目录

### 意义

防止程序中途失败时留下半截 HFile。

---

## 10.3 `HdfsWriter`

### 作用

把输出直接写到 HDFS。

### 现状

属于可选后端，不是默认路径。

---

## 10.4 `IoUringWriter`

### 作用

Linux 下尝试用 io_uring 做更高性能的异步写。

### 现状

也是可选实现，不是默认主路径。

---

## 11. `src/codec/` 与 `src/checksum/`

这两个目录比较纯粹，都是底层支撑能力。

### `codec/compressor.*`

负责把压缩算法封装成统一接口。

写入 block 时，`writer.cc` 不关心“底层具体是 LZ4 还是 Zstd”，只关心：

- 给我 raw bytes
- 还你 compressed bytes

### `checksum/crc32c.*`

负责 HFile block 的 CRC32C。

当前实现会优先使用硬件加速，否则走软件 fallback。

---

## 12. `src/partition/`：Bulk Load 路由层

### `CFGrouper`

负责：

- 注册允许的列族
- 校验某个 KV 的 family 是否合法

### `RegionPartitioner`

负责：

- 根据 row key 判断该条 KV 属于哪个 Region

Bulk Load 写多个 HFile 时，路由逻辑的核心就是：

```text
(family, region) -> 目标 HFileWriter
```

---

## 13. `src/jni/`：Java 与 C++ 的连接点

如果你以后主要从 Java 看这个项目，那么这里非常重要。

### `hfile_jni.cc`

暴露 3 个关键 native 方法：

- `convert`
- `configure`
- `getLastResult`

### `configure`

把 Java 端 JSON 配置解析成 C++ `WriterOptions` 和其他配置项。

### `convert`

把 Java 传进来的参数转成 `ConvertOptions`，然后直接调用：

- `src/convert/converter.cc` 里的 `convert()`

### `getLastResult`

把最近一次转换结果作为 JSON 返还给 Java。

### `json_utils.h`

这是一个“够用就行”的轻量 JSON 工具。

它并不是通用 JSON 库，而是只解析本项目配置里需要的几种结构：

- string
- integer
- string array

这样做的目的是避免引入更大的第三方依赖。

---

## 14. `src/memory/` 与 `src/metrics/`

这两个目录更偏“生产化能力”。

### `MemoryBudget`

这是现在真正已经被主链路使用的部分。

它负责：

- 记录预算上限
- 扣减/释放用量
- 预算超限时报错

写入和转换都已经在用。

### `ArenaAllocator` / `BlockPool`

更偏性能优化储备设施。

可以把它们理解成：

- 未来如果要减少频繁分配释放，就可以把对象/内存块复用起来

### `MetricsRegistry`

给计数器、直方图、gauge 做统一注册与输出。

当前接入较少，但设计上已经预留了性能指标采集能力。

---

## 15. 新手最推荐的阅读顺序

如果你不熟 C++，不要按文件树顺序读，建议按下面顺序。

### 第一阶段：先理解主流程

1. `include/hfile/writer.h`
2. `src/writer.cc`
3. `src/convert/converter.h`
4. `src/convert/converter.cc`
5. `src/arrow/row_key_builder.h`
6. `src/arrow/row_key_builder.cc`

### 第二阶段：理解数据如何被编码进 HFile

1. `src/block/data_block_encoder.h`
2. `src/block/none_encoder.h`
3. `src/block/prefix_encoder.h`
4. `src/index/block_index_writer.h`
5. `src/meta/file_info_builder.h`
6. `src/meta/trailer_builder.h`

### 第三阶段：理解高级特性

1. `src/bloom/compound_bloom_filter_writer.h`
2. `src/bulk_load_writer.cc`
3. `src/partition/cf_grouper.h`
4. `src/partition/region_partitioner.cc`
5. `src/jni/hfile_jni.cc`

### 第四阶段：再看优化和平台能力

1. `src/io/*`
2. `src/codec/*`
3. `src/checksum/*`
4. `src/memory/*`
5. `src/metrics/*`

---

## 16. 常见调试入口

### 16.1 HFile 文件读不出来

优先看：

1. `src/writer.cc`
2. `src/index/block_index_writer.*`
3. `src/bloom/compound_bloom_filter_writer.*`
4. `src/meta/trailer_builder.*`

### 16.2 Arrow 转换结果不对

优先看：

1. `src/convert/converter.cc`
2. `src/arrow/row_key_builder.*`
3. `src/arrow/arrow_to_kv_converter.*`

### 16.3 Bulk Load 生成文件数量/分区异常

优先看：

1. `src/bulk_load_writer.cc`
2. `src/partition/region_partitioner.cc`
3. `src/partition/cf_grouper.*`

### 16.4 Java 侧调用异常

优先看：

1. `src/jni/hfile_jni.cc`
2. `src/jni/json_utils.h`
3. `src/convert/converter.cc`

---

## 17. `src/` 一句话文件地图

为了更像“代码地图”，这里给一个压缩版索引。

### 顶层

- `writer.cc`：单 HFile 写入总控
- `bulk_load_writer.cc`：多 HFile 输出总控

### `arrow/`

- `arrow_to_kv_converter.*`：Arrow 行列转 KeyValue
- `row_key_builder.*`：rowKeyRule 编译和执行

### `block/`

- `data_block_encoder.h`：编码器统一接口
- `none/prefix/diff/fast_diff`：具体 block 编码实现
- `block_builder.cc`：编码器工厂

### `bloom/`

- `compound_bloom_filter_writer.*`：HBase 兼容 Bloom 结构

### `index/`

- `block_index_writer.*`：data/meta/root index 组织

### `meta/`

- `file_info_builder.*`：FileInfo
- `trailer_builder.*`：Trailer

### `codec/`

- `compressor.*`：压缩封装

### `checksum/`

- `crc32c.*`：CRC32C

### `io/`

- `buffered_writer.*`：默认本地写入
- `atomic_file_writer.*`：安全提交
- `hdfs_writer.*`：HDFS 后端
- `iouring_writer.*`：Linux io_uring 后端

### `partition/`

- `cf_grouper.*`：列族管理
- `region_partitioner.cc`：Region 路由

### `convert/`

- `convert_options.h`：转换参数/结果/错误码
- `converter.*`：Arrow 文件转 HFile 的完整实现

### `jni/`

- `hfile_jni.cc`：Java native 入口
- `jni_utils.h`：JNI 字符串/异常工具
- `json_utils.h`：轻量 JSON 解析

### `memory/`

- `memory_budget.*`：内存预算
- `arena_allocator.*`：Arena 分配器
- `block_pool.*`：块复用池

### `metrics/`

- `metrics_registry.*`：指标注册与聚合

---

## 18. 最后的理解建议

如果你现在只想“先能读懂 70%”，请抓住下面三句话：

1. **`writer.cc` 负责把一个个 `KeyValue` 组织成真正的 HFile 文件布局**
2. **`converter.cc` 负责把 Arrow 文件整理成适合写入 HFile 的顺序和单元格**
3. **`bulk_load_writer.cc` 负责在多 Region / 多列族场景下调度多个 `HFileWriter`**

只要先把这三条线看懂，其他模块就都能各归其位。
