# writer.cc 深度拆解

这份文档专门解释 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc)。

如果说整个项目里有一个文件最值得花时间看，那通常就是它。原因很简单：

- `converter.cc` 决定“写什么”
- `row_key_builder.cc` 决定“按什么 key 排序”
- **`writer.cc` 决定“最终文件怎么落盘”**

也就是说，`writer.cc` 是 HFile 物理布局的总装配车间。

这份文档重点回答：

- `writer.cc` 的职责边界是什么
- 内部维护了哪些状态
- 一条 `KeyValue` 如何进入 block
- 一个 block 如何变成真正 HFile block
- `finish()` 如何把 Bloom、Index、FileInfo、Trailer 串起来

---

## 1. 先给它一句话定义

`writer.cc` 的职责是：

> 把一系列已经准备好的、顺序正确的 `KeyValue`，编码、压缩、加校验、挂索引、补元信息，最终写成一个 HBase 可读的 HFile v3 文件。

它不是：

- Arrow 解析器
- rowKey 生成器
- Bulk Load 路由器

它只关心：

- 我收到什么 KV
- 我如何把它们组织成符合 HFile 规范的文件

---

## 2. 整体结构先看懂

### 主要对外类

- 公开 API： [HFileWriter](file:///Users/gauss/workspace/github_project/HFileSDK/include/hfile/writer.h)
- 真正实现类：`HFileWriterImpl`

### 文件结构可以分成 6 段

1. 工具函数
2. `HFileWriterImpl` 生命周期
3. `append()` 追加 KV
4. block flush / raw block 写入
5. `finish()` 收尾装配整份 HFile
6. `HFileWriter::Builder`

### 你读这个文件时建议按这个顺序

1. 看 `HFileWriterImpl` 里的成员变量
2. 看 `open()`
3. 看 `append()`
4. 看 `append_materialized_kv()`
5. 看 `flush_data_block()`
6. 看 `write_data_block()`
7. 看 `finish()`
8. 最后看 `build_file_info_block()` 和 `write_trailer()`

---

## 3. 文件开头的辅助函数在干什么

先不要跳过顶部这些“看起来像工具函数”的代码，因为它们其实直接参与主流程。

### 3.1 `free_disk_bytes()`

位置：[writer.cc:L41-L49](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L41-L49)

作用：

- 查询目标路径所在磁盘的剩余空间

为什么需要它：

- `writer.cc` 支持“最小剩余磁盘空间保护”
- 如果可用空间低于阈值，会停止写入并返回错误

这属于生产安全保护逻辑。

---

### 3.2 `validate_kv()`

位置：[writer.cc:L53-L68](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L53-L68)

作用：

- 校验每条 `KeyValue` 是否满足写入要求

主要检查：

- row key 不能为空
- row key 不能太长
- value 不能太大
- timestamp 不能为负

### 这一步的重要意义

它是“输入防线”的第一层。

如果这一步没过，后面根本不会进入 block 编码。

---

### 3.3 `sanitize_owned_kv()`

位置：[writer.cc:L70-L83](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L70-L83)

作用：

- 把只读 view 型 `KeyValue` 复制成拥有内存所有权的 `OwnedKeyValue`

为什么要这样做：

- 上层传进来的 `row/family/value` 可能只是 span/view
- 如果 `AutoSort` 要暂存很多 KV，就必须复制到自己管理的内存里

可以把它理解成：

> 把“借来的数据”变成“自己持有的数据”

---

### 3.4 `estimate_owned_kv_bytes()`

位置：[writer.cc:L85-L92](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L85-L92)

作用：

- 估算一条 `OwnedKeyValue` 占用多少内存

它主要服务：

- `AutoSort`
- 内存预算控制

---

### 3.5 `hbase_compression_codec()`

位置：[writer.cc:L94-L103](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L94-L103)

作用：

- 把项目内部的压缩枚举映射成 HBase Trailer 里用的 codec 编号

也就是说：

- 压缩算法不仅要真的用于压缩 block
- 还要在 Trailer 中“告诉读取方我用了什么压缩”

---

## 4. `HFileWriterImpl` 到底维护了什么状态

`HFileWriterImpl` 的成员很多，第一次看容易乱。更好的方式是按职责分组。

位置： [writer.cc:L655-L701](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L655-L701)

### 4.1 配置与目标

- `path_`
- `opts_`

它们决定：

- 写到哪个文件
- 使用什么压缩 / 编码 / Bloom / 策略

### 4.2 当前活跃组件

- `writer_`
- `atomic_writer_`
- `plain_writer_`
- `encoder_`
- `compressor_`
- `bloom_`
- `index_writer_`

你可以把它们理解成一条生产线上的工位：

- 文件工位
- 编码工位
- 压缩工位
- Bloom 工位
- 索引工位

### 4.3 当前运行中缓存

- `compress_buf_`
- `last_key_`
- `first_key_`
- `last_kv_`
- `auto_sorted_kvs_`

这些变量用于：

- 暂存压缩输出
- 记录首尾 key
- 校验排序
- 支持 AutoSort

### 4.4 资源管理

- `budget_`
- `fixed_budget_bytes_`
- `auto_sort_reserved_bytes_`
- `bytes_since_disk_check_`

这些属于“运行时约束”。

### 4.5 统计与元信息

- `entry_count_`
- `written_entry_count_`
- `skipped_rows_`
- `error_count_`
- `total_key_bytes_`
- `total_value_bytes_`
- `total_uncompressed_bytes_`
- `data_block_count_`
- `max_tags_len_`
- `max_memstore_ts_`
- `has_mvcc_cells_`
- `max_cell_size_`
- `first_data_block_offset_`
- `last_data_block_offset_`
- `prev_block_offset_`

这一组变量决定：

- FileInfo 里要写什么
- Trailer 里要写什么
- 下一个 block header 应该如何引用前一个 block

### 4.6 生命周期状态

- `start_time_`
- `opened_`
- `finished_`

这些帮助 writer 判断：

- 是否已打开
- 是否已经成功完成
- 析构时是否应删除半成品文件

---

## 5. `open()`：真正开始写文件前做了什么

位置：[writer.cc:L130-L170](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L130-L170)

### 5.1 建目录

如果输出路径有父目录，就先确保目录存在。

### 5.2 选择 I/O 后端

逻辑在 [writer.cc:L144-L152](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L144-L152)。

如果 `fsync_policy == Safe`：

- 用 `AtomicFileWriter`

否则：

- 用普通 `BlockWriter`

### 这里体现的设计思想

`writer.cc` 并不直接关心：

- 文件句柄怎么打开
- 临时文件怎么提交
- 是否走 rename

它只依赖统一的写入接口。

---

### 5.3 初始化编码器、压缩器、Bloom

逻辑在 [writer.cc:L154-L159](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L154-L159)。

这一步相当于把整条生产线搭起来：

- `encoder_`：负责 data block payload
- `compressor_`：负责压缩
- `bloom_`：负责 Bloom

---

### 5.4 准备压缩缓冲区和内存预算

逻辑在 [writer.cc:L160-L165](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L160-L165)。

为什么需要 `compress_buf_`：

- block 编码后可能需要压缩
- 压缩器需要一个足够大的输出缓冲区

为什么要把它计入 `MemoryBudget`：

- 它是长期驻留的大块内存

---

## 6. `append()`：一条 KV 进入 writer 时发生了什么

位置：[writer.cc:L173-L216](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L173-L216)

### 6.1 首先检查 finish 状态

如果已经 `finish()`，就拒绝继续写。

### 6.2 做输入校验

调用 `validate_kv()`。

如果失败：

- 计 error
- 触发 error callback
- 计 skipped rows
- 按 `ErrorPolicy` 决定：
  - `Strict`：直接返回错误
  - `SkipBatch`：返回特殊错误，让上层整批放弃
  - `SkipRow`：吞掉错误，继续

### 这里很重要

`writer.cc` 不只是“写文件”，它还承担了写入过程中的容错策略执行。

---

### 6.3 校验列族

当前 `HFileWriter` 只写一个列族。

所以每条 KV 的 family 必须和配置一致。

否则直接报错。

---

### 6.4 判断是否 AutoSort

如果是 `AutoSort`：

- 先把 KV 复制成 `OwnedKeyValue`
- 放进 `auto_sorted_kvs_`
- 暂时不写盘

否则：

- 直接走 `append_materialized_kv()`

### 这一步的意义

`AutoSort` 模式下，writer 自己负责最终排序；
而 `PreSortedVerified` 模式下，调用方必须已经保证顺序正确。

---

## 7. `append_materialized_kv()`：真正把 KV 放进当前 block

位置：[writer.cc:L601-L631](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L601-L631)

这是整个写入过程最关键的“热路径函数”之一。

### 它按顺序做这些事

#### 7.1 排序校验

如果 `verify_sort=true`：

- 用 `compare_keys()` 比较当前 KV 与上一个 KV
- 如果不是严格递增，报 `SORT_ORDER_VIOLATION`

### 为什么重要

HFile 读取严重依赖 key 顺序。

所以这里是 HFile 有序性保证的最后一道防线。

---

#### 7.2 磁盘空间检查

调用 `maybe_check_disk_space()`。

它不是每条 KV 都查，而是累计到一定阈值后才查一次，避免系统调用太频繁。

---

#### 7.3 尝试把 KV 追加进当前 encoder

如果当前 block 还放得下：

- `encoder_->append(kv)` 成功

如果放不下：

1. 先 `flush_data_block()`
2. 清掉当前 block
3. 再重试追加当前 KV

### 这就是 Data Block 的“滚动”机制

---

#### 7.4 更新 Bloom

逻辑在 [writer.cc:L620-L623](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L620-L623)。

如果 Bloom 开启：

- 记录 row
- 如果是 `RowCol`，还会记录 `(row, qualifier)`

---

#### 7.5 更新统计信息

包括：

- 平均 key/value 长度
- 最大 tags
- 最大 cell
- 最大 memstore ts

这些信息将来要写到 FileInfo 里。

---

#### 7.6 维护 first/last key

- 第一条 KV：记 `first_key_`
- 每条 KV：更新 `last_key_`

它们后面会被：

- block index
- FileInfo
- Trailer

使用。

---

## 8. `flush_data_block()`：当前 block 满了以后发生什么

位置：[writer.cc:L372-L405](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L372-L405)

### 这是一个标准的数据块收口流程

#### 第 1 步：从 encoder 取 raw block

```text
encoder -> finish_block()
```

如果为空，直接 reset 返回。

#### 第 2 步：记录当前文件位置

这就是此 block 的 offset。

#### 第 3 步：封 Bloom chunk

这一点容易忽略。

每个 data block 完成时，也意味着对应 Bloom chunk 可以封口。

#### 第 4 步：压缩

调用 `compressor_->compress(...)`。

如果配置是 `Compression::None`，就直接使用 raw bytes。

#### 第 5 步：写 data block

交给 `write_data_block(...)`。

#### 第 6 步：更新统计

- `total_uncompressed_bytes_`
- `data_block_count_`

#### 第 7 步：必要时 flush

如果是 `Paranoid` 模式，并且达到 block flush 间隔，就强制落盘。

#### 第 8 步：reset encoder

准备接收下一个 block。

---

## 9. `write_data_block()`：真正 HFile Data Block 的格式装配

位置：[writer.cc:L408-L454](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L408-L454)

这是 HFile 物理格式细节最密集的地方之一。

### 它做了 4 件关键事

#### 9.1 计算 checksum 区大小

根据：

- header + compressed payload 总长度
- `bytes_per_checksum`

推导需要多少个 checksum chunk。

#### 9.2 组装 33 字节 block header

header 里包括：

- block magic
- on-disk size
- uncompressed size
- prev block offset
- checksum 类型
- bytes per checksum
- on-disk-data-with-header

### 这里非常关键

HBase 读取 HFile block 时，就是靠这些字段知道：

- 当前 block 多大
- 前一个 block 在哪
- 怎样校验 checksum

#### 9.3 计算 checksum

先对：

- header
- payload

组合出的 block 数据做 CRC32C，再得到 checksum 区。

#### 9.4 写盘并更新 index

写入顺序是：

1. header
2. compressed payload
3. checksum bytes

随后把：

- `first_key`
- `offset`
- `size`

登记到 `index_writer_`。

---

## 10. `write_raw_block()` / `build_raw_block()`：索引和元块是怎么写的

位置：

- [write_raw_block](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L457-L464)
- [build_raw_block](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L466-L495)

### 为什么要有这一组函数

Data Block 有自己的写法；
而其他块，比如：

- Root Index
- Meta Root
- FileInfo

虽然 payload 内容不同，但 block 包装方式其实很像。

所以这里做了一个通用“raw block 封装器”。

### 这组函数的意义

让：

- 索引块
- 元信息块
- Bloom meta block

也能走统一的：

- header
- checksum
- prev block offset

流程。

---

## 11. `build_file_info_block()`：FileInfo 是怎么来的

位置：[writer.cc:L497-L518](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L497-L518)

### 它会从 writer 当前状态提炼出哪些信息

- `last_key`
- 平均 key 长度
- 平均 value 长度
- 最大 tags 长度
- memstore ts
- comparator
- data block encoding
- 创建时间
- 最大 cell 长度

### 为什么这些值要在这里才写

因为很多统计必须等到所有 KV 都写完后才能确定。

所以 FileInfo 天然是“收尾阶段”写出的元信息。

---

## 12. `write_trailer()`：文件尾巴怎么收口

位置：[writer.cc:L520-L546](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L520-L546)

Trailer 可以理解成：

> 这份 HFile 的总索引目录

### 它写进去的关键信息

- `file_info_offset`
- `load_on_open_offset`
- data index 大小
- data block 数量
- meta index 数量
- entry 总数
- 索引层级数
- first/last data block offset
- comparator
- compression codec

### 为什么读取方很依赖它

HBase 通常先看文件尾部 trailer，再倒推：

- load-on-open section 在哪里
- FileInfo 在哪里
- 要加载哪些索引

所以 trailer 出问题，读取方通常会直接认为 HFile 损坏。

---

## 13. `finish()`：整份 HFile 的最终装配过程

位置：[writer.cc:L218-L365](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L218-L365)

这是你最该反复看的函数。

它相当于一张 HFile 组装总流程图。

### 13.1 如果是 AutoSort，先排序并回放

逻辑在 [writer.cc:L228-L243](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L228-L243)。

### 做了什么

1. 对 `auto_sorted_kvs_` 按 key 排序
2. 逐条调用 `append_materialized_kv()`
3. 释放 AutoSort 占用的预算

### 为什么这一步在 finish 才做

因为 AutoSort 的设计就是：

- append 时只缓存
- finish 时统一排序写出

---

### 13.2 刷最后一个 data block

如果 encoder 里还有数据，就调用 `flush_data_block()`。

这是最后一个数据块收尾。

---

### 13.3 封最后一个 Bloom chunk

即便最后一个 chunk 不满，也要封口。

---

### 13.4 写 non-scanned block section

逻辑在 [writer.cc:L249-L265](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L249-L265)。

这里主要写 Bloom data blocks。

### 为什么叫 non-scanned

因为这些块不是正常扫描数据时逐块顺序遍历的数据区，而是辅助块。

---

### 13.5 开始写 load-on-open section

逻辑从 [writer.cc:L267](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L267) 开始。

这里是整个 HFile 收尾中最复杂的一段。

作者还专门在注释里标了 HBase load-on-open 区顺序：

1. Intermediate Index
2. Root Data Index
3. Meta Root Index
4. FileInfo
5. Bloom Meta Block

### 这段为什么难

因为这里不仅要写 payload，
还要算：

- offset
- size
- prev block offset
- trailer 引用位置

任何一个算错，HBase Reader 都可能读失败。

---

### 13.6 写 Data Index

调用：

- `index_writer_.finish(...)`

生成：

- intermediate index payload
- root index payload

然后写入文件。

### 你可以这样理解

Data Block 的条目是边写边登记的；
真正的索引块是在 finish 阶段统一生成。

---

### 13.7 写 Meta Root Index

这里目前主要是给 Bloom meta block 建入口。

逻辑在 [writer.cc:L293-L325](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L293-L325)。

### 为什么单独复杂

因为它要先“预估”后面 FileInfo 和 Bloom Meta Block 的位置，才能把偏移写对。

这类逻辑特别容易出现：

- offset 错
- size 错
- key length 编码错

也是 HBase 兼容性问题最常见的高风险区域之一。

---

### 13.8 写 FileInfo

位置在 [writer.cc:L335-L336](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L335-L336)。

这是把前面统计的各种信息真正写成块。

---

### 13.9 写 Bloom Meta Block

如果 Bloom 开启，就在 FileInfo 后面写 Bloom meta block。

---

### 13.10 写 Trailer

调用 `write_trailer()`。

Trailer 是整个文件最后的“总目录”。

---

### 13.11 commit / close

逻辑在 [writer.cc:L349-L355](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L349-L355)。

如果是 `AtomicFileWriter`：

- commit

否则：

- flush
- close

### 为什么 `finished_` 要最后才置 true

因为只有所有 I/O 都成功，文件才算真正完成。

这和析构函数的行为直接有关。

---

## 14. 析构函数为什么会删除部分文件

位置：[writer.cc:L115-L128](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L115-L128)

### 行为

如果：

- 已经 `open()`
- 但没有成功 `finish()`

析构时就会删除部分文件。

### 设计意图

防止上层误把“半成品 HFile”当成可用文件继续使用。

### 对使用者的意义

如果你在调试时发现“文件怎么没了”，先看是不是：

- `finish()` 失败
- 对象析构后自动清理掉了

---

## 15. AutoSort 模式单独说明

相关逻辑：

- `append()` 中分流
- `buffer_auto_sorted_kv()`： [writer.cc:L633-L653](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L633-L653)
- `finish()` 中回放排序结果

### 它的优点

- 调用方不必保证输入顺序

### 它的代价

- 要缓存全部 KV
- 内存占用高
- finish 时才统一排序和写盘

### 所以什么时候应该避免它

如果上层已经能保证有序，最好直接用 `PreSortedVerified`。

---

## 16. 统计字段最后分别流向哪里

这个点很容易混淆，所以单独整理一下。

### `entry_count_`

- 表示逻辑上成功接收的 KV 数
- 会进入 Trailer

### `written_entry_count_`

- 表示已经真正写入 encoder 的 KV 数
- 更多用于内部流程控制

### `first_key_` / `last_key_`

- 用于 block index / FileInfo

### `total_key_bytes_` / `total_value_bytes_`

- 用于算平均长度
- 最终写入 FileInfo

### `data_block_count_`

- 写入 Trailer

### `first_data_block_offset_` / `last_data_block_offset_`

- 写入 Trailer

### `prev_block_offset_`

- 写每一个 block header 时使用

---

## 17. 如果你要调试 `writer.cc`，推荐从哪里下手

### 场景 1：HFile 能生成，但 HBase 读失败

优先看：

1. `finish()`
2. `write_data_block()`
3. `build_raw_block()`
4. `write_trailer()`

因为这类问题通常不是“业务 KV 不对”，而是：

- block header
- offset
- size
- meta root
- trailer

有问题。

---

### 场景 2：写入时报排序错误

优先看：

- `append_materialized_kv()`
- `compare_keys(...)`

---

### 场景 3：内存占用过高

优先看：

- `AutoSort`
- `compress_buf_`
- `MemoryBudget`

---

### 场景 4：Bloom 相关读取失败

优先看：

- `finish()` 中 Bloom 部分
- `meta_root_payload`
- `bloom_->finish_data_blocks()`
- `bloom_->finish_meta_block()`

---

## 18. 对不熟 C++ 的读者，如何真正看懂这个文件

建议你不要一次读完整个 `writer.cc`。

### 第一次阅读

只看：

- `open()`
- `append()`
- `finish()`

目的是建立主流程。

### 第二次阅读

再看：

- `append_materialized_kv()`
- `flush_data_block()`
- `write_data_block()`

目的是看懂 data block。

### 第三次阅读

最后看：

- `build_raw_block()`
- `build_file_info_block()`
- `write_trailer()`

目的是理解文件尾部结构。

---

## 19. 一句话总结 `writer.cc`

如果要把 `writer.cc` 用一句话讲清楚：

> 它是整个项目里负责把“逻辑上的 HBase cell 序列”变成“物理上可被 HBase Reader 正确解析的 HFile 文件”的最终执行者。

所以你后面无论研究：

- HFile 兼容性
- block 编码
- Bloom 布局
- trailer 问题

几乎都绕不开它。
