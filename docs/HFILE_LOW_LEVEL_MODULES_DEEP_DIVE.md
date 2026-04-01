# HFile 底层组成模块深度拆解

这份文档不再从“Arrow 怎么转 HFile”出发，而是专门往更底层走，回答一个更核心的问题：

> 这个项目到底是怎样把一串有序的 Cell，组织成一个能被 HBase 正确读取的 HFile v3 文件？

如果把整个写文件过程抽象成一条装配线，可以先看这张总图：

1. 上层把已经排好序的 `KeyValue` 交给 writer。
2. `block/` 决定“当前 data block 里每条 cell 怎么编码、怎么累计”。
3. data block flush 时，把这个 block 的首 key、offset、大小登记给 `index/`。
4. 同时把 row 或 row+qualifier 喂给 `bloom/`，按 chunk 形成 Bloom 数据块。
5. 所有 data block 写完后，再统一写 Bloom chunk blocks。
6. 然后写 load-on-open 区域：intermediate index、root index、meta root、file info、bloom meta。
7. 最后由 `trailer_builder` 写固定 4096 字节 trailer，告诉 HBase 上述关键区域都在文件的什么位置。

整条主链路都汇总在 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L218-L347)。

---

## 1. 先建立整体心智模型

可以把一个 HFile 拆成五层：

- **Cell 层**：单条 `KeyValue` 的 HBase 二进制表示，由 [data_block_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/data_block_encoder.h#L13-L68) 里的 `serialize_kv()` 和 `serialize_key()` 定义。
- **Data Block 层**：很多条 Cell 被编码后装进一个 block，由 `block/` 负责。
- **辅助检索层**：为了让 HBase 不必扫描全文件，使用 `index/` 和 `bloom/` 提供“先定位、再读取”的能力。
- **元信息层**：`meta/` 负责补充 FileInfo 和 Trailer，让 Reader 知道比较器、编码方式、索引偏移、数据总量等。
- **文件装配层**：`writer.cc` 负责决定这些模块的写入顺序、相对偏移和最终物理布局。

其中最重要的一点是：

> `block/`、`bloom/`、`index/`、`meta/` 不是四个互不相干的模块，它们都只是 `writer.cc` 这台“总装机器”上的不同工位。

`open()` 时，writer 就把三大关键子模块接起来了：

- 创建 DataBlockEncoder：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L154-L155)
- 创建 Compressor：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L156)
- 创建 CompoundBloomFilterWriter：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L157-L158)

因此理解低层 HFile 组成的最佳方式，不是分别孤立地看这几个目录，而是始终带着这个问题：

> 当前这个模块给 `writer.cc` 提供了什么输入、输出和时序约束？

---

## 2. `block/` 编码器深度拆解

### 2.1 它解决的底层问题是什么

HBase 读写的最小逻辑单元是 Cell，但磁盘 I/O 的最小高效单位通常不是“单条 cell”，而是一批 cell 组成的 block。

所以 `block/` 要做两件事：

- 决定单条 Cell 如何落成 HFile v3 的线格式
- 决定一个 data block 内多条 Cell 如何压缩、如何累计，直到 block 满为止

这里的抽象边界非常清晰：

- `serialize_kv()` 负责“完整 Cell”序列化：[data_block_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/data_block_encoder.h#L15-L54)
- `serialize_key()` 负责“只序列化 internal key”：[data_block_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/data_block_encoder.h#L56-L68)
- `DataBlockEncoder` 则定义 block 级别的统一接口：[data_block_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/data_block_encoder.h#L72-L99)

也就是说，项目把“Cell 的线格式”与“Block 的内部编码策略”分成了两层。

### 2.2 HFile v3 Cell 在这里是怎么被序列化的

`serialize_kv()` 的输出顺序很关键：

1. `key_len` 4B
2. `value_len` 4B
3. row 长度与 row 数据
4. family 长度与 family 数据
5. qualifier 数据
6. timestamp 8B
7. key type 1B
8. value 数据
9. tags 长度 + tags
10. memstore ts 的 writable vint

对应实现见 [data_block_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/data_block_encoder.h#L15-L54)。

而 `serialize_key()` 只保留 HBase 用来比较和索引的 internal key 部分，不包含 value、tags、mvcc：[data_block_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/data_block_encoder.h#L56-L68)。

这个分层很重要，因为：

- `NoneEncoder` 需要完整 KV 直接顺序拼接
- `Prefix/Diff/FastDiff` 更关心 key 的共享前缀、时间戳、类型等字段差异
- `index/` 只关心首 key，不需要 value

### 2.3 `DataBlockEncoder` 抽象为什么合理

统一接口只有 6 个核心点：

- `append()`：向当前 block 追加一个 KV
- `finish_block()`：拿到当前 block 的原始未压缩字节
- `reset()`：开始下一个 block
- `first_key()`：返回当前 block 的首 key，供索引登记
- `current_size()`：查看当前 block 大小估计
- `num_kvs()`：当前 block 内的 cell 数

见 [data_block_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/data_block_encoder.h#L76-L99)。

这意味着 `writer.cc` 完全不必知道当前使用的是哪种编码器，它只要依赖统一契约：

- 先不断 `append()`
- append 失败则 flush 当前 block
- flush 时从 `finish_block()` 拿字节
- 从 `first_key()` 拿索引 key

这也是 `flush_data_block()` 能够不区分编码器类型的原因：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L372-L405)。

### 2.4 编码器工厂：策略切换发生在哪里

编码策略选择集中在 [block_builder.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/block_builder.cc#L10-L18)：

- `Encoding::None` -> `NoneEncoder`
- `Encoding::Prefix` -> `PrefixEncoder`
- `Encoding::Diff` -> `DiffEncoder`
- `Encoding::FastDiff` -> `FastDiffEncoder`

因此，上层配置中的 `data_block_encoding`，最终就是在这里决定真正的实现类。

### 2.5 `NoneEncoder`：最朴素，也最容易建立基线

`NoneEncoder` 的逻辑最简单：[none_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/none_encoder.h#L11-L59)

- `append()` 时直接按 `kv.encoded_size()` 扩容
- 用 `serialize_kv()` 把整条 KV 写进 buffer
- 首条 KV 时额外保存 `first_key_buf_`
- block 超出目标大小时返回 `false`

它的意义不是“高级压缩”，而是提供：

- 最低心智负担的正确性基线
- 对照其他编码器时最清晰的参照物
- 兼容未知编码 bug 时的兜底路径

可以把它理解成“纯粹的 Cell 拼包器”。

### 2.6 `PrefixEncoder`：利用有序 key 的公共前缀

`PrefixEncoder` 的核心思想是：

> 相邻 Cell 的 key 在有序 HFile 中通常高度相似，所以没必要每条都完整存一遍 key。

单条记录格式写得很清楚：[prefix_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/prefix_encoder.h#L14-L20)

- `keySharedLen`
- `keyUnsharedLen`
- `valueLen`
- `unsharedKeyBytes`
- `valueBytes`
- `tagsLen + tags`
- `mvcc`

它的执行过程是：

1. 先把当前 KV 的 key 序列化出来
2. 与 `prev_key_` 计算最长公共前缀
3. 只写“未共享后缀”
4. 再写 value、tags、mvcc
5. 把当前 key 保存成新的 `prev_key_`

对应代码在 [prefix_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/prefix_encoder.h#L29-L89)。

这里有两个很实用的实现点：

- 常见 key 走 512 字节栈缓冲，避免每次堆分配：[prefix_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/prefix_encoder.h#L33-L47)
- `prefix_len()` 在支持 SSE4.2 时走 SIMD 加速：[prefix_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/prefix_encoder.h#L111-L128)

所以 Prefix 的本质是：

- 空间上压缩 key 重复部分
- CPU 上尽量把“找公共前缀”做快

### 2.7 `DiffEncoder`：在 Prefix 基础上继续压缩“变化量”

`DiffEncoder` 的切入点比 Prefix 更细，它不只看 key 前缀，还看相邻 cell 的“字段是否重复”：[diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/diff_encoder.h#L15-L22)。

它用一个 flags 字节表达三类状态：

- timestamp 是否和上一条不同
- key type 是否与上一条相同
- value 长度是否与上一条相同

具体逻辑见 [diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/diff_encoder.h#L53-L68) 与 [diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/diff_encoder.h#L80-L109)。

这相当于把一条 KV 拆成两部分：

- **结构变化部分**：shared/unshared key、timestamp、type、valueLen
- **真实载荷部分**：unshared key suffix、value、tags、mvcc

如果某个字段和前一条一样，就不必重复存。

因此 Diff 比 Prefix 更进一步：

- Prefix 只压 key
- Diff 既压 key，也压时间戳、类型、value 长度这些重复元数据

### 2.8 `FastDiffEncoder`：为热路径减少分支代价

`FastDiffEncoder` 的目标不是改变大方向，而是进一步优化 DIFF 的热路径表现：[fast_diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/fast_diff_encoder.h#L15-L29)。

它的 flags 更丰富：

- `kFlagTimestampNew`
- `kFlagSameType`
- `kFlagSameValueLen`
- `kFlagUseTimeDelta`

定义见 [fast_diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/fast_diff_encoder.h#L32-L35)。

最核心的增强是时间戳处理：

- 第一条必须写完整 timestamp
- 后续若 timestamp 与前一条差值能落入 int8，则只写 1 字节 delta
- 否则再写完整 8 字节 timestamp

见 [fast_diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/fast_diff_encoder.h#L69-L80) 与 [fast_diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/fast_diff_encoder.h#L114-L122)。

这说明 FastDiff 的优化方向是：

- 继续利用“相邻 cell 通常时间戳接近”的现实
- 用更轻量的 flags 和更少的分支，降低 CPU pipeline 抖动

### 2.9 block 模块和 writer 的真实接线点

block 模块不是自己写文件，它只返回“当前 block 的原始内容”。

真正的交汇点在 `flush_data_block()`：

1. `encoder_->finish_block()` 取出原始 block 数据
2. `bloom_->finish_chunk()` 在 data block 边界 seal 一个 bloom chunk
3. `write_data_block()` 负责压缩、拼 HFile block header、计算 checksum、落盘
4. `index_writer_.add_entry()` 记录这个 data block 的首 key / offset / size
5. `encoder_->reset()` 清空，为下一个 data block 做准备

见 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L372-L405)。

这里非常关键的一点是：

> 编码器只负责 block 内部格式，不负责 block 外部包装。

block 外部包装包括：

- block magic
- 压缩后长度
- 未压缩长度
- 前驱 block offset
- checksum 类型
- bytes per checksum
- on-disk data size

这些统一由 writer 负责，见 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L407-L455)。

### 2.10 从原理上看 block 编码器的本质

从工程设计上，`block/` 做的是“局部压缩”和“局部边界控制”：

- 它不管全文件布局
- 不管 Bloom 的 chunk 偏移
- 不管索引层级
- 不管 FileInfo 与 Trailer

它只负责一件事：

> 把一串有序的 Cell，尽可能高效地组织成一个个可被后续封装的 data block。

---

## 3. `bloom/compound_bloom_filter_writer` 深度拆解

### 3.1 为什么 Bloom 过滤器必须存在

HFile 是有序存储，但“有序”只能高效回答范围定位问题，不能高效回答“某条 key 根本不存在吗”。

Bloom Filter 的价值就在这里：

- 告诉 Reader “大概率存在”
- 或“可以非常确定地判定不存在”

这样在点查、半点查时，Reader 不必把很多无意义的 data block 解压出来。

### 3.2 为什么这里不是简单 Bloom，而是 Compound Bloom

类名已经揭示设计方向：[compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h#L48-L50)。

它不是把整份 HFile 只做一个巨大位图，而是按 chunk 分块。

原因是：

- 一个超大 Bloom 位图不利于局部读取
- HFile 自身就是按 block 思维组织的
- chunk 化后，Bloom 元信息可以告诉 Reader 去读哪个 Bloom chunk

因此它的模型是：

- 写入阶段不断向当前 chunk 填 key
- chunk 满了就 seal 成一个 `BLMFBLK2`
- 所有 chunk 的偏移再汇总成一个 `BLMFMET2`

### 3.3 初始化阶段：先算每个 key 需要多少 bit

构造函数里最核心的数学是这个：[compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h#L56-L67)

- 根据 error rate 计算 `bits_per_key`
- 再推导 `chunk_keys_`
- 最后初始化当前 chunk

也就是说，这里先做的是容量规划，而不是立即写任何字节。

`kNumHashFunctions = 5`、`kDefaultErrorRate = 0.01`、`kMaxChunkSize = 128KB` 定义见 [compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h#L52-L55)。

### 3.4 `add()` 与 `add_row_col()`：写入的到底是什么 key

接口上提供两种喂法：

- `add(key)`：直接加 row key
- `add_row_col(row, qualifier)`：为 RowCol Bloom 准备

见 [compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h#L69-L90)。

这里要注意一个非常容易误解的地方：

- `Row` Bloom：只对 row 做哈希
- `RowCol` Bloom：把 row 和 qualifier 拼起来做哈希

而 writer 侧接线是：

- 总是先 `bloom_->add(kv.row)`
- 如果配置是 `RowCol`，再额外 `add_row_col(kv.row, kv.qualifier)`

见 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L620-L622)。

所以阅读时要把“Bloom 类型”和“writer 选择调用哪个入口”结合起来看。

### 3.5 哈希与置位：Bloom 的核心热路径

`set_bits()` 是 Bloom 真正的位运算核心：[compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h#L160-L169)。

它的做法是：

1. 用 Murmur3 生成 `h1`
2. 再用 `h1` 作为 seed 生成 `h2`
3. 用双哈希技巧生成多组 bit 位置
4. 在当前 chunk 位图中逐位设置

Murmur3 实现在同一文件顶部：[compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h#L15-L39)。

这说明 Bloom writer 的核心数据结构其实非常简单：

- 当前 chunk 的位图 `cur_chunk_`
- 已完成 chunk 集合 `chunks_`
- 每个 chunk 的 key 数量 `chunk_key_counts_`
- 每个 chunk 最终落盘 offset `chunk_offsets_`

### 3.6 为什么要在 data block 边界调用 `finish_chunk()`

很多人第一次看这里会疑惑：

> Bloom chunk 为什么不是“位图满了再切”，而是 writer 在 flush data block 时也会主动 seal？

答案在 [compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h#L92-L100) 和 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L379-L382)。

原因是这个实现希望让 Bloom chunk 与 data block 之间保持大致同步关系：

- 一个 data block 结束时
- 当前 Bloom chunk 也尽量结束

这样做的工程意义是：

- Bloom 的逻辑分块更接近实际数据块边界
- meta block 里的 chunk directory 更容易与 data block 访问模式对应

而 `finish()` 里再次调用一次 `finish_chunk()`：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L244-L247)

并不是重复工作，而是收尾最后一个未满的 partial chunk。

### 3.7 `finish_data_blocks()`：为什么 Bloom 数据块要统一后写

Bloom chunk 的真正写出发生在 `finish_data_blocks()`：[compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h#L102-L119)。

这里的动作有三个：

1. 如果当前 chunk 还没 seal，先补一次 `finish_chunk()`
2. 逐个 chunk 记录绝对文件偏移到 `chunk_offsets_`
3. 把每个 chunk 封装成 `BLMFBLK2` block

注意它要求传入 `current_file_offset`。

这说明 Bloom chunk offset 不是事后回填，而是在 writer 已经知道“现在文件写到哪”的前提下，边生成边记录的。

于是 `writer.cc` 才会先拿当前 `writer_->position()` 作为 `bloom_data_offset`，再统一写 Bloom chunk blocks：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L257-L265)。

### 3.8 `BLMFBLK2` 的物理结构

`write_bloom_chunk_block()` 负责把一个位图 chunk 包成标准 HFile block：[compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h#L171-L195)。

结构与普通 block 一样：

- 8 字节 magic：`BLMFBLK2`
- 33 字节 block header
- chunk_data
- CRC32C 校验数组

和 data block 的最大不同在于：

- data block 的 payload 是编码后的 Cell 序列
- bloom chunk block 的 payload 只是位图字节

但它们都服从同一套“block header + checksum”模型。

### 3.9 `BLMFMET2`：Bloom 的总目录

只有 Bloom chunk blocks 还不够，因为 Reader 还需要知道：

- 一共有多少 chunk
- 每个 chunk 在文件哪个 offset
- 每个 chunk 有多大
- 使用多少哈希函数
- Bloom 类型是什么

这些都写进 `BLMFMET2`，由 `write_bloom_meta_block()` 生成：[compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h#L197-L250)。

元数据内容包括：

- version
- numChunks
- totalByteSize
- errorRate
- numHashFunctions
- bloomType
- chunk directory：`[chunkOffset + chunkByteSize] * N`

所以从原理上说：

- `BLMFBLK2` 是 Bloom 的真实数据
- `BLMFMET2` 是 Bloom 的目录页

### 3.10 Bloom 为什么还需要 meta root 再包一层

虽然 `BLMFMET2` 已经有目录信息，但 HBase 在 load-on-open 阶段，首先读取的是 meta root，而不是直接猜某个 Bloom meta 在哪。

因此 `writer.cc` 还要额外构造一个 meta root payload，其中用 key `GENERAL_BLOOM_META` 指向 `BLMFMET2` 的 offset 和大小：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L293-L325)。

这个设计说明：

- meta root 是“元信息索引”
- `BLMFMET2` 是“Bloom 元信息正文”

也正因为这层关系，Bloom meta 的 offset 必须在 load-on-open 区布局已经推导清楚后才能写准。

### 3.11 这一层最容易出错的地方

Bloom 模块的坑几乎都和“时序”有关：

- 必须先生成 Bloom chunk blocks，才能知道每个 chunk 的真实 offset
- 有了真实 `chunk_offsets_`，才能正确生成 `BLMFMET2`
- 又必须知道 `BLMFMET2` 的 offset，meta root 才能正确指向它

这也是为什么 `writer.cc` 要先 probe `meta_root_block` 和 `file_info_block` 的大小，再反推出最终的 `bloom_meta_offset`：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L304-L321)。

如果这一层 offset 推导错了，HBase 就会在 load-on-open 阶段读偏。

---

## 4. `index/block_index_writer` 深度拆解

### 4.1 index 的职责不是排序，而是“块级寻址”

HFile 的 data block 已经按 key 顺序写好，但 Reader 仍然需要一个“从 key 快速定位到 block”的机制。

`BlockIndexWriter` 干的就是这件事：

- 输入：每个 data block 的首 key、文件 offset、block 大小
- 输出：root index，必要时再加 intermediate index

核心数据模型在 [block_index_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/index/block_index_writer.h#L13-L24)：

- `IndexEntry`
- `IndexWriteResult`

### 4.2 索引条目是何时产生的

并不是所有数据写完后再回头扫描文件建索引，而是在每个 data block flush 时立即登记。

接线点在 `write_data_block()` 末尾：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L443-L449)。

写入一个 data block 后，writer 立刻调用：

- `encoder_->first_key()` 取 block 首 key
- `index_writer_.add_entry(...)`

因此 index 层天然知道：

- 这个 data block 代表的 key 起点
- 它在文件中的绝对位置
- 它自身尺寸

### 4.3 1 层索引与 2 层索引的分界

头文件注释已经说明了设计：[block_index_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/index/block_index_writer.h#L26-L49)。

- 如果条目数不超过 `max_entries_per_block`
  - root 直接指向 data blocks
- 如果条目数超过阈值
  - 先把 data block 条目切成多个 intermediate index block
  - root 再指向这些 intermediate blocks

这就是典型的两级索引结构。

从工程意义上说，这么做是为了避免：

- 根索引无限膨胀
- load-on-open 时一次性读入过大的 root payload

### 4.4 root entry 的线格式

`write_entry()` 负责把单条索引记录序列化进 root payload：[block_index_writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/index/block_index_writer.cc#L26-L38)。

它的格式是：

- `offset` 8B
- `dataSize` 4B
- `keyLen` writable vint
- `first_key`

这里要特别注意：

> 这里的 `keyLen` 不是 4 字节固定长，而是 writable vint。

这和某些直觉上的“都写 BE32”不同，也是与 HBase 兼容时很关键的细节。

### 4.5 intermediate block 为什么长得不完全一样

`write_intermediate_block()` 的布局见 [block_index_writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/index/block_index_writer.cc#L45-L103)。

它的 payload 不是简单重复 root entry，而是：

1. `entryCount`
2. 一组 secondary offsets
3. 若干 entry 正文

entry 正文本身是：

- `offset` 8B
- `dataSize` 4B
- `first_key`

注意这里没有再单独写 writable vint keyLen。

这是因为 intermediate block 依靠 secondary offsets 来切分 entry 边界，Reader 不再需要像 root payload 那样靠 keyLen 自行界定。

这正是 index 模块里最容易被忽视的一个不对称性：

- root entry 用 `keyLen` 划边界
- intermediate entry 用“offset table”划边界

### 4.6 `finish()`：索引层级在这一刻真正成型

`finish()` 是索引模块最关键的函数：[block_index_writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/index/block_index_writer.cc#L107-L169)。

它分两条路径：

- **1-level**
  - 全部 `entries_` 直接写进 `root_out`
- **2-level**
  - 按 `max_per_block_` 分块
  - 每块生成一个 `IDXINTE2`
  - 每个 intermediate block 再生成一个 root entry 指向它

这里有两个输出缓冲：

- `intermed_blocks_out`
- `root_out`

说明索引模块不直接把 root 包成完整 block，而是只负责准备 payload。真正把 `root_out` 包装成 `IDXROOT2` block 的仍然是 `writer.cc`：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L281-L286)。

### 4.7 为什么 `intermed_start_offset` 必须由外部传进来

`finish()` 的参数里有个很关键的量：`intermed_start_offset`。

这不是索引模块自己能知道的，而必须由 writer 在“当前文件已经写到哪里”这个上下文里提供。

原因是：

- intermediate blocks 的 root entry 里要写绝对文件偏移
- 这个偏移依赖 Bloom chunk 是否启用、前面已经写了多少数据

因此 index 模块只能负责“按给定起点推导内部偏移”，不能脱离 writer 独立决定最终地址。

这再次说明模块边界非常清楚：

- index 负责结构
- writer 负责布局

### 4.8 `IDXROOT2` 在 writer 中是怎样被封装的

`BlockIndexWriter` 返回的 `root_out` 只是 payload，writer 接下来会调用 `build_raw_block()` 把它包成标准块：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L283-L286)。

`build_raw_block()` 本质是一个通用 block 包装器：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L466-L495)。

这意味着：

- root data index
- meta root
- file info

虽然语义不同，但在“外层块封装”层面共用同一套 header + checksum 逻辑。

### 4.9 index 的本质作用

如果把 HFile 看成一本很厚的书：

- data block 是正文分页
- Bloom 是“这页大概率有没有这个词”
- index 就是目录

并且这个目录不是按“单条 Cell”建的，而是按“每个 data block 的首 key”建的。

所以索引真正做到的是：

> 先把 Reader 的搜索空间缩到某个 block，再由 block 内部顺序或二次结构继续定位。

---

## 5. `meta/file_info_builder + trailer_builder` 深度拆解

### 5.1 为什么元信息必须分成 FileInfo 和 Trailer 两层

很多初学者看到 meta 容易把所有“元数据”混成一团，但这里实际上分工很明确：

- **FileInfo**
  - 更像文件属性表
  - 记录 last key、平均 key/value 长度、比较器、编码方式、创建时间等
- **Trailer**
  - 更像文件尾部导航页
  - 记录“关键区域在文件哪里”

一个偏向“内容属性”，一个偏向“布局导航”。

### 5.2 `FileInfoBuilder`：它在描述这份 HFile 的静态属性

`FileInfoBuilder` 提供了一组 setter：[file_info_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/file_info_builder.h#L23-L82)。

可以把这些字段分成四组：

- **范围与统计**
  - last key
  - avg key len
  - avg value len
  - len of biggest cell
- **版本与协议**
  - key value version
  - max memstore ts
- **Reader 行为相关**
  - comparator
  - data block encoding
- **构建信息**
  - create time
  - max tags len

writer 真正填这些字段的地方在 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L497-L517)。

这说明 FileInfo 并不是某个独立子系统自己知道的，而是 writer 在整个写入过程中持续累计统计，最后一次性灌进去。

### 5.3 为什么 `validate_required_fields()` 很重要

`validate_required_fields()` 列出了当前实现认为必须存在的字段：[file_info_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/file_info_builder.h#L84-L102)。

这一步很关键，因为它保证：

- 文件不是“勉强写出来”
- 而是关键元数据完整、可被 HBase 正常理解

从原理上说，这个校验相当于在写文件末尾加一道结构一致性断言。

### 5.4 FileInfo 为什么是 protobuf，但外面还包了 `PBUF`

`finish()` 的逻辑见 [file_info_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/file_info_builder.h#L104-L129)。

输出格式不是裸 protobuf，而是：

- `PBUF` magic
- protobuf 长度 varint
- protobuf bytes

这说明 FileInfo block 的 payload 本质是一个“带定界头的 protobuf blob”，而不是手写二进制字段拼接。

同时 `entries_` 用的是 `std::map`：[file_info_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/file_info_builder.h#L136-L137)

这意味着输出顺序稳定，有利于 deterministic serialization。

### 5.5 FileInfo block 在文件里并不是第一个 meta 块

很多人直觉会以为：

> 既然叫 FileInfo，那它应该最早写。

但 HBase 的 load-on-open 顺序并不是这样。这里的顺序由 `writer.cc` 明确控制：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L267-L347)：

1. intermediate index
2. root data index
3. meta root
4. file info
5. bloom meta
6. trailer

所以 FileInfo 是 load-on-open 区的一部分，但不是第一个块。

### 5.6 meta root 虽不在 `meta/` 目录，却必须和 FileInfo 一起理解

严格来说，当前项目里的 meta root 不是由 `FileInfoBuilder` 构造的，而是直接在 `writer.cc` 中组装：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L293-L325)。

但从阅读原理上，它必须和 FileInfo/Trailer 放在一起理解，因为三者共同定义了“元信息导航层”：

- meta root：告诉 Reader 某个 meta block 在哪里
- file info：给出文件属性
- trailer：告诉 Reader load-on-open 区起点、file info 偏移等全局导航信息

当前实现里，meta root 实际只承载一项：

- `GENERAL_BLOOM_META` -> `BLMFMET2`

也就是说：

> 当前 meta root 不是通用元信息目录系统，而是一个以 Bloom Meta 为核心的轻量元信息索引。

### 5.7 `TrailerBuilder`：为什么 Trailer 必须最后写

Trailer 的 setter 在 [trailer_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/trailer_builder.h#L26-L37)。

这些字段几乎都依赖“文件最终布局已经确定”：

- file info offset
- load-on-open offset
- uncompressed data index size
- data index count
- meta index count
- first/last data block offset
- entry count

因此 Trailer 不可能提前写，只能最后写。

这就是 `write_trailer()` 被放在整个 finish 流程末尾的原因：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L342-L345) 与 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L521-L545)。

### 5.8 Trailer 的物理结构为什么是固定 4096 字节

`TrailerBuilder::finish()` 里把这一点写得非常直接：[trailer_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/trailer_builder.h#L39-L69)。

Trailer 布局是：

- 8 字节 `TRABLK"$`
- delimited protobuf
- 零填充
- 最末尾 4 字节版本号

固定总大小为 `4096` 字节，常量定义在 [types.h](file:///Users/gauss/workspace/github_project/HFileSDK/include/hfile/types.h#L189-L190)。

固定尾长的好处是：

- Reader 可以稳定地从文件尾部倒着定位 Trailer
- 不必依赖额外外部索引来查 Trailer 位置

这也是典型的“尾部超级块”设计思路。

### 5.9 Trailer 里的版本号为什么单独写在最后 4 字节

`materialized_version` 的构造在 [trailer_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/trailer_builder.h#L63-L67)。

写法是：

- 高位放 minor version
- 低 24 位放 major version

虽然注释里写了 “LE-like materialized int”，但实现实际调用的是 `write_be32()`，这更说明这里的重点不是“按平台 native endian 输出”，而是写成 HFile 期望的稳定尾部版本字段。

### 5.10 `write_trailer()` 实际把哪些统计灌进去了

writer 最终填充 Trailer 的内容见 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L521-L545)：

- `file_info_offset`
- `load_on_open_offset`
- `idx.uncompressed_size`
- `total_uncompressed_bytes_`
- `data_block_count_`
- `bloom.enabled ? 1 : 0`
- `entry_count_`
- `idx.num_levels`
- `first_data_block_offset_`
- `last_data_block_offset_`
- comparator
- compression codec

这说明 Trailer 并不关心细节正文内容，而更像是：

> Reader 打开文件时必须掌握的全局导航摘要。

---

## 6. 这四个模块是怎样共同组成完整 HFile 的

把它们连起来看，一份 HFile 的形成过程可以压缩成下面这条因果链：

### 6.1 写入期

- 上层持续调用 `append_materialized_kv()`：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L601-L631)
- KV 进入 `block/` 当前编码器
- row/row+col 进入 `bloom/`
- writer 同时累计 last key、cell 大小、平均长度、MVCC 等统计

### 6.2 data block flush 时

- `block/` 交出当前 block 的原始 payload
- writer 把它压缩并包成 `DATABLK*` 或 `DATABLKE`
- `index/` 记录该 block 的首 key 与 offset
- `bloom/` 在 block 边界 seal 当前 chunk

### 6.3 所有 data block 写完后

- `bloom/` 输出所有 `BLMFBLK2`
- `index/` 输出 intermediate payload 与 root payload
- writer 把 root payload 包成 `IDXROOT2`

### 6.4 load-on-open 区构造时

- writer 组装 meta root，指向 `BLMFMET2`
- `FileInfoBuilder` 生成 `FILEINF2`
- `CompoundBloomFilterWriter` 生成 `BLMFMET2`

### 6.5 文件收尾时

- `TrailerBuilder` 把所有关键 offset、计数、编码、压缩信息写入固定尾部 trailer

这就是当前项目中的 HFile 物理装配闭环。

---

## 7. 最容易踩坑的底层细节

### 7.1 不要把“编码器输出”误认为“完整 data block”

编码器给出的只是 payload，不含完整 HFile block header、压缩结果、checksum。

真正的完整 block 组装发生在 writer 层：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L407-L455)。

### 7.2 不要把 root index entry 和 intermediate entry 当成同一种格式

它们都记录 offset/dataSize/key，但边界描述方式不同：

- root：`keyLen + key`
- intermediate：secondary offsets + entry body

见 [block_index_writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/index/block_index_writer.cc#L26-L38) 与 [block_index_writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/index/block_index_writer.cc#L50-L91)。

### 7.3 Bloom 的 offset 推导必须严格按时序来

不能先拍脑袋写 meta root，再回头补 Bloom meta offset，因为中间还隔着：

- root index block
- meta root block 自身
- file info block

偏移链条见 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L296-L321)。

### 7.4 Trailer 只适合在最终布局稳定后写

只要 load-on-open 区里任一块大小还不确定，Trailer 里的偏移就不可信。

所以 Trailer 不是“普通元数据块”，而是全文件最终导航快照。

### 7.5 当前 meta root 只承载 Bloom Meta，不要过度泛化理解

`meta_index_count` 目前也是根据 Bloom 是否启用写成 `0/1`：[writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L531-L531)。

这意味着当前实现里 meta root 主要是为 Bloom 服务，而不是完整通用 meta block 注册表。

---

## 8. 阅读顺序建议

如果你要真正吃透 HFile 的底层组成，建议按这个顺序看源码：

1. 先看块类型和常量定义：[types.h](file:///Users/gauss/workspace/github_project/HFileSDK/include/hfile/types.h#L161-L190)
2. 再看 Cell 如何序列化：[data_block_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/data_block_encoder.h#L13-L68)
3. 再看四种 DataBlockEncoder 的差异：
   - [none_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/none_encoder.h)
   - [prefix_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/prefix_encoder.h)
   - [diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/diff_encoder.h)
   - [fast_diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/fast_diff_encoder.h)
4. 再看 Bloom 的 chunk 机制：[compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h)
5. 再看索引层级如何形成：[block_index_writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/index/block_index_writer.cc)
6. 再看 FileInfo 和 Trailer：
   - [file_info_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/file_info_builder.h)
   - [trailer_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/trailer_builder.h)
7. 最后回到总装配入口 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc#L218-L347)

这样你会形成一个非常稳定的认知：

> HFile 不是“一个大文件里随便塞点块”，而是一套严格的分层装配协议：Cell 编码、block 封装、索引定位、Bloom 剪枝、元信息导航，共同组成最终可读的 HFile。
