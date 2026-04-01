# bulk_load_writer.cc 深度拆解

这份文档专门解释 [bulk_load_writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc)。

如果说：

- `writer.cc` 解决“一个 HFile 怎么写”
- `converter.cc` 解决“一个 Arrow 文件怎么转一个 HFile”

那么 `bulk_load_writer.cc` 解决的是另外一个问题：

> 当数据需要按 Region / 列族拆成多个 HFile 时，整个批量导入过程应该怎么组织？

它不是单纯的“再包一层 writer”，而是一个带有：

- 路由
- 生命周期管理
- 文件滚动
- 并行 finish
- 进度汇报

能力的批量编排器。

---

## 1. 先给它一句话定义

`bulk_load_writer.cc` 的职责是：

> 把一批 Arrow `RecordBatch` 或 `KeyValue`，根据列族和 Region 路由到多个 `HFileWriter`，并在适当时机滚动、关闭和汇总这些 HFile。

你可以把它理解成：

> 多 HFile 输出场景下的总调度器

---

## 2. 为什么它和 `writer.cc` 不一样

`writer.cc` 的世界很单纯：

- 一个文件
- 一个列族
- 一条顺序写入链路

但 Bulk Load 场景不同：

- 可能有多个列族
- 每个列族下又有多个 Region
- 每个 `(cf, region)` 都可能需要独立 HFile

所以 `bulk_load_writer.cc` 的核心问题就变成了：

```text
一条 KV 应该落到哪一个 writer？
什么时候开新 writer？
什么时候关旧 writer？
最后怎样并发 finish？
```

---

## 3. 文件整体结构怎么读

这个文件可以分成 7 块：

1. 轻量日志
2. 内置线程池
3. WriterKey / ActiveWriter
4. `BulkLoadWriterImpl` 主实现
5. `write_kv()`
6. `write_batch()`
7. `finish()`
8. Builder

建议阅读顺序：

1. 先看 `WriterKey` 和 `ActiveWriter`
2. 再看 `write_kv()`
3. 再看 `write_batch()`
4. 再看 `open_writer()`
5. 再看 `ensure_writer_slot_locked()`
6. 最后看 `finish()`

因为真正理解这个文件，关键在于先明白：

- 一个 writer 的身份是什么
- 一条 KV 如何找到它的 writer

---

## 4. 顶部日志函数：为什么这里还是 stderr 风格

位置：[bulk_load_writer.cc:L26-L30](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L26-L30)

和 `converter.cc`、`writer.cc` 一样，这里用的是很轻量的 stderr 打印：

- `info`
- `warn`
- `error`

### 原因

这个项目整体没有引入重型日志框架。

在核心数据路径里，作者更偏向：

- 少依赖
- 直观可用

---

## 5. `ThreadPool`：为什么 bulk writer 自带一个小线程池

位置：[bulk_load_writer.cc:L34-L68](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L34-L68)

### 它的用途

主要不是给写入过程做并发，
而是给 `finish()` 阶段并发关闭多个 writer。

### 为什么不把所有写入都并发化

因为每个 `HFileWriter` 本身不是线程安全写入器，
而 bulk writer 的主要并发机会在于：

- 多个目标 HFile 可以在最后并行 finish

这也是这个线程池存在的根本原因。

### 你可以这样理解

这个线程池更像“收尾并行器”，而不是“主写路径并行器”。

---

## 6. `WriterKey`：一个 writer 是怎么被唯一标识的

位置：[bulk_load_writer.cc:L72-L82](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L72-L82)

### 它只有两个字段

- `cf`
- `region`

### 这说明什么

在 Bulk Load 逻辑里，一个 writer 的身份就是：

```text
(column family, region)
```

也就是说：

- 同一个列族、同一个 Region 的数据，应该进入同一个 HFileWriter
- 不同 Region 或不同列族，必须分开写

### 为什么没有 table name

因为一个 `BulkLoadWriter` 实例本身就是围绕某张表工作的。

所以：

- 表维度已经被外层实例固定了
- writer 级身份不需要重复带上 table name

---

## 7. `ActiveWriter`：一个活跃 writer 还要保存什么

位置：[bulk_load_writer.cc:L84-L90](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L84-L90)

一个活跃 writer 包含：

- `writer`
- `rel_path`
- `last_use`
- `has_last_kv`
- `last_kv`

### 各字段作用

#### `writer`

真正的 `HFileWriter` 实例。

#### `rel_path`

这个 HFile 相对输出目录的路径。

例如可能像：

```text
cf/hfile_region_0003.hfile
```

#### `last_use`

用于 LRU 策略。

当 `max_open_files` 达到上限，需要淘汰最久未使用 writer。

#### `has_last_kv` / `last_kv`

用于记录这个 writer 最近写过的最后一条 KV。

### 为什么需要 `last_kv`

因为 `SkipBatch` 模式下，预校验要模拟：

- 如果这批 KV 真写进去，会不会破坏排序顺序？

这就需要知道当前 writer 已有数据的最后一条 key。

---

## 8. `validate_batch_kv()`：为什么 Bulk Load 自己也要再做一层校验

位置：[bulk_load_writer.cc:L92-L102](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L92-L102)

### 作用

在批量预校验阶段，先检查：

- row 是否为空
- row 是否过长
- value 是否过大
- timestamp 是否负数

### 为什么这里还要校验，`writer.cc` 里不是也会校验吗

因为 `SkipBatch` 模式要求：

- 在真正写任何一个目标 HFile 之前，先判断这整批能不能整体接受

如果只等到 `writer->append()` 时才发现问题，就可能出现：

- 一部分已经写入
- 一部分失败

这就不符合 `SkipBatch` 的语义预期。

---

## 9. `BulkLoadWriterImpl`：它内部维护了哪些状态

位置从 [bulk_load_writer.cc:L106](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L106) 开始，成员变量在 [L495-L526](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L495-L526)

建议按职责分组理解。

### 9.1 基本配置

- `table_name_`
- `column_families_`
- `output_dir_`
- `partitioner_`
- `opts_`
- `parallelism_`

这些决定：

- 写到哪
- 写哪些列族
- 如何分区
- 用什么压缩/编码等写入选项

---

### 9.2 列族与路由相关

- `cf_grouper_`

它负责列族注册和合法性判断。

---

### 9.3 进度回调

- `progress_cb_`
- `progress_interval_`
- `progress_running_`
- `progress_thread_`

这些变量支撑“周期性上报当前写入进度”。

---

### 9.4 活跃 writer 管理

- `writers_mu_`
- `writers_`
- `next_file_seq_`
- `use_clock_`

这是整个 bulk writer 最核心的一组状态。

#### `writers_`

是：

```text
WriterKey -> ActiveWriter
```

#### `next_file_seq_`

用于同一个 `(cf, region)` 滚动多个文件时生成序号。

#### `use_clock_`

一个递增计数器，用来做 LRU。

---

### 9.5 已完成与失败文件统计

- `completed_files_count_`
- `completed_files_`
- `failed_files_`
- `completed_entries_`
- `completed_bytes_`
- `first_finish_error_`

这些状态主要在 `finish()` 和 writer 淘汰时更新。

---

### 9.6 运行计数器

- `kv_written_`
- `kv_skipped_`
- `bytes_written_`
- `total_rows_`
- `batches_processed_`

它们是原子变量，因为不同阶段可能在多个线程里更新。

---

## 10. 构造函数：创建 bulk writer 时会做什么

位置：[bulk_load_writer.cc:L107-L156](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L107-L156)

### 10.1 注册列族

逻辑在 [L126-L128](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L126-L128)。

作者会把传入的所有 column family 注册进 `CFGrouper`。

### 10.2 创建输出目录

逻辑在 [L130-L132](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L130-L132)。

不仅会创建总输出目录，还会为每个列族单独建目录。

也就是说，输出布局天然是：

```text
output_dir/
  cf1/
  cf2/
  ...
```

### 10.3 启动进度线程

逻辑在 [L134-L151](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L134-L151)。

如果提供了 `progress_cb`：

- 启一个后台线程
- 每隔 `progress_interval_` 调一次 `make_progress_info()`

### 为什么进度线程和主写入线程分开

因为进度汇报不应阻塞主路径。

---

## 11. `write_kv()`：最核心的路由函数

位置：[bulk_load_writer.cc:L163-L202](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L163-L202)

这是整个文件最值得优先读的函数之一。

### 作用

把一条 `KeyValue` 路由到正确的 `HFileWriter`。

### 它的执行顺序

#### 第 1 步：取 family

从 `kv.family` 里读出列族字符串。

#### 第 2 步：校验列族是否合法

调用 `cf_grouper_.has_cf(...)`。

如果列族不在预注册列表中，立即失败。

#### 第 3 步：计算 region

调用：

- `partitioner_->region_for(kv.row)`

也就是说，row key 决定了 region。

#### 第 4 步：构造 `WriterKey`

```text
(cf, region)
```

#### 第 5 步：找活跃 writer

在 `writers_` 里查。

如果不存在：

1. 先尝试腾出 writer 槽位
2. 再 `open_writer()`
3. 放进 `writers_`

#### 第 6 步：更新 LRU

`last_use = ++use_clock_`

#### 第 7 步：把 KV 交给底层 writer

调用：

```text
it->second.writer->append(kv)
```

#### 第 8 步：更新统计与 `last_kv`

如果成功：

- `kv_written_++`
- `bytes_written_ += kv.encoded_size()`
- 保存 `last_kv`

如果失败：

- `kv_skipped_++`
- 打 warning
- 如果 `Strict`，把错误继续向上抛
- 否则吞掉

---

## 12. 为什么 `write_kv()` 是串行关键区

注意 [bulk_load_writer.cc:L172-L185](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L172-L185)：

整个：

- 查 writer
- 创建 writer
- 更新 last_use
- `writer->append(kv)`

都在 `writers_mu_` 锁里。

### 这意味着什么

主写入路径不是按 writer 并发，而是：

- 由同一把全局锁串行化

### 为什么作者这样做

因为简单、安全：

- 不需要让单个 `HFileWriter` 自己变成线程安全
- 不需要复杂处理 writer 生命周期竞争

代价是：

- 并发吞吐上限会受限于这把锁

---

## 13. `write_batch()`：Arrow batch 进入 Bulk Load 时怎么走

位置：[bulk_load_writer.cc:L204-L267](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L204-L267)

这是 Bulk Load 对 Arrow 的入口。

### 核心思想

先把 Arrow `RecordBatch` 转成 `KeyValue`，
再按每条 KV 路由到对应 writer。

---

## 14. `write_batch()` 第一步：准备回调

逻辑在 [L205-L224](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L205-L224)。

### 为什么这里要分 `SkipBatch` 和非 `SkipBatch`

#### 非 `SkipBatch`

回调里直接：

```text
write_kv(kv)
```

也就是：

- 边转换
- 边写入

#### `SkipBatch`

回调里不立刻写，
而是先把每条 KV 深拷贝进 `staged`。

### 为什么要这样

因为 `SkipBatch` 模式要求：

- 要么整批都写
- 要么整批都跳过

这就必须先把整批数据暂存下来，再统一预校验。

---

## 15. `write_batch()` 第二步：按模式调用 `ArrowToKVConverter`

逻辑在 [L225-L241](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L225-L241)

支持三种模式：

- `WideTable`
- `TallTable`
- `RawKV`

### 这意味着 bulk writer 本身不关心 Arrow 细节

它把：

- “一行 Arrow 如何变成 KV”

这件事完全委托给 `ArrowToKVConverter`。

这也是很清晰的分层。

---

## 16. `write_batch()` 第三步：处理转换结果

逻辑在 [L242-L250](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L242-L250)。

### 做的事情

- `batches_processed_++`
- `total_rows_ += batch.num_rows()`

如果转换失败：

- `SkipBatch`：整批算 skipped，然后返回 OK
- 否则：把错误继续向上返回

### 为什么 `SkipBatch` 会返回 OK

因为它的语义是：

- 允许跳过整批，不把这当作整个作业失败

---

## 17. `write_batch()` 第四步：批量预校验

逻辑在 [L253-L260](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L253-L260)。

只有 `SkipBatch` 会走这里。

它会调用：

- `prevalidate_batch(staged)`

### 为什么一定要先预校验

因为如果直接逐条写：

- 前几条成功
- 中间一条失败

那这批数据就已经“部分写入”了。

这与 `SkipBatch` 的预期语义相冲突。

---

## 18. `prevalidate_batch()`：如何模拟整批是否可接受

位置：[bulk_load_writer.cc:L374-L399](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L374-L399)

### 它做了什么

#### 第 1 步：复制当前每个 writer 的 `last_kv`

这样得到一个“模拟世界里的最后一条 KV 状态”。

#### 第 2 步：遍历 staged 里的每条 KV

对每条 KV：

1. 做基本合法性校验
2. 校验列族
3. 计算 region
4. 得到 `WriterKey`
5. 如果这个 writer 已有最后一条 KV，就做排序比较

### 核心判断

位置：[bulk_load_writer.cc:L392-L396](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L392-L396)

如果：

```text
compare_keys(new_kv, last_kv) <= 0
```

就认为排序被破坏，整批不合法。

### 这一步的意义

在真正落盘前就能知道：

- 这批数据会不会让某个目标 HFile 乱序

---

## 19. `ensure_writer_slot_locked()`：为什么会淘汰旧 writer

位置：[bulk_load_writer.cc:L401-L427](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L401-L427)

### 背景

如果：

- Region 很多
- 列族很多

那么活跃 writer 数可能暴涨。

系统通过：

- `max_open_files`

限制同时打开的 HFile 数量。

### 这个函数做什么

#### 第 1 步：如果没超上限，直接返回

#### 第 2 步：找最久未使用 writer

根据 `last_use` 找 LRU victim。

#### 第 3 步：先 finish victim

#### 第 4 步：从 `writers_` 中移除

#### 第 5 步：把结果统计进：

- `completed_files_`
- `completed_entries_`
- `completed_bytes_`

如果失败：

- 进 `failed_files_`
- 记录 `first_finish_error_`

### 为什么在这里就可能 finish

因为 Bulk Load 过程不是等所有数据都写完才 finish writer；
当打开文件数达到上限时，就会对冷 writer 做提前收尾。

---

## 20. `make_rel_path()`：为什么文件名长这样

位置：[bulk_load_writer.cc:L429-L439](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L429-L439)

命名规则大致是：

```text
cf/hfile_region_0003.hfile
cf/hfile_region_0003_0001.hfile
```

### 含义

- 第一层目录：列族
- 主编号：region id
- 可选后缀：同一个 `(cf, region)` 滚动后的序号

这样后续定位文件来源比较直观。

---

## 21. `open_writer()`：一个新的目标 HFileWriter 是怎么建出来的

位置：[bulk_load_writer.cc:L441-L468](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L441-L468)

### 它做了什么

1. 计算相对路径
2. 拼出绝对文件路径
3. 复制一份 `WriterOptions`
4. 强制：
   - `column_family = 当前 cf`
   - `sort_mode = PreSortedVerified`
5. 调 `HFileWriter::builder().build()`

### 为什么这里强制 `PreSortedVerified`

因为 Bulk Load 路由假设：

- 对于同一个 `(cf, region)` writer，
- 上层喂给它的 KV 顺序已经合法

所以它不应该再由底层 writer 额外做 AutoSort。

---

## 22. `make_progress_info()`：进度是怎么估的

位置：[bulk_load_writer.cc:L470-L488](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L470-L488)

### 它汇总哪些信息

- `total_kv_written`
- `total_bytes_written`
- `files_completed`
- `files_in_progress`
- `skipped_rows`
- `elapsed`
- `estimated_progress`

### 这里要注意

`estimated_progress` 是一个很粗的启发式值：

```text
batches / 1000.0
```

它不是严格意义上的真实作业进度。

所以如果以后你发现进度数字不精确，不要惊讶。

---

## 23. `finish()`：Bulk Load 的最终收尾流程

位置：[bulk_load_writer.cc:L269-L370](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L269-L370)

这是整个文件最重要的收尾函数。

### 它主要做 6 件事

1. 停掉进度线程
2. 先汇总当前已完成文件
3. 把所有活跃 writer 收集进 work 列表
4. 并发或串行调用 `writer->finish()`
5. 把成功/失败文件分类统计
6. 生成最终 `BulkLoadResult`

---

## 24. `finish()` 第一步：停进度线程

逻辑在 [bulk_load_writer.cc:L270-L272](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L270-L272)

### 为什么先停

因为 finish 阶段即将重组和清空 writer 状态。

如果进度线程还在跑，可能会看到中间态。

所以这里先把它停掉是正确的。

---

## 25. `finish()` 第二步：如果没有活跃 writer 怎么办

逻辑在 [bulk_load_writer.cc:L285-L293](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L285-L293)

### 这说明什么

有可能在调用 `finish()` 时：

- 所有 writer 都已经因为滚动而提前 finish 并移除了

这时 `writers_` 为空，但：

- `completed_files_`
- `failed_files_`

可能已经有内容。

所以这里直接根据累计状态返回结果。

---

## 26. `finish()` 第三步：收集 work 列表

逻辑在 [bulk_load_writer.cc:L296-L303](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L296-L303)

这里会把每个活跃 writer 变成三元组：

- `WriterKey`
- `HFileWriter*`
- `rel_path`

### 为什么不直接在锁里 finish

因为 finish 可能很慢：

- flush
- 索引写入
- trailer
- 文件关闭

所以这里先在锁里把工作列表复制出来，再锁外执行。

这是非常合理的设计。

---

## 27. `finish()` 第四步：决定串行还是并行 finish

逻辑在 [bulk_load_writer.cc:L304-L325](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L304-L325)

### 如果只有 1 个线程

直接 for 循环 finish。

### 如果有多个线程

用前面的 `ThreadPool` 提交任务。

### 为什么并行化点放在这里最划算

因为不同 writer 的 finish 彼此独立：

- 各写各的文件
- 没有共享排序状态

所以这个阶段很适合并行。

---

## 28. `finish()` 第五步：分类成功和失败文件

逻辑在 [bulk_load_writer.cc:L327-L349](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L327-L349)

### 成功时做什么

- 把 `rel_path` 加到 `result.files`
- 统计 `entry_count`
- 用实际文件大小更新 `result.total_bytes`

### 这里一个很重要的细节

作者没有用运行期的 `bytes_written_` 去当 HFile 文件大小，
而是重新读文件系统上的实际大小。

### 为什么

因为：

- `bytes_written_` 更接近逻辑单元格大小
- 不一定等于最终 HFile 文件大小

这是一个很好的工程细节。

### 失败时做什么

- 加入 `result.failed_files`
- 打 error 日志
- 记录第一条失败状态

---

## 29. `finish()` 第六步：返回什么状态

逻辑在 [bulk_load_writer.cc:L363-L370](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L363-L370)

有 3 种结果：

### 全部成功

- 返回 `Status::OK()`

### 部分成功

- 返回 `Status::IoError("PARTIAL_SUCCESS: ...")`

### 全部失败

- 返回第一条失败错误
- 或兜底的 `All files failed`

### 为什么“部分成功”不是 OK

因为对调用方来说：

- 虽然有产出
- 但作业并不完全成功

所以必须显式告知这一点。

---

## 30. Builder：外部怎么组装一个 BulkLoadWriter

位置：[bulk_load_writer.cc:L546-L633](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L546-L633)

### Builder 负责什么

把外部参数逐项填进：

- `table_name`
- `column_families`
- `output_dir`
- `partitioner`
- `WriterOptions`
- `parallelism`
- `progress callback`

### `build()` 的默认行为

如果没有提供 partitioner：

- 默认使用 `RegionPartitioner::none()`

这意味着：

- 所有 row key 都认为属于同一个 region

所以即使调用方没配置 Region 路由，Bulk Load 仍然能工作，只是不会真正分 Region。

---

## 31. 它和其他模块的关系

### 它依赖谁

- [arrow_to_kv_converter.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc)
  - 把 batch 转成 KV
- [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc)
  - 真正写单个 HFile
- [region_partitioner.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/partition/region_partitioner.cc)
  - 决定 row key 属于哪个 Region
- [cf_grouper.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/partition/cf_grouper.h)
  - 管理合法列族

### 谁依赖它

- 对外的 `BulkLoadWriter` API
- 上层任何想把 Arrow 批量落成多 HFile 的调用方

### 它在整体架构中的位置

可以画成：

```text
Arrow RecordBatch
  -> ArrowToKVConverter
  -> BulkLoadWriterImpl.write_kv()
  -> (cf, region) 路由
  -> 对应 HFileWriter
  -> 多个 HFile 文件
```

---

## 32. 如果你要调试这个文件，优先看哪里

### 场景 1：某条数据落到了错误的 HFile

优先看：

- `write_kv()`
- `partitioner_->region_for(kv.row)`
- `WriterKey`

---

### 场景 2：文件数比预期多

优先看：

- `max_open_files`
- `ensure_writer_slot_locked()`
- `next_file_seq_`

可能是因为 writer 被滚动了。

---

### 场景 3：`SkipBatch` 没有整体跳过

优先看：

- `write_batch()`
- `prevalidate_batch()`
- `write_kv()`

---

### 场景 4：finish 很慢

优先看：

- `parallelism_`
- `ThreadPool`
- 活跃 writer 数

---

### 场景 5：进度回调不准

优先看：

- `make_progress_info()`

特别注意：

- `estimated_progress` 只是启发式估算，不是严格真实值

---

## 33. 对不熟 C++ 的读者，最推荐阅读顺序

第一次读：

1. [WriterKey](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L72-L82)
2. [ActiveWriter](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L84-L90)
3. [write_kv()](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L163-L202)
4. [write_batch()](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L204-L267)

第二次读：

5. [prevalidate_batch()](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L374-L399)
6. [ensure_writer_slot_locked()](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L401-L427)
7. [open_writer()](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L441-L468)

第三次读：

8. [finish()](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L269-L370)
9. [Builder::build()](file:///Users/gauss/workspace/github_project/HFileSDK/src/bulk_load_writer.cc#L611-L633)

---

## 34. 最后一段，用一句话记住它

如果要用一句话概括 `bulk_load_writer.cc`：

> 它是整个项目里负责把“已经转换成 KeyValue 的批量数据”按列族和 Region 路由到多个 `HFileWriter`，并管理这些 writer 生命周期的批量导入调度器。

所以你以后只要想回答：

- 为什么会生成多个 HFile？
- 为什么同一个 Region 会滚出多个文件？
- 为什么某批数据会整批跳过？
- 为什么 finish 能并行关闭多个 writer？

答案基本都在这个文件里。
