# converter.cc 深度拆解

这份文档专门解释 [converter.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc)。

如果你已经看过：

- [ARROW_TO_HFILE_READING_GUIDE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ARROW_TO_HFILE_READING_GUIDE.md)
- [WRITER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/WRITER_CC_DEEP_DIVE.md)

那么这份文档就是中间那块最关键的拼图：

> Arrow 文件到底是怎样一步一步变成最终写入 HFile 的 ？

这个文件的核心职责不是“定义 HFile 格式”，而是：

- 打开 Arrow IPC Stream 文件
- 解析和执行 `rowKeyRule`
- 先构建排序索引
- 再按排序后的 row key 回放写入
- 最终把逻辑列值转成 HBase cell

---

## 1. 先给它一句话定义

`converter.cc` 的职责是：

> 把一个 Arrow IPC Stream 文件，经过“列过滤 + row key 构建 + 两遍扫描 + 排序 + cell 分组”，转换成一个符合 HBase 顺序要求的 HFile。

你可以把它看成项目里的“数据编排层”。

它不是：

- 纯 Arrow 读取器
- 纯 HFile 写入器
- JNI 层

它站在中间，负责把 Arrow 世界翻译成 HFile 写入世界。

---

## 2. 为什么这个文件重要

如果你使用的是：

- `tools/arrow-to-hfile`
- Java `HFileSDK.convert()`

最终都会走到这里。

所以它是这条主链路的业务核心：

```text
Arrow 文件
  -> converter.cc
  -> HFileWriter
  -> HFile 文件
```

---

## 3. 这个文件解决的核心难题是什么

Arrow 表数据和 HFile 写入要求之间有一个根本矛盾：

### Arrow 的特点

- 按列存储
- 输入顺序未必满足 HBase 要求
- 行结构比较自由

### HFile 的特点

- 写入顺序必须严格按 HBase key 顺序
- 同一 row 下的 cell 顺序也要稳定
- 读取方高度依赖有序性

### `converter.cc` 的解决办法

它采用**两遍扫描**：

1. **第一遍**：只负责构建排序索引
2. **第二遍**：按排序后的结果真正写 HFile

这就是理解 `converter.cc` 的第一把钥匙。

---

## 4. 文件结构怎么读

这个文件很长，但其实可以分成 8 块：

1. 日志与错误码映射
2. 列排除辅助函数
3. 基础结构：`SortEntry`、`GroupedCell`
4. Arrow 标量转换函数
5. Arrow 文件打开函数
6. 第一遍扫描：`build_sort_index()`
7. 第二遍写入辅助：`append_grouped_row_cells()`
8. 总入口：`convert()`

### 推荐阅读顺序

1. 先看 `convert()`
2. 再看 `build_sort_index()`
3. 再看 `append_grouped_row_cells()`
4. 然后回头看 `scalar_to_string()` 和 `scalar_to_bytes()`
5. 最后看列排除和错误码映射

---

## 5. 开头的日志与错误码映射在干什么

### 5.1 日志

位置：[converter.cc:L30-L35](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L30-L35)

这里只是一个很轻量的 stderr logger：

- `clog::info`
- `clog::warn`
- `clog::err`

它的用途不是做复杂日志框架，而是保证转换链路能把关键阶段打印出来。

---

### 5.2 `map_status_to_error_code()`

位置：[converter.cc:L37-L48](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L37-L48)

作用：

- 把内部 `Status` 映射成对外 `ErrorCode`

为什么需要它：

- C++ 内部可能只是一个 `Status::IoError(...)`
- 但对 Java / CLI / 调用者来说，必须拿到稳定的错误码，比如：
  - `SCHEMA_MISMATCH`
  - `DISK_EXHAUSTED`
  - `MEMORY_EXHAUSTED`

---

### 5.3 `map_pass1_status_to_error_code()`

位置：[converter.cc:L50-L58](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L50-L58)

它是第一遍扫描专用的错误映射。

### 为什么单独分一个

因为第一遍扫描阶段还没有真正进入写 HFile。

所以像：

- `INVALID_ROW_KEY_RULE`
- `SCHEMA_MISMATCH`
- `ARROW_FILE_ERROR`

会更常见；
而像 HFile finish 失败这种写入层错误，则通常发生在第二遍或收尾阶段。

---

## 6. 列排除逻辑：为什么 `_hoodie_*` 能被删掉

这一块非常值得单独看，因为你前面实际用到过 `--exclude-prefix _hoodie`。

### 6.1 `build_removal_indices()`

位置：[converter.cc:L72-L116](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L72-L116)

作用：

- 根据：
  - 精确列名排除
  - 前缀排除
- 计算出原始 Arrow schema 中要删除的列索引

### 为什么返回的是“降序索引”

因为后面删除列时是一个个调用 `RemoveColumn(idx)`。

如果不降序删除，前面删掉一列后，后面的索引都会偏移。

所以这里先排序成降序，是典型的“删除多个索引时防止位移”的技巧。

---

### 6.2 `apply_column_removal()`

位置：[converter.cc:L121-L132](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L121-L132)

作用：

- 对每个 `RecordBatch` 真正删除列

### 理解要点

这是作用在 **batch 实例** 上的。

也就是说，第一遍扫描读到每个 batch 后，都会先把不需要输出的列删掉，再继续后续处理。

---

### 6.3 `apply_schema_removal()`

位置：[converter.cc:L134-L145](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L134-L145)

作用：

- 对 schema 本身做同样的删除

### 为什么要有它

因为仅删 batch 不够。

rowKeyRule 索引校验需要知道：

- 过滤后的 schema 长什么样
- rowKeyRule 里的列索引是否还能合法指向某列

所以这里先对 schema 做一份“预演版删除”。

---

## 7. 两个关键结构：`SortEntry` 和 `GroupedCell`

---

## 7.1 `SortEntry`

位置：[converter.cc:L147-L153](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L147-L153)

它非常重要。

可以把它理解成：

> 第一遍扫描的“排序索引条目”

包含：

- `row_key`
- `batch_idx`
- `row_idx`

### 这意味着什么

第一遍不会把整行先转成最终 KV 再排序。

而是只记录：

- 这行最终对应什么 row key
- 它原来位于哪个 batch 的第几行

等第二遍再回去真正取数据。

### 设计好处

这样排序的对象更轻，不需要第一遍就完整物化全部 cell。

---

## 7.2 `GroupedCell`

位置：[converter.cc:L155-L158](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L155-L158)

它表示：

> 某一个 row key 下的一列 cell

包含：

- `qualifier`
- `value`

### 为什么没有 row 和 family

因为在第二遍写入时：

- 当前是在处理“某一个 row key 的一组 cell”
- 列族 `cf` 是统一参数

所以每个 `GroupedCell` 只需要关心：

- 自己的 qualifier
- 自己的 value

---

## 8. `as_bytes()`：为什么这里频繁用 span

位置：[converter.cc:L160-L166](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L160-L166)

作用：

- 把 `std::string` / `std::string_view` 转成 `std::span<const uint8_t>`

### 为什么要这么做

因为 `HFileWriter::append()` 的接口吃的是字节视图，而不是字符串对象。

这让 `converter.cc` 在第二遍写入时可以：

- 尽量避免重复创建临时 `vector<uint8_t>`
- 直接把 row key / qualifier 当作字节视图传过去

这是一个很典型的性能优化点。

---

## 9. `scalar_to_string()`：row key 构建时为什么先转成字符串

位置：[converter.cc:L207-L248](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L207-L248)

### 作用

把 Arrow 单元格转换成字符串表示，供 `rowKeyRule` 使用。

### 为什么 rowKey 构建要先字符串化

因为 `rowKeyRule` 本质上是“字符串拼接/编码规则”。

比如：

- 普通文本列直接拼接
- 数值列先转字符串
- timestamp 标准化为毫秒后再转字符串

所以 row key 的处理自然是字符串视角。

### 支持的类型

- string / large_string
- 各类整型
- float / double
- bool
- timestamp

### 遇到不支持的类型怎么办

返回：

- `SCHEMA_MISMATCH: unsupported Arrow type for row key field`

这说明：

- 不是所有 Arrow 类型都允许参与 row key 生成

---

## 10. `scalar_to_bytes()`：value 为什么转成大端字节

位置：[converter.cc:L252-L373](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L252-L373)

### 作用

把 Arrow 单元格变成真正写入 HFile value 的字节表示。

### 为什么很多数值类型要转成大端字节

因为 HBase/HFile 的 value 是纯二进制，不是字符串。

为了保持数值字段的稳定二进制表示，这里会按大端方式编码：

- int16
- int32
- int64
- uint16
- uint32
- uint64
- float
- double
- timestamp

### 为什么 row key 用字符串，而 value 用字节

因为两者在语义上完全不同：

- row key 需要按业务规则拼接、排序
- value 只是列值载荷

---

## 11. `open_arrow_stream()`：真正打开 Arrow IPC Stream

位置：[converter.cc:L377-L381](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L377-L381)

### 它做了什么

1. 打开本地文件
2. 用 Arrow IPC Stream Reader 打开

### 为什么这很薄

因为真正复杂的不是“打开文件”，而是：

- 读了以后如何解释 schema
- 如何构建排序索引
- 如何做第二遍回放

所以打开文件本身只是一个很薄的适配层。

---

## 12. 第一遍扫描：`build_sort_index()` 是整份文件最关键的函数之一

位置：[converter.cc:L385-L513](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L385-L513)

如果你只能认真读 `converter.cc` 里的一个函数，那建议先读这个。

### 它的职责

> 读取整个 Arrow 文件，生成排序索引，并缓存后续第二遍需要用到的 batch。

### 这个函数输出两份最重要的数据

1. `index_out`：也就是所有 `SortEntry`
2. `batches_out`：过滤后的所有 `RecordBatch`

---

### 12.1 第一步：打开 Arrow Reader

逻辑在 [converter.cc:L395-L400](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L395-L400)。

如果这里失败，通常会映射成：

- `ARROW_FILE_ERROR`

---

### 12.2 第二步：计算过滤后的 schema

逻辑在 [converter.cc:L401-L410](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L401-L410)。

### 为什么先算过滤后的 schema

因为列删除之后，rowKeyRule 中的列索引必须针对“删完列后的 schema”进行解释。

这是本实现的一个非常重要的设计选择：

- 调用方不需要手动根据 `_hoodie` 排除列去重新计算 rowKeyRule 索引
- 系统自己先做过滤，再按过滤后的 schema 理解索引

---

### 12.3 第三步：校验 rowKeyRule 索引是否合法

逻辑在 [converter.cc:L413-L446](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L413-L446)。

这里会检查两件事：

1. rowKeyRule 引用的列索引是否越界
2. 被引用列的 Arrow 类型是否支持参与 row key 构建

### 为什么只检查“被引用列”

因为 row key 只依赖那些列。

没被 rowKeyRule 用到的列，即使类型复杂，也不影响 row key 构建本身。

---

### 12.4 第四步：只收集“真正被 rowKeyRule 用到的列”

逻辑在 [converter.cc:L422-L439](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L422-L439)。

这里有两个变量值得记住：

- `referenced_cols`
- `referenced_col_indices`

### 设计意义

rowKeyRule 不一定引用很多列。

所以第一遍扫描时没有必要每一行都遍历整个 schema。

只遍历真正用到的列可以减少大量无意义转换。

---

### 12.5 第五步：逐 batch 读取

逻辑在 [converter.cc:L455-L480](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L455-L480)。

### 这里做了两件关键事

1. 如果需要，先对 batch 删除列
2. 把 batch 缓存在 `batches_out`

### 为什么必须缓存 batch

因为第二遍写入还要回到原来的行里取实际列值。

所以这套算法不是流式单遍写法，而是：

- 第一遍缓存数据
- 第二遍再用

这也是它内存占用较高的根本原因。

---

### 12.6 第六步：逐行生成 row key

逻辑在 [converter.cc:L482-L506](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L482-L506)。

流程是：

1. 清空 `fields`
2. 对 rowKeyRule 需要的列调用 `scalar_to_string()`
3. 用 `RowKeyBuilder.build_checked()` 生成 row key
4. 如果 row key 非法，返回错误
5. 把 `(row_key, batch_idx, row_idx)` 放入 `index_out`

### 这里最重要的理解

第一遍只关心：

- row key 是什么
- 数据在原文件哪里

而不是先构造最终 cell。

---

### 12.7 第七步：内存预算

逻辑散落在：

- [converter.cc:L475-L479](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L475-L479)
- [converter.cc:L500-L503](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L500-L503)

### 它在预算什么

1. batch 的底层 Arrow buffer 大小
2. 每个 `SortEntry` 以及 `row_key` 字符串的大小

### 为什么重要

如果输入很大，第一遍是最容易爆内存的阶段。

所以预算主要盯第一遍。

---

## 13. `convert()`：总控函数怎么读

位置：[converter.cc:L579-L836](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L579-L836)

你可以把它理解成一个大总控流程：

1. 输入校验
2. rowKeyRule 编译
3. 列排除列表构建
4. 第一遍扫描构建排序索引
5. 排序
6. 打开 HFileWriter
7. 第二遍回放写入
8. finish
9. 汇总统计

---

### 13.1 第一步：参数校验

逻辑在 [converter.cc:L587-L616](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L587-L616)。

主要检查：

- Arrow 路径是否为空
- HFile 路径是否为空
- Arrow 文件是否存在
- rowKeyRule 是否为空
- rowKeyRule 是否能编译

### 这一步要注意

这里的 `rowKeyRule` 只是“编译成功”，还没有真正跑在每一行数据上。

运行时数据问题仍然会在第一遍扫描时暴露。

---

### 13.2 第二步：构建列删除列表

逻辑在 [converter.cc:L619-L642](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L619-L642)。

注意它会额外打开一次 Arrow schema reader。

### 为什么这样做

因为这里只是想看 schema，不想真正读数据。

这样可以提前知道：

- 哪些列会被删掉
- 删除后 rowKeyRule 的列索引怎么理解

---

### 13.3 第三步：第一遍扫描

逻辑在 [converter.cc:L644-L663](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L644-L663)。

它调用 `build_sort_index()`。

返回后会拿到：

- `sort_index`
- `batches`

并更新：

- `arrow_batches_read`
- `arrow_rows_read`

---

### 13.4 第四步：过滤空 row key

逻辑在 [converter.cc:L665-L680](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L665-L680)。

### 为什么会出现空 row key

如果某些行在 row key 构建时被判定为空，系统会在第一遍先保留一个空 key 条目，再在这里统一删掉。

### 这里的一个细节

如果有内存预算，还会同步释放这些被丢弃条目的预算。

---

### 13.5 第五步：排序

逻辑在 [converter.cc:L682-L689](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L682-L689)。

排序键只有一个：

- `row_key`

使用的是 `stable_sort`。

### 为什么用稳定排序

如果两个 row key 恰好相同，那么它们的相对顺序仍保留输入顺序。

这和后面的“重复 row key 保留第一条”策略能配合起来。

---

### 13.6 第六步：打开 HFileWriter

逻辑在 [converter.cc:L698-L720](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L698-L720)。

注意这里强制设置：

- `wo.sort_mode = PreSortedVerified`

### 为什么一定要这样

因为 `converter.cc` 已经自己完成排序了。

所以这里不应该再让 `writer.cc` 进行 AutoSort。

否则会：

- 重复排序
- 增加内存消耗
- 复杂化错误处理

---

## 14. 第二遍写入：把排序结果真正变成 HFile

逻辑在 [converter.cc:L722-L809](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L722-L809)

这也是 `converter.cc` 里最值得细读的部分。

### 核心思想

不是“每个 `SortEntry` 直接写一条 KV”，而是：

> 先把同一 row key 的所有数据行收集起来，再统一按 qualifier 排序和去重后写入。

---

### 14.1 外层循环：按 row key 分组

看这段结构：

- `i`
- `j`
- `while sort_index[j].row_key == row_key`

这意味着系统会把：

- 相同 row key 的所有 `SortEntry`

聚成一个逻辑组。

---

### 14.2 内层循环：把这组 row key 的所有列都取出来

逻辑在 [converter.cc:L735-L755](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L735-L755)。

做的事情：

1. 找到原始 batch 和 row
2. 遍历该行的所有列
3. Null 列跳过
4. 用 `scalar_to_bytes()` 把列值转为 value bytes
5. 构造 `GroupedCell{qualifier, value}`

### 为什么这里直接遍历所有列

因为经过列排除后，batch 中剩下的列都视为要输出成 qualifier/value。

这和第一遍不同。

第一遍只关心 rowKeyRule 需要的列；
第二遍关心所有要落盘的业务列。

---

### 14.3 重复 row key 检测

逻辑在 [converter.cc:L758-L776](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L758-L776)。

如果同一个 row key 来自多条 Arrow 行：

- 打一条 group-level warning
- `duplicate_key_count++`

### 为什么只打一条 group-level warning

因为如果每个 qualifier 都打一条，日志会被刷爆。

这里的设计是：

- 每个冲突 row key 只打一条摘要式警告

---

### 14.4 调 `append_grouped_row_cells()`

这是第二遍真正与 `writer.cc` 连接的点。

调用位置在 [converter.cc:L778-L783](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L778-L783)。

---

## 15. `append_grouped_row_cells()`：同一 row key 的 cell 怎样写

位置：[converter.cc:L517-L575](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L517-L575)

它负责：

1. 按 qualifier 排序
2. 去掉重复 qualifier
3. 逐条调用 `writer.append(...)`

### 15.1 为什么要按 qualifier 排序

因为 HBase 读取时要求：

- 同一 row 下的 cell 顺序稳定

而这里选择 qualifier 递增顺序，保证输出确定性。

---

### 15.2 为什么同一 row key 下要去重 qualifier

因为如果多个 Arrow 行映射成同一 row key，而且还带来了相同 qualifier：

- 输出 HFile 时必须决定留谁

当前策略是：

- 保留第一个
- 后续重复 qualifier 跳过

### 这和前面的稳定排序策略是联动的

因为 row key 冲突组内仍保持稳定顺序，
所以“保留第一个”是可预测的。

---

### 15.3 timestamp 怎么定

逻辑在 [converter.cc:L555-L557](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L555-L557)。

如果 `default_timestamp > 0`：

- 用它

否则：

- 用当前系统时间（毫秒）

这说明 `converter.cc` 默认是可以在没有显式时间列的情况下，为所有输出 cell 统一打时间戳的。

---

### 15.4 真正写入 HFile 的那一行

位置：[converter.cc:L562-L566](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L562-L566)

就是：

```text
writer.append(row, cf, qualifier, ts, value)
```

这行是整条转换链路和 `writer.cc` 的真正连接点。

---

## 16. 第二遍错误处理策略怎么理解

逻辑在 [converter.cc:L784-L800](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L784-L800)。

### 它把错误分成两类

#### 致命错误

例如：

- `SORT_ORDER_VIOLATION`

一旦发生，说明：

- 排序索引有逻辑问题
- 或 writer 的有序性假设被破坏

此时必须中止。

#### 非致命错误

例如：

- 某个 row group 写入失败
- 某些列被跳过

这类错误会：

- 打 warning
- 增加 skipped 计数
- 继续处理后面的 row group

### 为什么这样设计

因为转换工具既要保证核心文件结构正确，
又要允许在某些容错策略下尽量多产出可用数据。

---

## 17. `writer->finish()` 之后还做了什么

逻辑在 [converter.cc:L811-L836](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L811-L836)。

### 做的事情

1. 检查 `finish()` 是否成功
2. 记录 `write_ms`
3. 读取最终 HFile 文件大小
4. 记录总耗时
5. 打印转换完成日志

### 所以 `ConvertResult` 最后包含哪些主要统计

- Arrow batches / rows
- kv_written_count
- kv_skipped_count
- duplicate_key_count
- hfile_size_bytes
- sort_ms
- write_ms
- elapsed_ms

这就是 CLI 最后 `summary()` 的来源。

---

## 18. 为什么说这是“两遍扫描而不是一遍流式转换”

这个问题很重要。

### 一遍流式转换的问题

如果直接读一行写一行：

- 输入 Arrow 不保证有序
- 写出的 HFile 很可能乱序

而 HFile 强依赖 key 顺序。

### 两遍扫描的解决思路

第一遍：

- 不急着写
- 先得到每一行的 row key
- 建立排序索引

第二遍：

- 按排好序的 row key 回去取数据
- 再写 HFile

### 优点

- 逻辑简单
- 结果稳定
- 与 HFileWriter 的接口天然兼容

### 代价

- 需要缓存 batch
- 需要缓存 sort index
- 大文件内存压力高

这也是为什么未来如果做更重的性能优化，通常会考虑外排序 / spill-to-disk。

---

## 19. 这个文件和其他核心文件的关系

### 它依赖谁

- [row_key_builder.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc)
  - 负责 row key 规则执行
- [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc)
  - 负责真正写 HFile
- [convert_options.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/convert_options.h)
  - 负责参数/结果/错误码定义
- Arrow C++ API
  - 负责读 IPC Stream

### 谁依赖它

- JNI `hfile_jni.cc`
- `arrow-to-hfile` Java 工具链

所以它处在一个很典型的位置：

```text
上游：Java / JNI / CLI
中间：converter.cc
下游：writer.cc
```

---

## 20. 如果你要调试 `converter.cc`，优先从哪里下手

### 场景 1：rowKeyRule 相关错误

优先看：

- `convert()` 开头的 compile
- `build_sort_index()`
- `row_key_builder.cc`

---

### 场景 2：输出 HFile 中列不对

优先看：

- `build_removal_indices()`
- 第二遍内层列遍历
- `append_grouped_row_cells()`

---

### 场景 3：重复 row key / qualifier 行为不符合预期

优先看：

- `stable_sort(sort_index)`
- `source_rows > 1` 逻辑
- `append_grouped_row_cells()` 里的 qualifier 去重

---

### 场景 4：内存占用高

优先看：

- `build_sort_index()` 对 batch 的缓存
- `sort_index` 大小
- `MemoryBudget`

---

### 场景 5：最后 HFile 结构读不出来

这时通常不是 `converter.cc` 自己的主要问题，而要连同：

- `writer.cc`
- `bloom/`
- `index/`
- `meta/`

一起看。

---

## 21. 对不熟 C++ 的读者，最推荐的阅读顺序

第一次读：

1. [convert()](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L579-L836)
2. [build_sort_index()](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L385-L513)
3. [append_grouped_row_cells()](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L517-L575)

第二次读：

4. [scalar_to_string()](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L207-L248)
5. [scalar_to_bytes()](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L252-L373)
6. [build_removal_indices()](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L72-L116)

第三次读：

7. 和 [row_key_builder.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc) 对照看
8. 和 [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc) 对照看

---

## 22. 最后一段，用一句话把它记住

如果要把 `converter.cc` 用一句话概括：

> 它是整个项目里负责把 Arrow 表数据整理成“有序、可落盘、符合 HBase cell 结构”的中间编排器。

所以你以后如果想回答下面这些问题：

- 为什么一行 Arrow 最后会变成 4 条或 20 条 KV？
- 为什么 `_hoodie_*` 被删掉了？
- 为什么相同 row key 只保留第一条？
- 为什么一定要先排序再写？

答案大概率都在这个文件里。
