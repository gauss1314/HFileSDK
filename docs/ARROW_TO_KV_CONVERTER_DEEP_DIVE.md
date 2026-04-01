# arrow_to_kv_converter.cc 深度拆解

这份文档专门解释：

- [arrow_to_kv_converter.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.h)
- [arrow_to_kv_converter.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc)

它是整个项目里“Arrow 表数据怎么变成 HBase `KeyValue`”的核心模块。

如果说：

- `row_key_builder.cc` 负责生成 row key
- `converter.cc` 负责两遍扫描、排序和分组

那么 `arrow_to_kv_converter.cc` 负责的是另一件非常基础的事：

> 一行 Arrow 数据，到底要翻译成几条 `KeyValue`？每条 `KeyValue` 的 row / family / qualifier / value 从哪里来？

---

## 1. 先给它一句话定义

`arrow_to_kv_converter.cc` 的职责是：

> 把一个 Arrow `RecordBatch`，按预定义映射模式，转换成若干条 HBase `KeyValue`，并通过回调逐条交给上层。

它本身不做：

- 文件读写
- HFile block 编码
- HFile 文件落盘

它只做“数据形态转换”。

---

## 2. 为什么这个模块重要

Bulk Load 场景下，上层通常直接把 Arrow `RecordBatch` 交给它。

它决定：

- 一行数据到底对应几条 KV
- 哪一列是 row key
- 哪一列是 family
- 哪一列是 qualifier
- value 按什么字节格式编码

所以如果你以后遇到：

- 生成的 KV 数不符合预期
- 某个列没有输出
- 某个值在 HBase 里看起来不像原始值

第一时间应该看这个模块。

---

## 3. 模块整体设计非常清晰：3 种模式

在头文件 [arrow_to_kv_converter.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.h) 中，公开了 3 个核心入口：

- `convert_wide_table(...)`
- `convert_tall_table(...)`
- `convert_raw_kv(...)`

你可以把它们理解成 3 种数据建模方式：

### 3.1 Wide Table

一行 Arrow → 多条 HBase KV

适合：

- 一行有多个业务列
- 所有列共享一个 row key
- 所有列共享一个列族

### 3.2 Tall Table

一行 Arrow → 一条 HBase KV

适合：

- 每行已经显式给出：
  - row key
  - family
  - qualifier
  - timestamp
  - value

### 3.3 RawKV

一行 Arrow → 一条 HBase KV

但这里的 key 列已经是“预编码好的 HBase 原始 key bytes”。

适合：

- 上游已经自己构造好了 HBase key 格式
- 当前模块只负责拆解并转成 `KeyValue`

---

## 4. 先看头文件：配置对象分别表示什么

---

## 4.1 `WideTableConfig`

位置：[arrow_to_kv_converter.h:L19-L25](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.h#L19-L25)

字段：

- `row_key_column`
  - 哪一列是 row key，默认 `__row_key__`
- `column_family`
  - 整张宽表统一使用哪个列族，默认 `cf`
- `default_timestamp`
  - 如果没指定时间，用什么默认时间戳
- `skip_null_columns`
  - null 列是否直接跳过

### 直觉理解

宽表模式下，除了 row key 列，其余列都会被看成：

```text
列名 -> qualifier
列值 -> value
```

---

## 4.2 `TallTableConfig`

位置：[arrow_to_kv_converter.h:L27-L35](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.h#L27-L35)

字段：

- `col_row_key`
- `col_cf`
- `col_qualifier`
- `col_timestamp`
- `col_value`

### 直觉理解

Tall Table 模式要求 schema 自己就把 HBase 需要的维度显式拆开了。

所以它更像“一张已经高度贴近 HBase 模型的表”。

---

## 4.3 `KVCallback`

位置：[arrow_to_kv_converter.h:L37-L38](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.h#L37-L38)

定义：

```text
Status(const KeyValue&)
```

### 为什么设计成回调，而不是返回一个 vector

因为一批 Arrow 行可能会转出很多 KV。

如果先全部收集成一个大 vector：

- 内存压力更大
- 上层无法边生成边消费

回调设计允许上层：

- 立刻写入 HFile
- 或立刻路由到 Bulk Load writer

这是一种更流式的接口设计。

---

## 5. 这个文件的真实核心：`serialize_scalar_checked()`

位置：[arrow_to_kv_converter.cc:L21-L132](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L21-L132)

如果只能认真读这个文件的一个函数，那建议先读它。

### 作用

把 Arrow 某一行某一列的值，转换成一段字节。

### 为什么这是基础中的基础

因为无论是：

- row key 列
- family 列
- qualifier 列
- value 列
- timestamp 列

最后都必须先从 Arrow 单元格变成“项目内部可用的字节表示”。

---

## 6. `serialize_scalar_checked()` 的整体逻辑

### 第一步：清空输出

位置：[arrow_to_kv_converter.cc:L26-L27](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L26-L27)

它会先把 `out` 清空。

### 第二步：null 直接返回 OK

位置：[arrow_to_kv_converter.cc:L27](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L27)

这点很关键：

- null 不被视为“类型错误”
- 而是“没有值”

具体要不要跳过，由上层模式决定。

---

## 7. 为什么数值类型会写成大端字节

例如：

- int16：[arrow_to_kv_converter.cc:L40-L45](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L40-L45)
- int32：[arrow_to_kv_converter.cc:L46-L51](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L46-L51)
- int64：[arrow_to_kv_converter.cc:L52-L57](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L52-L57)

### 原因

项目里对数值 value 的选择不是“转成字符串”，而是“转成稳定的二进制表示”。

这样有几个好处：

- 表示更紧凑
- 与 HBase/HFile 的字节世界更一致
- 不依赖 locale 或格式化细节

### 为什么是大端

因为项目内部约定使用 `write_be16/32/64()` 系列函数。

这让所有数值 value 的编码方式统一。

---

## 8. 浮点数和时间戳怎么处理

### 8.1 float / double

逻辑在：

- float：[arrow_to_kv_converter.cc:L80-L86](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L80-L86)
- double：[arrow_to_kv_converter.cc:L87-L93](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L87-L93)

### 处理方式

1. 取出浮点值
2. 把其 bit pattern 拷贝成整数
3. 再按大端写出

也就是说：

- 存的不是字符串 `"3.14"`
- 而是 IEEE 754 位模式

---

### 8.2 timestamp

逻辑在 [arrow_to_kv_converter.cc:L116-L128](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L116-L128)

### 处理方式

1. 读原始时间值
2. 根据 Arrow timestamp unit 归一化到毫秒
3. 再写成 8 字节大端

### 为什么要归一化

因为 Arrow 的 timestamp unit 可能是：

- second
- milli
- micro
- nano

而 HBase/HFile 链路里最终统一使用毫秒。

---

## 9. 字符串和二进制类型怎么处理

### STRING / LARGE_STRING

逻辑在 [arrow_to_kv_converter.cc:L94-L104](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L94-L104)

处理方式：

- 直接取 view
- 按 UTF-8 原始字节输出

### BINARY / LARGE_BINARY

逻辑在 [arrow_to_kv_converter.cc:L105-L115](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L105-L115)

处理方式：

- 直接按原始字节拷贝

### 也就是说

这个模块不区分：

- “字符串 value”
- “原始二进制 value”

对 HFile 来说，它们最终都只是字节数组。

---

## 10. 为什么会返回 `NotSupported`

逻辑在 [arrow_to_kv_converter.cc:L129-L130](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L129-L130)

对于不支持的 Arrow 类型，当前策略是：

- 直接报错

而不是以前那种“静默转空值”。

### 这为什么更安全

因为如果：

- list
- struct
- map

这类复杂类型被悄悄转成空值，
会让数据看起来“成功写入了”，但实际上已经损坏了业务语义。

现在显式失败更可控。

---

## 11. `serialize_scalar()`：为什么还保留一个简化版

位置：[arrow_to_kv_converter.cc:L134-L140](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L134-L140)

这是一个兼容包装：

- 内部调用 `serialize_scalar_checked()`
- 如果失败，就返回空 vector

### 但现在主链路更推荐谁

更推荐：

- `serialize_scalar_checked()`

因为它能返回明确错误。

---

## 12. `convert_wide_table()`：一行为什么会变成多条 KV

位置：[arrow_to_kv_converter.cc:L144-L201](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L144-L201)

这就是 Wide Table 模式的核心逻辑。

### 它的模型是

```text
一行数据
  -> 一个 row key
  -> 固定列族
  -> 每个非 row-key 列都变成一个 qualifier/value
```

### 直觉例子

假设一行里有：

- `__row_key__ = row1`
- `name = alice`
- `age = 18`

那么会生成两条 KV：

1. `(row1, cf, name) -> alice`
2. `(row1, cf, age) -> 18 的字节表示`

---

## 13. `convert_wide_table()` 的执行顺序

### 第一步：找到 row key 列

逻辑在 [arrow_to_kv_converter.cc:L149-L153](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L149-L153)

如果找不到，直接报错。

### 第二步：确定默认时间戳

逻辑在 [arrow_to_kv_converter.cc:L155-L160](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L155-L160)

如果配置没给：

- 就用当前系统时间（毫秒）

### 第三步：逐行遍历

逻辑从 [arrow_to_kv_converter.cc:L165](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L165) 开始。

对每一行：

1. 先序列化 row key
2. 如果 row key 列是 null，就报错
3. 再遍历所有非 row-key 列

### 第四步：逐列发 KV

对于每一列：

- 列名就是 qualifier
- 列值就是 value
- family 用配置里的统一列族

最后通过 `callback(kv)` 交给上层。

---

## 14. 为什么 Wide Table 下 null 列通常会被跳过

逻辑在 [arrow_to_kv_converter.cc:L177-L180](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L177-L180)

由配置：

- `skip_null_columns`

控制。

### 这样做的意义

因为在 HBase 里，很多时候“没有这个 cell”比“写一个空值 cell”更自然。

所以默认行为是：

- null 列不生成 KV

---

## 15. `convert_tall_table()`：为什么一行只对应一条 KV

位置：[arrow_to_kv_converter.cc:L205-L286](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L205-L286)

Tall Table 模式和 Wide Table 模式完全不同。

### 它要求 schema 自己已经显式包含 HBase 维度

列名默认应该有：

- `row_key`
- `cf`
- `qualifier`
- `timestamp`
- `value`

### 所以一行天然就是一个完整 cell

不需要再“拆成多列”。

---

## 16. `convert_tall_table()` 的执行顺序

### 第一步：找到关键列索引

逻辑在 [arrow_to_kv_converter.cc:L210-L224](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L210-L224)

如果缺少：

- row_key
- cf
- qualifier
- value

会立即失败。

### 第二步：逐行读 4~5 个关键列

逻辑在 [arrow_to_kv_converter.cc:L226-L267](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L226-L267)

### 第三步：时间戳处理

如果 `timestamp` 列存在且能解析出 8 字节：

- 用列值

否则：

- 用当前系统时间

### 第四步：组装一条 `KeyValue`

逻辑在 [arrow_to_kv_converter.cc:L275-L283](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L275-L283)

然后回调给上层。

---

## 17. 为什么 Tall Table 模式比 Wide Table 更“贴近 HBase”

因为 Wide Table 模式里：

- qualifier 来自列名
- family 是统一固定值

而 Tall Table 模式里：

- family 就是输入列
- qualifier 也是输入列
- timestamp 也是输入列

它相当于把 HBase cell 模型直接表格化了。

所以如果你的上游已经明确知道每条 cell 的 family/qualifier/timestamp，
Tall Table 会更自然。

---

## 18. `convert_raw_kv()`：什么时候要用 RawKV

位置：[arrow_to_kv_converter.cc:L290-L347](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L290-L347)

RawKV 模式适用于：

> 上游已经自己构造好了 HBase 原始 key bytes。

也就是说，Arrow 里 key 列不是“业务字段”，而是“编码后的 HBase key”。

### 那这个模块负责什么

把这段原始 key bytes 再拆回：

- row
- family
- qualifier
- timestamp
- key type

再和 value 组合成 `KeyValue`。

---

## 19. `convert_raw_kv()` 的 key 格式假设是什么

代码里写得很清楚，逻辑分布在 [arrow_to_kv_converter.cc:L316-L334](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L316-L334)

它假设 key 的格式是：

```text
rowLen(2)
row
familyLen(1)
family
qualifier
timestamp(8)
type(1)
```

也就是标准 HBase key 结构的一种字节布局。

### 这说明 RawKV 模式对输入要求最苛刻

因为它不是“帮你生成 key”，而是“假设你已经生成好了”。

---

## 20. `convert_raw_kv()` 为什么有很多边界检查

例如：

- `key too short`
- `truncated row`
- `truncated family`
- `truncated qualifier+ts+type`

这些检查都在：

- [arrow_to_kv_converter.cc:L316-L329](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L316-L329)

### 为什么要这么严格

因为 RawKV 模式直接信任字节布局。

如果不做边界检查，就可能：

- 越界读
- 错误解码
- 生成损坏 KV

这类问题在 C++ 里尤其危险，所以这里的防御必须很强。

---

## 21. `callback(kv)`：为什么整个模块最终都回到这一步

在三种模式中，最终都会走到：

- `callback(kv)`

例如：

- Wide Table：[arrow_to_kv_converter.cc:L197](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L197)
- Tall Table：[arrow_to_kv_converter.cc:L283](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L283)
- RawKV：[arrow_to_kv_converter.cc:L344](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L344)

### 这说明模块设计的核心边界非常明确

它只负责：

- 生成 `KeyValue`

至于：

- 是立刻写入 HFile
- 还是送去 Bulk Load 分区
- 还是先暂存

全部交给上层决定。

这就是一个很干净的模块边界。

---

## 22. 三种模式的对比，一次记住

### Wide Table

- 一行 → 多条 KV
- qualifier = 列名
- family = 固定列族
- row key = 指定列

### Tall Table

- 一行 → 一条 KV
- row/family/qualifier/value 都来自显式列

### RawKV

- 一行 → 一条 KV
- key 已经是原始 HBase key bytes
- 当前模块只负责拆解

---

## 23. 和 `converter.cc` 的关系是什么

严格来说，它和 `converter.cc` 不是一条完全相同的路径。

### 在文件级 `convert()` 链路中

- `converter.cc` 自己会做 row key 构建、排序和 value 转换
- 不直接依赖 `ArrowToKVConverter`

### 在 Bulk Load 链路中

- `bulk_load_writer.cc` 的 `write_batch()`
- 会直接调用 `ArrowToKVConverter`

所以：

- `converter.cc` 更像“文件转换总控”
- `arrow_to_kv_converter.cc` 更像“RecordBatch 到 KeyValue 的模式化转换器”

两者有重叠的类型转换逻辑，但职责不同。

---

## 24. 如果输出的 KV 数量不对，应该先看哪里

### 场景 1：宽表模式下数量偏多/偏少

优先看：

- 是否把 row key 列也当成业务列输出了
- `skip_null_columns`
- 某些列是不是被 null 跳过了

也就是重点看 [convert_wide_table](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L144-L201)

---

### 场景 2：Tall Table 模式下某些行没输出

优先看：

- 必填列是否缺失
- row/cf/qualifier 是否是 null
- timestamp 是否解析异常

重点看 [convert_tall_table](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L205-L286)

---

### 场景 3：RawKV 读出来像损坏

优先看：

- key 的原始字节格式是否符合约定
- rowLen / famLen 是否写对
- qualifier / timestamp / type 是否完整

重点看 [convert_raw_kv](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.cc#L290-L347)

---

## 25. 对不熟 C++ 的读者，最推荐阅读顺序

第一次读：

1. [arrow_to_kv_converter.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/arrow_to_kv_converter.h)
2. `WideTableConfig`
3. `TallTableConfig`
4. `convert_wide_table()`
5. `convert_tall_table()`
6. `convert_raw_kv()`

第二次读：

7. `serialize_scalar_checked()`
8. 再回过头看三种模式如何调用它

第三次读：

9. 和 `bulk_load_writer.cc` 对照，看看生成的 KV 最后去了哪里

---

## 26. 最后一段，用一句话记住它

如果要用一句话概括 `arrow_to_kv_converter.cc`：

> 它是整个项目里负责把 Arrow `RecordBatch` 按 Wide / Tall / RawKV 三种模型翻译成 HBase `KeyValue` 的模式化转换器。

所以你以后只要想回答：

- 一行 Arrow 为什么会变成多条 KV？
- qualifier 为什么是列名？
- value 为什么是大端字节？
- RawKV 为什么会报 truncated family？

答案基本都在这个文件里。
