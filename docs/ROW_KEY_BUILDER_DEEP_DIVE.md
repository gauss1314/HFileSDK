# row_key_builder.cc 深度拆解

这份文档专门解释：

- [row_key_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.h)
- [row_key_builder.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc)

如果说 `converter.cc` 负责把 Arrow 文件“编排”成 HFile，可是 **真正决定“每一行 Arrow 最终用什么 row key”** 的，是 `row_key_builder.cc`。

所以这个模块虽然不大，却非常关键。

它主要回答两个问题：

1. `rowKeyRule` 这串文本到底怎么解析？
2. 对某一行数据，最终 row key 是怎么一步一步拼出来的？

---

## 1. 先给它一句话定义

`row_key_builder.cc` 的职责是：

> 把一条 `rowKeyRule` 文本规则编译成内部段列表，然后根据一行数据的字段值，生成最终的 row key 字符串。

这里的“编译”不是 C++ 编译器那种编译，而是：

- 先解析文本
- 校验是否合法
- 转成便于运行时执行的结构

---

## 2. 为什么它重要

整个 HFile 链路都围绕 row key 展开：

- `converter.cc` 第一遍要先生成 row key
- 然后按 row key 排序
- 第二遍再按排好序的 row key 写 HFile

换句话说，row key 一旦理解错：

- 排序会错
- 重复 key 判断会错
- 最终 HFile 逻辑也会错

所以你可以把这个模块理解成：

> Arrow → HFile 转换过程中最关键的“业务规则解释器”

---

## 3. 先看头文件：它定义了什么概念

先看 [row_key_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.h)。

里面最重要的是两个对象：

- `RowKeySegment`
- `RowKeyBuilder`

---

## 4. `RowKeySegment`：一条规则里的一个片段

位置：[row_key_builder.h:L35-L49](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.h#L35-L49)

一条 `rowKeyRule` 不是只能有一个字段。

它支持这样的形式：

```text
SEG1#SEG2#SEG3...
```

也就是说：

- 一条完整规则
- 由多个 segment 组成

每个 segment 会被编译成一个 `RowKeySegment` 对象。

### 它包含哪些信息

- `type`
  - 这一段是什么类型
- `name`
  - 原始段名
- `col_index`
  - 这一段从第几列取值
- `reverse`
  - 最终是否反转
- `pad_len`
  - 目标长度
- `pad_right`
  - 左填充还是右填充
- `pad_char`
  - 用什么字符填充
- `encode_kind`
  - 是否做 `long()` / `short()` 编码
- `transforms`
  - 是否有 `hash()` 之类的前置变换

### 也就是说，一个 segment 不只是“取哪一列”

它还可能同时包含：

- 取值
- 编码
- 填充
- 反转

---

## 5. segment 的几种类型

在 [row_key_builder.h:L36-L38](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.h#L36-L38) 定义了几种核心类型。

### 5.1 `ColumnRef`

最普通的情况。

含义：

- 直接从某一列取值

例如：

```text
REFID,0,true,0,LEFT
```

这类一般就是 `ColumnRef`。

---

### 5.2 `Random`

特殊段名：

- `$RND$`
- `RANDOM`
- `RANDOM_COL`

它不读取业务列，而是生成随机数字串。

### 用途

通常用于：

- 增加 row key 随机性
- 分散热点

---

### 5.3 `Fill`

特殊段名：

- `FILL`
- `FILL_COL`

它的含义是：

- 初始值为空串
- 但仍允许后续做 pad/reverse

### 你可以把它理解成

“我不取数据，但我要插入一个固定长度的填充段”。

---

### 5.4 `EncodedColumn`

这是最特殊的一类：

- `long(...)`
- `short(...)`

它的作用不是直接拿原值，而是先做编码，再参与 row key 拼接。

例如：

```text
long()
short(hash)
```

---

## 6. 规则文本的语法到底是什么

头文件注释里已经写了格式，位置：

- [row_key_builder.h:L14-L34](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.h#L14-L34)

### 一条完整规则格式

```text
SEG1#SEG2#SEG3...
```

### 每个 segment 的格式

```text
name,index,isReverse,padLen[,padMode][,padContent]
```

### 字段解释

- `name`
  - 段名称
  - 可以是列名，也可以是特殊表达式
- `index`
  - 这一段要用哪一列
- `isReverse`
  - `true` / `false`
- `padLen`
  - 目标长度
- `padMode`
  - `LEFT` / `RIGHT`
- `padContent`
  - 填充字符，默认 `'0'`

---

## 7. 用你的规则来拆一遍

你前面实际用到的是：

```text
REFID,0,true,0,LEFT,,,
```

这个规则只包含一个 segment。

### 拆开看

- `name = REFID`
- `index = 0`
- `isReverse = true`
- `padLen = 0`
- `padMode = LEFT`
- `padContent = 默认 '0'`

### 含义

1. 从字段数组的第 `0` 列取值
2. 不做 padding（因为 `padLen = 0`）
3. 最后把字符串反转

如果输入值是：

```text
47820206294
```

最终 row key 就会变成：

```text
49260202874
```

### 这是为什么

因为 `apply_segment()` 里会在最后执行 `reverse`。

---

## 8. `compile()`：规则编译的主入口

位置：[row_key_builder.cc:L188-L289](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L188-L289)

这是整个模块最重要的函数之一。

它的职责是：

> 把一条规则字符串变成 `RowKeyBuilder` 内部的 `segments_`

### 它的输出

返回：

- `RowKeyBuilder`
- `Status`

如果规则非法，就在编译阶段直接失败。

---

## 9. `compile()` 第一步：空规则检查

逻辑在 [row_key_builder.cc:L191-L197](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L191-L197)。

如果：

- 规则为空
- 或 split 后没有 segment

直接返回错误。

### 为什么要尽早失败

因为 row key 是整个转换流程的前提。

一旦规则本身都不合法，后面根本没有继续执行的意义。

---

## 10. `split_sv()`：为什么这里大量使用 `string_view`

位置：[row_key_builder.cc:L15-L31](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L15-L31)

### 作用

把字符串按分隔符切开，返回 `string_view` 列表。

### 为什么不用一开始就生成 `std::string`

因为编译阶段主要是解析，不需要立刻拷贝所有文本。

`string_view` 的好处是：

- 少一次内存分配
- 少一次拷贝

这是一种典型的“解析阶段先轻量切片，真正需要持久化时再复制”的写法。

---

## 11. `compile()` 第二步：按 `#` 拆 segment

逻辑在 [row_key_builder.cc:L194-L200](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L194-L200)。

这意味着：

```text
A#B#C
```

会变成 3 个 segment。

### 一个重要细节

空 segment 会跳过。

也就是说，尾部多一个 `#` 不会立刻报错。

---

## 12. `compile()` 第三步：按 `,` 拆字段

逻辑在 [row_key_builder.cc:L202-L208](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L202-L208)。

每个 segment 至少要求 4 个字段：

- name
- index
- isReverse
- padLen

如果少于 4 个，直接报错。

这也是为什么你的规则虽然尾部有多余逗号，但前 4 段必须完整。

---

## 13. `compile()` 第四步：解析 `index`

逻辑在 [row_key_builder.cc:L213-L226](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L213-L226)。

### 它要求什么

- 必须是合法整数
- 必须 `>= 0`

### 注意

这里的 index 只是“规则里引用第几列”，
并不在编译阶段判断“这列是否真的存在”。

因为：

- 编译规则时还没看到真正的 Arrow schema

真正的越界校验会在 `converter.cc` 的第一遍扫描里完成。

---

## 14. `compile()` 第五步：解析 `isReverse`

逻辑在 [row_key_builder.cc:L228-L234](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L228-L234)。

要求非常严格：

- 只能是 `true`
- 或 `false`

其他任何值都会失败。

### 为什么这是好事

因为如果配置错误却被静默吞掉，后面生成的 row key 会完全变样，而且很难排查。

现在这种严格校验更适合生产使用。

---

## 15. `compile()` 第六步：解析 `padLen`

逻辑在 [row_key_builder.cc:L236-L249](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L236-L249)。

要求：

- 必须是整数
- 必须 `>= 0`

### `padLen = 0` 的含义

就是不做 padding。

这也是你当前规则的情况。

---

## 16. `compile()` 第七步：解析 `padMode` 和 `padContent`

逻辑在：

- `padMode`：[row_key_builder.cc:L251-L259](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L251-L259)
- `padContent`：[row_key_builder.cc:L261-L263](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L261-L263)

### `padMode`

只能是：

- `LEFT`
- `RIGHT`

默认是 `LEFT`。

### `padContent`

- 如果没给，默认 `'0'`
- 如果给了，只取第一个字符

### 举个例子

```text
REFID,0,false,10,LEFT,9
```

表示：

- 不足 10 位时
- 左侧补 `'9'`

---

## 17. `compile()` 第八步：识别特殊类型

逻辑在 [row_key_builder.cc:L265-L276](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L265-L276)。

判断顺序是：

1. 先看是不是编码表达式
2. 否则看是不是随机段
3. 否则看是不是填充段
4. 否则就当普通列引用

### 为什么先判断编码表达式

因为像：

```text
long(hash)
```

既不像列名，也不是特殊保留字。

它必须优先走表达式解析。

---

## 18. `parse_encode_expr()`：编码表达式是怎么解析的

位置：[row_key_builder.cc:L51-L96](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L51-L96)

### 它支持什么

外层只支持：

- `long(...)`
- `short(...)`

内层当前只支持：

- `hash`

### 举例

#### `long()`

表示：

- 把原值解析为 int64
- 转成 8 字节大端
- 再做 Base64 编码

#### `short(hash)`

表示：

1. 先做 `hash`
2. 把结果当 int16
3. 再做 2 字节大端
4. 再做 Base64 编码

### 为什么用这种表达方式

因为历史 Java 规则里就有这类写法；
这里是为了兼容那套规则语义。

---

## 19. `java_hash_numeric_string()`：为什么叫 Java 兼容

位置：[row_key_builder.cc:L98-L111](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L98-L111)

这个函数的目标是：

> 尽量模拟 Java 侧历史逻辑对数字字符串的 hash 方式

它要求输入必须是纯数字字符串。

否则直接失败。

### 这点非常重要

以前系统容易把解析失败静默降级成 0，
现在改成了显式失败，这样更安全。

---

## 20. `encode_long_or_short()`：编码段到底怎么执行

位置：[row_key_builder.cc:L149-L184](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L149-L184)

### 它的处理顺序

1. 先应用 transforms
   - 例如 `hash`
2. 再根据 `encode_kind` 决定走：
   - `Int64Base64`
   - `Int16Base64`

### `long()` 路径

流程：

```text
原始字符串
 -> parse_i64
 -> 写成 8 字节大端
 -> Base64
```

### `short()` 路径

流程：

```text
原始字符串
 -> parse_i16
 -> 写成 2 字节大端
 -> Base64
```

### 为什么还要 Base64

因为 row key 最终仍是字符串。

所以二进制编码后，还要转成一个可以拼接进字符串 row key 的表示。

---

## 21. `parse_i64()` 和 `parse_i16()`：为什么这里这么严格

位置：

- [parse_i64](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L113-L116)
- [parse_i16](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L118-L128)

### 它们要求

- 必须整个字符串都能解析
- 不能只解析前半截
- `short()` 还要做范围检查

### 例如

- `"123abc"`：失败
- `"40000"` 作为 int16：失败

### 为什么要这样

因为 row key 错误比普通 value 错误更危险。

一旦 row key 错了：

- 排序会错
- 去重会错
- Region 路由也可能错

所以宁可失败，也不应该悄悄容错。

---

## 22. `max_col_index_`：为什么编译阶段就要记录最大列索引

逻辑在 [row_key_builder.cc:L277-L280](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L277-L280)。

### 作用

记录规则里引用过的最大列索引。

### 它后面被谁用

在 `converter.cc` 里，第一遍扫描会拿这个值来：

- 预分配字段数组大小
- 先做越界判断

### 这是一个典型的“编译结果携带执行辅助信息”的设计

---

## 23. `build()` 和 `build_checked()`：为什么要分两个接口

位置：

- [build](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L293-L298)
- [build_checked](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L300-L327)

### `build()`

更宽松：

- 内部调用 `build_checked()`
- 如果失败，就直接返回空串

### `build_checked()`

更严格：

- 失败时返回明确 `Status`

### 为什么现在主链路更偏向 `build_checked()`

因为 row key 错误是高风险错误。

相比“悄悄返回空串”，显式错误更容易发现问题。

---

## 24. `build_checked()`：真正生成 row key 的执行过程

位置：[row_key_builder.cc:L300-L327](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L300-L327)

你可以把它看成一个小型解释器。

### 它对每个 segment 按顺序执行

#### 第 1 步：如果是 `Random`

调用 `random_digits(seg.pad_len)`。

这类 segment 不看输入字段。

#### 第 2 步：如果是 `Fill`

先取空串。

后面仍然允许做：

- padding
- reverse

#### 第 3 步：如果是普通列或编码列

先根据 `seg.col_index` 从 `fields` 中取值。

如果越界，直接报错：

```text
rowKeyRule references missing field index X
```

#### 第 4 步：如果是编码列

调用 `encode_long_or_short()`

#### 第 5 步：统一调用 `apply_segment()`

做：

- padding
- reverse

#### 第 6 步：追加到最终输出字符串

---

## 25. `apply_segment()`：padding 和 reverse 的真正执行顺序

位置：[row_key_builder.cc:L331-L348](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L331-L348)

### 顺序非常关键

它是：

1. 先 padding
2. 再 reverse

### 为什么要特别强调

因为很多人会以为“先反转再补齐”。

但这里明确是：

```text
pad first -> reverse later
```

这和注释中提到的 Java 历史行为保持一致。

---

## 26. 用你的规则实际走一遍

你的规则是：

```text
REFID,0,true,0,LEFT,,,
```

假设输入字段数组：

```text
fields[0] = "47820206294"
```

### 编译阶段

会得到一个 `RowKeySegment`：

- type = `ColumnRef`
- name = `"REFID"`
- col_index = `0`
- reverse = `true`
- pad_len = `0`
- pad_right = `false`
- pad_char = `'0'`

### 执行阶段

#### 取值

```text
val = "47820206294"
```

#### 编码

没有，因为不是 `EncodedColumn`

#### padding

没有，因为 `pad_len = 0`

#### reverse

得到：

```text
"49260202874"
```

#### 输出

最终 row key 就是：

```text
49260202874
```

### 所以你当前规则的本质

不是“复杂拼接规则”，而是：

> 取第 0 列 REFID，再整体反转

---

## 27. `random_digits()`：为什么随机数范围是 0–8

位置：[row_key_builder.cc:L352-L360](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L352-L360)

### 注意这里不是 0–9

分布是：

```text
uniform_int_distribution(0, 8)
```

### 为什么不是 9

注释里写得很清楚：

- 为了匹配历史 Java 版本 `nextInt(9)`

所以这里是“兼容性优先”，而不是“直觉优先”。

---

## 28. `split_row_value()`：它在当前项目里还重要吗

位置：[row_key_builder.cc:L365-L367](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L365-L367)

### 它的作用

把 pipe 分隔字符串拆成字段数组。

### 当前你会在哪类场景看到它

如果某些历史链路仍然把整行先拼成一个：

```text
a|b|c|d
```

再交给 RowKeyBuilder，就会用到它。

### 但在当前 `converter.cc` 里

更常见的是直接构造：

- `std::vector<std::string_view> fields`

不一定需要先把整行组装成 pipe 字符串。

所以这个函数更像“历史兼容辅助函数”。

---

## 29. 这个模块和 `converter.cc` 的关系怎么理解

`converter.cc` 会这样使用它：

1. 先 `RowKeyBuilder::compile(rule)`
2. 第一遍扫描里对每行准备 `fields`
3. 调 `build_checked(fields, &rk)`
4. 把生成的 `rk` 放进 `SortEntry`

所以：

- `converter.cc` 决定“什么时候生成 row key”
- `row_key_builder.cc` 决定“row key 具体怎么生成”

---

## 30. 如果 row key 结果不符合预期，优先看哪里

### 场景 1：规则编译失败

优先看：

- `compile()`
- `parse_encode_expr()`

---

### 场景 2：生成结果和你预期不同

优先看：

- `build_checked()`
- `apply_segment()`

尤其注意：

- padding 在前
- reverse 在后

---

### 场景 3：`long()/short()` 编码出错

优先看：

- `encode_long_or_short()`
- `parse_i64()`
- `parse_i16()`

---

### 场景 4：带 `hash()` 的规则结果异常

优先看：

- `java_hash_numeric_string()`

并确认输入是不是纯数字字符串。

---

## 31. 对不熟 C++ 的读者，最推荐阅读顺序

第一次读：

1. [row_key_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.h)
2. `RowKeySegment`
3. `compile()`
4. `build_checked()`

第二次读：

5. `parse_encode_expr()`
6. `encode_long_or_short()`
7. `apply_segment()`

第三次读：

8. 带着自己的真实规则，例如：
   - `REFID,0,true,0,LEFT,,,`
   - 手工模拟一遍执行

---

## 32. 最后一段，用一句话记住它

如果要用一句话概括 `row_key_builder.cc`：

> 它是整个项目里负责把“文本化 rowKeyRule”解释成“可执行 row key 生成逻辑”的规则引擎。

所以你以后只要想回答：

- 为什么这个 row key 长这样？
- 为什么这里被 reverse 了？
- 为什么 `short(hash)` 会生成这种 Base64 字符串？

答案大概率都在这个文件里。
