# 04 — Row Key 规则引擎

本文档覆盖 `src/arrow/row_key_builder.cc` 和 `row_key_builder.h`，以及 `converter.cc` 中列排除机制与 Row Key 构建的交互。

---

## 1. rowKeyRule 语法

### 1.1 总体格式

```
rowKeyRule = "SEG1#SEG2#SEG3#..."
```

段之间用 `#` 分隔。每段的格式：

```
name,index,isReverse,padLen[,padMode][,padContent]
```

| 字段 | 类型 | 说明 |
|------|------|------|
| name | 字符串 | 列名（信息性标签，也用于检测特殊段类型） |
| index | 整数 ≥ 0 | 在 Arrow Schema 中的列索引（0-based） |
| isReverse | true/false | 是否在 padding 之后反转字符串 |
| padLen | 整数 ≥ 0 | 目标长度；0 = 不 padding |
| padMode | LEFT/RIGHT | 填充方向，默认 LEFT |
| padContent | 单字符 | 填充字符，默认 '0' |

示例：

```
STARTTIME,0,false,10#IMSI,1,true,15#$RND$,2,false,4
```

这个规则生成的 Row Key 结构为：`[col[0] 左填充到10位][col[1] 右填充到15位后反转][4位随机数]`

### 1.2 特殊段类型

| name 匹配规则 | 类型 | 行为 |
|---|---|---|
| `$RND$`, `RND$`, `$RND`, `RANDOM`, `RANDOM_COL` | Random | 生成 padLen 个随机数字（0-8） |
| `FILL`, `FILL_COL` | Fill | 使用空字符串，仅应用 pad + reverse |
| `long(...)`, `short(...)` | EncodedColumn | 对列值做数值编码再 Base64 |
| 其他 | ColumnRef | 直接引用列值 |

匹配是**大小写不敏感**的（`iequal()` 函数）。

### 1.3 编码段（EncodedColumn）

`long(hash)` 和 `short(hash)` 是 Java 端 `UniverseHbaseBeanUtil` 的兼容实现：

- `short(hash)`：先对列值做 Java 风格的数值 hash（`java_hash_numeric_string`），得到 int16，再 Big-Endian 编码为 2 字节，最后 Base64 编码
- `long(hash)`：同上，但输出 int64 → 8 字节 → Base64

`java_hash_numeric_string()` 的算法精确复刻了 Java 端：

```cpp
part1 = value >> 32;
part2 = value & ((2LL << 32) - 1LL);
result = 31 * (31 * 1 + part1) + part2;
return (int16_t)(result % 65535);
```

---

## 2. 编译过程（RowKeyBuilder::compile）

`compile()` 将规则字符串解析为 `std::vector<RowKeySegment>`。

### 2.1 解析步骤

```
1. 按 '#' 分割规则字符串
2. 对每个段，按 ',' 分割字段（至少 4 个）
3. 解析 index、isReverse、padLen 等字段
4. 检测特殊段类型（Random/Fill/Encoded）
5. 记录 max_col_index_（用于后续 Schema 校验）
```

### 2.2 错误检测

编译阶段能检测到的错误：

- 规则为空
- 段字段不足 4 个
- index 不是有效整数或为负数
- isReverse 不是 true/false
- padLen 不是有效整数或为负数
- padMode 不是 LEFT/RIGHT
- 编码段语法错误（括号不匹配、不支持的变换）
- 解析后没有任何有效段

所有错误通过返回值 `Status` 报告。`[!]` 编译**不检查** index 是否在 Arrow Schema 范围内——这个检查推迟到 `converter.cc` 的 `build_sort_index()` 中。

---

## 3. 执行过程（build / build_checked）

### 3.1 输入

`fields`：一个 `std::vector<std::string_view>`，按列索引排列。`fields[i]` 是 Arrow 表第 i 列在当前行的字符串值。

在 `converter.cc` 中，这个数组由 `scalar_to_string()` 填充。该函数将各种 Arrow 类型转为字符串表示：Int → `std::to_string()`，String → 直接引用，Timestamp → 毫秒数的字符串形式。

### 3.2 逐段构建

```
for each segment in segments_:
    if Random → 生成随机数字
    if Fill → val = ""
    if ColumnRef → val = fields[seg.col_index]
    if EncodedColumn → val = fields[seg.col_index] → encode_long_or_short()

    val = apply_segment(seg, val)  // padding + reverse
    out += val                     // 拼接到输出
```

### 3.3 apply_segment：填充与反转

```cpp
// 1. 填充
if (padLen > 0 && val.size() < padLen) {
    if (pad_right) val += padding;  // 右填充
    else           val = padding + val;  // 左填充（默认）
}

// 2. 反转
if (reverse) std::reverse(val.begin(), val.end());
```

顺序很重要：**先填充，再反转**。这与 Java 端 `UniverseHbaseBeanUtil.setValue()` 的行为一致。

`[!]` 如果值长度已经 ≥ padLen，不会截断。padLen 只是最小长度保证。

### 3.4 随机数生成

```cpp
std::uniform_int_distribution<int> dist(0, 8);  // 注意：0-8，不是 0-9
```

这精确匹配 Java 端 `RANDOM.nextInt(SEED)` 其中 `SEED = 9`。每个 `RowKeyBuilder` 实例有自己的 `std::mt19937` PRNG，由 `std::random_device` 初始化。

---

## 4. 列排除机制

### 4.1 场景

Hudi 或 CDC 数据的 Arrow 文件通常包含元数据列（如 `_hoodie_commit_time`），这些列不应写入 HBase。用户通过 `configure()` 设置：

```json
{
  "excluded_columns": ["_hoodie_commit_time", "_hoodie_file_name"],
  "excluded_column_prefixes": ["_hoodie"]
}
```

### 4.2 处理流程

```
converter.cc: convert()
  │
  ├── 打开 Arrow 文件读 Schema
  ├── build_removal_indices(schema, names, prefixes)
  │     → 匹配列名/前缀 → 返回降序索引列表
  │
  ├── Pass 1: build_sort_index()
  │     for each batch:
  │       batch = apply_column_removal(batch, removal_indices)
  │       // batch 现在是过滤后的 Schema
  │       // rowKeyRule 索引引用过滤后的列位置
  │       build row keys from filtered batch...
  │
  └── Pass 2: 同样使用过滤后的 batch
```

### 4.3 索引语义变化

这是最容易出错的地方。假设原始 Arrow Schema 为：

```
[0] _hoodie_commit_time  
[1] _hoodie_record_key   
[2] STARTTIME            
[3] IMSI                 
[4] MSISDN               
```

配置 `excluded_column_prefixes: ["_hoodie"]` 后，过滤后的 Schema 为：

```
[0] STARTTIME   (原索引 2)
[1] IMSI        (原索引 3)
[2] MSISDN      (原索引 4)
```

所以 `rowKeyRule` 中引用 STARTTIME 应该用 `index=0`，而不是原始的 `index=2`。

`[!]` 这与 DESIGN.md §4.2.1 中的 `${column_name}` 语法不同。实际实现中 rowKeyRule 使用**数字索引**而非列名引用。列排除改变了索引的含义——Java 调用方必须知道排除后的 Schema 来正确设置索引。

### 4.4 降序删除

`removal_indices` 是**降序排列**的。当从 RecordBatch 中逐个 `RemoveColumn()` 时，先删除高索引的列不会影响低索引列的位置：

```
删除 [4, 1]（降序）:
  RemoveColumn(4) → [0,1,2,3]
  RemoveColumn(1) → [0,2,3]   ✓ 正确

如果是升序 [1, 4]:
  RemoveColumn(1) → [0,2,3,4] → 原来的 4 变成了 3
  RemoveColumn(4) → 越界！    ✗ 错误
```

---

## 5. Row Key 与排序的关系

`converter.cc` 中排序只按 `row_key` 字符串的字典序（`std::string::operator<`）。由于 Row Key 是 UTF-8 字节串，字典序等价于 HBase 的字节序比较。

但 HBase 的完整排序规则是 `Row ASC → Family ASC → Qualifier ASC → Timestamp DESC`。Row Key 排序只保证了第一级。同一 Row Key 下的 qualifier 排序在 Pass 2 的 `append_grouped_row_cells()` 中处理：

```cpp
std::sort(cells.begin(), cells.end(),
          [](const GroupedCell& a, const GroupedCell& b) {
              return a.qualifier < b.qualifier;
          });
```

Timestamp 在当前实现中对同一 row 的所有列都相同，所以不需要额外排序。

---

## 6. 性能特征

`RowKeyBuilder::build_checked()` 在 Pass 1 中对**每一行**调用一次。对于百万行的文件，这个函数被调用百万次。

当前实现的性能特征：

- 每次调用都会分配一个新的 `std::string`（输出 row key）——`out->reserve(64)` 减少了小 key 的重分配
- `apply_segment()` 中 padding 会创建临时 `std::string`
- Random 段每次调用都通过 `std::uniform_int_distribution` 生成
- EncodedColumn 段涉及 `std::to_string` + Base64 编码

对于性能敏感场景，RowKeyBuilder 的主要瓶颈是字符串分配。如果 profiling 发现这是热点，可以改为预分配缓冲区 + append 模式。
