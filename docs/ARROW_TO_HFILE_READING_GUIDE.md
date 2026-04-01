# arrow-to-hfile 源码阅读路线图

这份文档是给“不熟 C++、但想顺着一条真实调用链把项目读懂”的读者准备的。

目标非常明确：

- 从 `tools/arrow-to-hfile` 的 Java CLI 开始
- 一路跟到 JNI
- 再跟到 `src/convert/converter.cc`
- 最后落到 HFile 写入主逻辑

你可以把它理解成一份“按顺序带你跳文件”的阅读手册。

---

## 1. 为什么先看这条链路

这个项目虽然 C++ 模块很多，但如果你是实际使用者，最自然的入口通常不是：

- `writer.cc`
- `block encoder`
- `bloom`

而是这条命令：

```bash
java -jar tools/arrow-to-hfile/target/arrow-to-hfile-4.0.0.jar ...
```

也就是说，最符合真实使用场景的阅读顺序是：

1. Java CLI 接收参数
2. Java 组装配置
3. Java 调 JNI
4. JNI 转成 C++ 参数
5. C++ 做 Arrow → row key → sort → HFile
6. HFileWriter 真正把文件写出来

---

## 2. 整条主链路先记住

先背下来这个跳转路径：

```text
tools/arrow-to-hfile
  -> ArrowToHFileConverter.main()
  -> ArrowToHFileConverter.convert()
  -> NativeLibLoader.load()
  -> com.hfile.HFileSDK.configure()
  -> com.hfile.HFileSDK.convert()
  -> JNI: Java_com_hfile_HFileSDK_convert()
  -> hfile::convert(opts)
  -> build_sort_index()
  -> append_grouped_row_cells()
  -> HFileWriter::append()
  -> HFileWriter::finish()
```

如果你先对这条路径有整体印象，再看代码就不容易迷路。

---

## 3. 第一站：CLI 入口

### 看哪里

- [ArrowToHFileConverter.java](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/ArrowToHFileConverter.java)

### 为什么先看它

因为这是用户真正碰到的第一层。

### 建议阅读顺序

#### 3.1 先看 `main()`

位置在 [ArrowToHFileConverter.java:L182-L324](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/ArrowToHFileConverter.java#L182-L324)。

它做了这些事情：

1. 解析命令行参数
2. 校验 `--arrow`、`--hfile`、`--rule`
3. 确保输出目录存在
4. 提前加载 native 库
5. 组装 `ConvertOptions`
6. 调 `convert()`
7. 根据 `ConvertResult` 打印成功/失败信息

### 你读这一段时要重点抓 3 个问题

- 参数是怎么映射到 `ConvertOptions` 的？
- 为什么这里要提前加载 native？
- CLI 失败时，错误码是在哪一层决定的？

#### 3.2 再看 `convert()`

位置在 [ArrowToHFileConverter.java:L119-L151](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/ArrowToHFileConverter.java#L119-L151)。

它做的事情比 `main()` 更“业务化”：

1. 选择 native lib 路径
2. 调 `NativeLibLoader.load()`
3. 创建新的 `HFileSDK` 实例
4. 如果有配置，就先 `sdk.configure(configJson)`
5. 再调 `sdk.convert(...)`
6. 最后把 `sdk.getLastResult()` 解析成 `ConvertResult`

### 这一层最重要的理解

Java 自己 **并不做 Arrow 转换**。

Java 这一层的角色主要是：

- 参数与配置整理
- 错误包装
- JNI 调用桥接

真正的数据处理逻辑都在 C++。

---

## 4. 第二站：Java 配置对象

### 看哪里

- [ConvertOptions.java](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/ConvertOptions.java)

### 为什么要看

因为它决定了“CLI 参数最终怎样变成 JNI 配置”。

### 建议重点看

#### `toConfigJson()`

这个函数会把 Java 层的：

- compression
- block size
- column family
- encoding
- bloom type
- fsync policy
- error policy
- excluded columns
- excluded prefixes

转成 JSON，再传给 JNI 的 `configure()`。

### 这一步的关键认识

Java 没有逐个字段直接调用 C++ setter。

而是：

```text
Java options -> JSON -> JNI configure() -> C++ WriterOptions / InstanceState
```

这就是为什么后面你会在 JNI 层看到一个 JSON 解析器。

---

## 5. 第三站：Native 库加载

### 看哪里

- [NativeLibLoader.java](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/NativeLibLoader.java)

### 为什么要看

这不是业务转换逻辑，但它决定“程序能不能成功进入 C++”。

### 建议重点看

#### `load()`

入口在 [NativeLibLoader.java:L59-L66](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/NativeLibLoader.java#L59-L66)。

#### `doLoad()`

核心搜索顺序在 [NativeLibLoader.java:L73-L106](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/NativeLibLoader.java#L73-L106)：

1. `--native-lib`
2. `HFILESDK_NATIVE_LIB`
3. `HFILESDK_NATIVE_DIR`
4. `java.library.path`

#### `loadAbsolute()`

重点在 [NativeLibLoader.java:L108-L129](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/NativeLibLoader.java#L108-L129)。

这里做了两件关键事：

1. `System.load(absPath)`
2. 标记 `hfilesdk.native.loaded=true`

### 为什么重要

因为 Java 侧的 `HFileSDK` 类本身还有静态 `System.loadLibrary("hfilesdk")`。

如果不先处理好，类初始化阶段就可能报 `UnsatisfiedLinkError`。

---

## 6. 第四站：Java SDK 包装层

### 看哪里

- [tools/arrow-to-hfile/HFileSDK.java](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/com/hfile/HFileSDK.java)

### 它是什么

这是 Java 调 JNI 的最薄一层包装。

### 先看哪里

#### 静态块

位置在 [HFileSDK.java:L72-L78](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/com/hfile/HFileSDK.java#L72-L78)。

它说明：

- 如果 native 已经被 `NativeLibLoader` 预加载，就不再重复 `loadLibrary`
- 否则自己尝试 `System.loadLibrary("hfilesdk")`

#### native 方法声明

关注这几个：

- `convert(...)`
- `configure(...)`
- `getLastResult()`

### 这一层的本质

你可以把它理解成 Java 世界里的“C++ 远程控制器”。

它本身不做转换，只是把 JNI 方法签名暴露给上层。

---

## 7. 第五站：JNI 入口

### 看哪里

- [hfile_jni.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc)

### 为什么它关键

这是 Java 世界和 C++ 世界的交界线。

理解这层，你就知道：

- Java 参数如何变成 C++ 参数
- Java 配置如何落到 `WriterOptions`
- Java 结果如何拿回 JSON

### 阅读顺序建议

#### 7.1 先看 `InstanceState`

位置在 [hfile_jni.cc:L24-L32](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L24-L32)。

这里保存每个 Java `HFileSDK` 实例对应的状态：

- `WriterOptions`
- 上一次 `ConvertResult`
- `last_result_json`
- `excluded_columns`
- `excluded_column_prefixes`

### 为什么要先看它

因为后面的 `configure()` 和 `convert()` 都围绕这个实例状态展开。

#### 7.2 再看 `configure`

位置在 [hfile_jni.cc:L259-L386](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L259-L386)。

它做的事情：

1. 把 Java 传来的 JSON 字符串取出来
2. 用 `json_utils.h` 解析成轻量配置对象
3. 映射到 `WriterOptions`
4. 保存到当前实例对应的 `InstanceState`

### 你要重点看什么

- compression 怎样映射成 C++ 枚举
- block_size 怎样变成 `WriterOptions.block_size`
- excluded columns/prefixes 为什么不在 `WriterOptions` 里，而在 `InstanceState` 里

#### 7.3 再看 `convert`

位置在 [hfile_jni.cc:L134-L232](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L134-L232)。

这是真正跳入 C++ 业务逻辑的地方。

它会：

1. 把 `jstring` 转成 `std::string`
2. 校验必填参数
3. 组装 `ConvertOptions`
4. 从 `InstanceState` 里取 `WriterOptions` 和列排除配置
5. 调 `hfile::convert(opts)`
6. 把返回的 `ConvertResult` 存回实例状态

### 这一层最重要的理解

JNI 自己不做 Arrow 转换。

JNI 做的只是：

```text
Java string/json -> C++ ConvertOptions/WriterOptions -> 调 convert()
```

#### 7.4 最后看 `getLastResult`

位置在 [hfile_jni.cc:L238-L252](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L238-L252)。

它只是把当前实例里缓存的结果 JSON 返回给 Java。

---

## 8. 第六站：真正的 C++ 入口 `convert()`

### 看哪里

- [converter.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L579-L836)

### 为什么这是核心中的核心

因为从这里开始，真正进入：

- Arrow 文件读取
- rowKeyRule 编译
- 排序
- HFile 写入

### 建议阅读顺序

#### 8.1 看函数开头：输入校验

位置在 [converter.cc:L587-L616](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L587-L616)。

这里会检查：

- Arrow 路径是否为空
- HFile 路径是否为空
- Arrow 文件是否存在
- rowKeyRule 是否为空
- rowKeyRule 是否能成功编译

### 这一步的重要性

如果你后面看见错误码：

- `INVALID_ARGUMENT`
- `INVALID_ROW_KEY_RULE`
- `ARROW_FILE_ERROR`

很多就是在这里决定的。

#### 8.2 看列排除构建

位置在 [converter.cc:L619-L642](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L619-L642)。

这里做的是：

- 根据 `excluded_columns`
- 或 `excluded_column_prefixes`
- 计算哪些列要在后续处理中删除

### 为什么这一段值得看

这是 `_hoodie_*` 列为什么可以在输出 HFile 时被排除掉的根源。

#### 8.3 看第一遍扫描：`build_sort_index()`

主调位置在 [converter.cc:L644-L662](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L644-L662)。

而真正实现看：

- [build_sort_index](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L385-L513)

### 这是你最该仔细读的函数之一

它做了下面这些事情：

1. 打开 Arrow IPC Stream
2. 读取 schema
3. 应用列删除后得到过滤后的 schema
4. 检查 rowKeyRule 中引用列的类型是否合法
5. 逐个 batch 读取 Arrow 数据
6. 对每一行：
   - 提取 rowKey 所需字段
   - 调 `RowKeyBuilder.build_checked()` 生成 row key
   - 记录 `(row_key, batch_idx, row_idx)`

### 你要特别盯住几个变量

- `sort_index`
- `batches`
- `rkb`
- `removal_indices`

### 理解它后，你就明白两件事

1. 为什么输入 Arrow 不需要预排序
2. 为什么内存占用会比较高（因为 batch 和 sort index 都要保留）

#### 8.4 看排序逻辑

位置在 [converter.cc:L682-L689](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L682-L689)。

只有一小段：

```text
stable_sort(sort_index by row_key)
```

但是这段非常关键，因为它决定了最终 HFile 的写入顺序。

#### 8.5 看打开 HFile writer

位置在 [converter.cc:L698-L720](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L698-L720)。

这里把 `ConvertOptions.writer_opts` 映射到 `HFileWriter::builder()`。

### 为什么它重要

因为很多 CLI/JNI 配置最终就是在这里真正落到写入器上的。

例如：

- compression
- block size
- data block encoding
- bloom type
- fsync policy
- error policy

#### 8.6 看第二遍回放

位置在 [converter.cc:L722-L809](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L722-L809)。

逻辑是：

1. 顺着排序后的 `sort_index`
2. 找到同一个 row key 对应的所有 Arrow 行
3. 把这些行转成 `GroupedCell`
4. 再统一交给 `append_grouped_row_cells()`

### 这一段要特别理解的点

- 为什么按 row key 分组
- 为什么同一 row key 的 cell 还要排序
- 为什么会统计 `duplicate_key_count`

#### 8.7 看最终 `writer->finish()`

位置在 [converter.cc:L811-L836](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L811-L836)。

从这里就正式跳到 HFile 物理写入收尾阶段。

---

## 9. 第七站：rowKeyRule 真正怎么执行

### 看哪里

- [row_key_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.h)
- [row_key_builder.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc)

### 阅读顺序

#### 9.1 先看 `compile()`

位置在 [row_key_builder.cc:L186-L289](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L186-L289)。

### 这一步做什么

把文本规则：

```text
REFID,0,true,0,LEFT,,,
```

变成内部的 segment 列表。

每个 segment 记录：

- 引用哪一列
- 是否 reverse
- pad 长度
- pad 方向
- 是否编码

#### 9.2 再看 `build_checked()`

位置在 [row_key_builder.cc:L300-L327](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc#L300-L327)。

### 这一步做什么

对每一行：

1. 取出 segment 所需字段
2. 必要时编码
3. 应用 pad/reverse
4. 拼成最终 row key

### 这一层的理解建议

把它想成“字符串模板引擎”，只是模板内容不是 HTML，而是 HBase row key。

---

## 10. 第八站：cell 是怎么拼出来的

### 看哪里

- [append_grouped_row_cells](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc#L517-L575)

### 这一步做了什么

对于某一个 row key：

1. 取这一组所有 `GroupedCell`
2. 先按 qualifier 排序
3. 去掉重复 qualifier
4. 逐条调用 `writer.append(row, cf, qualifier, ts, value)`

### 为什么它关键

这是从“逻辑列数据”变成“真正 HBase cell”的最后一步。

所以如果你以后遇到：

- qualifier 顺序不对
- 重复列被覆盖
- 同一 row key 只保留第一条

通常都要回来这里看。

---

## 11. 第九站：最终 HFile 是怎么写出来的

### 看哪里

- [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc)

### 阅读建议

不要一上来从头读完，而是按这条线看：

#### 11.1 先找 `append()`

它回答：

> 一条 KV 进入 writer 后，会发生什么？

#### 11.2 再看 `append_materialized_kv()`

它是真正把 row/family/qualifier/value 送进 encoder 的地方。

#### 11.3 再看 `flush_data_block()`

它回答：

> 当前 block 满了以后，怎样变成真正的 HFile block？

这里会涉及：

- block encoder
- compressor
- checksum
- block index entry

#### 11.4 再看 `finish()`

它回答：

> HFile 文件尾部的各种索引、元信息、Trailer 是怎样收尾写进去的？

### 为什么最后才读 `writer.cc`

因为如果你前面还没理解：

- row key 怎么来
- Arrow 行如何映射成 cell
- 为什么要排序

直接看 `writer.cc` 会很难，因为它处理的是“已经准备好的 KV”。

---

## 12. 第十站：如果要更深入，再看这些支撑模块

当你把主链路读通后，再按下面顺序补：

### Data Block 编码

- [data_block_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/data_block_encoder.h)
- [none_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/none_encoder.h)
- [prefix_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/prefix_encoder.h)
- [diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/diff_encoder.h)
- [fast_diff_encoder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/block/fast_diff_encoder.h)

### Bloom / Index / Trailer

- [compound_bloom_filter_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/bloom/compound_bloom_filter_writer.h)
- [block_index_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/index/block_index_writer.h)
- [file_info_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/file_info_builder.h)
- [trailer_builder.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/meta/trailer_builder.h)

### 底层后端

- [buffered_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/io/buffered_writer.h)
- [atomic_file_writer.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/io/atomic_file_writer.h)
- [compressor.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/codec/compressor.h)
- [crc32c.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/checksum/crc32c.h)

---

## 13. 你可以带着哪些问题去读

如果你是第一次读，建议每一层只回答一个核心问题。

### Java CLI 层

- 我输入的命令行参数最后进了哪个对象？

### JNI 层

- Java 配置怎样变成 C++ 的 `ConvertOptions` 和 `WriterOptions`？

### converter 层

- 为什么要两遍扫描？

### rowKeyBuilder 层

- rowKeyRule 是怎样被解释和执行的？

### writer 层

- 一组 `KeyValue` 最终怎样变成符合 HBase 规范的 HFile？

---

## 14. 对 C++ 初学者最友好的阅读顺序总结

如果你只想按顺序点文件，直接照这个读：

1. [ArrowToHFileConverter.java](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/ArrowToHFileConverter.java)
2. [ConvertOptions.java](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/ConvertOptions.java)
3. [NativeLibLoader.java](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/io/hfilesdk/converter/NativeLibLoader.java)
4. [tools/HFileSDK.java](file:///Users/gauss/workspace/github_project/HFileSDK/tools/arrow-to-hfile/src/main/java/com/hfile/HFileSDK.java)
5. [hfile_jni.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc)
6. [convert_options.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/convert_options.h)
7. [converter.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc)
8. [row_key_builder.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/arrow/row_key_builder.cc)
9. [writer.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/writer.cc)
10. 再回头看 `block/`、`bloom/`、`index/`、`meta/`

---

## 15. 一句话收尾

如果你现在只记住一件事，请记住：

> `arrow-to-hfile` 这条链路的本质，是 **Java 负责接参数，JNI 负责翻译，converter 负责排序和映射，writer 负责把结果按 HFile 规范落盘。**

只要把这 4 层关系理顺，整个项目的 80% 核心逻辑就已经建立起来了。
