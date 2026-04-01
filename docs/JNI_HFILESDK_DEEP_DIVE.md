# hfile_jni.cc 深度拆解

这份文档专门解释：

- [hfile_jni.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc)
- [jni_utils.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/jni_utils.h)
- [json_utils.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/json_utils.h)

如果你已经理解了：

- Java CLI 怎么调用 `HFileSDK`
- `converter.cc` 怎么处理 Arrow → HFile

那么 JNI 这一层就是中间桥梁：

> Java 世界的参数、配置、错误码、结果，是怎么进到 C++，又怎么从 C++ 回到 Java 的？

`hfile_jni.cc` 就是在干这件事。

---

## 1. 先给它一句话定义

`hfile_jni.cc` 的职责是：

> 把 Java 层的 `configure / convert / getLastResult` 三个 native 调用，翻译成 C++ 世界里的 `WriterOptions / ConvertOptions / ConvertResult` 操作。

它不是：

- 真正的 Arrow 转换器
- HFile 写入器
- 完整 JSON 框架

它只做“桥接”和“翻译”。

---

## 2. 为什么这一层重要

你从 Java 调项目时，真实链路是：

```text
Java CLI / Java SDK
  -> HFileSDK.java
  -> JNI hfile_jni.cc
  -> convert()
  -> writer.cc
```

如果没有 JNI 这一层：

- Java 无法直接调用 C++ 的 `convert()`
- Java 侧配置也无法落入 `WriterOptions`

所以你可以把它看成：

> 项目的“Java 入口门卫”

---

## 3. 文件整体结构怎么读

这个文件可以分成 5 块：

1. `InstanceState` 与结果 JSON 序列化
2. 全局实例状态表
3. 实例状态管理辅助函数
4. `convert`
5. `getLastResult`
6. `configure`

阅读顺序建议：

1. 先看 `InstanceState`
2. 再看 `get_or_create_instance_state_locked()`
3. 再看 `configure`
4. 再看 `convert`
5. 最后看 `getLastResult`

这样最容易建立“每个 Java 对象在 C++ 里到底保存了什么状态”的理解。

---

## 4. `InstanceState`：每个 Java 实例在 C++ 里对应什么

位置：[hfile_jni.cc:L24-L32](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L24-L32)

这是整个 JNI 文件最关键的结构之一。

### 它保存了什么

- `weak_ref`
  - 指向 Java `HFileSDK` 对象的弱全局引用
- `writer_opts`
  - 当前实例的写入配置
- `last_result`
  - 最近一次转换结果
- `last_result_json`
  - 最近一次转换结果的 JSON 字符串
- `excluded_columns`
  - 需要排除的列名列表
- `excluded_column_prefixes`
  - 需要排除的列名前缀列表

### 这说明了什么

Java `HFileSDK` 对象在 C++ 这边不是无状态的。

而是拥有一个“实例上下文”：

- 先 `configure()`
- 再 `convert()`
- 随后可以 `getLastResult()`

这三步共享的是同一份 `InstanceState`。

---

## 5. 为什么要有 `weak_ref`

字段位置：[hfile_jni.cc:L25](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L25)

### 它的作用

把 C++ 世界里的状态和 Java 实例对象对应起来。

### 为什么不是强引用

因为强全局引用会阻止 Java 对象被 GC 回收。

这里使用弱全局引用有两个好处：

1. 不强行延长 Java 对象生命周期
2. 可以在后续清理无效实例状态

所以这里的设计是：

- C++ 持有“可识别对象身份，但不阻止 GC”的引用

---

## 6. `result_to_json()`：为什么结果要缓存成 JSON

位置：[hfile_jni.cc:L34-L50](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L34-L50)

### 作用

把 `ConvertResult` 序列化成一段 JSON。

### 为什么需要它

Java 层的 `getLastResult()` 返回值是 `String`，不是复杂对象。

所以 JNI 最方便的方式就是：

- C++ 内部保留结构体版 `ConvertResult`
- 同时缓存一个字符串版 JSON

这样 `getLastResult()` 就只需要把字符串返给 Java。

### 输出内容包含什么

- `error_code`
- `error_message`
- `arrow_batches_read`
- `arrow_rows_read`
- `kv_written_count`
- `kv_skipped_count`
- `duplicate_key_count`
- `hfile_size_bytes`
- `elapsed_ms`
- `sort_ms`
- `write_ms`

这正是 CLI 和 Java SDK 能看到的那份结果摘要。

---

## 7. `make_error_result()`：为什么 JNI 层自己也会造结果对象

位置：[hfile_jni.cc:L52-L57](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L52-L57)

### 作用

构造一个只有：

- 错误码
- 错误消息

的 `ConvertResult`

### 为什么这里需要它

因为有些错误发生在：

- 还没进入 `converter.cc`
- 还没开始真正执行转换

比如：

- Java 传进来的字符串是 null
- 配置 JSON 非法

此时也必须返回一个统一格式的错误结果给 Java。

---

## 8. 全局状态表：为什么是一个 vector

位置：[hfile_jni.cc:L63-L64](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L63-L64)

```text
static std::mutex g_config_mutex;
static std::vector<InstanceState> g_instance_states;
```

### 含义

整个 JNI 进程级共享：

- 一把互斥锁
- 一个实例状态表

### 为什么需要全局表

因为 JNI 的 native 方法调用进来时，必须根据：

- 当前 Java 对象 `obj`

找到对应的那份 C++ 状态。

所以需要一个全局容器来保存所有活跃实例的状态。

---

## 9. `cleanup_instance_states_locked()`：为什么要清理失效实例

位置：[hfile_jni.cc:L66-L76](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L66-L76)

### 它做了什么

遍历全局状态表：

- 如果某个 `weak_ref` 对应的 Java 对象已经失效
- 就删除它的弱引用
- 并从状态表中移除

### 为什么重要

否则随着 Java 侧创建越来越多 `HFileSDK` 实例，C++ 里的状态表会无限增长。

这是一种基本的生命周期清理。

---

## 10. `find_instance_state_locked()`：如何从 Java 对象定位 C++ 状态

位置：[hfile_jni.cc:L78-L85](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L78-L85)

### 它做了什么

1. 先清理失效实例
2. 遍历现有状态表
3. 用 `env->IsSameObject(state.weak_ref, obj)` 判断是不是同一个 Java 对象

### 为什么不用地址比较

因为 JNI 对象引用本身不是普通 C++ 指针，不能拿来直接比较业务身份。

`IsSameObject` 才是标准方式。

---

## 11. `get_or_create_instance_state_locked()`：为什么 configure 前也能直接 convert

位置：[hfile_jni.cc:L87-L95](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L87-L95)

### 它的行为

如果这个 Java 对象已经有状态：

- 直接返回

如果没有：

- 创建一个新的 `InstanceState`
- 给默认列族 `cf`
- 放进全局表

### 这意味着什么

即使 Java 层没有显式先调用 `configure()`，
`convert()` 也依然能工作，
因为 JNI 会自动给这个实例创建默认状态。

这是一种“懒初始化”设计。

---

## 12. `set_instance_result()`：为什么每次都要回写结果

位置：[hfile_jni.cc:L97-L102](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L97-L102)

### 作用

把本次转换结果写回实例状态：

- `last_result`
- `last_result_json`

### 为什么这是必须的

因为 Java 侧的 `getLastResult()` 本质上就是读这里。

所以无论：

- 成功
- 普通失败
- JNI 参数错误
- C++ 异常

都必须尽量把结果回写到实例状态里。

---

## 13. `get_instance_writer_opts()` 和 `get_instance_snapshot()`

位置：

- [get_instance_writer_opts](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L104-L107)
- [get_instance_snapshot](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L116-L120)

### 为什么需要 snapshot

`convert()` 真正执行时会跑很久。

如果整个执行期一直拿着全局锁：

- 会阻塞其他线程
- 配置管理也会变差

所以当前做法是：

1. 在锁内把当前配置复制出来
2. 生成一个 `InstanceSnapshot`
3. 后续转换逻辑都用这份快照，不再持锁

这是一个很典型的“锁内复制，锁外执行”设计。

---

## 14. `convert()` JNI 方法：真实主入口

位置：[hfile_jni.cc:L134-L232](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L134-L232)

这是整个 JNI 文件最重要的函数。

### 它的职责

把 Java 的 4 个字符串参数：

- arrowPath
- hfilePath
- tableName
- rowKeyRule

翻译成 `ConvertOptions`，然后调用 C++ 的：

- `hfile::convert(opts)`

---

## 15. `convert()` 第一步：jstring 转 std::string

逻辑在 [hfile_jni.cc:L144-L182](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L144-L182)。

这里用到了：

- [jstring_to_string](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/jni_utils.h#L9-L20)
- [optional_jstring_to_string](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/jni_utils.h#L22-L28)

### 为什么分 required 和 optional

- `arrowPath` / `hfilePath` 必须存在
- `tableName` / `rowKeyRule` 允许以“可选字符串”方式读取，然后再在后面做业务校验

### 如果转换失败怎么办

JNI 会直接构造 `ConvertResult` 错误对象并返回。

也就是说：

- Java 字符串提取失败
- 并不会抛 Java 异常到上层
- 而是继续沿用统一错误码机制

---

## 16. `convert()` 第二步：参数基本校验

逻辑在 [hfile_jni.cc:L184-L190](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L184-L190)。

这里只检查非常基本的事情：

- `arrowPath`
- `hfilePath`

不能为空。

### 为什么这里只做轻校验

因为更细的业务校验（例如文件是否存在、rowKeyRule 是否能编译）会交给真正的 `converter.cc`。

JNI 层只做最粗的参数守门。

---

## 17. `convert()` 第三步：组装 `ConvertOptions`

逻辑在 [hfile_jni.cc:L192-L208](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L192-L208)。

### 这里做了什么

把刚才读到的 Java 参数塞进：

- `opts.arrow_path`
- `opts.hfile_path`
- `opts.table_name`
- `opts.row_key_rule`

然后再从实例快照里取：

- `writer_opts`
- `excluded_columns`
- `excluded_column_prefixes`

### 这一步特别重要

说明配置分成两类：

#### 直接来自 convert 调用参数

- Arrow 输入路径
- HFile 输出路径
- tableName
- rowKeyRule

#### 来自实例 configure 状态

- compression
- block_size
- encoding
- bloom_type
- fsync_policy
- error_policy
- excluded columns

这就是为什么 Java 层是先 `configure()` 再 `convert()`。

---

## 18. `convert()` 第四步：同步列族

逻辑在 [hfile_jni.cc:L204-L208](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L204-L208)。

### 为什么这里要多做一步

因为：

- `WriterOptions` 里可能已经有 `column_family`
- `ConvertOptions` 里也有 `column_family`

这两个地方最终要保持一致。

所以这里的逻辑是：

1. 如果实例配置里设置了列族，就覆盖 `ConvertOptions.column_family`
2. 再把 `writer_opts.column_family` 反写成同一个值

### 目的

保证后面无论哪个模块读取列族，看到的都是一致值。

---

## 19. `convert()` 第五步：真正调 C++

逻辑在 [hfile_jni.cc:L210-L215](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L210-L215)。

这里只有一句最核心的话：

```text
hfile::ConvertResult result = hfile::convert(opts);
```

这说明：

- JNI 不参与业务转换
- 真正业务处理全部交给 `converter.cc`

JNI 的角色到这里就切换成“收结果、回写状态、返回错误码”。

---

## 20. `convert()` 的异常处理策略

逻辑在 [hfile_jni.cc:L217-L231](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L217-L231)。

### 它没有把异常直接抛给 Java

而是：

1. 捕获 `std::exception`
2. 捕获未知异常
3. 构造 `INTERNAL_ERROR`
4. 写入实例结果
5. 返回错误码

### 为什么这样设计

因为项目整体的错误处理策略更偏：

- “统一结果对象 + 错误码”

而不是：

- “JNI 抛 Java 异常”

这样对 CLI 和 SDK 都更统一。

---

## 21. `getLastResult()`：为什么这么简单

位置：[hfile_jni.cc:L238-L252](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L238-L252)

### 它做的事情

1. 加锁
2. 找到当前实例对应状态
3. 取出 `last_result_json`
4. 返回 Java `String`

### 为什么能这么简单

因为真正复杂的事情前面都已经做完了：

- 转换结果早已被缓存
- JSON 也早已序列化好

所以这里读取是 O(1) 风格的状态访问。

---

## 22. `configure()`：为什么它是整个 JNI 文件第二重要的函数

位置：[hfile_jni.cc:L259-L386](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L259-L386)

它的重要性仅次于 `convert()`。

### 它负责什么

把 Java 传入的 JSON 配置变成：

- `WriterOptions`
- 实例级列排除设置

### 你可以把它看成

> Java SDK 和 C++ 写入配置之间的“翻译器”

---

## 23. `configure()` 第一步：拿到配置字符串

逻辑在 [hfile_jni.cc:L262-L274](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L262-L274)。

### 如果 config 为空会怎样

直接返回成功，并把 `last_result` 置成默认空结果。

这说明：

- `configure(null)` / `configure("")`
- 被视为“不要改配置”

---

## 24. `configure()` 第二步：拿锁并复制当前选项

逻辑在 [hfile_jni.cc:L276-L284](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L276-L284)。

### 为什么先复制 `state.writer_opts` 到 `next_opts`

因为配置更新不是“全部重建”，而是“按字段覆盖”。

也就是说：

- 某次 configure 只改 compression
- 其他字段应保留旧值

所以这里采用：

```text
next_opts = 当前配置副本
再逐项改 next_opts
最后整体写回
```

这是一种安全的“增量更新”策略。

---

## 25. `fail_config` lambda：为什么 configure 也走统一结果对象

逻辑在 [hfile_jni.cc:L279-L284](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L279-L284)。

### 它做了什么

如果某个配置项非法：

1. 生成 `INVALID_ARGUMENT`
2. 写入 `last_result`
3. 写入 `last_result_json`
4. 返回错误码

### 设计意义

即便是“配置阶段失败”，Java 层依然能通过 `getLastResult()` 看见统一格式的错误结果。

---

## 26. `json_utils.h`：为什么项目自己写了一个小 JSON 解析器

看 [json_utils.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/json_utils.h)。

### 作用

解析非常受限的配置 JSON：

- string
- integer
- string array

### 为什么不用第三方 JSON 库

原因通常有 3 类：

1. 避免增加 JNI 侧依赖
2. 减少编译和打包复杂度
3. 当前需求非常简单，用重库不划算

### `parse_json_config()` 会做什么

位置：[json_utils.h:L103-L180](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/json_utils.h#L103-L180)

它会：

- 解析对象
- 解析字符串
- 解析整数
- 解析字符串数组
- 检查 duplicate key

这已经足够覆盖当前 `configure()` 需要的配置格式。

---

## 27. `configure()` 第三步：校验允许的 key

逻辑在 [hfile_jni.cc:L295-L311](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L295-L311)。

### 为什么要做白名单

这样可以避免：

- 用户拼错 key
- 配置里混入未知字段
- 项目默默忽略错误配置

只允许的 key 包括：

- `compression`
- `block_size`
- `column_family`
- `data_block_encoding`
- `fsync_policy`
- `error_policy`
- `bloom_type`
- `include_mvcc`
- `excluded_columns`
- `excluded_column_prefixes`

---

## 28. `configure()` 第四步：逐项翻译配置

从 [hfile_jni.cc:L313-L374](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L313-L374) 开始。

每个配置项都遵循同一个模式：

1. 从 JSON 对象取值
2. 判断是否合法
3. 转换成 C++ 枚举/字段
4. 写入 `next_opts` 或实例状态

---

### 28.1 compression

支持：

- `none`
- `lz4`
- `zstd`
- `snappy`
- `gzip`

最终映射到 `hfile::Compression`。

---

### 28.2 block_size

要求：

- 必须大于 0

最后写入：

- `WriterOptions.block_size`

---

### 28.3 column_family

要求：

- 不能是空字符串

最后写入：

- `WriterOptions.column_family`

---

### 28.4 data_block_encoding

支持：

- `NONE`
- `PREFIX`
- `DIFF`
- `FAST_DIFF`

映射到：

- `hfile::Encoding`

---

### 28.5 fsync_policy

支持：

- `safe`
- `fast`
- `paranoid`

映射到：

- `hfile::FsyncPolicy`

---

### 28.6 error_policy

支持：

- `strict`
- `skip_row`
- `skip_batch`

映射到：

- `hfile::ErrorPolicy`

---

### 28.7 bloom_type

支持：

- `none`
- `row`
- `rowcol`

映射到：

- `hfile::BloomType`

---

### 28.8 include_mvcc

要求：

- 只能是 `0` 或 `1`

然后转成 bool。

---

## 29. 为什么列排除不放在 `WriterOptions` 里

相关逻辑在 [hfile_jni.cc:L366-L374](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L366-L374)。

### 这里的设计很重要

`excluded_columns` 和 `excluded_column_prefixes` 没有塞进 `WriterOptions`，
而是保存在 `InstanceState` 里。

### 为什么

因为列排除不是 HFile 写入器本身的概念，而是：

- Arrow → HFile 转换阶段的概念

`writer.cc` 并不知道 Arrow schema，也不该关心 `_hoodie_*`。

所以这个设计是正确分层的：

- `WriterOptions`：写文件行为
- `InstanceState.excluded_*`：转换行为

---

## 30. `configure()` 第五步：提交配置

逻辑在 [hfile_jni.cc:L376-L379](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L376-L379)。

做两件事：

1. 把 `next_opts` 写回 `state.writer_opts`
2. 重置 `last_result`

### 为什么要重置结果

因为一次新的配置应用后，旧的转换结果已经不再能准确反映当前实例状态。

所以这里选择把结果清空，避免误导调用方。

---

## 31. `jni_utils.h`：字符串桥接为什么单独做工具函数

位置：[jni_utils.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/jni_utils.h)

最常用的是：

- `jstring_to_string()`
- `optional_jstring_to_string()`

### 为什么要单独封装

因为 JNI 字符串提取本身很容易出错：

- null 处理
- `GetStringUTFChars` 失败
- 忘记 `ReleaseStringUTFChars`

把这套逻辑收拢成工具函数，可以让 `hfile_jni.cc` 更聚焦业务流程，而不是 JNI 细节。

---

## 32. 这份 JNI 设计的整体特点

可以总结成 4 个关键词。

### 32.1 实例有状态

不是无状态函数调用，而是：

- 一个 Java 对象对应一份 C++ 状态

### 32.2 配置与执行分离

- `configure()` 负责设置状态
- `convert()` 负责执行

### 32.3 错误统一结果化

尽量不抛 Java 异常，而是返回：

- 错误码
- JSON 结果

### 32.4 最小依赖

没有引入重型 JSON/JNI 辅助框架，而是自己做一套够用的轻实现。

---

## 33. 如果你要调试 JNI 层，优先看哪里

### 场景 1：Java 传的参数没生效

优先看：

- `configure()`
- `get_instance_snapshot()`
- `convert()` 中 `opts.writer_opts = snap.writer_opts`

---

### 场景 2：`getLastResult()` 返回值不对

优先看：

- `set_instance_result()`
- `result_to_json()`
- `getLastResult()`

---

### 场景 3：列排除没生效

优先看：

- `configure()` 中 `excluded_columns` / `excluded_column_prefixes`
- `convert()` 中 `opts.excluded_*`
- 下游 `converter.cc`

---

### 场景 4：多个 Java 实例互相污染

优先看：

- `InstanceState`
- `weak_ref`
- `find_instance_state_locked()`
- `get_or_create_instance_state_locked()`

---

## 34. 对不熟 C++ 的读者，最推荐阅读顺序

第一次读：

1. [InstanceState](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L24-L32)
2. [configure](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L259-L386)
3. [convert](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L134-L232)
4. [getLastResult](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/hfile_jni.cc#L238-L252)

第二次读：

5. [json_utils.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/json_utils.h)
6. [jni_utils.h](file:///Users/gauss/workspace/github_project/HFileSDK/src/jni/jni_utils.h)

第三次读：

7. 对照 Java 侧 [HFileSDK.java](file:///Users/gauss/workspace/github_project/HFileSDK/java/src/main/java/com/hfile/HFileSDK.java)
8. 再对照 [converter.cc](file:///Users/gauss/workspace/github_project/HFileSDK/src/convert/converter.cc)

---

## 35. 最后一段，用一句话记住它

如果要用一句话概括 `hfile_jni.cc`：

> 它是整个项目里负责把“Java 世界的配置、参数、结果”转换成“C++ 世界的写入选项、转换参数、执行结果”的桥接中枢。

所以你以后只要想回答：

- Java `configure()` 到底改了哪里？
- Java `convert()` 为什么会走到 C++？
- `getLastResult()` 为什么能看到上次转换详情？

答案基本都会在这个文件里。
