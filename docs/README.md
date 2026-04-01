# docs 文档索引

这个目录集中存放项目的源码阅读文档与底层原理文档，适合两类目标：

- 想快速建立对整个仓库的全局理解
- 想沿着某条链路或某个模块深入读源码

如果你对 C++ 不够熟，建议不要一上来就从单个 `.cc` 文件硬啃，最好先按下面的阅读顺序进入。

---

## 1. 推荐阅读顺序

### 1.1 第一步：先建立全局地图

- [SRC_CODE_MAP.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/SRC_CODE_MAP.md)

适合先回答这些问题：

- `src/` 目录怎么分层
- 哪些文件是主入口
- Arrow -> HFile、Bulk Load、JNI 分别走哪条链
- 调试时应该优先看哪里

### 1.2 第二步：沿真实调用链走一遍

- [ARROW_TO_HFILE_READING_GUIDE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ARROW_TO_HFILE_READING_GUIDE.md)

这份文档会按实际使用路径带你从：

- `tools/arrow-to-hfile` Java CLI
- 走到 JNI
- 再走到 `converter.cc`
- 最后落到 `writer.cc`

如果你是“项目使用者”而不是纯底层开发者，这通常是最自然的入口。

### 1.3 第三步：看核心主链路的两个关键文件

- [CONVERTER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/CONVERTER_CC_DEEP_DIVE.md)
- [WRITER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/WRITER_CC_DEEP_DIVE.md)

两者分工可以这样记：

- `converter.cc`：决定 Arrow 数据怎样被编排成待写入的 HFile 数据流
- `writer.cc`：决定这些数据最终怎样组织成真正的 HFile 物理结构

### 1.4 第四步：补齐几个关键子模块

- [ROW_KEY_BUILDER_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ROW_KEY_BUILDER_DEEP_DIVE.md)
- [ARROW_TO_KV_CONVERTER_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ARROW_TO_KV_CONVERTER_DEEP_DIVE.md)
- [JNI_HFILESDK_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/JNI_HFILESDK_DEEP_DIVE.md)

这三份文档分别回答：

- `rowKeyRule` 怎样被解析并执行
- Arrow 表字段怎样变成 HBase `KeyValue`
- Java 到 C++ 的 JNI 桥接是怎么做的

### 1.5 第五步：按场景看批量写入

- [BULK_LOAD_WRITER_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/BULK_LOAD_WRITER_DEEP_DIVE.md)

适合在你已经理解“单个 HFile 怎么写”之后再看。它重点解释：

- 为什么会拆成多个 HFile
- 怎样按 Region / 列族路由
- 错误策略、资源管理、批量导入流程如何组织

### 1.6 第六步：最后啃底层 HFile 组成原理

- [HFILE_LOW_LEVEL_MODULES_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/HFILE_LOW_LEVEL_MODULES_DEEP_DIVE.md)

这份文档更偏底层实现原理，重点覆盖：

- `block/` 编码器
- `bloom/compound_bloom_filter_writer`
- `index/block_index_writer`
- `meta/file_info_builder + trailer_builder`

如果你已经理解 `writer.cc`，再回来看这份文档，会更容易把整套 HFile 物理布局真正串起来。

---

## 2. 按目标选文档

### 2.1 想快速理解整个项目

建议顺序：

1. [SRC_CODE_MAP.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/SRC_CODE_MAP.md)
2. [ARROW_TO_HFILE_READING_GUIDE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ARROW_TO_HFILE_READING_GUIDE.md)
3. [CONVERTER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/CONVERTER_CC_DEEP_DIVE.md)
4. [WRITER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/WRITER_CC_DEEP_DIVE.md)

### 2.2 想理解 row key 与 Arrow 转换

建议顺序：

1. [ROW_KEY_BUILDER_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ROW_KEY_BUILDER_DEEP_DIVE.md)
2. [ARROW_TO_KV_CONVERTER_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ARROW_TO_KV_CONVERTER_DEEP_DIVE.md)
3. [CONVERTER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/CONVERTER_CC_DEEP_DIVE.md)

### 2.3 想理解最终 HFile 物理写盘

建议顺序：

1. [WRITER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/WRITER_CC_DEEP_DIVE.md)
2. [HFILE_LOW_LEVEL_MODULES_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/HFILE_LOW_LEVEL_MODULES_DEEP_DIVE.md)

### 2.4 想理解 Java CLI / JNI 调用路径

建议顺序：

1. [ARROW_TO_HFILE_READING_GUIDE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ARROW_TO_HFILE_READING_GUIDE.md)
2. [JNI_HFILESDK_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/JNI_HFILESDK_DEEP_DIVE.md)

### 2.5 想理解批量导入而不是单文件写入

- [BULK_LOAD_WRITER_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/BULK_LOAD_WRITER_DEEP_DIVE.md)

---

## 3. 文档清单

- [SRC_CODE_MAP.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/SRC_CODE_MAP.md)：`src/` 全局代码地图
- [ARROW_TO_HFILE_READING_GUIDE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ARROW_TO_HFILE_READING_GUIDE.md)：Arrow -> JNI -> Converter -> Writer 阅读路线
- [CONVERTER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/CONVERTER_CC_DEEP_DIVE.md)：两遍扫描、排序、写入编排
- [WRITER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/WRITER_CC_DEEP_DIVE.md)：HFile 物理布局与总装配
- [ROW_KEY_BUILDER_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ROW_KEY_BUILDER_DEEP_DIVE.md)：`rowKeyRule` 编译与执行
- [ARROW_TO_KV_CONVERTER_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ARROW_TO_KV_CONVERTER_DEEP_DIVE.md)：Arrow 行转 HBase `KeyValue`
- [JNI_HFILESDK_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/JNI_HFILESDK_DEEP_DIVE.md)：Java/C++ JNI 桥接
- [BULK_LOAD_WRITER_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/BULK_LOAD_WRITER_DEEP_DIVE.md)：按 Region / 列族拆分多 HFile
- [HFILE_LOW_LEVEL_MODULES_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/HFILE_LOW_LEVEL_MODULES_DEEP_DIVE.md)：HFile block、bloom、index、meta 底层原理

---

## 4. 最短入门路径

如果你只想用最少时间建立有效认知，直接读这 4 份：

1. [SRC_CODE_MAP.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/SRC_CODE_MAP.md)
2. [ARROW_TO_HFILE_READING_GUIDE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/ARROW_TO_HFILE_READING_GUIDE.md)
3. [CONVERTER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/CONVERTER_CC_DEEP_DIVE.md)
4. [WRITER_CC_DEEP_DIVE.md](file:///Users/gauss/workspace/github_project/HFileSDK/docs/WRITER_CC_DEEP_DIVE.md)

读完这 4 份后，再根据兴趣补子模块或底层原理，会轻松很多。
