# HFileSDK 源码文档

本目录帮助开发者深入理解 `src/` 下的 C++ 代码。文档基于实际源码编写，不是设计稿的复述。

## 阅读顺序

**第一步 — 建立全局地图**

[01_CODE_MAP.md](01_CODE_MAP.md) — 目录结构、模块职责、核心数据结构、模块间依赖关系。读完后你能回答"某个功能在哪个文件"。

**第二步 — 走通主链路**

[02_CONVERSION_PIPELINE.md](02_CONVERSION_PIPELINE.md) — 从 Java 调用 `convert()` 开始，经过 JNI 层、两遍扫描排序、到最终 HFile 落盘的完整数据流。这是最核心的文档，覆盖了 `hfile_jni.cc` → `converter.cc` → `writer.cc` 的全链路。

**第三步 — 理解 HFile 二进制格式**

[03_HFILE_V3_FORMAT.md](03_HFILE_V3_FORMAT.md) — SDK 生成的 HFile v3 文件的完整二进制布局。从 Block Header 33 字节结构、KV 编码格式（含 v3 Tags + MVCC）、到 FileInfo/Trailer 的 ProtoBuf 序列化。每个字节都有对应的代码位置。

**第四步 — 深入关键子模块**

按需阅读：

- [07_WRITER_DEEP_DIVE.md](07_WRITER_DEEP_DIVE.md) — **writer.cc 的逐层拆解**：从 Builder 到 open()、append() 三道关卡、flush_data_block 物理写入、finish() 的偏移量依赖链求解、析构安全网、全部状态变量解析
- [04_ROW_KEY_ENGINE.md](04_ROW_KEY_ENGINE.md) — rowKeyRule 编译与执行引擎，列排除机制，与 Arrow Schema 的交互
- [05_BLOCK_ENCODERS.md](05_BLOCK_ENCODERS.md) — 当前唯一数据块编码器 `NONE` 的实现与约束
- [06_PRODUCTION_FEATURES.md](06_PRODUCTION_FEATURES.md) — 崩溃安全、内存控制、输入校验、JNI 异常隔离、可观测性

## 最短路径

只想快速了解项目？读前两份就够了：CODE_MAP + CONVERSION_PIPELINE。

想修改 HFile 写入逻辑？在前两份基础上加读 07_WRITER_DEEP_DIVE.md，它逐行拆解了 writer.cc 的设计。

想修改某个模块？先读 CODE_MAP 定位文件，再读 CONVERSION_PIPELINE 理解该模块在流水线中的位置，然后按需读对应的深入文档。

想理解 HFile 格式兼容性问题？直接读 03_HFILE_V3_FORMAT.md。

## 约定

- 代码引用格式：`文件名:行号` 或 `类名::方法名`
- "当前"指代码实际行为，不是设计文档中的计划
- 文档中标注 `[!]` 的地方表示与 DESIGN.md 有差异或需要特别注意的实现细节
