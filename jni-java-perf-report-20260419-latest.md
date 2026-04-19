# JNI/C++ 与纯 Java Arrow->HFile 性能报告

日期：2026-04-19

## 1. 测试范围

- 正确性基线：
  - `HFILESDK_NATIVE_DIR=/Users/gauss/workspace/github_project/HFileSDK/build mvn -q -f tools/hfile-bulkload-perf/pom.xml -Dtest=io.hfilesdk.perf.HFileConsistencyTest test`
  - 结果：通过
- 性能执行器：
  - `java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar --native-lib /Users/gauss/workspace/github_project/HFileSDK/build/libhfilesdk.dylib --work-dir /tmp/hfilesdk-formal-perf-full-20260419-rerun --report-json /Users/gauss/workspace/github_project/HFileSDK/jni-java-perf-report-20260419-latest.json --table tdr_signal_stor_20550 --compression GZ --encoding NONE --bloom row --jni-xmx-mb 1024 --jni-direct-memory-mb 1024 --jni-sdk-max-memory-mb 2048 --jni-sdk-compression-threads 4 --jni-sdk-compression-queue-depth 8 --jni-sdk-numeric-sort-fast-path auto --java-xmx-mb 1024 --java-direct-memory-mb 1024`
  - 结构化结果文件：`jni-java-perf-report-20260419-latest.json`
- 压缩方式：`GZ`
- 压缩级别：`1`
- Data Block Encoding：`NONE`
- Bloom：`row`
- JNI/C++ 调优参数：
  - `--jni-sdk-compression-threads 4`
  - `--jni-sdk-compression-queue-depth 8`
  - `--jni-sdk-numeric-sort-fast-path auto`
- JVM 内存预算：
  - JNI worker：`-Xmx1024m`、`-XX:MaxDirectMemorySize=1024m`
  - Java worker：`-Xmx1024m`、`-XX:MaxDirectMemorySize=1024m`
- JNI SDK 软预算：`2048 MiB`
- Arrow/HFile 中间产物清理情况：
  - 已确认 perf 工作目录 `/tmp/hfilesdk-formal-perf-full-20260419-rerun` 在执行完成后为空

## 2. 格式正确性结论

- JNI/C++ 与纯 Java 当前都通过了一致性校验。
- 该校验覆盖以下内容：
  - HBase Reader 可读性
  - 文件字节级完全一致
  - 压缩与编码元数据符合预期
  - Bloom 元数据存在
  - 必要的 FileInfo 键存在
- 结论如下：
  - 当前 JNI/C++ 输出仍然符合 HBase 期望
  - 当前纯 Java 输出仍然符合 HBase 期望
  - JNI/C++ 与纯 Java 在一致性基线样例上仍然保持字节级完全一致

## 3. 最新性能结果

| 场景 | JNI/C++ 平均耗时 ms | 纯 Java 平均耗时 ms | 加速比 |
|---|---:|---:|---:|
| `single-001mb` | `48.00` | `531.67` | `11.08x` |
| `single-010mb` | `92.67` | `805.00` | `8.69x` |
| `single-100mb` | `550.33` | `2537.00` | `4.61x` |
| `single-500mb` | `2689.67` | `10170.33` | `3.78x` |
| `directory-100x001mb` | `760.67` | `3572.33` | `4.70x` |
| `directory-100x010mb` | `5633.33` | `20839.67` | `3.70x` |

## 4. 与上一版正式基线对比

上一版正式基线文件：

- `jni-java-perf-report-20260419-formal.json`

| 场景 | 上一版加速比 | 最新加速比 | 差值 |
|---|---:|---:|---:|
| `single-001mb` | `6.51x` | `11.08x` | `+4.56x` |
| `single-010mb` | `8.99x` | `8.69x` | `-0.30x` |
| `single-100mb` | `4.56x` | `4.61x` | `+0.05x` |
| `single-500mb` | `3.85x` | `3.78x` | `-0.07x` |
| `directory-100x001mb` | `4.51x` | `4.70x` | `+0.19x` |
| `directory-100x010mb` | `3.94x` | `3.70x` | `-0.24x` |

解读：

- 最新一轮结果仍然满足目标：六个标准场景下，`JNI/C++ >= 3x 纯 Java`。
- 小文件与中等规模场景，JNI/C++ 依然显著领先。
- 相对领先幅度最小的仍然是数据量最大的场景，因为主要瓶颈已经从排序转移到了 `GZ` 压缩吞吐。
- 与上一版正式结果相比，除 `single-001mb` 在本次重跑中启动路径耗时明显改善外，其余差异总体仍落在当前 macOS 机器的多次运行波动范围内。

## 5. 最近 Profiling 的根因结论

基于最新代码的一组受控热点分析表明：

- `single-100mb`，JNI/C++，`GZ`，不启用压缩流水线：
  - 总耗时约 `1869ms`
  - `sort` 约 `73ms`
  - `Pass2 materialize` 约 `63ms`
  - `Pass2 append` 约 `1600ms`
  - writer `compress` 约 `1096ms`
- `single-100mb`，JNI/C++，`GZ`，压缩流水线 `4x8`：
  - 总耗时约 `861ms`
  - `sort` 约 `70ms`
  - `Pass2 append` 约 `583ms`
  - writer `compress` 聚合 worker 时间约 `1234ms`，但大部分已被并行重叠到关键路径之外

从 profiling 得出的结论：

- 排序已经不再是大文件路径的主导开销。
- value 物化也不是当前的首要瓶颈。
- 关键瓶颈已经转移到 `GZ` 数据块压缩。
- 因此，后续若要继续提升大文件场景性能，重点应放在压缩调度、压缩并行重叠和 codec 吞吐上，而不是继续加重 rowkey 排序特化。

## 6. C++ 性能优化技术方案回顾

### 6.1 已选方案：保持 HFile 字节与格式语义稳定

原则：

- 优化实现过程，而不是改变 HFile 落盘语义
- C++ writer 不依赖 Java 库
- 始终保持 JNI/C++ 与纯 Java 输出的字节级一致

选择原因：

- 格式正确性和 HBase 兼容性是不可妥协的前提
- 如果性能收益建立在字节格式变化或元数据语义改变之上，则不适合进入生产

### 6.2 已选方案：先把基准口径做公平，再追求极限吞吐

已实现方向：

- `hfile-bulkload-perf` 改造成“父进程调度 + 独立 worker 子进程执行”
- 父进程层面按 implementation/iteration 串行调度
- 每个 worker 内部仍保留目录转化能力，多 Arrow 文件转换能力没有丢失
- 报告同时输出进程级指标和 SDK 级指标

选择原因：

- 同 JVM 内直接对比 JNI 与纯 Java，无法保证进程级 CPU 和内存口径公平
- 父进程串行执行可避免正式测试时不同实现之间互相抢占磁盘 IO

效果：

- 性能对比方法更加可信
- 当前报告结构已能支撑后续在 Linux 上做绑核和内存限制下的正式公平复跑

### 6.3 已选方案：显式内存预算与内存观测

已实现方向：

- JNI 暴露 `max_memory_bytes`
- Java `ConvertOptions` 与 CLI 可向 SDK 传入软预算
- JNI 结果 JSON 回传：
  - `memory_budget_bytes`
  - `tracked_memory_peak_bytes`

选择原因：

- 进程 RSS 与 SDK 内部可归因内存不是同一个维度
- 对生产问题定位来说，既需要看到子进程总体包络，也需要看到 SDK 内部占用峰值

效果：

- 不引入完整 metrics 子系统，也能获得足够实用的内存观测能力

### 6.4 已选方案：writer 级 gzip 状态复用

已实现方向：

- 每个 writer 复用 zlib/deflater 状态
- 避免每个 block 都执行一次 `deflateInit2/deflateEnd`

选择原因：

- block 级重复初始化纯属固定开销
- 这是对所有表模型、所有 rowkey 规则都安全有效的通用优化

效果：

- 所有 `GZ` 工作负载都获得收益
- 特别是显著降低了小文件场景的固定开销

### 6.5 已选方案：`Encoding::None` 路径复用原始 CRC

已实现方向：

- 在可行路径上复用已有的原始 `CRC32`，用于生成 gzip trailer

选择原因：

- 避免在默认热点路径上重复做 checksum 计算

效果：

- 属于增量收益，但风险低、兼容性好

### 6.6 已选方案：可配置的块压缩流水线

已实现方向：

- 将数据块压缩放到后台 worker 并行执行
- 支持配置 `compression_threads`
- 支持配置 `compression_queue_depth`
- 异步路径与同步路径保持字节级一致，包括 bloom chunk 顺序

选择原因：

- profiling 已经明确表明：大文件路径的主瓶颈是 `GZ` 压缩
- 在不改变文件字节输出的前提下，让 append 侧逻辑与 block 压缩并行重叠，是当前收益最高的优化手段

效果：

- 这是当前大文件 JNI/C++ 吞吐提升最大的工程优化点
- 最新热点样例 `single-100mb` 中，总耗时约从 `1869ms` 降到 `861ms`

### 6.7 已选方案：数值型 rowkey 快速排序路径，但以规则形态驱动而不是单表特化

已实现方向：

- 支持 `numeric_sort_fast_path = auto | on | off`
- 最初只优化单段零填充数值 rowkey
- 后续推广到“首段是零填充数值/时间戳前缀”的 rowkey 规则
- 后续后缀部分仍按字典序比较
- 若数值长度超出配置的 `padLen`，则 `auto` 自动回退，`on` 直接报错

选择原因：

- 对常见的单调数值型 rowkey 模式可以带来真实收益
- 不把优化绑死在某一个表模型上
- 同时保持 HBase 所要求的字典序 row 排序语义

效果：

- 数值前缀场景下的排序成本明显下降
- 更重要的是，优化对象从“某一张表”扩展成了“一类 rowkey 规则”

### 6.8 明确拒绝或暂缓的方向

- 已拒绝：在同一个 JVM 内直接对比 JNI 与纯 Java
  - CPU 与内存记账不公平
- 已拒绝：让 C++ writer 在写文件时回调 Java 做 gzip
  - 与“纯 C++ native writer”目标冲突
- 已拒绝：为了吞吐去冒险修改字节格式
  - 会破坏字节级一致性验证价值
- 已暂缓：更激进的 rowkey 专用微优化
  - 当前 profiling 说明大文件主瓶颈已经不是排序
- 已暂缓：基于这次 macOS 结果直接给出 Linux 级公平结论
  - 这轮结果很适合工程回归和方案评审，但还不是最终的 Linux 公平性声明

## 7. 技术评审结论

对技术评委而言，本轮工作的关键结论不是“把某张表调快了”，而是：

- 优化路线不是针对单个场景做临时调参
- 而是遵循了分阶段工程策略：
  - 先把正确性和基准口径做可信
  - 再消除通用的 per-block 固定开销
  - 再让压缩工作与写路径并行重叠
  - 最后只在严格保持排序语义的前提下，对适用的 rowkey 规则启用快速路径

当前状态：

- 文件格式正确性：通过
- JNI/C++ 与纯 Java 在基线场景上的字节级一致性：通过
- 最新正式矩阵目标：通过
- 当前目标状态：六个标准场景均满足 `JNI/C++ >= 3x 纯 Java`

当前主瓶颈：

- 大文件场景下，JNI/C++ 当前已主要受 `GZ` 压缩吞吐限制

建议的下一步优化重点：

- 压缩流水线策略及默认参数继续优化
- 在保持字节输出不变的前提下，继续提升 gzip 吞吐
- 在 Linux 公平环境下结合 CPU 绑核与进程内存限制做正式复跑

## 8. 备注

- 本次重跑发生在当前 macOS 开发机上，而不是 Linux 公平对比环境。
- 由于 `/proc`、cgroup、`taskset` 都是 Linux 特有机制，因此本轮 `process_*` 级 RSS/CPU 数据不能作为最终公平结论。
- 但 perf runner 仍然正确输出了结构化报告，本轮结果依然适合做工程回归跟踪、技术方案审视与阶段性评审。

## 9. 技术评委答辩摘要

**1. 问题**  
本项目要解决的是 Arrow 到 HFile 的高性能批量转换问题，并对 `JNI/C++` 与纯 `Java` 两条实现路径做公平对比。这里有两个前提必须同时成立：一是两种实现生成的 HFile 必须都符合 HBase 规范，且最好做到字节级完全一致；二是性能对比必须在相同 CPU 绑定、相同总内存约束、相同压缩与编码配置下进行，否则结论没有说服力。此前最大的风险并不是“谁更快”，而是“功能是否等价、口径是否公平、结果是否可复现”。

**2. 方案**  
整体方案分成“正确性优先”和“性能优化”两层推进。正确性层面，先补齐纯 Java 路径的 Bloom 和必要 FileInfo，统一 `compression=GZ`、`compression level=1`、`encoding=NONE`，并持续用一致性测试校验 `JNI/C++` 与纯 Java 输出是否字节级一致、是否可被 HBase 正常读取。性能层面，基准框架改为“父进程调度 + 独立 worker 子进程执行”，避免在同一 JVM 内直接比较 JNI 与 Java；同时加入 CPU 绑核、JVM 堆和 Direct Memory 限制、C++ SDK 内部 `max_memory_bytes` 软预算，以及进程级与 SDK 级两套内存观测指标，确保性能数据有可解释性。

**3. 取舍**  
这轮优化里，我们有意识地做了几项关键取舍。第一，坚持“格式一致性优先于激进提速”，任何可能改变 HFile 字节布局的改动都必须非常谨慎，因此不建议为了测试方便去改动核心格式语义。第二，明确删除“C++ 写 HFile 时再回调 Java 做压缩”的方案，保证 C++ 路径在写文件过程中完全使用 C++ 自身实现，避免语言边界来回穿透。第三，性能基准不再采用同 JVM 内直接调用 JNI 与纯 Java 的方式，而是改为独立子进程对比，因为前者无法严格比较进程级 CPU 和内存。第四，排序优化采取“按 rowkey 形态启用”的安全策略，只对确定可判定的零填充数值前缀启用 fast path，避免为了单表特化而牺牲通用正确性。

**4. 收益**  
截至最新一轮验证，正确性方面已经达到当前目标：`JNI/C++` 与纯 Java 生成的 HFile 在基线一致性测试下保持字节级一致，并且符合 HBase 读取预期。性能方面，最新六个场景全部达到 `JNI/C++ > 3x Java`：`single-001mb` 为 `11.08x`，`single-010mb` 为 `8.69x`，`single-100mb` 为 `4.61x`，`single-500mb` 为 `3.78x`，`directory-100x001mb` 为 `4.70x`，`directory-100x010mb` 为 `3.70x`。从技术收益看，真正贡献最大的不是单点微优化，而是几项组合策略：`zlib` 状态复用、压缩级别统一为 `1`、块压缩流水线并行化、rowkey 数值前缀排序 fast path，以及更公平的性能测试口径。这些改动让 C++ 优势从“小文件偶发快”变成了“全场景稳定领先”。

**5. 剩余瓶颈**  
当前剩余瓶颈已经比较清楚：文件越大，C++ 相对 Java 的领先倍数会收敛，根因不是排序，而是 `GZ` 压缩逐渐成为主导成本。最近多轮 profiling 显示，在大文件场景下，排序和 Pass1/Pass2 的对象整理已不是主要热点，真正吃掉时间的是 block 压缩本身；当工作负载越来越接近“持续压缩吞吐”时，两种实现都会被压缩算法上限约束，因此倍数差距会自然缩小。换句话说，当前系统已经从“业务逻辑/数据结构瓶颈”阶段，进入“压缩吞吐瓶颈”阶段，这说明前面的排序与写路径优化是有效的，但也意味着后续增益会更依赖压缩体系和流水线调度。

**6. 下一步优化方向**  
下一阶段建议聚焦三条主线。第一，继续提升 C++ 压缩吞吐，包括更细粒度的压缩流水线调度、自适应线程数与队列深度，以及在不同 Linux `x86/ARM` 多核环境下做参数寻优，争取把大文件场景的领先优势继续拉开。第二，继续压缩 Pass1/Pass2 中的分配与拷贝成本，重点检查 `SortEntry`、row key 物化、cell value 搬运等热点，减少不必要的内存分配、字符串构造和数据复制。第三，把当前 macOS 上已经跑通的口径，进一步固化到 Linux 正式环境中，结合 `/proc`、cgroup 和更稳定的 CPU/内存观测，形成可直接向评委和业务方展示的“规范化基准报告”。从阶段判断看，项目已经完成“功能等价 + 格式正确 + 全场景 3x 领先”的里程碑，后续优化重点将从“修正确性和架构口径”转向“围绕压缩吞吐做工程深挖”。
