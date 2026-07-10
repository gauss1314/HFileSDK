# HFileSDK

高性能 C++20 SDK，将 Apache Arrow 内存数据写入 HFile v3 格式，用于 HBase 2.6.1 Bulk Load。

**性能目标**：写入吞吐量 ≥ Java `HFile.Writer` 的 3 倍。

***

## 平台支持

| 平台                 | 编译器                   | 支持级别       | 说明                                       |
| ------------------ | --------------------- | ---------- | ---------------------------------------- |
| Linux x86-64       | GCC 12+               | ✅ **完整支持** | 所有功能，含 io\_uring、SSE4.2                  |
| Linux x86-64       | Clang 16+             | ✅ **完整支持** | 同上                                       |
| macOS x86-64/arm64 | Apple Clang 15+       | ✅ **完整支持** | 除 io\_uring（Apple Silicon 含 CRC32C 硬件指令） |
| Windows x86-64     | Clang 17+（MSYS2 `clang/clang++`） | ✅ **核心功能** | 在 MSYS2 `CLANG64` 环境中直接执行 `scripts/*.sh` |

### Windows + MSYS2 Clang 支持说明

核心编码、压缩、索引、Bloom Filter 等模块**完全跨平台**，当前 Windows 路径统一约定为 **MSYS2 + clang/clang++ + 直接执行 `.sh` 脚本**。

以下功能在 Windows 上**自动禁用**（CMake 自动检测，无需手动配置）：

| 功能              | 原因                        | 影响                                           |
| --------------- | ------------------------- | -------------------------------------------- |
| `IoUringWriter` | Linux 内核专有 API            | 仅影响可选的异步 I/O 后端；`BufferedFileWriter` 在所有平台可用 |
| `HdfsWriter`    | `libhdfs3` 无官方 Windows 构建 | 仅影响直写 HDFS；本地文件写入不受影响                        |

以下特性在 Windows + MSYS2 Clang 上**完全支持**：

- SSE4.2 CRC32C 硬件加速（`nmmintrin.h` 在 Clang 中可用）
- SIMD 前缀扫描（`_mm_cmpeq_epi8` 等）
- `__builtin_ctz`、`__builtin_expect`（Clang 完全支持）
- `std::filesystem`（C++20 标准，Windows 完全支持）
- Arrow C++ 15+、Protobuf、zlib/zlib-ng（均有官方 Windows 构建）

***

## 快速开始

### Linux / macOS

```bash
# 安装依赖（Ubuntu）
sudo apt install \
    libarrow-dev libprotobuf-dev protobuf-compiler \
    liblz4-dev libzstd-dev libsnappy-dev zlib1g-dev \
    libgtest-dev

# 配置 + 编译
bash scripts/build.sh -DCMAKE_CXX_FLAGS="-O3 -march=native"

# 运行测试
bash scripts/test.sh
```

### Windows（MSYS2 Clang64）

Windows 下当前只维护 **MSYS2 内的 Clang/LLVM 工具链** 这一路径。请先进入 **MSYS2 Clang64 Shell**，再直接执行仓库中的 `.sh` 脚本：

```bash
bash scripts/build.sh
bash scripts/test.sh
bash scripts/coverage.sh
bash scripts/hfile-bulkload-perf-runner.sh --skip-login -- --help
```

命令顺序与 Linux / macOS 侧保持一致：**build → test → coverage → perf**。推荐前提：

- `MSYSTEM=CLANG64`
- `cmake`、`clang/clang++`、`llvm-cov`、`llvm-profdata` 在该 MSYS2 环境中可见
- Arrow 依赖可通过 `CMAKE_PREFIX_PATH` 或 `Arrow_DIR` 被 CMake 发现
- 更完整的脚本参数与平台说明见 [scripts/README.md](file:///Users/gauss/workspace/github_project/HFileSDK/scripts/README.md)

***

## API 快速示例

### 单文件写入

```cpp
#include <hfile/hfile.h>

auto [writer, status] = hfile::HFileWriter::builder()
    .set_path("/tmp/staging/cf1/hfile_0001.hfile")
    .set_column_family("cf1")
    .set_compression(hfile::Compression::GZip)
    .set_data_block_encoding(hfile::Encoding::None)
    .set_bloom_type(hfile::BloomType::Row)
    .build();

if (!status.ok()) { /* handle error */ }

// 默认 AutoSort：可追加无序 KV，writer 在 finish() 前做内存排序
for (auto& kv : input_kvs) {
    auto s = writer->append(kv);
    if (!s.ok()) { /* handle error */ }
}

writer->finish();  // AutoSort + 写 Index、Bloom、FileInfo、Trailer
```

### 当前收口范围

当前版本只保留单文件转换主路径：`Arrow/JNI -> convert() -> 1 个 HFile`。
多列族、多 Region 自动拆分、多 HFile 批量编排能力已移除。

***

## JNI/C++ 性能调优

以下建议以**可分配给转换进程的物理 CPU 核数** `C` 为基准，而不是机器的逻辑线程总数。生产环境还会同时运行 JVM、HDFS 客户端和其他任务时，应先按容器的 CPU quota / cpuset 折算 `C`。

数据块压缩使用进程级共享线程池。多个 Writer 都配置 `compression_threads=T` 时，全进程只有 `T` 个压缩 worker，而不是 `P*T` 个；线程池只会增长到进程内出现过的最大 `T`。实现还会用 Linux `sched_getaffinity` 和 cgroup CPU quota、macOS 物理核数或 Windows active processors 对请求值做上限保护，但生产配置仍应按实际 CPU、NUMA 和文件系统压测后确定。

### 关键参数

JNI `configure()` 和性能工具使用以下参数：

| JNI 配置 | 性能工具参数 | 含义 |
| --- | --- | --- |
| `compression_threads` | `--jni-sdk-compression-threads` | 进程级共享 GZip worker 目标数，同时限制单个 Writer 已提交/执行的压缩任务数；`0` 表示各文件在调用线程同步压缩 |
| `compression_queue_depth` | `--jni-sdk-compression-queue-depth` | 每个转换任务允许存在的 in-flight 数据块数；`0` 表示自动取 `clamp(4 * compression_threads, 4, 64)`，显式值范围 `1～4096` |
| `max_memory_bytes` | `--jni-sdk-max-memory-mb` | SDK 可归因内存预算；生产环境建议显式设置，`0` 仅适合资源已被外层严格限制的压测或专用进程 |
| `numeric_sort_fast_path` | `--jni-sdk-numeric-sort-fast-path` | `auto`、`on` 或 `off`；生产默认用 `auto` |

`compression_queue_depth=0` 是推荐值。自动队列会随压缩线程数增长，同时封顶 64。队列深度仍然是**每个 Writer** 的值，所以目录并行时流水线缓冲内存约随 `P * queue_depth` 增长；显式值主要用于复现实验，队列过小会让压缩线程断粮，过大通常只增加内存。

### 按 CPU 核数选择起点

单个大文件追求最低延迟时使用 `parallelism=1`，从下表起步：

| 可用物理核 `C` | `compression_threads` | `compression_queue_depth` |
| ---: | ---: | ---: |
| 4 | 2 | 0（自动为 8） |
| 8 | 4～6 | 0（自动为 16～24） |
| 16 | 8～12 | 0（自动为 32～48） |
| 32 | 12～16 | 0（自动为 48～64） |
| 64 及以上 | 16～24 | 0（自动封顶 64） |

目录批量转换追求总吞吐时，先确定并发文件数 `P=parallelism`，再确定全进程共享压缩线程数 `T=compression_threads`。异步压缩模式的线程量近似为：

```text
P + T <= C
```

其中 `P` 个线程负责各自文件的读取、排序、编码和写出，`T` 个线程在所有 Writer 之间共享。`T=0` 时压缩直接在这 `P` 个文件线程上执行，不再额外创建压缩线程。独占 CPU 集或专用转换节点可扫描到 `P+T=C`；与业务 JVM、HDFS 客户端共享 CPU 时，先只使用 `C` 的 75%～90%，保留余量。

先按文件形态选择起点，再用真实数据 sweep：

| 输入形态 | `P` 起点 | 共享 `T` 起点 | 说明 |
| --- | ---: | ---: | --- |
| 单个大文件或合并后单文件 | 1 | `max(1, C-2)`，先封顶 16～24 | 让主线程负责读取/排序/写出，其余核用于 GZip |
| 大量约 1MiB 独立文件 | `max(1, C-2)` | 0 | 文件级同步压缩通常最省调度开销；本机 8 核的 100×1MiB 矩阵使用 `P=6,T=0` |
| 大量约 10MiB 或更大的独立文件 | `max(1, C/4)` | `max(1, C-P)` | 降低同时排序/编码的文件数，把其余核交给共享压缩池；本机 8 核的 100×10MiB 矩阵使用 `P=2,T=6` |
| 多个中大文件 | `C/2` | `C/2-1` | 这是通用起点；还要向低 `P`、高 `T` 一侧扫描，避免多个文件同时争抢压缩 CPU |
| CPU 配额经常变化的容器 | `C/2` | `C/2-1` | 留出至少 1 核给 JVM、GC、文件系统与回调 |

`merge_threshold` 会改变 HFile 文件集合，不是纯性能参数。需要保持一输入一 HFile 时不得为了提速切到合并策略；需要合并输出时，则把每个合并批次视为一个大文件调优。

如果机器启用了 SMT/超线程，GZip 通常不能把两个逻辑线程当成两个完整物理核使用；优先按物理核配额起步。跨 NUMA 节点时，优先把进程 CPU 和内存绑定在同一节点，并让输入、临时目录和输出落在本地 NVMe/SSD。

### 内存、排序与格式参数

- 每个活跃 writer 的压缩流水线内存大致随 `queue_depth * block_size` 线性增长，实际还包含未压缩块、压缩输出上界、Bloom、索引、Arrow batches 和排序索引。并发转换时必须再乘以 `P`。
- `max_memory_bytes` 是 SDK 内部 soft budget，不等于整个进程 RSS。生产值应从进程硬限制中扣除 JVM heap、Arrow direct memory、JNI/JVM 固定开销和系统余量；压测后确认 `tracked_memory_peak_bytes` 不贴近预算。
- `numeric_sort_fast_path=auto` 会只在首段 row key 是非负数、左侧补 `0` 且值不超过 `padLen` 时启用；纯数值 row key 使用稳定基数排序，预算不足则自动回退到原地比较排序。`on` 适合校验数据是否完全满足快路径前提，`off` 仅用于诊断或 A/B。
- 保持 `block_size=65536`、`compression=GZ`、`compression_level=1`、`encoding=NONE` 可维持当前文件布局和读写兼容基线。修改 block size、压缩方式、Bloom 或编码会改变 HFile 字节内容，必须重新做 HBase Reader 与 Bulk Load 验证。
- `compression=NONE` 不启用压缩流水线，此时压缩线程和队列参数不会带来收益。

### JNI 会话复用

`HFileSDK` 和 `ArrowToHFileConverter` 都实现了 `AutoCloseable`。Converter 使用有界 prepared-session 租赁池；每次转换独占一个 session，配置 JSON 不变时可跨调用复用，同时不会因短命线程或虚拟线程的历史数量无限保留 native handle。业务代码也应复用 Converter，避免每个小文件重新初始化：

```java
try (ArrowToHFileConverter converter =
         ArrowToHFileConverter.withNativeLib("/opt/hfilesdk/libhfilesdk.so")) {
    for (ConvertOptions options : files) {
        ConvertResult result = converter.convert(options);
        if (!result.isSuccess()) {
            throw new ConvertException(result);
        }
    }
}
```

共享同一个 Converter 可并发调用：每个活跃转换独占租赁 session，`close()` 会等待已经开始的 `convert + getLastResult` 完成。Java interrupt/线程池 `shutdownNow()` 不能中止已经进入 JNI 的转换，只能阻止尚未开始的任务。

新 native library 保留旧版已经编译的三个 JNI 方法符号，因此支持“旧 Java class + 新 native library”。反方向不兼容：当前 Java class 需要新的 session JNI ABI，不能搭配旧 `.so/.dylib/.dll`。生产滚动发布必须先部署 native library，或将 Java jar 与匹配的 native library 作为一个原子版本发布；版本不匹配时构造器会返回明确的 ABI 错误。

### 在目标机器上做三轮 sweep

性能结论必须在生产同型号 CPU、文件系统和典型 Arrow 数据上复核。每次只改变一个维度，性能工具固定执行三轮；比较 `average_ms`、每轮波动、`process_peak_rss_bytes`、`sdk_tracked_memory_peak_bytes`，并用 `hfile-verify` 校验结果。目录对比还必须确认 `comparison_valid=true`；工具只有在双方 `strategy`、实际 `worker_parallelism`、`output_layout`、`hfile_count` 和 `kv_written_count` 全部一致时才生成 `java_over_jni_speedup`。一一对应目录场景会让 JNI 与纯 Java 使用相同的有界并行度；JNI 小文件合并场景仍只用于生产策略评估，不计入引擎加速比。

例如 16 个可用物理核的单文件 GZip 起步命令：

```bash
java -jar tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar \
  --native-lib ./build/libhfilesdk.so \
  --implementations arrow-to-hfile,arrow-to-hfile-java \
  --scenario-filter single-500mb \
  --compression GZ \
  --encoding NONE \
  --bloom row \
  --block-size 65536 \
  --parallelism 1 \
  --jni-sdk-compression-threads 8 \
  --jni-sdk-compression-queue-depth 0 \
  --jni-sdk-numeric-sort-fast-path auto \
  --work-dir /data/tmp/hfilesdk-perf \
  --report-json /data/tmp/hfilesdk-perf/report.json
```

单文件建议依次测试 `T` 的相邻值，例如 16 核测试 `6/8/10/12`；约 1MiB 的小文件目录先测试 `T=0` 下的 `P`，约 10MiB 及以上的独立文件再比较 `P+T≈C` 且 `P` 分别为 `C/4`、`C/2`、`3C/4` 的分配。若增加线程后均值不再下降或波动、RSS 上升，就使用前一个配置。完整矩阵和 CPU/内存隔离命令见 [TESTING.md](TESTING.md) 与 [性能工具说明](tools/hfile-bulkload-perf/README.md)。

***

## 模块架构

```
include/hfile/          公开 API
src/
  checksum/             CRC32C（SSE4.2 + 标量回退）
  memory/               ArenaAllocator + BlockPool（零热路径分配）
  block/                数据块编码器（None / Prefix / Diff / FastDiff）
  codec/                压缩（GZip / None）
  bloom/                Compound Bloom Filter（Murmur3，分块）
  index/                2级 Block Index（单级 + 中间索引块）
  meta/                 FileInfo（10个必填字段）+ ProtoBuf Trailer
  io/                   BufferedFileWriter（跨平台）
                         IoUringWriter（Linux only，双缓冲）
                         HdfsWriter（Linux/macOS only）
  partition/            RegionPartitioner
  arrow/                RowKeyRule 相关 Arrow 处理
proto/                  FileTrailerProto（HFile v3）
 test/                   主路径与回归测试（已全部纳入 ctest）
tools/                  Java 转换器 / 性能对比工具 / 验证工具 / hfile-chaos / Python HTML 报告生成器
```

***

## 测试文档

完整测试覆盖矩阵、运行方式和当前边界说明见 [TESTING.md](file:///Users/gauss/workspace/github_project/HFileSDK/TESTING.md)。
当前 `ctest` 还额外纳入了 Java JNI 端到端集成测试，以及基于 HBase 原生 Reader 的黑盒校验测试，后者会校验关键元数据与固定 cell 内容顺序。

本地构建与测试脚本说明见 [scripts/README.md](file:///Users/gauss/workspace/github_project/HFileSDK/scripts/README.md)。

覆盖率报表可通过 `llvm-cov` 工作流生成：

```bash
cmake -B build-coverage -DCMAKE_BUILD_TYPE=Release -DHFILE_ENABLE_COVERAGE=ON
cmake --build build-coverage -j8
cmake --build build-coverage --target hfile_coverage
```

本地也可直接运行：

```bash
bash scripts/coverage.sh
```

其中 `build-coverage/artifacts/coverage-html/` 约定为 CI 上传的 HTML 产物目录。

***

## HBase 兼容性约束

以下任何一条违反都会导致文件无法被 HBase 2.6.1 加载：

1. 每个 HFile **只能包含一个** Column Family
2. HFile 版本必须是 **v3**（major=3, minor=3）
3. 每个 Cell 必须包含 `tags_length`（2B）和 `mvcc`（VarInt），即使为零
4. Trailer 必须用 **ProtoBuf** 序列化，尾部为固定 4 KB 区域：`[TRABLK"$][varint pb_length][FileTrailerProto][padding][materialized_version]`
5. FileInfo 必须包含全部 **10 个必填字段**
6. Bulk Load 目录结构：`<output_dir>/<cf_name>/<hfile>`
7. 文件内 KV **严格有序**：Row↑ → Family↑ → Qualifier↑ → Timestamp↓ → Type↓
8. 所有多字节整数均为 **Big-Endian**

***

## 许可证

Apache License 2.0
