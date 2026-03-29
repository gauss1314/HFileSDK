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
| Windows x86-64     | Clang 17+（`clang-cl`） | ✅ **核心功能** | 见下方说明                                    |
| Windows x86-64     | MSVC 2022             | ⚠️ **需验证** | `__builtin_*` 需替换为 MSVC 等价物              |

### Windows + Clang 支持说明

核心编码、压缩、索引、Bloom Filter 等模块**完全跨平台**，可在 Windows + Clang 上直接编译。

以下功能在 Windows 上**自动禁用**（CMake 自动检测，无需手动配置）：

| 功能              | 原因                        | 影响                                           |
| --------------- | ------------------------- | -------------------------------------------- |
| `IoUringWriter` | Linux 内核专有 API            | 仅影响可选的异步 I/O 后端；`BufferedFileWriter` 在所有平台可用 |
| `HdfsWriter`    | `libhdfs3` 无官方 Windows 构建 | 仅影响直写 HDFS；本地文件写入不受影响                        |

以下特性在 Windows Clang 上**完全支持**：

- SSE4.2 CRC32C 硬件加速（`nmmintrin.h` 在 MSVC/Clang-cl 中可用）
- SIMD 前缀扫描（`_mm_cmpeq_epi8` 等）
- `__builtin_ctz`、`__builtin_expect`（Clang-cl 完全支持）
- `std::filesystem`（C++20 标准，Windows 完全支持）
- Arrow C++ 15+、Protobuf、LZ4、ZSTD、Snappy（均有官方 Windows 构建）

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
cmake -B build -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_FLAGS="-O3 -march=native"
cmake --build build -j$(nproc)

# 运行测试
cd build && ctest --output-on-failure
```

### Windows（Clang + vcpkg）

```powershell
# 1. 安装依赖（通过 vcpkg）
vcpkg install arrow:x64-windows protobuf:x64-windows `
              lz4:x64-windows zstd:x64-windows snappy:x64-windows `
              zlib:x64-windows gtest:x64-windows

# 2. 配置（使用 Clang-cl）
cmake -B build -G "Ninja" `
      -DCMAKE_BUILD_TYPE=Release `
      -DCMAKE_CXX_COMPILER=clang-cl `
      -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" `
      -DHFILE_ENABLE_IO_URING=OFF `
      -DHFILE_ENABLE_HDFS=OFF

# 3. 编译
cmake --build build

# 4. 测试
cd build && ctest --output-on-failure
```

***

## API 快速示例

### 单文件写入

```cpp
#include <hfile/hfile.h>

auto [writer, status] = hfile::HFileWriter::builder()
    .set_path("/tmp/staging/cf1/hfile_0001.hfile")
    .set_column_family("cf1")
    .set_compression(hfile::Compression::LZ4)
    .set_data_block_encoding(hfile::Encoding::FastDiff)
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

### Bulk Load 写入（推荐）

```cpp
#include <hfile/hfile.h>

auto [bulk, status] = hfile::BulkLoadWriter::builder()
    .set_output_dir("/tmp/staging/my_table")
    .set_column_families({"cf1", "cf2"})
    .set_partitioner(hfile::RegionPartitioner::from_splits(split_points))
    .set_compression(hfile::Compression::LZ4)
    .set_parallelism(4)   // 并行 finish 4 个 HFile
    .build();

// 写入（自动按 CF + Region 路由到对应 HFile）
for (auto& batch : arrow_batches)
    bulk->write_batch(batch, hfile::MappingMode::WideTable);

auto [result, s] = bulk->finish();
// result.staging_dir = "/tmp/staging/my_table"
// result.files = ["cf1/hfile_region_0000.hfile", "cf2/hfile_region_0001.hfile", ...]
// 若 max_open_files 触发滚动关闭，同一 Region 可能生成 *_0001.hfile 等后续分片

// 之后通过 BulkLoadHFilesTool 加载到 HBase
```

***

## 模块架构

```
include/hfile/          公开 API
src/
  checksum/             CRC32C（SSE4.2 + 标量回退）
  memory/               ArenaAllocator + BlockPool（零热路径分配）
  block/                数据块编码器（None / Prefix / Diff / FastDiff）
  codec/                压缩（LZ4 / ZSTD / Snappy / GZip / None）
  bloom/                Compound Bloom Filter（Murmur3，分块）
  index/                2级 Block Index（单级 + 中间索引块）
  meta/                 FileInfo（10个必填字段）+ ProtoBuf Trailer
  io/                   BufferedFileWriter（跨平台）
                         IoUringWriter（Linux only，双缓冲）
                         HdfsWriter（Linux/macOS only）
  partition/            RegionPartitioner + CFGrouper
  arrow/                Arrow → KV 转换（WideTable / TallTable / RawKV）
proto/                  FileTrailerProto（HFile v3）
test/                   21 个测试文件（已全部纳入 ctest）
bench/                  微基准 + 端到端基准
tools/                  Java 验证工具 + hfile-chaos + Python HTML 报告生成器
```

***

## 测试文档

完整测试覆盖矩阵、运行方式和当前边界说明见 [TESTING.md](file:///Users/gauss/workspace/github_project/HFileSDK/TESTING.md)。

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
4. Trailer 必须用 **ProtoBuf** 序列化，尾部固定格式 `[pb_offset(4B)][3][3]`
5. FileInfo 必须包含全部 **10 个必填字段**
6. Bulk Load 目录结构：`<output_dir>/<cf_name>/<hfile>`
7. 文件内 KV **严格有序**：Row↑ → Family↑ → Qualifier↑ → Timestamp↓ → Type↓
8. 所有多字节整数均为 **Big-Endian**

***

## 许可证

Apache License 2.0
