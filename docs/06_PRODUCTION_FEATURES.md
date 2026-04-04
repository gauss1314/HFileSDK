# 06 — 生产可靠性特性

本文档覆盖 SDK 中所有与生产环境安全运行相关的机制：崩溃安全、内存控制、输入校验、JNI 异常隔离、可观测性。

---

## 1. 崩溃安全：AtomicFileWriter

### 1.1 核心保证

**任何时刻断电，最终输出路径上要么有完整的 HFile，要么没有文件。绝不会出现写了一半的损坏文件。**

### 1.2 实现机制

`AtomicFileWriter`（`src/io/atomic_file_writer.cc`）将写入分为两个阶段：

**写入阶段**：所有数据写入临时文件 `<parent>/.tmp/<uuid>_<basename>.tmp`。UUID 由 `std::random_device` + `std::mt19937_64` 生成 32 位十六进制字符串，确保并发调用不冲突。

**提交阶段**（`commit()`）：

```
1. inner_->flush()           → 应用层缓冲 → FILE*
2. fflush(file)              → C 库缓冲 → OS 页缓存
3. fsync(fileno(file))       → 页缓存 → 磁盘扇区
4. fclose(file)              → 关闭文件描述符
5. fsync(.tmp 目录)          → 临时文件的目录项持久化
6. rename(.tmp → 最终路径)    → POSIX 原子操作
7. fsync(最终路径父目录)      → rename 的目录项持久化
```

步骤 5 和 7 通过 `fsync_directory()` 实现，打开目录只读 fd 然后 fsync。

### 1.3 失败清理

`AtomicFileWriter` 的析构函数检查 `committed_` 标志。如果 `commit()` 从未成功调用（异常/错误导致提前退出），析构函数自动调用 `abort()`——关闭并删除临时文件。

`HFileWriterImpl` 的析构函数也有类似逻辑：如果 `opened_ && !finished_`，调用 `atomic_writer_->abort()` 或手动删除 plain writer 的输出文件。

### 1.4 FsyncPolicy 选项

| 策略 | 行为 | 适用场景 |
|------|------|---------|
| `Safe`（默认） | 写 .tmp → fsync → rename → fsync dir | 生产环境 |
| `Fast` | 直接写最终路径，finish 时 fsync 一次 | 可重跑的批处理 |
| `Paranoid` | 每 N 个 Block fsync 一次 | 不可靠存储 |

`Safe` 模式使用 `AtomicFileWriter`，其他模式使用普通 `BufferedFileWriter`。

---

## 2. 内存控制

### 2.1 MemoryBudget

`memory::MemoryBudget`（`src/memory/memory_budget.h`）是一个原子计数器，追踪 SDK 自己声明的内存使用量。

```cpp
class MemoryBudget {
    std::atomic<size_t> used_;   // 当前使用量
    std::atomic<size_t> peak_;   // 峰值
    size_t              max_bytes_;  // 上限

    Status reserve(size_t bytes);  // 申请配额
    void   release(size_t bytes);  // 释放配额
};
```

`reserve()` 使用 `fetch_add` + 范围检查。如果超限，立即 `fetch_sub` 回滚并返回错误。这保证了线程安全和无锁高性能。

**配额追踪点**（在 `converter.cc` 中）：

- 每加载一个 Arrow RecordBatch：`budget->reserve(TotalBufferSize(batch))`
- 每创建一个 SortEntry：`budget->reserve(sizeof(SortEntry) + row_key.size())`

**配额追踪点**（在 `writer.cc` 中）：

- AutoSort 模式下每缓存一个 OwnedKeyValue：`budget->reserve(estimate_owned_kv_bytes(kv))`
- 压缩缓冲区初始化：`budget->reserve(compress_buf_.size())`

`[!]` 这是一个**软限制**。MemoryBudget 不拦截 `malloc`/`new`——Arrow 库内部的内存分配、std::string 的分配等不在追踪范围内。实际内存使用可能高于 budget 报告的值。

### 2.2 内存预算的 RAII Guard

```cpp
struct Guard {
    Guard(MemoryBudget& b, size_t bytes);  // 构造时 reserve
    ~Guard();                               // 析构时 release
    bool ok() const;                       // reserve 是否成功
};
```

目前代码中未使用 Guard，直接调用 reserve/release。

### 2.3 ArenaAllocator

`memory::ArenaAllocator`（`src/memory/arena_allocator.h`）提供 bump-pointer 分配：

- 预分配 256KB chunk，64 字节对齐
- `allocate()` 只做指针递增，无锁、无系统调用
- `reset()` 一次性释放所有内存

目前 ArenaAllocator 和 BlockPool 已实现但**未在热路径中使用**——converter.cc 的 Pass 1/2 直接使用 std::string 和 std::vector。它们是为未来性能优化预留的基础设施。

---

## 3. 输入校验

### 3.1 JNI 入口校验

`hfile_jni.cc` 中对所有 JNI 参数做防御性检查：

| 检查项 | 失败返回 |
|--------|---------|
| jstring 为 null | `INVALID_ARGUMENT` |
| `GetStringUTFChars` 失败 | `INTERNAL_ERROR`（并清除 Java 异常） |
| arrowPath 或 hfilePath 为空 | `INVALID_ARGUMENT` |
| configure 的 JSON 解析失败 | `INVALID_ARGUMENT` |
| configure 中出现未知 key | `INVALID_ARGUMENT` |
| configure 中值不合法 | `INVALID_ARGUMENT`（含具体字段名） |

### 3.2 转换层校验

`converter.cc: convert()` 的校验：

| 检查项 | 失败返回 |
|--------|---------|
| Arrow 文件不存在 | `ARROW_FILE_ERROR` |
| rowKeyRule 为空 | `INVALID_ROW_KEY_RULE` |
| rowKeyRule 语法错误 | `INVALID_ROW_KEY_RULE` |
| 列索引超出 Schema 范围 | `SCHEMA_MISMATCH` |
| 列类型不支持作为 Row Key | `SCHEMA_MISMATCH` |
| Arrow 文件无法打开/读取 | `ARROW_FILE_ERROR` |
| 内存超限 | `MEMORY_EXHAUSTED` |

### 3.3 写入层校验

`writer.cc: validate_kv()` 对每个 KV 做实时校验：

| 检查项 | 错误类型 |
|--------|---------|
| Row Key 为空 | `ROW_KEY_EMPTY` |
| Row Key 超过 32KB | `ROW_KEY_TOO_LONG` |
| Value 超过 10MB | `VALUE_TOO_LARGE` |
| Timestamp 为负 | `NEGATIVE_TIMESTAMP` |
| KV 排序违规（Verified 模式） | `SORT_ORDER_VIOLATION`（致命） |
| Column Family 不匹配 | 立即返回错误 |

### 3.4 ErrorPolicy：校验失败后怎么做

| 策略 | 行为 |
|------|------|
| `Strict` | 第一个校验错误就返回错误 |
| `SkipRow`（默认） | 跳过有问题的行，继续处理剩余数据 |
| `SkipBatch` | 跳过整个 RecordBatch |

SkipRow 模式下有累计上限 `max_error_count`（默认 1000），超过后仍然终止。

每个跳过的行都会：
1. 递增 `error_count_` 和 `skipped_rows_`
2. 发出 WARN 日志
3. 如果设置了 `error_callback`，调用回调函数

---

## 4. JNI 异常隔离

### 4.1 原则

**C++ 异常绝不能穿透 JNI 边界到达 JVM。** 如果 C++ 异常到达 JVM，后果是 JVM 进程立即崩溃（SIGABRT），无法恢复。

### 4.2 实现

所有三个 JNI 导出函数都遵循同一模式：

```cpp
JNIEXPORT jint JNICALL Java_com_hfile_HFileSDK_convert(...) {
    try {
        // 全部业务逻辑
        return result.error_code;
    } catch (const std::exception& e) {
        fprintf(stderr, "[ERROR] JNI convert: %s\n", e.what());
        return INTERNAL_ERROR;
    } catch (...) {
        fprintf(stderr, "[ERROR] JNI convert: unknown exception\n");
        return INTERNAL_ERROR;
    }
}
```

`catch(...)` 是最后一道防线，捕获所有非 `std::exception` 的异常（如 abi::__forced_unwind 等）。

### 4.3 Java 异常处理

JNI 字符串提取失败时（`GetStringUTFChars` 返回 null），代码会检查并清除 Java 异常：

```cpp
if (env->ExceptionCheck()) env->ExceptionClear();
```

这防止了 Java 异常与 C++ 异常的交叉污染。

### 4.4 哪些地方可能抛异常

SDK 代码本身遵循"热路径不抛异常"的原则，但以下场景可能抛出：

- `BufferedFileWriter` 构造函数：`fopen` 失败时抛 `std::runtime_error`
- `IoUringWriter` 构造函数：`io_uring_queue_init` 失败时抛
- `std::vector::resize`/`reserve`：内存不足时抛 `std::bad_alloc`
- Arrow C++ API：内部可能抛异常（但通常返回 `arrow::Status`）

所有这些都被 JNI 层的 try-catch 兜住。

---

## 5. 磁盘空间检查

### 5.1 写入前检查

`HFileWriterImpl::open()` 中没有显式的磁盘空间检查——这是因为文件创建本身就是最好的检查。

### 5.2 写入中周期检查

`maybe_check_disk_space()`（`writer.cc`）每写入 `disk_check_interval_bytes`（默认 256MB）就检查一次剩余空间：

```cpp
int64_t free = free_disk_bytes(path_);
if (free < min_free_disk_bytes)  // 默认 512MB
    return IoError("DISK_SPACE_EXHAUSTED");
```

`free_disk_bytes()` 使用 `std::filesystem::space()` 查询可用空间。如果路径不存在则检查父目录。查询失败返回 `INT64_MAX`（安全退化——不因检查失败而阻止写入）。

---

## 6. 可观测性

### 6.1 日志

SDK 使用 `fprintf(stderr, ...)` 而非 spdlog。日志格式：

```
[INFO]  hfile: HFile started: path=/staging/cf/events.hfile cf=cf
[WARN]  hfile: Row skipped: ROW_KEY_EMPTY
[ERROR] hfile: DISK_SPACE_EXHAUSTED: only 23 MB free

[INFO]  convert: Convert started: arrow=/data/events.arrow hfile=...
[WARN]  convert: DUPLICATE_KEY: row key 'abc123' was produced by 3 source rows
[INFO]  convert: Pass 1 done: rows=1523400 sort=450ms
[INFO]  convert: Convert done: kvs=15234000 skipped=23 hfile=845KB elapsed=1230ms
```

两个日志命名空间：`hfile:` 来自 writer.cc，`convert:` 来自 converter.cc。

`[!]` DESIGN.md 计划使用 spdlog，但实际代码直接写 stderr。这简化了依赖（不需要链接 spdlog），但不支持日志级别过滤。所有日志都会输出。

### 6.2 getLastResult() 指标

每次 `convert()` 完成后，结果以 JSON 格式保存在 InstanceState 中。Java 侧通过 `getLastResult()` 读取：

```json
{
  "error_code": 0,
  "error_message": "",
  "arrow_batches_read": 156,
  "arrow_rows_read": 1523400,
  "kv_written_count": 15234000,
  "kv_skipped_count": 23,
  "duplicate_key_count": 5,
  "hfile_size_bytes": 845000000,
  "elapsed_ms": 1230,
  "sort_ms": 450,
  "write_ms": 520
}
```

JSON 由手写的 `result_to_json()` 生成（`hfile_jni.cc`），不依赖任何 JSON 库。字符串字段通过 `json_escape()` 转义。

### 6.3 MetricsRegistry

`src/metrics/metrics_registry.h` 提供了 Counter/Gauge/Histogram 三种指标类型，全部线程安全。但目前 **writer.cc 和 converter.cc 都没有使用它**——统计信息直接记录在成员变量中。MetricsRegistry 是为未来更精细的性能监控预留的。

---

## 7. 并发安全

### 7.1 当前限制

`convert()` 函数**不是线程安全的**——它使用非线程安全的 `RowKeyBuilder`（内含 `std::mt19937` PRNG）和 `HFileWriter`。

但多个 Java `HFileSDK` 实例可以并发调用 `convert()`，因为：

- 每个实例有独立的 `InstanceState`
- `g_config_mutex` 只在读写 InstanceState 时短暂持有
- `convert()` 执行期间不持锁

### 7.2 线程安全的组件

- `MemoryBudget`：使用 `std::atomic`，完全线程安全
- `BlockPool`：使用 `std::mutex`，acquire/release 线程安全
- `MetricsRegistry`：使用 `std::mutex`，所有操作线程安全
- `g_instance_states`：通过 `g_config_mutex` 保护

### 7.3 非线程安全的组件

- `HFileWriter` / `HFileWriterImpl`：明确标注 "NOT thread-safe"
- `RowKeyBuilder`：内含 PRNG
- `DataBlockEncoder` 的所有实现：内含可变缓冲区
- `CompoundBloomFilterWriter`：内含可变状态
