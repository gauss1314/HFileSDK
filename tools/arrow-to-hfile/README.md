# arrow-to-hfile

Arrow IPC Stream → HBase HFile v3 转换工具，基于 HFileSDK C++ 共享库实现。

## 快速开始

### 1. 编译

```bash
cd tools/arrow-to-hfile
mvn package -q
# 输出: target/arrow-to-hfile-4.0.0.jar
```

### 2. 命令行使用（目标1）

```bash
java -jar target/arrow-to-hfile-4.0.0.jar \
  --native-lib /path/to/libhfilesdk.so \
  --arrow      /data/events.arrow \
  --hfile      /staging/cf/events.hfile \
  --table      events_table \
  --rule       "STARTTIME,0,false,10#IMSI,1,true,15#MSISDN,2,false,11"
```

或通过环境变量指定 native 库路径：

```bash
export HFILESDK_NATIVE_LIB=/path/to/libhfilesdk.so
java -jar target/arrow-to-hfile-4.0.0.jar \
  --arrow /data/events.arrow \
  --hfile /staging/cf/events.hfile \
  --rule  "ID,0,false,0"
```

查看所有参数：

```bash
java -jar target/arrow-to-hfile-4.0.0.jar --help
```

输出示例：

```
Converting
  arrow : /data/events.arrow
  hfile : /staging/cf/events.hfile
  table : events_table
  rule  : STARTTIME,0,false,10#IMSI,1,true,15
Result: OK  kvs=1,523,400  skipped=23  hfile=845.0MB  elapsed=1230ms  sort=450ms  write=260ms
Throughput: 612.4 MB/s
```

**退出码**：`0` 成功；`1` 参数错误；`2` native 库加载失败；其他值对应 SDK 错误码。

---

### CLI 参数参考

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--arrow PATH` | ✓ | — | Arrow IPC Stream 输入文件路径 |
| `--hfile PATH` | ✓ | — | HFile v3 输出路径 |
| `--rule RULE` | ✓ | — | Row Key 规则表达式（见下文） |
| `--table NAME` | | `""` | HBase 表名（仅用于日志） |
| `--native-lib PATH` | | 从环境变量 | `libhfilesdk.so` 的绝对路径 |
| `--cf CF` | | `cf` | Column Family 名称 |
| `--compression ALG` | | `lz4` | 压缩算法：`none`/`lz4`/`zstd`/`snappy`/`gzip` |
| `--encoding ENC` | | `FAST_DIFF` | 块编码：`NONE`/`PREFIX`/`DIFF`/`FAST_DIFF` |
| `--bloom TYPE` | | `row` | Bloom Filter：`none`/`row`/`rowcol` |
| `--block-size BYTES` | | `65536` | 数据块大小（字节） |
| `--fsync-policy` | | `safe` | Fsync 策略：`safe`/`fast`/`paranoid` |
| `--error-policy` | | `skip_row` | 行错误策略：`strict`/`skip_row`/`skip_batch` |

---

### Native Library 加载顺序

1. `--native-lib /absolute/path/libhfilesdk.so`（命令行参数）
2. `HFILESDK_NATIVE_LIB` 环境变量（绝对路径）
3. `HFILESDK_NATIVE_DIR` 环境变量（目录，自动拼接文件名）
4. `-Djava.library.path=/dir` JVM 参数

---

## 生产系统集成（目标2）

### 安装到本地 Maven 仓库

```bash
# 先编译 C++ SDK，产生 libhfilesdk.so
cmake -B build -DCMAKE_BUILD_TYPE=Release -DHFILE_ENABLE_TESTS=OFF
cmake --build build -j$(nproc)

# 安装 arrow-to-hfile 到本地 Maven 仓库
cd tools/arrow-to-hfile
mvn install -q
```

### 在生产工程中引入依赖

```xml
<!-- pom.xml -->
<dependency>
    <groupId>io.hfilesdk</groupId>
    <artifactId>arrow-to-hfile</artifactId>
    <version>4.0.0</version>
</dependency>
```

### 代码示例

#### 最简单的用法

```java
import io.hfilesdk.converter.*;

// 应用启动时加载 native 库（只需一次）
ArrowToHFileConverter converter =
    ArrowToHFileConverter.withNativeLib("/opt/hfilesdk/libhfilesdk.so");

// 每次转换
ConvertResult result = converter.convert(
    ConvertOptions.builder()
        .arrowPath("/data/events.arrow")
        .hfilePath("/staging/cf/events.hfile")
        .tableName("events_table")
        .rowKeyRule("STARTTIME,0,false,10#IMSI,1,true,15")
        .columnFamily("cf")
        .compression("lz4")
        .build());

if (!result.isSuccess()) {
    throw new ConvertException(result);
}
log.info("Converted: {}", result.summary());
log.info("Throughput: {:.1f} MB/s", result.throughputMbps());
```

#### 异常方式（convertOrThrow）

```java
try {
    ConvertResult r = converter.convertOrThrow(opts);
    // r.isSuccess() == true guaranteed here
    log.info("{}", r.summary());
} catch (ConvertException e) {
    if (e.getErrorCode() == ConvertResult.ARROW_FILE_ERROR) {
        // Arrow 文件缺失或格式错误
    } else if (e.getErrorCode() == ConvertResult.DISK_EXHAUSTED) {
        // 磁盘空间不足
    }
    log.error("Conversion failed: {}", e.getMessage());
}
```

#### Spring Boot 集成示例

```java
@Component
public class ArrowToHFileService {

    private final ArrowToHFileConverter converter;
    private final String columnFamily;

    public ArrowToHFileService(
            @Value("${hfilesdk.native-lib}") String nativeLib,
            @Value("${hfilesdk.column-family:cf}") String cf) {
        this.converter    = ArrowToHFileConverter.withNativeLib(nativeLib);
        this.columnFamily = cf;
    }

    /**
     * 将 Arrow 文件转换为 HFile，返回输出路径。
     *
     * @param arrowPath  输入 Arrow IPC Stream 文件路径
     * @param hfilePath  输出 HFile 路径（原子写入）
     * @param tableName  HBase 表名
     * @param rowKeyRule Row Key 规则表达式
     * @throws ConvertException 转换失败时抛出
     */
    public ConvertResult convert(String arrowPath, String hfilePath,
                                  String tableName, String rowKeyRule) {
        return converter.convertOrThrow(
            ConvertOptions.builder()
                .arrowPath(arrowPath)
                .hfilePath(hfilePath)
                .tableName(tableName)
                .rowKeyRule(rowKeyRule)
                .columnFamily(columnFamily)
                .build());
    }
}
```

```yaml
# application.yml
hfilesdk:
  native-lib: /opt/hfilesdk/libhfilesdk.so
  column-family: cf
```

#### 批量转换（线程安全）

```java
// ArrowToHFileConverter 实例是线程安全的：
// 每次 convert() 调用创建独立的 HFileSDK C++ 对象，互不干扰。

ExecutorService pool = Executors.newFixedThreadPool(4);
ArrowToHFileConverter converter =
    ArrowToHFileConverter.withNativeLib("/opt/hfilesdk/libhfilesdk.so");

List<Future<ConvertResult>> futures = files.stream()
    .map(arrowFile -> pool.submit(() ->
        converter.convert(ConvertOptions.builder()
            .arrowPath(arrowFile.toString())
            .hfilePath(outputDir + "/" + arrowFile.getFileName())
            .rowKeyRule("ID,0,false,0")
            .build())))
    .toList();

for (Future<ConvertResult> f : futures) {
    ConvertResult r = f.get();
    if (!r.isSuccess()) log.error("Failed: {}", r.summary());
    else                log.info("OK: {}", r.summary());
}
```

---

## Row Key 规则格式

规则字符串由 `#` 分隔的段组成，每段：

```
name,index,isReverse,padLen[,padMode][,padContent]
```

| 字段 | 说明 | 默认 |
|------|------|------|
| `name` | 列名标签（信息性，不参与逻辑） | 必填 |
| `index` | Arrow Schema 中的列索引（0-based） | 必填 |
| `isReverse` | `true` = 先填充后反转 | 必填 |
| `padLen` | 目标宽度；0=不填充；值超长时不截断 | 必填 |
| `padMode` | `LEFT`（默认）或 `RIGHT` | `LEFT` |
| `padContent` | 填充字符 | `0` |

特殊名：
- `$RND$` / `RANDOM`：生成 `padLen` 位随机数字（每位 0–8）
- `FILL`：使用空串后继续填充/反转
- `long(hash)` / `short(hash)`：对字段做 Java 兼容的数值编码

**示例：**

```
# col[0] 左填充到10位 + col[1] 反转 + col[2] 右填充到11位 + 4位随机数
STARTTIME,0,false,10#IMSI,1,true,15#MSISDN,2,false,11,RIGHT,#$RND$,3,false,4

# 直接使用 col[0] 作为 Row Key，不做任何变换
ID,0,false,0
```

---

## 错误码参考

| 错误码 | 名称 | 说明 |
|--------|------|------|
| 0 | OK | 成功 |
| 1 | INVALID_ARGUMENT | 参数非法 |
| 2 | ARROW_FILE_ERROR | Arrow 文件无法打开或格式错误 |
| 3 | SCHEMA_MISMATCH | Arrow Schema 与 rowKeyRule 不匹配 |
| 4 | INVALID_ROW_KEY_RULE | rowKeyRule 语法错误 |
| 5 | SORT_VIOLATION | KV 排序违规（逻辑 Bug） |
| 10 | IO_ERROR | 文件 I/O 错误 |
| 11 | DISK_EXHAUSTED | 磁盘空间不足 |
| 12 | MEMORY_EXHAUSTED | 内存超限 |
| 20 | INTERNAL_ERROR | C++ 内部错误 |

---

## 项目结构

```
tools/arrow-to-hfile/
├── pom.xml
├── README.md
└── src/main/java/
    ├── com/hfile/
    │   └── HFileSDK.java              # JNI 桥接（自包含副本）
    └── io/hfilesdk/converter/
        ├── ArrowToHFileConverter.java  # 主类：CLI + 生产 API
        ├── ConvertOptions.java         # 转换参数（Builder 模式）
        ├── ConvertResult.java          # 转换结果（含 JSON 解析）
        ├── ConvertException.java       # 失败时的异常
        ├── NativeLibLoader.java        # native 库加载工具
        └── NativeLibLoadException.java # 加载失败异常
```
