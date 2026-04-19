# arrow-to-hfile-java

纯 Java 版 Arrow → HFile 转换器，使用 HBase 原生 `StoreFileWriter` 写出 HFile，用于与 JNI 版 `tools/arrow-to-hfile` 做对照性能测试。

## 能力

- 输入单个 Arrow IPC Stream 文件
- 依据 `rowKeyRule` 构造 Row Key，并在内存中完成内部排序
- 按过滤后的 Arrow Schema 顺序引用列索引
- 输出单个本地 HFile 文件
- 支持未排序输入，输出始终按 Row Key 有序
- 支持 `ROW` Bloom Filter，并补齐 HBase 原生 writer 会写出的核心 FileInfo
- 提供 CLI 与可复用 Java API

## 构建

```bash
mvn -q -f tools/arrow-to-hfile-java/pom.xml test
mvn -q -f tools/arrow-to-hfile-java/pom.xml package
```

## CLI 用法

```bash
java -jar tools/arrow-to-hfile-java/target/arrow-to-hfile-java-1.0.0.jar \
  --arrow /tmp/input.arrow \
  --hfile /tmp/output.hfile \
  --table perf_table \
  --rule USER_ID,0,false,0 \
  --cf cf \
  --compression GZ \
  --encoding NONE
```

## Java API

```java
JavaConvertResult result = new ArrowToHFileJavaConverter().convert(
    JavaConvertOptions.builder()
        .arrowPath("/tmp/input.arrow")
        .hfilePath("/tmp/output.hfile")
        .tableName("perf_table")
        .rowKeyRule("USER_ID,0,false,0")
        .columnFamily("cf")
        .build()
);
```

`JavaConvertResult` 现在会同时回传：

- `elapsed_ms`
- `sort_ms`
- `write_ms`

## 失败语义

- Arrow 文件不存在：返回 `ARROW_FILE_ERROR`
- `rowKeyRule` 非法：返回 `INVALID_ROW_KEY_RULE`
- 过滤后列为空或索引越界：返回 `INVALID_ARGUMENT`
- 文件写出异常：返回 `IO_ERROR`

## 当前约束

- 当前实现会像 JNI 版本一样在内存中构建 sort index 后再写出 HFile
- 默认输出采用与 C++ 写入链路一致的 `GZ + NONE`
- 即使调用方传入 `PREFIX`、`DIFF`、`FAST_DIFF`，当前落盘仍统一按 `NONE` 处理
- 比较 Comparator 时应以 Trailer 为准；HBase 原生 Java writer 不要求在 FileInfo 中额外重复写 `hfile.COMPARATOR`
- 推荐与 `tools/hfile-bulkload-perf` 配合使用同一批 mock Arrow 数据
