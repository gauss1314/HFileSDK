# mock-arrow

生成用于 HFileSDK 端到端验证的 Arrow IPC Stream 样例文件。

## 构建

```bash
mvn -q -f tools/mock-arrow/pom.xml package
```

## 生成默认样例

```bash
java -jar tools/mock-arrow/target/mock-arrow-4.0.0.jar \
  --output /tmp/mock_signal.arrow
```

默认会生成以下 schema：

- `_hoodie_commit_time`
- `_hoodie_commit_seqno`
- `_hoodie_record_key`
- `_hoodie_partition_path`
- `_hoodie_file_name`
- `REFID`
- `TIME`
- `SIGSTORE`
- `BIT_MAP`

默认 rowKey 规则：

```text
REFID,0,true,0,LEFT,,,
```

转换时建议同时排除 Hoodie 元数据列：

```text
--exclude-prefix _hoodie
```

## 自定义 REFID

```bash
java -jar tools/mock-arrow/target/mock-arrow-4.0.0.jar \
  --output /tmp/mock_signal.arrow \
  --refids 47820206294,47820201464,47820208445
```

## 端到端示例

```bash
java -jar tools/mock-arrow/target/mock-arrow-4.0.0.jar \
  --output /tmp/mock_signal.arrow

java -jar tools/arrow-to-hfile/target/arrow-to-hfile-4.0.0.jar \
  --native-lib build/libhfilesdk.dylib \
  --arrow /tmp/mock_signal.arrow \
  --hfile /tmp/mock_signal.hfile \
  --rule "REFID,0,true,0,LEFT,,," \
  --cf cf \
  --exclude-prefix _hoodie

java -jar tools/hfile-verify/target/hfile-verify-1.0.0.jar \
  --hfile /tmp/mock_signal.hfile \
  --expect-entry-count 20
```
