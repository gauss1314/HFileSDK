# mock-arrow

为 HFileSDK 端到端测试生成 Arrow IPC Stream 格式的模拟数据文件。

支持两种表结构，可控制输出文件大小。

## 快速开始

```bash
cd tools/mock-arrow
mvn package -q
# 输出：target/mock-arrow-1.0.0.jar
```

## 命令行用法

```bash
java -jar target/mock-arrow-1.0.0.jar [选项]
```

### 参数说明

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--output FILE` | ✓ | — | 输出的 Arrow IPC Stream 文件路径 |
| `--table TABLE` | | `tdr_signal_stor_20550` | 要生成的表名（见下文） |
| `--size MiB` | | `10` | 目标文件大小（MiB） |
| `--batch ROWS` | | `1000` | 每个 RecordBatch 的行数 |
| `--seed SEED` | | `42` | 随机种子（固定种子保证复现性） |

### 支持的表

| 表名 | 说明 |
|------|------|
| `tdr_signal_stor_20550` | **默认**。新信令存储表，列：REFID/TIME/SIGSTORE/BIT_MAP/no |
| `tdr_mock` | 原始话单/CDR 表，列：STARTTIME/IMSI/MSISDN/DURATION/BYTES_UP/BYTES_DW/CELL_ID/RAT_TYPE |

---

## 使用示例

### 生成默认 10 MiB 文件（tdr_signal_stor_20550）

```bash
java -jar target/mock-arrow-1.0.0.jar \
  --output /data/tdr_20550.arrow
```

输出：
```
Generating Arrow IPC Stream file
  table  : tdr_signal_stor_20550
  output : /data/tdr_20550.arrow
  target : 10 MiB
  batch  : 1000 rows
  seed   : 42
  rowKey : REFID,0,false,15

Done:
  rows written : 57,575
  batches      : 58
  file size    : 10.01 MiB

rowKeyRule for arrow-to-hfile:
  REFID,0,false,15
```

### 生成 100 MiB 原始 CDR 文件

```bash
java -jar target/mock-arrow-1.0.0.jar \
  --output /data/tdr_mock.arrow \
  --table  tdr_mock \
  --size   100
```

### 生成 1 GiB 的大文件（性能压测用）

```bash
java -jar target/mock-arrow-1.0.0.jar \
  --output /data/tdr_20550_1g.arrow \
  --size   1024 \
  --batch  5000
```

---

## 端到端测试流程

### tdr_signal_stor_20550

```bash
# 1. 生成 Arrow 文件
java -jar tools/mock-arrow/target/mock-arrow-1.0.0.jar \
  --output /data/tdr_20550.arrow \
  --size   50

# 2. 转换为 HFile
java -jar tools/arrow-to-hfile/target/arrow-to-hfile-4.0.0.jar \
  --native-lib /opt/hfilesdk/libhfilesdk.so \
  --arrow      /data/tdr_20550.arrow \
  --hfile      /staging/tdr_20550/cf/tdr_20550.hfile \
  --table      tdr_signal_stor_20550 \
  --rule       "REFID,0,false,15" \
  --cf         cf

# 3. 上传并 Bulk Load
hdfs dfs -put /staging/tdr_20550 /hbase/staging/
hbase org.apache.hadoop.hbase.tool.BulkLoadHFilesTool \
  /hbase/staging/tdr_20550 tdr_signal_stor_20550
```

### tdr_mock

```bash
# 1. 生成
java -jar tools/mock-arrow/target/mock-arrow-1.0.0.jar \
  --output /data/tdr_mock.arrow \
  --table  tdr_mock \
  --size   50

# 2. 转换（STARTTIME 左填充 10 位 + IMSI 反转 15 位作为 Row Key）
java -jar tools/arrow-to-hfile/target/arrow-to-hfile-4.0.0.jar \
  --native-lib /opt/hfilesdk/libhfilesdk.so \
  --arrow  /data/tdr_mock.arrow \
  --hfile  /staging/tdr_mock/cf/tdr_mock.hfile \
  --table  tdr_mock \
  --rule   "STARTTIME,0,false,10#IMSI,1,true,15" \
  --cf     cf
```

---

## 表结构说明

### tdr_signal_stor_20550

数据样例（5行，字段按空格分隔）：
```
183971587293920 1775537735 cs,12111,100,100,81460000212971260,0,3,,,1775537764,1775537764,3,37,16,,,10,486,; 12 0
183971587295360 1775537735 cs,12111,100,100,61452380310991690,0,3,,,1775537765,1775537765,9,34,101,,,10,327,; 12 0
183971587295630 1775537735 cs,12111,100,100,91452380310999256,0,3,,,1775537765,1775537765,9,34,4,,,10,11,; 12 0
183971587295720 1775537735 cs,12111,120,121,...; 12 0
183971587295810 1775537735 cs,12111,100,100,61452380210999059,0,3,,,1775537765,1775537765,9,34,98,,,10,334,; 12 0
```

| 列 | Arrow 类型 | 说明 |
|----|----------|------|
| REFID | bigint | 唯一键，15位，自增 |
| TIME | bigint | Epoch 秒 |
| SIGSTORE | string | 分号结尾的 CSV 信令记录，长度可变（50-300 字节） |
| BIT_MAP | bigint | 位图指标，样例值为 12 |
| no | string | 单字符数字 "0"-"9"，循环分配 |

**Row Key 规则**：`REFID,0,false,15`
（REFID 左填充到 15 位，无反转）

### tdr_mock

| 列 | Arrow 类型 | 说明 |
|----|----------|------|
| STARTTIME | bigint | 会话开始时间（epoch-ms） |
| IMSI | string | 15 位 IMSI |
| MSISDN | string | 11 位 MSISDN（138xxxxxxxx） |
| DURATION | bigint | 会话时长（秒） |
| BYTES_UP | bigint | 上行字节数 |
| BYTES_DW | bigint | 下行字节数 |
| CELL_ID | string | 6 字符十六进制小区 ID |
| RAT_TYPE | string | 无线接入类型：LTE/NR/UMTS |

**Row Key 规则**：`STARTTIME,0,false,10#IMSI,1,true,15`
（STARTTIME 左填充 10 位 + IMSI 反转 15 位拼接）

---

## 项目结构

```
tools/mock-arrow/
├── pom.xml
├── README.md
└── src/main/java/io/hfilesdk/mock/
    ├── MockArrowGenerator.java   # 主类：CLI main() + 编程式 API
    ├── TableSchema.java          # 表定义（schema + rowKeyRule）
    └── RowGenerator.java         # 逐行数据生成（自增 ID、随机信令串等）
```
