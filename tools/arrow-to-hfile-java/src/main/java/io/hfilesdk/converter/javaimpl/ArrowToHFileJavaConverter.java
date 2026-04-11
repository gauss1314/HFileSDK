package io.hfilesdk.converter.javaimpl;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public final class ArrowToHFileJavaConverter {

    private static final String RANDOM_DIGITS = "$RND$";
    private static final int RANDOM_SEED_RANGE = 9;

    public JavaConvertResult convert(JavaConvertOptions options) {
        long start = System.nanoTime();
        java.nio.file.Path arrowPath = Paths.get(options.arrowPath()).toAbsolutePath().normalize();
        java.nio.file.Path hfilePath = Paths.get(options.hfilePath()).toAbsolutePath().normalize();
        long rowsRead = 0L;
        long kvWritten = 0L;

        try {
            if (!Files.exists(arrowPath) || !Files.isRegularFile(arrowPath)) {
                return JavaConvertResult.error(
                    JavaConvertResult.ARROW_FILE_ERROR,
                    "Arrow 文件不存在: " + arrowPath,
                    arrowPath.toString(),
                    hfilePath.toString(),
                    0L,
                    0L,
                    elapsedMillis(start)
                );
            }
            if (hfilePath.getParent() != null) {
                Files.createDirectories(hfilePath.getParent());
            }

            Configuration configuration = HBaseConfiguration.create();
            FileSystem fileSystem = createLocalFileSystem(configuration);
            Path outputPath = new Path(hfilePath.toString());
            byte[] columnFamily = Bytes.toBytes(options.columnFamily());
            Compression.Algorithm compression = normalizeCompression(options.compression());
            DataBlockEncoding encoding = normalizeEncoding(options.dataBlockEncoding());
            BloomType bloomType = BloomType.valueOf(options.bloomType().toUpperCase(Locale.ROOT));

            HFileContext context = new HFileContextBuilder()
                .withCompression(compression)
                .withDataBlockEncoding(encoding)
                .withCellComparator(CellComparatorImpl.COMPARATOR)
                .withBlockSize(options.blockSize())
                .withColumnFamily(columnFamily)
                .withIncludesTags(true)
                .build();

            try (InputStream inputStream = Files.newInputStream(arrowPath);
                 ArrowStreamReader reader = new ArrowStreamReader(inputStream, new RootAllocator(Long.MAX_VALUE));
                 HFile.Writer writer = HFile.getWriterFactory(configuration, new CacheConfig(configuration))
                     .withPath(fileSystem, outputPath)
                     .withFileContext(context)
                     .create()) {

                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                ProjectedSchema projectedSchema = ProjectedSchema.from(root, options);
                JavaRowKeyBuilder rowKeyBuilder = JavaRowKeyBuilder.compile(options.rowKeyRule(), projectedSchema.fieldCount());
                byte[] previousRowKey = null;

                while (reader.loadNextBatch()) {
                    int rowCount = root.getRowCount();
                    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                        byte[] rowKey = rowKeyBuilder.buildRowKey(projectedSchema.visibleFields(), rowIndex);
                        if (previousRowKey != null && Bytes.compareTo(previousRowKey, rowKey) > 0) {
                            return JavaConvertResult.error(
                                JavaConvertResult.SORT_VIOLATION,
                                "输入 Arrow 未按 rowKeyRule 结果升序排列，纯 Java 实现不会执行额外排序",
                                arrowPath.toString(),
                                hfilePath.toString(),
                                rowsRead,
                                kvWritten,
                                elapsedMillis(start)
                            );
                        }
                        previousRowKey = rowKey;
                        long timestamp = Long.MAX_VALUE - rowsRead;
                        for (VisibleField visibleField : projectedSchema.qualifierFields()) {
                            byte[] value = extractValueBytes(visibleField.vector(), rowIndex);
                            writer.append(new KeyValue(rowKey, columnFamily, visibleField.qualifier(), timestamp, value));
                            kvWritten++;
                        }
                        rowsRead++;
                    }
                }
            }

            long sizeBytes = Files.exists(hfilePath) ? Files.size(hfilePath) : 0L;
            return JavaConvertResult.success(
                arrowPath.toString(),
                hfilePath.toString(),
                rowsRead,
                kvWritten,
                sizeBytes,
                elapsedMillis(start)
            );
        } catch (JavaConvertException e) {
            deleteQuietly(hfilePath);
            return JavaConvertResult.error(
                e.result().errorCode,
                e.result().errorMessage,
                arrowPath.toString(),
                hfilePath.toString(),
                rowsRead,
                kvWritten,
                elapsedMillis(start)
            );
        } catch (IllegalArgumentException e) {
            deleteQuietly(hfilePath);
            return JavaConvertResult.error(
                JavaConvertResult.INVALID_ARGUMENT,
                e.getMessage(),
                arrowPath.toString(),
                hfilePath.toString(),
                rowsRead,
                kvWritten,
                elapsedMillis(start)
            );
        } catch (IOException e) {
            deleteQuietly(hfilePath);
            return JavaConvertResult.error(
                JavaConvertResult.IO_ERROR,
                e.getMessage(),
                arrowPath.toString(),
                hfilePath.toString(),
                rowsRead,
                kvWritten,
                elapsedMillis(start)
            );
        } catch (Exception e) {
            deleteQuietly(hfilePath);
            return JavaConvertResult.error(
                JavaConvertResult.INTERNAL_ERROR,
                e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage(),
                arrowPath.toString(),
                hfilePath.toString(),
                rowsRead,
                kvWritten,
                elapsedMillis(start)
            );
        }
    }

    public JavaConvertResult convertOrThrow(JavaConvertOptions options) {
        JavaConvertResult result = convert(options);
        if (!result.isSuccess()) {
            throw new JavaConvertException(result);
        }
        return result;
    }

    public static void main(String[] args) {
        Options options = buildOptions();
        CommandLine commandLine;
        try {
            commandLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            printHelp(options);
            System.exit(1);
            return;
        }

        if (commandLine.hasOption("help")) {
            printHelp(options);
            return;
        }

        try {
            JavaConvertOptions convertOptions = JavaConvertOptions.builder()
                .arrowPath(required(commandLine, "arrow"))
                .hfilePath(required(commandLine, "hfile"))
                .tableName(commandLine.getOptionValue("table", ""))
                .rowKeyRule(required(commandLine, "rule"))
                .columnFamily(commandLine.getOptionValue("cf", "cf"))
                .compression(commandLine.getOptionValue("compression", "GZ"))
                .dataBlockEncoding(commandLine.getOptionValue("encoding", "NONE"))
                .bloomType(commandLine.getOptionValue("bloom", "ROW"))
                .blockSize(parsePositiveInt(commandLine.getOptionValue("block-size", "65536"), "block-size"))
                .excludedColumns(parseCsv(commandLine.getOptionValue("exclude-cols", "")))
                .excludedPrefixes(parseCsv(commandLine.getOptionValue("exclude-prefix", "")))
                .build();
            JavaConvertResult result = new ArrowToHFileJavaConverter().convert(convertOptions);
            if (!result.isSuccess()) {
                System.err.println("FAILED: " + result.summary());
                System.exit(result.errorCode);
                return;
            }
            System.out.println("Result: " + result.summary());
            System.out.println(result.toJson());
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            printHelp(options);
            System.exit(1);
        }
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("arrow").hasArg().argName("PATH").desc("输入 Arrow IPC Stream 文件").build());
        options.addOption(Option.builder().longOpt("hfile").hasArg().argName("PATH").desc("输出 HFile 路径").build());
        options.addOption(Option.builder().longOpt("table").hasArg().argName("NAME").desc("表名，仅用于日志与元数据").build());
        options.addOption(Option.builder().longOpt("rule").hasArg().argName("RULE").desc("rowKeyRule").build());
        options.addOption(Option.builder().longOpt("cf").hasArg().argName("CF").desc("列族名，默认 cf").build());
        options.addOption(Option.builder().longOpt("compression").hasArg().argName("ALG").desc("压缩算法，默认 GZ").build());
        options.addOption(Option.builder().longOpt("encoding").hasArg().argName("ENC").desc("Data Block Encoding，默认 NONE").build());
        options.addOption(Option.builder().longOpt("bloom").hasArg().argName("TYPE").desc("Bloom 类型，默认 ROW").build());
        options.addOption(Option.builder().longOpt("block-size").hasArg().argName("BYTES").desc("block size，默认 65536").build());
        options.addOption(Option.builder().longOpt("exclude-cols").hasArg().argName("COL1,COL2").desc("排除列名").build());
        options.addOption(Option.builder().longOpt("exclude-prefix").hasArg().argName("PFX1,PFX2").desc("排除列名前缀").build());
        options.addOption(Option.builder().longOpt("help").desc("显示帮助").build());
        return options;
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp("java -jar tools/arrow-to-hfile-java/target/arrow-to-hfile-java-1.0.0.jar", options);
    }

    private static String required(CommandLine commandLine, String option) {
        String value = commandLine.getOptionValue(option);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("--" + option + " 为必填参数");
        }
        return value;
    }

    private static int parsePositiveInt(String raw, String optionName) {
        int value;
        try {
            value = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + optionName + " 必须为正整数");
        }
        if (value <= 0) {
            throw new IllegalArgumentException("--" + optionName + " 必须为正整数");
        }
        return value;
    }

    private static Compression.Algorithm normalizeCompression(String rawCompression) {
        String compression = rawCompression == null ? "" : rawCompression.trim().toUpperCase(Locale.ROOT);
        if (compression.isEmpty()) {
            return Compression.Algorithm.GZ;
        }
        return switch (compression) {
            case "GZ", "GZIP" -> Compression.Algorithm.GZ;
            case "NONE" -> Compression.Algorithm.NONE;
            case "LZ4" -> Compression.Algorithm.LZ4;
            case "SNAPPY" -> Compression.Algorithm.SNAPPY;
            case "ZSTD" -> Compression.Algorithm.ZSTD;
            default -> throw new IllegalArgumentException("不支持的压缩算法: " + rawCompression);
        };
    }

    private static DataBlockEncoding normalizeEncoding(String rawEncoding) {
        String encoding = rawEncoding == null ? "" : rawEncoding.trim().replace('-', '_').toUpperCase(Locale.ROOT);
        if (encoding.isEmpty() || encoding.equals("NONE")) {
            return DataBlockEncoding.NONE;
        }
        return switch (encoding) {
            case "PREFIX", "DIFF", "FAST_DIFF" -> DataBlockEncoding.NONE;
            default -> throw new IllegalArgumentException("不支持的 Data Block Encoding: " + rawEncoding);
        };
    }

    private static List<String> parseCsv(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        return List.of(raw.split(",")).stream().map(String::trim).filter(value -> !value.isBlank()).toList();
    }

    private static long elapsedMillis(long start) {
        return (System.nanoTime() - start) / 1_000_000L;
    }

    private static void deleteQuietly(java.nio.file.Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
        }
    }

    private static FileSystem createLocalFileSystem(Configuration configuration) throws IOException {
        RawLocalFileSystem fileSystem = new RawLocalFileSystem();
        fileSystem.initialize(java.net.URI.create("file:///"), configuration);
        return fileSystem;
    }

    private static byte[] extractValueBytes(FieldVector vector, int rowIndex) {
        if (vector.isNull(rowIndex)) {
            return new byte[0];
        }
        if (vector instanceof VarCharVector varCharVector) {
            return varCharVector.get(rowIndex);
        }
        if (vector instanceof VarBinaryVector varBinaryVector) {
            return varBinaryVector.get(rowIndex);
        }
        if (vector instanceof BigIntVector bigIntVector) {
            return Bytes.toBytes(bigIntVector.get(rowIndex));
        }
        if (vector instanceof IntVector intVector) {
            return Bytes.toBytes(intVector.get(rowIndex));
        }
        if (vector instanceof SmallIntVector smallIntVector) {
            return Bytes.toBytes(smallIntVector.get(rowIndex));
        }
        if (vector instanceof TinyIntVector tinyIntVector) {
            return new byte[]{tinyIntVector.get(rowIndex)};
        }
        if (vector instanceof UInt8Vector uint8Vector) {
            return Bytes.toBytes(uint8Vector.get(rowIndex));
        }
        if (vector instanceof UInt4Vector uint4Vector) {
            return Bytes.toBytes(uint4Vector.get(rowIndex));
        }
        if (vector instanceof UInt2Vector uint2Vector) {
            return Bytes.toBytes((short) uint2Vector.get(rowIndex));
        }
        if (vector instanceof UInt1Vector uint1Vector) {
            return new byte[]{(byte) uint1Vector.get(rowIndex)};
        }
        if (vector instanceof Float8Vector float8Vector) {
            return Bytes.toBytes(float8Vector.get(rowIndex));
        }
        if (vector instanceof Float4Vector float4Vector) {
            return Bytes.toBytes(float4Vector.get(rowIndex));
        }
        if (vector instanceof BitVector bitVector) {
            return Bytes.toBytes(bitVector.get(rowIndex) != 0);
        }
        Object value = vector.getObject(rowIndex);
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        return Bytes.toBytes(String.valueOf(value));
    }

    private static String extractStringValue(FieldVector vector, int rowIndex) {
        if (vector.isNull(rowIndex)) {
            return "";
        }
        if (vector instanceof VarCharVector varCharVector) {
            return new String(varCharVector.get(rowIndex));
        }
        if (vector instanceof VarBinaryVector varBinaryVector) {
            return Base64.getEncoder().encodeToString(varBinaryVector.get(rowIndex));
        }
        Object value = vector.getObject(rowIndex);
        return value == null ? "" : String.valueOf(value);
    }

    private record VisibleField(String name, byte[] qualifier, FieldVector vector) {}

    private record ProjectedSchema(List<VisibleField> visibleFields, List<VisibleField> qualifierFields) {
        private static ProjectedSchema from(VectorSchemaRoot root, JavaConvertOptions options) {
            List<VisibleField> visibleFields = new ArrayList<>();
            for (FieldVector vector : root.getFieldVectors()) {
                if (isExcluded(vector.getName(), options)) {
                    continue;
                }
                visibleFields.add(new VisibleField(vector.getName(), Bytes.toBytes(vector.getName()), vector));
            }
            if (visibleFields.isEmpty()) {
                throw new IllegalArgumentException("过滤后没有可写入的列");
            }
            List<VisibleField> qualifierFields = visibleFields.stream()
                .sorted(Comparator.comparing(VisibleField::name))
                .toList();
            return new ProjectedSchema(List.copyOf(visibleFields), qualifierFields);
        }

        private int fieldCount() {
            return visibleFields.size();
        }
    }

    private static boolean isExcluded(String fieldName, JavaConvertOptions options) {
        if (options.excludedColumns().contains(fieldName)) {
            return true;
        }
        for (String prefix : options.excludedPrefixes()) {
            if (fieldName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private enum SegmentType {
        COLUMN_REF,
        RANDOM,
        FILL,
        ENCODED
    }

    private enum EncodeKind {
        INT16_BASE64,
        INT64_BASE64
    }

    private enum Transform {
        HASH
    }

    private record Segment(
        String name,
        SegmentType type,
        int columnIndex,
        boolean reverse,
        int padLength,
        boolean padRight,
        char padChar,
        EncodeKind encodeKind,
        List<Transform> transforms
    ) {}

    private static final class JavaRowKeyBuilder {
        private final List<Segment> segments;
        private final Random random = new Random();

        private JavaRowKeyBuilder(List<Segment> segments) {
            this.segments = segments;
        }

        private static JavaRowKeyBuilder compile(String rule, int fieldCount) {
            if (rule == null || rule.isBlank()) {
                throw new JavaConvertException(JavaConvertResult.error(
                    JavaConvertResult.INVALID_ROW_KEY_RULE,
                    "rowKeyRule 不能为空",
                    "",
                    "",
                    0L,
                    0L,
                    0L
                ));
            }
            List<Segment> segments = new ArrayList<>();
            for (String rawSegment : rule.split("#")) {
                if (rawSegment.isBlank()) {
                    continue;
                }
                String[] fields = rawSegment.split(",", -1);
                if (fields.length < 4) {
                    throw invalidRule("rowKeyRule 段字段数量不足: " + rawSegment);
                }
                String name = fields[0];
                int columnIndex = parseNonNegative(fields[1], "index");
                boolean reverse;
                if ("true".equalsIgnoreCase(fields[2])) {
                    reverse = true;
                } else if ("false".equalsIgnoreCase(fields[2])) {
                    reverse = false;
                } else {
                    throw invalidRule("rowKeyRule isReverse 必须为 true/false: " + rawSegment);
                }
                int padLength = parseNonNegative(fields[3], "padLen");
                boolean padRight = false;
                if (fields.length >= 5 && !fields[4].isBlank()) {
                    if ("RIGHT".equalsIgnoreCase(fields[4])) {
                        padRight = true;
                    } else if (!"LEFT".equalsIgnoreCase(fields[4])) {
                        throw invalidRule("rowKeyRule padMode 必须为 LEFT 或 RIGHT: " + rawSegment);
                    }
                }
                char padChar = fields.length >= 6 && !fields[5].isEmpty() ? fields[5].charAt(0) : '0';
                SegmentType type = SegmentType.COLUMN_REF;
                EncodeKind encodeKind = null;
                List<Transform> transforms = List.of();
                if (isRandomName(name)) {
                    type = SegmentType.RANDOM;
                } else if (isFillName(name)) {
                    type = SegmentType.FILL;
                } else {
                    ParseEncoded parseEncoded = parseEncoded(name);
                    if (parseEncoded != null) {
                        type = SegmentType.ENCODED;
                        encodeKind = parseEncoded.encodeKind();
                        transforms = parseEncoded.transforms();
                    }
                }
                if ((type == SegmentType.COLUMN_REF || type == SegmentType.ENCODED) && columnIndex >= fieldCount) {
                    throw invalidRule("rowKeyRule 引用了不存在的过滤后列索引: " + columnIndex);
                }
                segments.add(new Segment(name, type, columnIndex, reverse, padLength, padRight, padChar, encodeKind, transforms));
            }
            if (segments.isEmpty()) {
                throw invalidRule("rowKeyRule 没有可用段");
            }
            return new JavaRowKeyBuilder(List.copyOf(segments));
        }

        private byte[] buildRowKey(List<VisibleField> visibleFields, int rowIndex) {
            StringBuilder builder = new StringBuilder(64);
            for (Segment segment : segments) {
                String value = switch (segment.type()) {
                    case RANDOM -> randomDigits(segment.padLength());
                    case FILL -> "";
                    case COLUMN_REF -> extractStringValue(visibleFields.get(segment.columnIndex()).vector(), rowIndex);
                    case ENCODED -> encodeValue(segment, extractStringValue(visibleFields.get(segment.columnIndex()).vector(), rowIndex));
                };
                builder.append(applyPaddingAndReverse(segment, value));
            }
            return Bytes.toBytes(builder.toString());
        }

        private String encodeValue(Segment segment, String value) {
            String encoded = value;
            List<Transform> transforms = segment.transforms();
            for (int index = transforms.size() - 1; index >= 0; index--) {
                Transform transform = transforms.get(index);
                if (transform == Transform.HASH) {
                    short hashed = javaHashNumericString(encoded);
                    encoded = Short.toString(hashed);
                }
            }
            try {
                if (segment.encodeKind() == EncodeKind.INT64_BASE64) {
                    long parsed = Long.parseLong(encoded);
                    return Base64.getEncoder().encodeToString(Bytes.toBytes(parsed));
                }
                short parsed = Short.parseShort(encoded);
                return Base64.getEncoder().encodeToString(Bytes.toBytes(parsed));
            } catch (NumberFormatException e) {
                throw invalidRule("rowKeyRule 编码段要求数值列: " + segment.name());
            }
        }

        private String applyPaddingAndReverse(Segment segment, String value) {
            String result = value == null ? "" : value;
            if (segment.padLength() > 0 && result.length() < segment.padLength()) {
                String pad = String.valueOf(segment.padChar()).repeat(segment.padLength() - result.length());
                result = segment.padRight() ? result + pad : pad + result;
            }
            if (segment.reverse()) {
                result = new StringBuilder(result).reverse().toString();
            }
            return result;
        }

        private String randomDigits(int length) {
            if (length <= 0) {
                return "";
            }
            StringBuilder builder = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                builder.append(random.nextInt(RANDOM_SEED_RANGE));
            }
            return builder.toString();
        }

        private static short javaHashNumericString(String value) {
            long parsed;
            try {
                parsed = Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw invalidRule("rowKeyRule hash 仅支持数值字符串: " + value);
            }
            long part1 = parsed >> 32;
            long part2 = parsed & ((2L << 32) - 1L);
            long result = 1L;
            result = 31L * result + part1;
            result = 31L * result + part2;
            return (short) (result % 65535L);
        }

        private static ParseEncoded parseEncoded(String name) {
            int open = name.indexOf('(');
            if (open < 0) {
                return null;
            }
            if (!name.endsWith(")")) {
                throw invalidRule("rowKeyRule 编码段括号不完整: " + name);
            }
            String outer = name.substring(0, open);
            EncodeKind encodeKind;
            if ("long".equalsIgnoreCase(outer)) {
                encodeKind = EncodeKind.INT64_BASE64;
            } else if ("short".equalsIgnoreCase(outer)) {
                encodeKind = EncodeKind.INT16_BASE64;
            } else {
                throw invalidRule("rowKeyRule 不支持的编码段: " + name);
            }
            String inner = name.substring(open + 1, name.length() - 1);
            if (inner.isBlank()) {
                return new ParseEncoded(encodeKind, List.of());
            }
            List<Transform> transforms = new ArrayList<>();
            while (!inner.isBlank()) {
                int next = inner.indexOf('(');
                String functionName = next < 0 ? inner : inner.substring(0, next);
                if (!"hash".equalsIgnoreCase(functionName)) {
                    throw invalidRule("rowKeyRule 不支持的编码变换: " + functionName);
                }
                transforms.add(Transform.HASH);
                if (next < 0) {
                    break;
                }
                inner = inner.substring(next + 1);
                if (!inner.endsWith(")")) {
                    throw invalidRule("rowKeyRule 编码段括号不完整: " + name);
                }
                inner = inner.substring(0, inner.length() - 1);
            }
            return new ParseEncoded(encodeKind, List.copyOf(transforms));
        }

        private static boolean isRandomName(String name) {
            return RANDOM_DIGITS.equals(name)
                || "RND$".equals(name)
                || "$RND".equals(name)
                || "RANDOM".equalsIgnoreCase(name)
                || "RANDOM_COL".equalsIgnoreCase(name);
        }

        private static boolean isFillName(String name) {
            return "FILL".equalsIgnoreCase(name) || "FILL_COL".equalsIgnoreCase(name);
        }

        private static int parseNonNegative(String raw, String name) {
            try {
                int value = Integer.parseInt(raw);
                if (value < 0) {
                    throw invalidRule("rowKeyRule " + name + " 必须 >= 0");
                }
                return value;
            } catch (NumberFormatException e) {
                throw invalidRule("rowKeyRule " + name + " 非法: " + raw);
            }
        }

        private static JavaConvertException invalidRule(String message) {
            return new JavaConvertException(JavaConvertResult.error(
                JavaConvertResult.INVALID_ROW_KEY_RULE,
                message,
                "",
                "",
                0L,
                0L,
                0L
            ));
        }
    }

    private record ParseEncoded(EncodeKind encodeKind, List<Transform> transforms) {}
}
