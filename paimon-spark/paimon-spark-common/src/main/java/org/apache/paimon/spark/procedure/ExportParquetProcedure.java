/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.procedure;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.ParquetWriterFactory;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Export projected Paimon rows to parquet files without constructing a Spark SQL wide Project.
 *
 * <pre><code>
 * CALL sys.export_parquet(
 *   table => 'db.tbl',
 *   columns => 'id,f1,f2',
 *   output_path => 'obs://bucket/path',
 *   where => "dt = '2026-05-14' and user_id >= 100")
 * </code></pre>
 */
public class ExportParquetProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("columns", StringType),
                ProcedureParameter.required("output_path", StringType),
                ProcedureParameter.optional("where", StringType),
                ProcedureParameter.optional("parallelism", IntegerType),
                ProcedureParameter.optional("compression", StringType),
                ProcedureParameter.optional("overwrite", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty()),
                        new StructField("rows", LongType, false, Metadata.empty())
                    });

    protected ExportParquetProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String columns = args.getString(1);
        String outputPath = args.getString(2);
        String where = args.isNullAt(3) ? null : args.getString(3);
        int parallelism =
                args.isNullAt(4)
                        ? spark().sparkContext().defaultParallelism()
                        : Math.max(1, args.getInt(4));
        String compression = args.isNullAt(5) ? "zstd" : args.getString(5);
        boolean overwrite = !args.isNullAt(6) && args.getBoolean(6);

        Table table = loadSparkTable(tableIdent).getTable();
        try {
            long rows =
                    export(table, columns, outputPath, where, parallelism, compression, overwrite);
            return new InternalRow[] {newInternalRow(true, rows)};
        } catch (Exception e) {
            throw new RuntimeException("Failed to export parquet files", e);
        }
    }

    private long export(
            Table table,
            String columns,
            String outputPath,
            @Nullable String where,
            int parallelism,
            String compression,
            boolean overwrite)
            throws Exception {
        RowType tableRowType = table.rowType();
        int[] outputProjection = parseOutputProjection(tableRowType, columns);
        Predicate predicate = parsePredicate(tableRowType, where);
        int[] readProjection = readProjection(tableRowType, outputProjection, predicate);
        RowType outputType = Projection.of(outputProjection).project(tableRowType);

        Predicate projectedPredicate =
                predicate == null
                        ? null
                        : predicate
                                .visit(PredicateProjectionConverter.fromProjection(readProjection))
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "Cannot project predicate to read columns."));

        ReadBuilder readBuilder = table.newReadBuilder();
        if (predicate != null) {
            readBuilder = readBuilder.withFilter(predicate);
        }
        readBuilder = readBuilder.withProjection(readProjection);

        final ReadBuilder finalReadBuilder = readBuilder;
        final List<SerializedSplit> splits =
                finalReadBuilder.newScan().plan().splits().stream()
                        .map(ExportParquetProcedure::copySplit)
                        .map(ExportParquetProcedure::serializeSplit)
                        .collect(Collectors.toList());
        final Path outputDir = new Path(trimTrailingSlash(outputPath));
        FileIO outputFileIO = outputFileIO(table, outputDir);
        prepareOutputDirectory(outputFileIO, outputDir, overwrite);

        final Table exportTable = table;
        final RowType finalOutputType = outputType;
        final Predicate finalProjectedPredicate = projectedPredicate;
        final int outputFieldCount = outputProjection.length;
        final String finalCompression = compression;
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
        List<Long> counts =
                jsc.parallelize(
                                splits,
                                Math.max(1, Math.min(parallelism, Math.max(1, splits.size()))))
                        .map(
                                serializedSplit ->
                                        exportSplit(
                                                exportTable,
                                                finalReadBuilder,
                                                serializedSplit.split(),
                                                outputDir,
                                                finalOutputType,
                                                finalProjectedPredicate,
                                                outputFieldCount,
                                                finalCompression))
                        .collect();

        long rows = 0L;
        for (Long count : counts) {
            rows += count;
        }
        outputFileIO.newOutputStream(new Path(outputDir, "_SUCCESS"), true).close();
        return rows;
    }

    private static long exportSplit(
            Table table,
            ReadBuilder readBuilder,
            Split split,
            Path outputDir,
            RowType outputType,
            @Nullable Predicate predicate,
            int outputFieldCount,
            String compression)
            throws Exception {
        TableRead read = readBuilder.newRead();
        FileIO fileIO = outputFileIO(table, outputDir);
        String fileName = "part-" + UUID.randomUUID() + ".parquet";
        Path filePath = new Path(outputDir, fileName);
        ProjectedRow outputRow = ProjectedRow.from(identityProjection(outputFieldCount));

        FormatWriter writer = null;
        long count = 0L;
        try (RecordReader<org.apache.paimon.data.InternalRow> reader = read.createReader(split);
                CloseableIterator<org.apache.paimon.data.InternalRow> iterator =
                        reader.toCloseableIterator()) {
            while (iterator.hasNext()) {
                org.apache.paimon.data.InternalRow row = iterator.next();
                if (predicate == null || predicate.test(row)) {
                    if (writer == null) {
                        writer =
                                new ParquetWriterFactory(
                                                new RowDataParquetBuilder(
                                                        outputType, new Options()))
                                        .create(
                                                fileIO.newOutputStream(filePath, false),
                                                compression);
                    }
                    writer.addElement(outputRow.replaceRow(row));
                    count++;
                }
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        return count;
    }

    private static SerializedSplit serializeSplit(Split split) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(split);
            }
            return new SerializedSplit(bytes.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Paimon split.", e);
        }
    }

    private static Split copySplit(Split split) {
        if (split instanceof DataSplit) {
            DataSplit dataSplit = (DataSplit) split;
            DataSplit.Builder builder =
                    DataSplit.builder()
                            .withSnapshot(dataSplit.snapshotId())
                            .withPartition(dataSplit.partition().copy())
                            .withBucket(dataSplit.bucket())
                            .withBucketPath(dataSplit.bucketPath())
                            .withTotalBuckets(dataSplit.totalBuckets())
                            .withDataFiles(dataSplit.dataFiles())
                            .rawConvertible(dataSplit.rawConvertible())
                            .isStreaming(dataSplit.isStreaming());
            if (dataSplit.deletionFiles().isPresent()) {
                builder.withDataDeletionFiles(dataSplit.deletionFiles().get());
            }
            return builder.build();
        }
        if (split instanceof IncrementalSplit) {
            IncrementalSplit incrementalSplit = (IncrementalSplit) split;
            return new IncrementalSplit(
                    incrementalSplit.snapshotId(),
                    incrementalSplit.partition().copy(),
                    incrementalSplit.bucket(),
                    incrementalSplit.totalBuckets(),
                    incrementalSplit.beforeFiles(),
                    incrementalSplit.beforeDeletionFiles(),
                    incrementalSplit.afterFiles(),
                    incrementalSplit.afterDeletionFiles(),
                    incrementalSplit.isStreaming());
        }
        return split;
    }

    private static void prepareOutputDirectory(FileIO fileIO, Path outputDir, boolean overwrite)
            throws IOException {
        if (fileIO.exists(outputDir)) {
            if (!overwrite) {
                throw new IOException(
                        "Output path already exists, set overwrite => true to replace it: "
                                + outputDir);
            }
            fileIO.delete(outputDir, true);
        }
        fileIO.mkdirs(outputDir);
    }

    private static FileIO outputFileIO(Table table, Path outputPath) throws IOException {
        if (table instanceof FileStoreTable) {
            CatalogContext context = ((FileStoreTable) table).catalogEnvironment().catalogContext();
            if (context != null) {
                return FileIO.get(outputPath, context);
            }
        }
        return table.fileIO();
    }

    private static int[] parseOutputProjection(RowType rowType, String columns) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(columns), "Columns should not be empty.");
        if ("*".equals(columns.trim())) {
            int[] projection = new int[rowType.getFieldCount()];
            for (int i = 0; i < projection.length; i++) {
                projection[i] = i;
            }
            return projection;
        }

        List<String> names =
                splitComma(columns).stream()
                        .map(ExportParquetProcedure::unquoteIdentifier)
                        .collect(Collectors.toList());
        Set<String> deduplicated = new LinkedHashSet<>(names);
        int[] projection = new int[deduplicated.size()];
        int pos = 0;
        for (String name : deduplicated) {
            int index = rowType.getFieldIndex(name);
            Preconditions.checkArgument(index >= 0, "Cannot find column '%s'.", name);
            projection[pos++] = index;
        }
        return projection;
    }

    private static int[] readProjection(
            RowType rowType, int[] outputProjection, @Nullable Predicate predicate) {
        Set<Integer> indexes = new LinkedHashSet<>();
        for (int index : outputProjection) {
            indexes.add(index);
        }
        for (String fieldName : PredicateVisitor.collectFieldNames(predicate)) {
            int index = rowType.getFieldIndex(fieldName);
            Preconditions.checkArgument(index >= 0, "Cannot find filter column '%s'.", fieldName);
            indexes.add(index);
        }

        int[] projection = new int[indexes.size()];
        int pos = 0;
        for (Integer index : indexes) {
            projection[pos++] = index;
        }
        return projection;
    }

    @Nullable
    private static Predicate parsePredicate(RowType rowType, @Nullable String where) {
        if (StringUtils.isNullOrWhitespaceOnly(where)) {
            return null;
        }

        List<Predicate> predicates = new ArrayList<>();
        PredicateBuilder builder = new PredicateBuilder(rowType);
        for (String condition : splitAnd(where)) {
            predicates.add(parseCondition(rowType, builder, condition));
        }
        return PredicateBuilder.and(predicates);
    }

    private static Predicate parseCondition(
            RowType rowType, PredicateBuilder builder, String condition) {
        String trimmed = condition.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (lower.endsWith(" is not null")) {
            String field =
                    unquoteIdentifier(trimmed.substring(0, lower.lastIndexOf(" is not null")));
            return builder.isNotNull(fieldIndex(rowType, field));
        }
        if (lower.endsWith(" is null")) {
            String field = unquoteIdentifier(trimmed.substring(0, lower.lastIndexOf(" is null")));
            return builder.isNull(fieldIndex(rowType, field));
        }

        int inPos = indexOfKeyword(trimmed, "in");
        if (inPos > 0) {
            String field = unquoteIdentifier(trimmed.substring(0, inPos));
            String values = trimmed.substring(inPos + 2).trim();
            Preconditions.checkArgument(
                    values.startsWith("(") && values.endsWith(")"),
                    "Invalid IN predicate: %s",
                    condition);
            int index = fieldIndex(rowType, field);
            DataType type = rowType.getTypeAt(index);
            List<Object> literals =
                    splitComma(values.substring(1, values.length() - 1)).stream()
                            .map(value -> parseLiteral(type, value))
                            .collect(Collectors.toList());
            return builder.in(index, literals);
        }

        for (String op : Arrays.asList(">=", "<=", "!=", "<>", "=", ">", "<")) {
            int opPos = indexOfOperator(trimmed, op);
            if (opPos > 0) {
                String field = unquoteIdentifier(trimmed.substring(0, opPos));
                int index = fieldIndex(rowType, field);
                Object literal =
                        parseLiteral(
                                rowType.getTypeAt(index), trimmed.substring(opPos + op.length()));
                switch (op) {
                    case "=":
                        return literal == null
                                ? builder.isNull(index)
                                : builder.equal(index, literal);
                    case "!=":
                    case "<>":
                        return literal == null
                                ? builder.isNotNull(index)
                                : builder.notEqual(index, literal);
                    case ">":
                        return builder.greaterThan(index, literal);
                    case ">=":
                        return builder.greaterOrEqual(index, literal);
                    case "<":
                        return builder.lessThan(index, literal);
                    case "<=":
                        return builder.lessOrEqual(index, literal);
                    default:
                        throw new IllegalArgumentException("Unsupported operator: " + op);
                }
            }
        }

        throw new IllegalArgumentException("Unsupported predicate condition: " + condition);
    }

    private static int fieldIndex(RowType rowType, String field) {
        int index = rowType.getFieldIndex(field);
        Preconditions.checkArgument(index >= 0, "Cannot find filter column '%s'.", field);
        return index;
    }

    @Nullable
    private static Object parseLiteral(DataType type, String literal) {
        String value = stripQuotes(literal.trim());
        if ("null".equalsIgnoreCase(value)) {
            return null;
        }

        Object javaObject;
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                javaObject = Boolean.parseBoolean(value);
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                javaObject = Long.parseLong(value);
                break;
            case FLOAT:
            case DOUBLE:
                javaObject = Double.parseDouble(value);
                break;
            case DECIMAL:
                javaObject = new BigDecimal(value);
                break;
            case CHAR:
            case VARCHAR:
                javaObject = value;
                break;
            case DATE:
                javaObject = LocalDate.parse(value);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                javaObject = LocalTime.parse(value);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                javaObject = LocalDateTime.parse(normalizeDateTime(value));
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                javaObject =
                        hasTimeZone(value)
                                ? Instant.parse(value)
                                : Timestamp.valueOf(normalizeTimestamp(value));
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported filter literal type: " + type.getTypeRoot());
        }
        return PredicateBuilder.convertJavaObject(type, javaObject);
    }

    private static int[] identityProjection(int fieldCount) {
        int[] projection = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            projection[i] = i;
        }
        return projection;
    }

    private static String normalizeDateTime(String value) {
        return value.indexOf('T') >= 0 ? value : value.replace(' ', 'T');
    }

    private static String normalizeTimestamp(String value) {
        return value.indexOf('T') >= 0 ? value.replace('T', ' ') : value;
    }

    private static boolean hasTimeZone(String value) {
        return value.endsWith("Z") || value.matches(".*[+-][0-9]{2}:[0-9]{2}$");
    }

    private static String stripQuotes(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return value.substring(1, value.length() - 1)
                        .replace(String.valueOf(first) + first, String.valueOf(first));
            }
        }
        return value;
    }

    private static String unquoteIdentifier(String identifier) {
        String value = identifier.trim();
        if (value.length() >= 2
                && value.charAt(0) == '`'
                && value.charAt(value.length() - 1) == '`') {
            return value.substring(1, value.length() - 1).replace("``", "`");
        }
        return value;
    }

    private static List<String> splitAnd(String input) {
        List<String> result = new ArrayList<>();
        int start = 0;
        char quote = 0;
        int parens = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (c == '(') {
                parens++;
            } else if (c == ')') {
                parens--;
            } else if (parens == 0 && matchesKeyword(input, i, "and")) {
                result.add(input.substring(start, i).trim());
                start = i + 3;
                i += 2;
            }
        }
        result.add(input.substring(start).trim());
        return result.stream().filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    private static List<String> splitComma(String input) {
        List<String> result = new ArrayList<>();
        int start = 0;
        char quote = 0;
        int parens = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"' || c == '`') {
                quote = c;
            } else if (c == '(') {
                parens++;
            } else if (c == ')') {
                parens--;
            } else if (c == ',' && parens == 0) {
                result.add(input.substring(start, i).trim());
                start = i + 1;
            }
        }
        result.add(input.substring(start).trim());
        return result.stream().filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    private static int indexOfKeyword(String input, String keyword) {
        char quote = 0;
        int parens = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (c == '(') {
                parens++;
            } else if (c == ')') {
                parens--;
            } else if (parens == 0 && matchesKeyword(input, i, keyword)) {
                return i;
            }
        }
        return -1;
    }

    private static int indexOfOperator(String input, String op) {
        char quote = 0;
        for (int i = 0; i <= input.length() - op.length(); i++) {
            char c = input.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (input.startsWith(op, i)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean matchesKeyword(String input, int pos, String keyword) {
        int end = pos + keyword.length();
        if (end > input.length()) {
            return false;
        }
        if (!input.regionMatches(true, pos, keyword, 0, keyword.length())) {
            return false;
        }
        return isBoundary(input, pos - 1) && isBoundary(input, end);
    }

    private static boolean isBoundary(String input, int pos) {
        return pos < 0
                || pos >= input.length()
                || !Character.isLetterOrDigit(input.charAt(pos)) && input.charAt(pos) != '_';
    }

    private static String trimTrailingSlash(String path) {
        String result = path.trim();
        while (result.length() > 1 && result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static class SerializedSplit implements Serializable {

        private static final long serialVersionUID = 1L;

        private final byte[] bytes;

        private SerializedSplit(byte[] bytes) {
            this.bytes = bytes;
        }

        private Split split() {
            try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                return (Split) in.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Failed to deserialize Paimon split.", e);
            }
        }
    }

    public static ProcedureBuilder builder() {
        return new Builder<ExportParquetProcedure>() {
            @Override
            public ExportParquetProcedure doBuild() {
                return new ExportParquetProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ExportParquetProcedure";
    }
}
