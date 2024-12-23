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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for schema evolution in {@link FileStoreTable}. */
public abstract class SchemaEvolutionTableTestBase {
    protected static final List<DataField> SCHEMA_0_FIELDS =
            Arrays.asList(
                    new DataField(0, "a", VarCharType.STRING_TYPE),
                    new DataField(1, "pt", new IntType()),
                    new DataField(2, "b", new IntType()),
                    new DataField(3, "c", VarCharType.STRING_TYPE),
                    new DataField(4, "kt", new BigIntType()),
                    new DataField(5, "d", VarCharType.STRING_TYPE));
    protected static final List<DataField> SCHEMA_1_FIELDS =
            Arrays.asList(
                    new DataField(1, "pt", new IntType()),
                    new DataField(2, "d", new IntType()),
                    new DataField(4, "kt", new BigIntType()),
                    new DataField(6, "a", new IntType()),
                    new DataField(7, "f", VarCharType.STRING_TYPE),
                    new DataField(8, "b", VarCharType.STRING_TYPE));
    protected static final List<String> PARTITION_NAMES = Collections.singletonList("pt");
    protected static final List<String> PRIMARY_KEY_NAMES = Arrays.asList("pt", "kt");

    protected static final List<DataField> SCHEMA_FIELDS =
            Arrays.asList(
                    new DataField(0, "a", new IntType()),
                    new DataField(1, "b", new CharType(10)),
                    new DataField(2, "c", new VarCharType(10)),
                    new DataField(3, "d", new DecimalType(10, 2)),
                    new DataField(4, "e", new SmallIntType()),
                    new DataField(5, "f", new IntType()),
                    new DataField(6, "g", new BigIntType()),
                    new DataField(7, "h", new FloatType()),
                    new DataField(8, "i", new DoubleType()),
                    new DataField(9, "j", new DateType()),
                    new DataField(10, "k", new TimestampType(2)),
                    new DataField(11, "l", new BinaryType(100)));
    protected static final List<String> SCHEMA_PARTITION_NAMES = Collections.singletonList("a");
    protected static final List<String> SCHEMA_PRIMARY_KEYS =
            Arrays.asList("a", "b", "c", "d", "e");

    protected Path tablePath;
    protected FileIO fileIO;
    protected String commitUser;
    protected final Options tableConfig = new Options();

    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void before() throws Exception {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        fileIO = FileIOFinder.find(tablePath);
        commitUser = UUID.randomUUID().toString();
        tableConfig.set(CoreOptions.TABLE_SCHEMA_PATH, tablePath.toString());
        tableConfig.set(CoreOptions.BUCKET, 1);
    }

    @AfterEach
    public void after() throws IOException {
        // assert all connections are closed
        java.util.function.Predicate<Path> pathPredicate =
                path -> path.toString().contains(tempDir.toString());
        assertThat(TraceableFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(TraceableFileIO.openOutputStreams(pathPredicate)).isEmpty();
    }

    protected List<String> getPrimaryKeyNames() {
        return PRIMARY_KEY_NAMES;
    }

    protected abstract FileStoreTable createFileStoreTable(Map<Long, TableSchema> tableSchemas);

    public static <R> void writeAndCheckFileResult(
            Function<Map<Long, TableSchema>, R> firstChecker,
            BiConsumer<R, Map<Long, TableSchema>> secondChecker,
            List<String> primaryKeyNames,
            Options tableConfig,
            Function<Map<Long, TableSchema>, FileStoreTable> createFileStoreTable)
            throws Exception {
        Map<Long, TableSchema> tableSchemas = new HashMap<>();
        // Create schema with SCHEMA_0_FIELDS
        tableSchemas.put(
                0L,
                new TableSchema(
                        0,
                        SCHEMA_0_FIELDS,
                        5,
                        PARTITION_NAMES,
                        primaryKeyNames,
                        tableConfig.toMap(),
                        ""));
        FileStoreTable table = createFileStoreTable.apply(tableSchemas);
        StreamTableWrite write = table.newWrite("user");
        StreamTableCommit commit = table.newCommit("user");

        write.write(rowData("S001", 1, 11, "S11", 111L, "S111"));
        write.write(rowData("S002", 2, 12, "S12", 112L, "S112"));
        write.write(rowData("S003", 1, 13, "S13", 113L, "S113"));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData("S004", 1, 14, "S14", 114L, "S114"));
        write.write(rowData("S005", 2, 15, "S15", 115L, "S115"));
        write.write(rowData("S006", 2, 16, "S16", 116L, "S116"));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();
        R result = firstChecker.apply(tableSchemas);

        /**
         * The table fields are: 0->a, 1->pt, 2->b, 3->c, 4->kt, 5->d. We will alter the table as
         * follows:
         *
         * <ul>
         *   <li>1. delete fields "a", "c" and "d"
         *   <li>2. rename "b" to "d"
         *   <li>3. add new fields "a", "f", "b"
         * </ul>
         *
         * <p>The result table fields will be: 1->pt, 2->d, 4->kt, 6->a, 7->f, 8->b.
         */
        tableSchemas.put(
                1L,
                new TableSchema(
                        1,
                        SCHEMA_1_FIELDS,
                        8,
                        PARTITION_NAMES,
                        primaryKeyNames,
                        tableConfig.toMap(),
                        ""));
        table = createFileStoreTable.apply(tableSchemas);
        write = table.newWrite("user");
        commit = table.newCommit("user");

        write.write(rowData(1, 17, 117L, 1117, "S007", "S17"));
        write.write(rowData(2, 18, 118L, 1118, "S008", "S18"));
        write.write(rowData(1, 19, 119L, 1119, "S009", "S19"));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 20, 120L, 1120, "S010", "S20"));
        write.write(rowData(1, 21, 121L, 1121, "S011", "S21"));
        write.write(rowData(1, 22, 122L, 1122, "S012", "S22"));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        secondChecker.accept(result, tableSchemas);
    }

    public static <R> void writeAndCheckFileResultForColumnType(
            Function<Map<Long, TableSchema>, R> firstChecker,
            BiConsumer<R, Map<Long, TableSchema>> secondChecker,
            List<String> primaryKeyNames,
            Options tableConfig,
            Function<Map<Long, TableSchema>, FileStoreTable> createFileStoreTable)
            throws Exception {
        Map<Long, TableSchema> tableSchemas = new HashMap<>();
        // Create schema with SCHEMA_FIELDS
        tableSchemas.put(
                0L,
                new TableSchema(
                        0,
                        SCHEMA_FIELDS,
                        12,
                        SCHEMA_PARTITION_NAMES,
                        primaryKeyNames,
                        tableConfig.toMap(),
                        ""));
        FileStoreTable table = createFileStoreTable.apply(tableSchemas);
        StreamTableWrite write = table.newWrite("user");
        StreamTableCommit commit = table.newCommit("user");

        /**
         * Generate two files:
         *
         * <ul>
         *   <li>file1 with one data: 1,"100","101",(102),short)
         *       103,104,105L,106F,107D,108,toTimestamp(109 * millsPerDay),"110".getBytes()
         *   <li>file2 with two data:
         *       <ul>
         *         <li>2,"200","201",toDecimal(202),(short)
         *             203,204,205L,206F,207D,208,toTimestamp(209 * millsPerDay),toBytes("210")
         *         <li>2,"300","301",toDecimal(302),(short)
         *             303,304,305L,306F,307D,308,toTimestamp(309 * millsPerDay),toBytes("310")
         *       </ul>
         * </ul>
         */
        final long millsPerDay = 86400000;
        write.write(
                rowData(
                        1,
                        "100",
                        "101",
                        toDecimal(102),
                        (short) 103,
                        104,
                        105L,
                        106F,
                        107D,
                        108,
                        toTimestamp(109 * millsPerDay),
                        "110".getBytes()));
        write.write(
                rowData(
                        2,
                        "200",
                        "201",
                        toDecimal(202),
                        (short) 203,
                        204,
                        205L,
                        206F,
                        207D,
                        208,
                        toTimestamp(209 * millsPerDay),
                        toBytes("210")));
        write.write(
                rowData(
                        2,
                        "300",
                        "301",
                        toDecimal(302),
                        (short) 303,
                        304,
                        305L,
                        306F,
                        307D,
                        308,
                        toTimestamp(309 * millsPerDay),
                        toBytes("310")));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        R result = firstChecker.apply(tableSchemas);

        /**
         * Fields before and after column type evolution:
         *
         * <ul>
         *   <li>Before: a->int, b->char[10], c->varchar[10], d->decimal, e->smallint, f->int,
         *       g->bigint, h->float, i->double, j->date, k->timestamp, l->binary
         *   <li>After: a->int, b->varchar[10], c->varchar[10], d->double, e->int, f->decimal,
         *       g->float, h->double, i->decimal, j->date, k->date, l->varbinary
         * </ul>
         */
        //
        List<DataField> evolutionFields = new ArrayList<>(SCHEMA_FIELDS);
        evolutionFields.set(1, new DataField(1, "b", new VarCharType(10)));
        evolutionFields.set(3, new DataField(3, "d", new DoubleType()));
        evolutionFields.set(4, new DataField(4, "e", new IntType()));
        evolutionFields.set(5, new DataField(5, "f", new DecimalType(10, 2)));
        evolutionFields.set(6, new DataField(6, "g", new FloatType()));
        evolutionFields.set(7, new DataField(7, "h", new DoubleType()));
        evolutionFields.set(8, new DataField(8, "i", new DecimalType(10, 2)));
        evolutionFields.set(10, new DataField(10, "k", new DateType()));
        evolutionFields.set(11, new DataField(11, "l", new VarBinaryType(100)));
        tableSchemas.put(
                1L,
                new TableSchema(
                        1,
                        evolutionFields,
                        12,
                        SCHEMA_PARTITION_NAMES,
                        primaryKeyNames,
                        tableConfig.toMap(),
                        ""));
        table = createFileStoreTable.apply(tableSchemas);
        write = table.newWrite("user");
        commit = table.newCommit("user");

        /**
         * Generate another two files:
         *
         * <ul>
         *   <li>file1 with one data:
         *       2,"400","401",402D,403,toDecimal(404),405F,406D,toDecimal(407),408,409,toBytes("410")
         *   <li>file2 with two data:
         *       <ul>
         *         <li>1,"500","501",502D,503,toDecimal(504),505F,506D,toDecimal(507),508,509,toBytes("510")
         *         <li>1,"600","601",602D,603,toDecimal(604),605F,606D,toDecimal(607),608,609,toBytes("610")
         *       </ul>
         * </ul>
         */
        write.write(
                rowData(
                        2,
                        "400",
                        "401",
                        402D,
                        403,
                        toDecimal(404),
                        405F,
                        406D,
                        toDecimal(407),
                        408,
                        409,
                        toBytes("410")));
        write.write(
                rowData(
                        1,
                        "500",
                        "501",
                        502D,
                        503,
                        toDecimal(504),
                        505F,
                        506D,
                        toDecimal(507),
                        508,
                        509,
                        toBytes("510")));
        write.write(
                rowData(
                        1,
                        "600",
                        "601",
                        602D,
                        603,
                        toDecimal(604),
                        605F,
                        606D,
                        toDecimal(607),
                        608,
                        609,
                        toBytes("610")));
        commit.commit(1, write.prepareCommit(true, 1));
        write.close();
        commit.close();

        secondChecker.accept(result, tableSchemas);
    }

    private static Decimal toDecimal(int val) {
        return Decimal.fromBigDecimal(new BigDecimal(val), 10, 2);
    }

    private static Timestamp toTimestamp(long mills) {
        return Timestamp.fromEpochMillis(mills);
    }

    private static byte[] toBytes(String val) {
        return val.getBytes();
    }

    protected static InternalRow rowData(Object... values) {
        List<Object> valueList = new ArrayList<>(values.length);
        for (Object value : values) {
            if (value instanceof String) {
                valueList.add(BinaryString.fromString((String) value));
            } else {
                valueList.add(value);
            }
        }
        return GenericRow.of(valueList.toArray(new Object[0]));
    }

    protected static List<DataFileMeta> toDataFileMetas(List<DataSplit> splits) {
        return splits.stream().flatMap(s -> s.dataFiles().stream()).collect(Collectors.toList());
    }

    protected static void checkFilterRowCount(
            List<DataFileMeta> fileMetaList, long expectedRowCount) {
        assertThat(fileMetaList.stream().mapToLong(DataFileMeta::rowCount).sum())
                .isEqualTo(expectedRowCount);
    }

    protected List<String> getResult(
            TableRead read, List<Split> splits, Function<InternalRow, String> rowDataToString) {
        try {
            List<ReaderSupplier<InternalRow>> readers = new ArrayList<>();
            for (Split split : splits) {
                readers.add(() -> read.createReader(split));
            }
            RecordReader<InternalRow> recordReader = ConcatRecordReader.create(readers);
            RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(recordReader);
            List<String> result = new ArrayList<>();
            while (iterator.hasNext()) {
                InternalRow rowData = iterator.next();
                result.add(rowDataToString.apply(rowData));
            }
            iterator.close();
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected List<String> getResult(
            TableRead read, List<Split> splits, List<InternalRow.FieldGetter> fieldGetterList) {
        return getResult(
                read,
                splits,
                row -> {
                    List<String> stringResultList = new ArrayList<>(fieldGetterList.size());
                    for (InternalRow.FieldGetter getter : fieldGetterList) {
                        Object result = getter.getFieldOrNull(row);
                        stringResultList.add(
                                result == null
                                        ? "null"
                                        : (result instanceof byte[]
                                                ? new String((byte[]) result)
                                                : result.toString()));
                    }
                    return StringUtils.join(stringResultList, "|");
                });
    }

    /** {@link SchemaManager} subclass for testing. */
    public static class TestingSchemaManager extends SchemaManager {
        private final Map<Long, TableSchema> tableSchemas;

        public TestingSchemaManager(Path tableRoot, Map<Long, TableSchema> tableSchemas) {
            super(FileIOFinder.find(tableRoot), tableRoot);
            this.tableSchemas = tableSchemas;
        }

        @Override
        public Optional<TableSchema> latest() {
            return Optional.of(
                    tableSchemas.get(
                            tableSchemas.keySet().stream()
                                    .max(Long::compareTo)
                                    .orElseThrow(IllegalStateException::new)));
        }

        @Override
        public List<TableSchema> listAll() {
            return new ArrayList<>(tableSchemas.values());
        }

        @Override
        public List<Long> listAllIds() {
            return new ArrayList<>(tableSchemas.keySet());
        }

        @Override
        public TableSchema createTable(Schema schema) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableSchema commitChanges(List<SchemaChange> changes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableSchema schema(long id) {
            return checkNotNull(tableSchemas.get(id));
        }
    }

    protected List<Split> toSplits(List<DataSplit> dataSplits) {
        return new ArrayList<>(dataSplits);
    }
}
