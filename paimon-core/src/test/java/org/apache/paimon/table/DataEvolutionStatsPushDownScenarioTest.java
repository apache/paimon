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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_MERGED_READ_STATS_PUSHDOWN_ENABLED;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenarios of pushing filters into merged data evolution groups by file statistics. Every read is
 * compared with the same read without the pushdown and with an in-memory model of the table.
 */
public class DataEvolutionStatsPushDownScenarioTest extends TableTestBase {

    private static final int RANGE_ROWS = 3000;

    // ---------------------------------------------------------------- scenarios

    /** Several row id ranges, so the candidates of the later ones do not start at row id 0. */
    @Test
    public void testOnlyLaterRangeUpdated() throws Exception {
        Model m = create("later_range", "parquet", false, Collections.emptyMap());
        for (int r = 0; r < 3; r++) {
            writeRange(m, RANGE_ROWS, null, row -> base(row));
        }
        // only the last range can match, at its end, far from row id 0
        update(
                m,
                2,
                Collections.singletonList("a"),
                (row, col) -> 100_000 + (int) (row - 2 * RANGE_ROWS));

        Result result =
                assertMatchesModel(
                        m, builder(m).greaterOrEqual(index(m, "a"), 100_000 + RANGE_ROWS - 100));
        assertThat(result.matched).hasSize(100);
        assertThat(result.returned).isLessThan(RANGE_ROWS / 4);
    }

    /** The candidates of the statistics and the rows of a bitmap index are intersected. */
    @Test
    public void testBitmapIndexAndStatisticsBothPrune() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(
                FileIndexOptions.FILE_INDEX
                        + "."
                        + BitmapFileIndexFactory.BITMAP_INDEX
                        + "."
                        + CoreOptions.COLUMNS,
                "b");
        // as a write option of the base file only: a partial update can not index a column it
        // does not write
        Model m = create("bitmap", "parquet", false, Collections.emptyMap());
        m.table = m.table.copy(options);
        writeRange(m, RANGE_ROWS, null, row -> base(row, (int) (row % 10)));
        update(m, 0, Collections.singletonList("a"), (row, col) -> (int) (RANGE_ROWS - 1 - row));

        PredicateBuilder builder = builder(m);
        Predicate filter =
                PredicateBuilder.and(
                        builder.lessThan(index(m, "a"), 300), builder.equal(index(m, "b"), 3));
        Result both = assertMatchesModel(m, filter);
        assertThat(both.matched).hasSize(30);

        long statsOnly =
                read(copy(m.table, "file-index.read.enabled", "false"), filter, null).returned;
        long bitmapOnly =
                read(
                                copy(
                                        m.table,
                                        DATA_EVOLUTION_MERGED_READ_STATS_PUSHDOWN_ENABLED.key(),
                                        "false"),
                                filter,
                                null)
                        .returned;
        assertThat(both.returned).isLessThan(statsOnly).isLessThan(bitmapOnly);
    }

    @Test
    public void testPartitionedTable() throws Exception {
        Model m = create("partitioned", "parquet", true, Collections.emptyMap());
        writeRange(m, RANGE_ROWS, "p0", row -> base(row));
        writeRange(m, RANGE_ROWS, "p1", row -> base(row));
        for (int r = 0; r < 2; r++) {
            update(m, r, Collections.singletonList("a"), (row, col) -> 100_000 + row.intValue());
        }

        PredicateBuilder builder = builder(m);
        int last = 100_000 + 2 * RANGE_ROWS;
        Result result = assertMatchesModel(m, builder.greaterOrEqual(index(m, "a"), last - 100));
        assertThat(result.matched).hasSize(100);
        assertThat(result.returned).isLessThan(RANGE_ROWS / 4);
        Result p0 =
                assertMatchesModel(
                        m,
                        PredicateBuilder.and(
                                builder.equal(index(m, "pt"), BinaryString.fromString("p0")),
                                builder.greaterThan(index(m, "a"), 100_000 + RANGE_ROWS - 100)));
        assertThat(p0.matched).hasSize(99);
    }

    /** A column added after the base file was written lives only in the update file. */
    @Test
    public void testAddedColumnBackfilled() throws Exception {
        Model m = create("added", "parquet", false, Collections.emptyMap());
        writeRange(m, RANGE_ROWS, null, row -> base(row));
        writeRange(m, RANGE_ROWS, null, row -> base(row));
        addColumn(m, "x", DataTypes.INT());
        // backfill the first range only, the second keeps nulls
        update(m, 0, Collections.singletonList("x"), (row, col) -> (int) (RANGE_ROWS - row));

        PredicateBuilder builder = builder(m);
        Result result = assertMatchesModel(m, builder.lessThan(index(m, "x"), 100));
        assertThat(result.matched).hasSize(99);
        // the range which is not backfilled is a single file, read as before
        assertThat(result.returned).isLessThan(result.baseline - RANGE_ROWS / 2);
        assertThat(assertMatchesModel(m, builder.isNull(index(m, "x"))).matched)
                .hasSize(RANGE_ROWS);
    }

    @Test
    public void testWidenedColumnType() throws Exception {
        Model m = create("widened", "parquet", false, Collections.emptyMap());
        writeRange(m, RANGE_ROWS, null, row -> base(row));
        update(m, 0, Collections.singletonList("a"), (row, col) -> (int) (RANGE_ROWS - row));
        alterType(m, "a", DataTypes.BIGINT());

        assertThat(assertMatchesModel(m, builder(m).lessThan(index(m, "a"), 100L)).matched)
                .hasSize(99);
    }

    /** Formats without candidate rows and groups the pushdown skips read as before. */
    @ParameterizedTest
    @ValueSource(strings = {"orc", "avro"})
    public void testOtherFormats(String format) throws Exception {
        Model m = create("format_" + format, format, false, Collections.emptyMap());
        writeRange(m, RANGE_ROWS, null, row -> base(row));
        update(m, 0, Collections.singletonList("a"), (row, col) -> (int) (RANGE_ROWS - row));

        assertThat(assertMatchesModel(m, builder(m).lessThan(index(m, "a"), 100)).matched)
                .hasSize(99);
    }

    @Test
    public void testNestedFieldEvolutionTable() throws Exception {
        Model m =
                create(
                        "nested",
                        "parquet",
                        false,
                        Collections.singletonMap(
                                CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true"));
        writeRange(m, RANGE_ROWS, null, row -> base(row));
        update(m, 0, Collections.singletonList("a"), (row, col) -> (int) (RANGE_ROWS - row));

        assertThat(assertMatchesModel(m, builder(m).lessThan(index(m, "a"), 100)).matched)
                .hasSize(99);
    }

    /** A group with a blob file is not pruned, and reads as without the pushdown. */
    @Test
    public void testGroupWithBlobFile() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("a", DataTypes.INT())
                        .column("f", DataTypes.BLOB())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option("parquet.page.size", "1024")
                        .build();
        catalog.createTable(identifier("blob"), schema, false);
        FileStoreTable table = getTable(identifier("blob"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < RANGE_ROWS; i++) {
                write.write(GenericRow.of((long) i, i, new BlobData(new byte[] {(byte) i})));
            }
            commit.commit(write.prepareCommit());
        }
        builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(
                                        table.rowType().project(Collections.singletonList("a")));
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < RANGE_ROWS; i++) {
                write.write(GenericRow.of(RANGE_ROWS - 1 - i));
            }
            List<CommitMessage> messages = write.prepareCommit();
            assignFirstRowId(messages, 0);
            commit.commit(messages);
        }
        table = getTable(identifier("blob"));

        Predicate filter = new PredicateBuilder(table.rowType()).lessThan(1, 100);
        List<String> expected = new ArrayList<>();
        for (int i = RANGE_ROWS - 100; i < RANGE_ROWS; i++) {
            expected.add(i + ":" + (RANGE_ROWS - 1 - i) + ":" + (byte) i);
        }
        Collections.sort(expected);
        for (boolean enabled : new boolean[] {true, false}) {
            ReadBuilder read = pushDown(table, enabled).newReadBuilder().withFilter(filter);
            List<String> matched = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    read.newRead().createReader(read.newScan().plan().splits())) {
                reader.forEachRemaining(
                        row -> {
                            if (row.getInt(1) < 100) {
                                matched.add(
                                        row.getLong(0)
                                                + ":"
                                                + row.getInt(1)
                                                + ":"
                                                + row.getBlob(2).toData()[0]);
                            }
                        });
            }
            Collections.sort(matched);
            assertThat(matched).as("pushdown %s", enabled).isEqualTo(expected);
        }
    }

    /**
     * A filter on a column outside the projection is not pushed down at all, and a predicate the
     * format can not evaluate prunes nothing.
     */
    @Test
    public void testFiltersWhichCanNotPrune() throws Exception {
        Model m = create("not_pruned", "parquet", false, Collections.emptyMap());
        writeRange(m, RANGE_ROWS, null, row -> base(row));
        update(m, 0, Arrays.asList("a", "s"), (row, col) -> updated(row, col, RANGE_ROWS));

        PredicateBuilder builder = builder(m);
        Predicate onA = builder.lessThan(index(m, "a"), 100);
        // s is updated too, so the projection still merges the base and the update file
        RowType projection = m.table.rowType().project(Arrays.asList("id", "s"));
        assertThat(read(pushDown(m.table, true), onA, projection).all)
                .isEqualTo(read(pushDown(m.table, false), onA, projection).all);

        assertMatchesModel(m, builder.startsWith(index(m, "s"), BinaryString.fromString("u0001")));
    }

    /** The mappings computing candidate rows are keyed without the filters. */
    @Test
    public void testFilterChangedOnTheSameRead() throws Exception {
        Model m = create("refiltered", "parquet", false, Collections.emptyMap());
        writeRange(m, RANGE_ROWS, null, row -> base(row));
        update(m, 0, Collections.singletonList("a"), (row, col) -> (int) (RANGE_ROWS - row));

        PredicateBuilder builder = builder(m);
        Predicate first = builder.lessThan(index(m, "a"), 100);
        Predicate second = builder.greaterThan(index(m, "a"), RANGE_ROWS - 100);
        ReadBuilder readBuilder = pushDown(m.table, true).newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        InnerTableRead read = (InnerTableRead) readBuilder.newRead();

        read.withFilter(first);
        assertThat(matched(read, splits, first, m)).isEqualTo(expected(m, first));
        read.withFilter(second);
        assertThat(matched(read, splits, second, m)).isEqualTo(expected(m, second));
    }

    /**
     * Random tables of several row id ranges, with nulls, a string column, a column added and
     * backfilled for some ranges, deletion vectors and random filters.
     */
    @Test
    public void testRandomTables() throws Exception {
        Random random = new Random(20261013L);
        for (int round = 0; round < 20; round++) {
            boolean deletionVectors = round % 3 == 0;
            Map<String, String> options =
                    deletionVectors
                            ? Collections.singletonMap(
                                    CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                            : Collections.emptyMap();
            Model m = create("random_" + round, "parquet", false, options);
            int ranges = 2 + random.nextInt(3);
            for (int r = 0; r < ranges; r++) {
                int rows = 500 + random.nextInt(2000);
                writeRange(m, rows, null, row -> randomBase(row, random));
            }
            int updates = 1 + random.nextInt(3);
            for (int u = 0; u < updates; u++) {
                List<String> columns = randomColumns(random);
                BiFunction<Long, String, Object> values = randomValues(random);
                for (int r = 0; r < ranges; r++) {
                    if (random.nextInt(10) < 6) {
                        update(m, r, columns, values);
                    }
                }
            }
            if (random.nextBoolean()) {
                addColumn(m, "x", DataTypes.INT());
                BiFunction<Long, String, Object> values = randomValues(random);
                for (int r = 0; r < ranges; r++) {
                    if (random.nextBoolean()) {
                        update(m, r, Collections.singletonList("x"), values);
                    }
                }
            }
            if (deletionVectors) {
                List<Long> rowIds = new ArrayList<>();
                for (int d = 0; d < 30; d++) {
                    rowIds.add((long) random.nextInt(m.rows.size()));
                }
                delete(m, rowIds);
            }
            for (int f = 0; f < 6; f++) {
                assertMatchesModel(m, randomFilter(random, m));
            }
        }
    }

    // -------------------------------------------------------------- random data

    private static Map<String, Object> randomBase(long row, Random random) {
        Map<String, Object> values = base(row);
        if (random.nextInt(20) == 0) {
            values.put("a", null);
        }
        if (random.nextInt(20) == 0) {
            values.put("s", null);
        }
        values.put("b", random.nextInt(1000));
        return values;
    }

    private static List<String> randomColumns(Random random) {
        List<String> columns = new ArrayList<>();
        for (String column : Arrays.asList("a", "b", "s")) {
            if (random.nextBoolean()) {
                columns.add(column);
            }
        }
        if (columns.isEmpty()) {
            columns.add("a");
        }
        return columns;
    }

    private static BiFunction<Long, String, Object> randomValues(Random random) {
        int pattern = random.nextInt(5);
        int shift = random.nextInt(5000);
        long seed = random.nextLong();
        return (row, column) -> {
            Random cell = new Random(seed + row * 31 + column.hashCode());
            // half nulls for pattern 4, a tenth for the others
            if (pattern == 4 ? cell.nextBoolean() : cell.nextInt(10) == 0) {
                return null;
            }
            long value;
            switch (pattern) {
                case 0:
                    value = row + shift;
                    break;
                case 1:
                    value = 20000 - row + shift;
                    break;
                case 2:
                    value = cell.nextInt(20000);
                    break;
                default:
                    value = (row / 97) * 3 + shift;
            }
            return column.equals("s") ? String.format("u%06d", value) : (Object) (int) value;
        };
    }

    private static Predicate randomFilter(Random random, Model m) {
        Predicate first = randomLeaf(random, m);
        switch (random.nextInt(3)) {
            case 0:
                return first;
            case 1:
                return PredicateBuilder.and(first, randomLeaf(random, m));
            default:
                return PredicateBuilder.or(first, randomLeaf(random, m));
        }
    }

    private static Predicate randomLeaf(Random random, Model m) {
        List<String> columns = new ArrayList<>(Arrays.asList("a", "b", "s"));
        if (m.columns.containsKey("x")) {
            columns.add("x");
        }
        String column = columns.get(random.nextInt(columns.size()));
        int field = index(m, column);
        PredicateBuilder builder = builder(m);
        int value = random.nextInt(20000);
        Object literal =
                column.equals("s")
                        ? BinaryString.fromString(String.format("u%06d", value))
                        : (Object) value;
        Object upper =
                column.equals("s")
                        ? BinaryString.fromString(String.format("u%06d", value + 1000))
                        : (Object) (value + 1000);
        switch (random.nextInt(9)) {
            case 0:
                return builder.lessThan(field, literal);
            case 1:
                return builder.greaterThan(field, literal);
            case 2:
                return builder.equal(field, literal);
            case 3:
                return builder.notEqual(field, literal);
            case 4:
                return builder.between(field, literal, upper);
            case 5:
                return builder.isNull(field);
            case 6:
                return builder.isNotNull(field);
            case 7:
                return builder.in(field, Arrays.asList(literal, upper));
            default:
                return column.equals("s")
                        ? builder.startsWith(field, BinaryString.fromString("u00"))
                        : builder.notIn(field, Arrays.asList(literal, upper));
        }
    }

    private static Map<String, Object> base(long row) {
        return base(row, (int) row);
    }

    private static Map<String, Object> base(long row, int b) {
        Map<String, Object> values = new HashMap<>();
        values.put("id", row);
        values.put("a", (int) row);
        values.put("b", b);
        values.put("s", String.format("s%06d", row));
        return values;
    }

    private static Object updated(long row, String column, int rows) {
        long value = rows - row;
        return column.equals("s") ? String.format("u%06d", value) : (Object) (int) value;
    }

    // -------------------------------------------------------------- the model

    /** The values of every row of a table, by row id, and its row id ranges. */
    private static class Model {
        private final String name;
        private final boolean partitioned;
        private final LinkedHashMap<String, DataType> columns = new LinkedHashMap<>();
        private final List<Map<String, Object>> rows = new ArrayList<>();
        private final List<long[]> ranges = new ArrayList<>();
        private final List<String> rangePartitions = new ArrayList<>();
        private final Set<Long> deleted = new HashSet<>();
        private FileStoreTable table;

        private Model(String name, boolean partitioned) {
            this.name = name;
            this.partitioned = partitioned;
        }
    }

    private Model create(
            String name, String format, boolean partitioned, Map<String, String> options)
            throws Exception {
        Model m = new Model(name, partitioned);
        if (partitioned) {
            m.columns.put("pt", DataTypes.STRING());
        }
        m.columns.put("id", DataTypes.BIGINT());
        m.columns.put("a", DataTypes.INT());
        m.columns.put("b", DataTypes.INT());
        m.columns.put("s", DataTypes.STRING());
        Schema.Builder builder = Schema.newBuilder();
        m.columns.forEach(builder::column);
        if (partitioned) {
            builder.partitionKeys("pt");
        }
        builder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.FILE_FORMAT.key(), format)
                // several row groups and pages per file
                .option("parquet.block.size", String.valueOf(16 * 1024))
                .option("parquet.page.size", String.valueOf(1024));
        options.forEach(builder::option);
        catalog.createTable(identifier(name), builder.build(), false);
        m.table = getTable(identifier(name));
        return m;
    }

    private void writeRange(
            Model m, int count, String partition, Function<Long, Map<String, Object>> values)
            throws Exception {
        long firstRowId = m.rows.size();
        BatchWriteBuilder builder = m.table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (long row = firstRowId; row < firstRowId + count; row++) {
                Map<String, Object> rowValues = new HashMap<>(values.apply(row));
                if (partition != null) {
                    rowValues.put("pt", partition);
                }
                m.rows.add(rowValues);
                write.write(toRow(m, rowValues, new ArrayList<>(m.columns.keySet())));
            }
            commit.commit(write.prepareCommit());
        }
        m.table = getTable(identifier(m.name));
        m.ranges.add(new long[] {firstRowId, count});
        m.rangePartitions.add(partition);
        assertThat(
                        m.table.store().newScan().plan().files().stream()
                                .map(ManifestEntry::file)
                                .anyMatch(f -> f.nonNullFirstRowId() == firstRowId))
                .as("the new base file starts at row id %s", firstRowId)
                .isTrue();
    }

    private void update(
            Model m, int range, List<String> columns, BiFunction<Long, String, Object> values)
            throws Exception {
        long firstRowId = m.ranges.get(range)[0];
        long count = m.ranges.get(range)[1];
        String partition = m.rangePartitions.get(range);
        List<String> writeColumns = new ArrayList<>();
        if (m.partitioned) {
            writeColumns.add("pt");
        }
        writeColumns.addAll(columns);
        BatchWriteBuilder builder = m.table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite().withWriteType(m.table.rowType().project(writeColumns));
                BatchTableCommit commit = builder.newCommit()) {
            for (long row = firstRowId; row < firstRowId + count; row++) {
                Map<String, Object> rowValues = m.rows.get((int) row);
                for (String column : columns) {
                    rowValues.put(column, values.apply(row, column));
                }
                if (partition != null) {
                    rowValues.put("pt", partition);
                }
                write.write(toRow(m, rowValues, writeColumns));
            }
            List<CommitMessage> messages = write.prepareCommit();
            assignFirstRowId(messages, firstRowId);
            commit.commit(messages);
        }
        m.table = getTable(identifier(m.name));
    }

    private void addColumn(Model m, String column, DataType type) throws Exception {
        catalog.alterTable(identifier(m.name), SchemaChange.addColumn(column, type), false);
        m.columns.put(column, type);
        m.table = getTable(identifier(m.name));
    }

    private void alterType(Model m, String column, DataType type) throws Exception {
        catalog.alterTable(identifier(m.name), SchemaChange.updateColumnType(column, type), false);
        m.columns.put(column, type);
        m.table = getTable(identifier(m.name));
    }

    private void delete(Model m, List<Long> rowIds) throws Exception {
        TreeMap<Long, List<DataFileMeta>> groups = new TreeMap<>();
        for (ManifestEntry entry : m.table.store().newScan().plan().files()) {
            groups.computeIfAbsent(entry.file().nonNullFirstRowId(), k -> new ArrayList<>())
                    .add(entry.file());
        }
        Map<String, DeletionVector> vectors = new HashMap<>();
        for (long rowId : rowIds) {
            Map.Entry<Long, List<DataFileMeta>> group = groups.floorEntry(rowId);
            DataFileMeta anchor = retrieveAnchorFile(group.getValue(), f -> f);
            vectors.computeIfAbsent(anchor.fileName(), k -> new BitmapDeletionVector())
                    .delete(rowId - group.getKey());
            m.deleted.add(rowId);
        }
        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        m.table.store().newIndexFileHandler(),
                        m.table.latestSnapshot().get(),
                        BinaryRow.EMPTY_ROW);
        vectors.forEach(maintainer::notifyNewDeletionVector);
        List<IndexFileMeta> added = new ArrayList<>();
        List<IndexFileMeta> removed = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            (entry.kind() == FileKind.ADD ? added : removed).add(entry.indexFile());
        }
        try (BatchTableCommit commit = m.table.newBatchWriteBuilder().newCommit()) {
            commit.commit(
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    BinaryRow.EMPTY_ROW,
                                    0,
                                    null,
                                    new DataIncrement(
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            added,
                                            removed),
                                    CompactIncrement.emptyIncrement())));
        }
        m.table = getTable(identifier(m.name));
    }

    private static void assignFirstRowId(List<CommitMessage> messages, long firstRowId) {
        for (CommitMessage message : messages) {
            CommitMessageImpl impl = (CommitMessageImpl) message;
            List<DataFileMeta> files = new ArrayList<>(impl.newFilesIncrement().newFiles());
            impl.newFilesIncrement().newFiles().clear();
            files.forEach(
                    f -> impl.newFilesIncrement().newFiles().add(f.assignFirstRowId(firstRowId)));
        }
    }

    private static GenericRow toRow(Model m, Map<String, Object> values, List<String> columns) {
        Object[] fields = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            fields[i] = internal(values.get(columns.get(i)), m.columns.get(columns.get(i)));
        }
        return GenericRow.of(fields);
    }

    private static Object internal(Object value, DataType type) {
        if (value == null) {
            return null;
        }
        if (type.getTypeRoot() == DataTypeRoot.VARCHAR) {
            return BinaryString.fromString((String) value);
        }
        if (type.getTypeRoot() == DataTypeRoot.BIGINT) {
            return ((Number) value).longValue();
        }
        return value;
    }

    // ------------------------------------------------------------------- reads

    private static class Result {
        private final List<String> matched = new ArrayList<>();
        private final List<String> all = new ArrayList<>();
        private long returned;
        // rows returned by the same read without the pushdown
        private long baseline;
    }

    private Result assertMatchesModel(Model m, Predicate filter) throws Exception {
        List<String> expected = expected(m, filter);
        Result disabled = read(pushDown(m.table, false), filter, null);
        Result enabled = read(pushDown(m.table, true), filter, null);
        assertThat(disabled.matched).as("%s, disabled, %s", m.name, filter).isEqualTo(expected);
        assertThat(enabled.matched).as("%s, enabled, %s", m.name, filter).isEqualTo(expected);
        assertThat(enabled.returned).isLessThanOrEqualTo(disabled.returned);
        enabled.baseline = disabled.returned;
        return enabled;
    }

    private static List<String> expected(Model m, Predicate filter) {
        RowType rowType = m.table.rowType();
        List<String> expected = new ArrayList<>();
        for (int row = 0; row < m.rows.size(); row++) {
            if (m.deleted.contains((long) row)) {
                continue;
            }
            GenericRow genericRow = toRow(m, m.rows.get(row), new ArrayList<>(m.columns.keySet()));
            if (filter.test(genericRow)) {
                expected.add(format(genericRow, rowType));
            }
        }
        Collections.sort(expected);
        return expected;
    }

    private static Result read(FileStoreTable table, Predicate filter, RowType projection)
            throws Exception {
        RowType readType = projection == null ? table.rowType() : projection;
        ReadBuilder builder = table.newReadBuilder().withReadType(readType).withFilter(filter);
        Result result = new Result();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan().splits())) {
            reader.forEachRemaining(
                    row -> {
                        result.returned++;
                        String formatted = format(row, readType);
                        result.all.add(formatted);
                        if (projection == null && filter.test(row)) {
                            result.matched.add(formatted);
                        }
                    });
        }
        Collections.sort(result.matched);
        Collections.sort(result.all);
        return result;
    }

    private static List<String> matched(
            InnerTableRead read, List<Split> splits, Predicate filter, Model m) throws Exception {
        List<String> matched = new ArrayList<>();
        RowType rowType = m.table.rowType();
        try (RecordReader<InternalRow> reader = read.createReader(splits)) {
            reader.forEachRemaining(
                    row -> {
                        if (filter.test(row)) {
                            matched.add(format(row, rowType));
                        }
                    });
        }
        Collections.sort(matched);
        return matched;
    }

    private static String format(InternalRow row, RowType rowType) {
        InternalRow.FieldGetter[] getters =
                InternalRowUtils.createFieldGetters(rowType.getFieldTypes());
        List<String> values = new ArrayList<>(getters.length);
        for (InternalRow.FieldGetter getter : getters) {
            values.add(String.valueOf(getter.getFieldOrNull(row)));
        }
        return String.join(",", values);
    }

    private static FileStoreTable pushDown(FileStoreTable table, boolean enabled) {
        return copy(
                table,
                DATA_EVOLUTION_MERGED_READ_STATS_PUSHDOWN_ENABLED.key(),
                String.valueOf(enabled));
    }

    private static FileStoreTable copy(FileStoreTable table, String key, String value) {
        return table.copy(Collections.singletonMap(key, value));
    }

    private static PredicateBuilder builder(Model m) {
        return new PredicateBuilder(m.table.rowType());
    }

    private static int index(Model m, String column) {
        List<String> names =
                m.table.rowType().getFields().stream()
                        .map(DataField::name)
                        .collect(Collectors.toList());
        return names.indexOf(column);
    }
}
