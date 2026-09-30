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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.fs.Path;
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
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_MERGED_READ_STATS_PUSHDOWN_ENABLED;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests pushing filters into merged data evolution groups by file statistics. */
public class DataEvolutionStatsPushDownTest extends TableTestBase {

    private static final int ROWS = 20_000;

    private int[] a;
    private int[] b;
    private final Set<Long> deleted = new HashSet<>();

    @Override
    public Schema schemaDefault() {
        return schema(false, ROWS, Collections.emptyMap());
    }

    private Schema schema(boolean deletionVectors, int rows, Map<String, String> options) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.STRING())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        // several row groups and pages per file
                        .option("parquet.block.size", String.valueOf(rows * 2))
                        .option("parquet.page.size", String.valueOf(1024));
        if (deletionVectors) {
            builder.option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        }
        options.forEach(builder::option);
        return builder.build();
    }

    @Test
    public void testStaleCopyDoesNotExcludeMatches() throws Exception {
        FileStoreTable table = create("stale", false, ROWS);
        update(table, "a", i -> ROWS - 1 - i);

        Predicate filter = builder(table).lessThan(1, 200);
        Result result = assertMatchesModel(table, filter);
        assertThat(result.matched).hasSize(200);
        assertThat(result.returned).isLessThan(ROWS / 2);
    }

    @Test
    public void testNewestUpdateWins() throws Exception {
        FileStoreTable table = create("newest", false, ROWS);
        update(table, "a", i -> i + ROWS);
        update(table, "a", i -> ROWS - 1 - i);

        Result result = assertMatchesModel(table, builder(table).lessThan(1, 200));
        assertThat(result.matched).hasSize(200);
        assertThat(result.returned).isLessThan(ROWS / 2);
    }

    @Test
    public void testFiltersWonByDifferentFiles() throws Exception {
        FileStoreTable table = create("different", false, ROWS);
        update(table, "a", i -> ROWS - 1 - i);
        update(table, "b", i -> i * 2);

        PredicateBuilder builder = builder(table);
        Predicate filter =
                PredicateBuilder.and(
                        builder.lessThan(1, 2000), builder.greaterOrEqual(2, (ROWS - 500) * 2));
        Result result = assertMatchesModel(table, filter);
        assertThat(result.matched).hasSize(500);
        assertThat(result.returned).isLessThan(ROWS / 2);
    }

    @Test
    public void testOrAcrossFiles() throws Exception {
        FileStoreTable table = create("or", false, ROWS);
        update(table, "a", i -> ROWS - 1 - i);
        update(table, "b", i -> i);

        PredicateBuilder builder = builder(table);
        Result result =
                assertMatchesModel(
                        table,
                        PredicateBuilder.or(builder.lessThan(1, 100), builder.lessThan(2, 100)));
        assertThat(result.matched).hasSize(200);
    }

    @Test
    public void testWithDeletionVectors() throws Exception {
        FileStoreTable table = create("dv", true, ROWS);
        update(table, "a", i -> ROWS - 1 - i);
        List<Long> toDelete = new ArrayList<>();
        for (long id = ROWS - 1; id >= ROWS - 100; id -= 3) {
            toDelete.add(id);
        }
        toDelete.add(5L);
        deleteRows(table, toDelete);

        Result result = assertMatchesModel(table, builder(table).lessThan(1, 200));
        assertThat(result.matched).hasSize(200 - 34);
        assertThat(result.returned).isLessThan(ROWS / 2);
    }

    @Test
    public void testWithRowRanges() throws Exception {
        FileStoreTable table = create("ranges", false, ROWS);
        update(table, "a", i -> ROWS - 1 - i);

        Predicate filter = builder(table).lessThan(1, 200);
        List<Range> rowRanges = Collections.singletonList(new Range(ROWS - 150, ROWS - 1));
        for (boolean enabled : new boolean[] {true, false}) {
            Result result = read(withPushDown(table, enabled), filter, rowRanges);
            assertThat(result.matched).containsExactlyElementsOf(expected(filter, rowRanges));
            assertThat(result.matched).hasSize(150);
        }
    }

    @Test
    public void testRenamedColumn() throws Exception {
        FileStoreTable table = create("renamed", false, ROWS);
        update(table, "a", i -> ROWS - 1 - i);
        catalog.alterTable(identifier("renamed"), SchemaChange.renameColumn("a", "a2"), false);
        table = getTable(identifier("renamed"));

        Result result = assertMatchesModel(table, builder(table).lessThan(1, 200));
        assertThat(result.matched).hasSize(200);
        assertThat(result.returned).isLessThan(ROWS / 2);
    }

    @Test
    public void testLostFileBehavesAsWithoutPushDown() throws Exception {
        FileStoreTable table = create("lost", false, ROWS);
        update(table, "a", i -> ROWS - 1 - i);
        table = getTable(identifier("lost"));
        table.fileIO().deleteQuietly(updateFilePath(table));

        assertSameOutcome(table, CoreOptions.SCAN_IGNORE_LOST_FILE.key());
    }

    @Test
    public void testCorruptFileBehavesAsWithoutPushDown() throws Exception {
        FileStoreTable table = create("corrupt", false, ROWS);
        update(table, "a", i -> ROWS - 1 - i);
        table = getTable(identifier("corrupt"));
        table.fileIO().overwriteFileUtf8(updateFilePath(table), "not a parquet file");

        assertSameOutcome(table, CoreOptions.SCAN_IGNORE_CORRUPT_FILE.key());
    }

    @Test
    public void testWithRowSidecar() throws Exception {
        FileStoreTable table =
                create(
                        "sidecar",
                        false,
                        ROWS,
                        Collections.singletonMap(
                                CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true"));
        update(table, "a", i -> ROWS - 1 - i);
        table = getTable(identifier("sidecar"));
        // the few rows left by the candidate ranges are read from the sidecars
        assertThat(
                        table.store().newScan().plan().files().stream()
                                .map(ManifestEntry::file)
                                .anyMatch(
                                        f ->
                                                f.extraFiles().stream()
                                                        .anyMatch(name -> name.endsWith(".row"))))
                .isTrue();

        Result result = assertMatchesModel(table, builder(table).lessThan(1, 100));
        assertThat(result.matched).hasSize(100);
        assertThat(result.returned).isLessThan(ROWS / 10);
    }

    private Path updateFilePath(FileStoreTable table) {
        DataFileMeta update =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(f -> f.writeCols() != null && f.writeCols().contains("a"))
                        .findFirst()
                        .get();
        return table.store()
                .pathFactory()
                .createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0)
                .toPath(update);
    }

    /** The pushdown must not change what a read does, with the file option on and off. */
    private void assertSameOutcome(FileStoreTable table, String ignoreOption) {
        Predicate filter = builder(table).lessThan(1, 200);
        for (boolean ignore : new boolean[] {false, true}) {
            FileStoreTable options =
                    table.copy(Collections.singletonMap(ignoreOption, String.valueOf(ignore)));
            assertThat(outcome(withPushDown(options, true), filter))
                    .as("%s=%s", ignoreOption, ignore)
                    .isEqualTo(outcome(withPushDown(options, false), filter));
        }
    }

    private String outcome(FileStoreTable table, Predicate filter) {
        try {
            return "rows " + read(table, filter, null).matched;
        } catch (Exception e) {
            return "error " + e.getClass().getName();
        }
    }

    @Test
    public void testDisabled() throws Exception {
        FileStoreTable table = create("disabled", false, ROWS);
        update(table, "a", i -> ROWS - 1 - i);

        Predicate filter = builder(table).lessThan(1, 200);
        Result result = read(withPushDown(table, false), filter, null);
        assertThat(result.matched).containsExactlyElementsOf(expected(filter, null));
        assertThat(result.returned).isEqualTo(ROWS);
    }

    @Test
    public void testRandomUpdatesAndFilters() throws Exception {
        int rows = 3000;
        Random random = new Random(20260929L);
        for (int round = 0; round < 30; round++) {
            FileStoreTable table = create("random_" + round, round % 3 == 0, rows);
            int updates = 1 + random.nextInt(3);
            for (int u = 0; u < updates; u++) {
                List<String> columns = new ArrayList<>();
                if (random.nextBoolean()) {
                    columns.add("a");
                }
                if (columns.isEmpty() || random.nextBoolean()) {
                    columns.add("b");
                }
                IntUnaryOperator values = randomValues(random, rows);
                update(table, columns, values);
            }
            if (round % 3 == 0) {
                List<Long> toDelete = new ArrayList<>();
                for (int d = 0; d < 20; d++) {
                    toDelete.add((long) random.nextInt(rows));
                }
                deleteRows(table, toDelete);
            }
            for (int f = 0; f < 5; f++) {
                assertMatchesModel(table, randomFilter(random, builder(table), rows));
            }
        }
    }

    // ------------------------------------------------------------------ model

    private static IntUnaryOperator randomValues(Random random, int rows) {
        int shift = random.nextInt(rows);
        switch (random.nextInt(4)) {
            case 0:
                return i -> i + shift;
            case 1:
                return i -> rows - 1 - i + shift;
            case 2:
                long seed = random.nextLong();
                return i -> new Random(seed + i).nextInt(rows * 2);
            default:
                return i -> (i / 97) * 3 + shift;
        }
    }

    private static Predicate randomFilter(Random random, PredicateBuilder builder, int rows) {
        Predicate first = randomLeaf(random, builder, rows);
        if (random.nextBoolean()) {
            return first;
        }
        Predicate second = randomLeaf(random, builder, rows);
        return random.nextBoolean()
                ? PredicateBuilder.and(first, second)
                : PredicateBuilder.or(first, second);
    }

    private static Predicate randomLeaf(Random random, PredicateBuilder builder, int rows) {
        int field = 1 + random.nextInt(2);
        int value = random.nextInt(rows * 2);
        switch (random.nextInt(4)) {
            case 0:
                return builder.lessThan(field, value);
            case 1:
                return builder.greaterThan(field, value);
            case 2:
                return builder.equal(field, value);
            default:
                return builder.between(field, value, value + rows / 20);
        }
    }

    private Result assertMatchesModel(FileStoreTable table, Predicate filter) throws Exception {
        List<String> expected = expected(filter, null);
        Result disabled = read(withPushDown(table, false), filter, null);
        Result enabled = read(withPushDown(table, true), filter, null);
        assertThat(disabled.matched).as("disabled, %s", filter).isEqualTo(expected);
        assertThat(enabled.matched).as("enabled, %s", filter).isEqualTo(expected);
        return enabled;
    }

    private List<String> expected(Predicate filter, List<Range> rowRanges) {
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < a.length; i++) {
            if (deleted.contains((long) i) || !inRanges(rowRanges, i)) {
                continue;
            }
            GenericRow row = GenericRow.of((long) i, a[i], b[i], BinaryString.fromString("c-" + i));
            if (filter.test(row)) {
                expected.add(format(row));
            }
        }
        return expected;
    }

    private static boolean inRanges(List<Range> rowRanges, long rowId) {
        return rowRanges == null
                || rowRanges.stream().anyMatch(r -> r.from <= rowId && rowId <= r.to);
    }

    // --------------------------------------------------------------- table io

    private FileStoreTable create(String name, boolean deletionVectors, int rows) throws Exception {
        return create(name, deletionVectors, rows, Collections.emptyMap());
    }

    private FileStoreTable create(
            String name, boolean deletionVectors, int rows, Map<String, String> options)
            throws Exception {
        catalog.createTable(identifier(name), schema(deletionVectors, rows, options), false);
        FileStoreTable table = getTable(identifier(name));
        a = new int[rows];
        b = new int[rows];
        deleted.clear();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < rows; i++) {
                a[i] = i;
                b[i] = i;
                write.write(GenericRow.of((long) i, i, i, BinaryString.fromString("c-" + i)));
            }
            commit.commit(write.prepareCommit());
        }
        return getTable(identifier(name));
    }

    private void update(FileStoreTable table, String column, IntUnaryOperator values)
            throws Exception {
        update(table, Collections.singletonList(column), values);
    }

    private void update(FileStoreTable table, List<String> columns, IntUnaryOperator values)
            throws Exception {
        table = getTable(identifier(table.name()));
        RowType writeType = table.rowType().project(columns);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < a.length; i++) {
                int value = values.applyAsInt(i);
                Object[] fields = new Object[columns.size()];
                for (int c = 0; c < columns.size(); c++) {
                    fields[c] = value + c;
                    if (columns.get(c).equals("a")) {
                        a[i] = value + c;
                    } else {
                        b[i] = value + c;
                    }
                }
                write.write(GenericRow.of(fields));
            }
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                CommitMessageImpl impl = (CommitMessageImpl) message;
                List<DataFileMeta> files = new ArrayList<>(impl.newFilesIncrement().newFiles());
                impl.newFilesIncrement().newFiles().clear();
                files.forEach(f -> impl.newFilesIncrement().newFiles().add(f.assignFirstRowId(0)));
            }
            commit.commit(messages);
        }
    }

    private void deleteRows(FileStoreTable table, List<Long> rowIds) throws Exception {
        table = getTable(identifier(table.name()));
        List<DataFileMeta> files =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());
        DataFileMeta anchor = retrieveAnchorFile(files, f -> f);
        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.latestSnapshot().get(),
                        BinaryRow.EMPTY_ROW);
        DeletionVector deletionVector = new BitmapDeletionVector();
        for (long rowId : rowIds) {
            deletionVector.delete(rowId - anchor.nonNullFirstRowId());
            deleted.add(rowId);
        }
        maintainer.notifyNewDeletionVector(anchor.fileName(), deletionVector);

        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            if (entry.kind() == FileKind.ADD) {
                newIndexFiles.add(entry.indexFile());
            } else {
                deletedIndexFiles.add(entry.indexFile());
            }
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
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
                                            newIndexFiles,
                                            deletedIndexFiles),
                                    CompactIncrement.emptyIncrement())));
        }
    }

    private static FileStoreTable withPushDown(FileStoreTable table, boolean enabled) {
        return table.copy(
                Collections.singletonMap(
                        DATA_EVOLUTION_MERGED_READ_STATS_PUSHDOWN_ENABLED.key(),
                        String.valueOf(enabled)));
    }

    private static PredicateBuilder builder(FileStoreTable table) {
        return new PredicateBuilder(table.rowType());
    }

    private Result read(FileStoreTable table, Predicate filter, List<Range> rowRanges)
            throws Exception {
        ReadBuilder builder = table.newReadBuilder().withFilter(filter);
        if (rowRanges != null) {
            builder.withRowRanges(rowRanges);
        }
        Result result = new Result();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan().splits())) {
            reader.forEachRemaining(
                    row -> {
                        result.returned++;
                        if (filter.test(row)) {
                            result.matched.add(format(row));
                        }
                    });
        }
        result.matched.sort(null);
        return result;
    }

    private static String format(InternalRow row) {
        return String.format(
                "%010d,%d,%d,%s", row.getLong(0), row.getInt(1), row.getInt(2), row.getString(3));
    }

    private static class Result {
        private final List<String> matched = new ArrayList<>();
        private long returned;
    }
}
