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
import org.apache.paimon.Snapshot;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.dataevolution.AlignedCompactRewriter;
import org.apache.paimon.mergetree.dataevolution.ColumnFileGroups;
import org.apache.paimon.mergetree.dataevolution.ColumnFileWriter;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.system.ReadOptimizedTable;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for primary-key tables with data evolution, backfilling columns by alignment range. */
public class PrimaryKeyDataEvolutionTableTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return schema(dataEvolutionOptions());
    }

    private static Map<String, String> dataEvolutionOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return options;
    }

    private static Schema schema(Map<String, String> options) {
        return Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .column("c", DataTypes.INT())
                .primaryKey("k")
                .options(options)
                .build();
    }

    private static GenericRow row(int k, int a, String b, int c) {
        return GenericRow.of(k, a, BinaryString.fromString(b), c);
    }

    private void writeRange(FileStoreTable table, int from, int to) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int k = from; k <= to; k++) {
            rows.add(row(k, k * 10, "b" + k, k * 100));
        }
        write(table, rows.toArray(new InternalRow[0]));
    }

    private static List<ManifestEntry> files(FileStoreTable table) {
        return table.store().newScan().plan().files();
    }

    private static List<DataFileMeta> baseFiles(FileStoreTable table) {
        return files(table).stream()
                .map(ManifestEntry::file)
                .filter(file -> file.writeCols() == null)
                .sorted(Comparator.comparing(DataFileMeta::firstRowId))
                .collect(Collectors.toList());
    }

    private static List<DataFileMeta> columnFiles(FileStoreTable table) {
        return files(table).stream()
                .map(ManifestEntry::file)
                .filter(file -> file.writeCols() != null)
                .collect(Collectors.toList());
    }

    private static Iterator<InternalRow> rows(DataFileMeta base, IntFunction<InternalRow> row) {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < base.rowCount(); i++) {
            rows.add(row.apply(i));
        }
        return rows.iterator();
    }

    private CommitMessage backfill(
            FileStoreTable table,
            DataFileMeta base,
            List<String> columns,
            IntFunction<InternalRow> row,
            List<DataFileMeta> replaced)
            throws Exception {
        return backfill(table, BinaryRow.EMPTY_ROW, 0, base, columns, row, replaced);
    }

    private CommitMessage backfill(
            FileStoreTable table,
            BinaryRow partition,
            int bucket,
            DataFileMeta base,
            List<String> columns,
            IntFunction<InternalRow> row,
            List<DataFileMeta> replaced)
            throws Exception {
        return ColumnFileWriter.write(
                table, partition, bucket, base, columns, rows(base, row), replaced);
    }

    private void commit(FileStoreTable table, List<CommitMessage> messages) throws Exception {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }

    private List<String> readSorted(FileStoreTable table) throws Exception {
        return read(table).stream()
                .sorted(Comparator.comparingInt(r -> r.getInt(0)))
                .map(
                        r ->
                                String.format(
                                        "%s,%s,%s,%s",
                                        r.getInt(0),
                                        r.isNullAt(1) ? null : r.getInt(1),
                                        r.isNullAt(2) ? null : r.getString(2),
                                        r.isNullAt(3) ? null : r.getInt(3)))
                .collect(Collectors.toList());
    }

    @Test
    public void testValidation() throws Exception {
        createTableDefault();

        Map<String, String> input = dataEvolutionOptions();
        input.put(CoreOptions.CHANGELOG_PRODUCER.key(), "input");
        catalog.createTable(identifier("input"), schema(input), false);

        assertRejected(
                CoreOptions.ROW_TRACKING_ENABLED.key(),
                "true",
                "Cannot define primary-key for row tracking table");
        assertRejected(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        assertRejected(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup");
        assertRejected(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction");
        assertRejected(CoreOptions.FORCE_LOOKUP.key(), "true");
        assertRejected(CoreOptions.MERGE_ENGINE.key(), "first-row");
        assertRejected(CoreOptions.BUCKET.key(), "-2");
        assertRejected(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "hadoop-catalog");

        Map<String, String> partialUpdate = dataEvolutionOptions();
        partialUpdate.put(CoreOptions.MERGE_ENGINE.key(), "partial-update");
        catalog.createTable(identifier("partial_update"), schema(partialUpdate), false);
        Map<String, String> aggregate = dataEvolutionOptions();
        aggregate.put(CoreOptions.MERGE_ENGINE.key(), "aggregation");
        catalog.createTable(identifier("aggregate"), schema(aggregate), false);
        Map<String, String> dynamicBucket = dataEvolutionOptions();
        dynamicBucket.put(CoreOptions.BUCKET.key(), "-1");
        catalog.createTable(identifier("dynamic_bucket"), schema(dynamicBucket), false);

        FileStoreTable table = getTableDefault();
        assertThat(table.rowType().getFieldNames()).containsExactly("k", "a", "b", "c");
    }

    private void assertRejected(String key, String value) {
        assertRejected(key, value, "Data evolution on a primary key table");
    }

    private void assertRejected(String key, String value, String message) {
        Map<String, String> options = dataEvolutionOptions();
        options.put(key, value);
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        identifier("t_" + key.replace('.', '_') + "_" + value),
                                        schema(options),
                                        false))
                .as("%s=%s", key, value)
                .hasStackTraceContaining(message);
    }

    @Test
    public void testAlignmentRangesAssignedAtCommit() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 5);
        writeRange(table, 6, 8);

        List<DataFileMeta> bases = baseFiles(table);
        assertThat(bases).hasSize(2);
        assertThat(bases.get(0).firstRowId()).isEqualTo(0L);
        assertThat(bases.get(0).rowCount()).isEqualTo(5L);
        assertThat(bases.get(1).firstRowId()).isEqualTo(5L);
        assertThat(bases.get(1).rowCount()).isEqualTo(3L);
        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(8L);
        assertThat(readSorted(table)).hasSize(8);
    }

    @Test
    public void testBackfillColumn() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 3);
        DataFileMeta base = baseFiles(table).get(0);
        long recordCount = table.snapshotManager().latestSnapshot().totalRecordCount();

        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                base,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("new" + (i + 1))),
                                Collections.emptyList())));

        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(snapshot.totalRecordCount()).isEqualTo(recordCount);
        assertThat(snapshot.deltaRecordCount()).isEqualTo(0L);
        assertThat(table.newSnapshotReader().partitionEntries().get(0).recordCount()).isEqualTo(3L);
        assertThat(baseFiles(table)).containsExactly(base);
        List<DataFileMeta> columnFiles = columnFiles(table);
        assertThat(columnFiles).hasSize(1);
        assertThat(columnFiles.get(0).level()).isEqualTo(0);
        assertThat(columnFiles.get(0).firstRowId()).isEqualTo(base.firstRowId());
        assertThat(columnFiles.get(0).writeCols()).containsExactly("b");

        assertThat(readSorted(table))
                .containsExactly("1,10,new1,100", "2,20,new2,200", "3,30,new3,300");

        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        assertThat(splits).hasSize(1);
        DataSplit split = (DataSplit) splits.get(0);
        assertThat(split.dataFiles()).hasSize(2);
        assertThat(split.rawConvertible()).isFalse();
        assertThat(split.mergedRowCount()).isEmpty();
    }

    @Test
    public void testPartialUpdateMergesStitchedRow() throws Exception {
        Map<String, String> options = dataEvolutionOptions();
        options.put(CoreOptions.MERGE_ENGINE.key(), "partial-update");
        catalog.createTable(identifier("partial"), schema(options), false);
        FileStoreTable table = getTable(identifier("partial"));
        write(table, row(1, 10, "b1", 100));
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                baseFiles(table).get(0),
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("new")),
                                Collections.emptyList())));
        assertThat(readSorted(table)).containsExactly("1,10,new,100");

        // null fields keep the stitched value; a later non-null value replaces it
        write(table, GenericRow.of(1, 50, null, null));
        assertThat(readSorted(table)).containsExactly("1,50,new,100");
        write(table, GenericRow.of(1, null, BinaryString.fromString("win"), null));
        assertThat(readSorted(table)).containsExactly("1,50,win,100");

        compact(table, BinaryRow.EMPTY_ROW, 0);
        assertThat(columnFiles(table)).isEmpty();
        assertThat(readSorted(table)).containsExactly("1,50,win,100");
    }

    @Test
    public void testAggregateUsesStitchedValue() throws Exception {
        Map<String, String> options = dataEvolutionOptions();
        options.put(CoreOptions.MERGE_ENGINE.key(), "aggregation");
        options.put("fields.a.aggregate-function", "max");
        catalog.createTable(identifier("aggregate"), schema(options), false);
        FileStoreTable table = getTable(identifier("aggregate"));
        write(table, row(1, 10, "b1", 100));
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                baseFiles(table).get(0),
                                Collections.singletonList("a"),
                                i -> GenericRow.of(7),
                                Collections.emptyList())));
        write(table, row(1, 4, "b2", 200));

        // max(stitched 7, new 4) = 7. Without the stitch the max would be max(10, 4).
        assertThat(readSorted(table)).containsExactly("1,7,b2,200");
    }

    @Test
    public void testDynamicBucketBackfill() throws Exception {
        Map<String, String> options = dataEvolutionOptions();
        options.put(CoreOptions.BUCKET.key(), "-1");
        catalog.createTable(identifier("dynamic"), schema(options), false);
        FileStoreTable table = getTable(identifier("dynamic"));
        // dynamic bucket does not hash the row; the caller supplies the bucket, as the sink does
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit batchCommit = writeBuilder.newCommit()) {
            write.write(row(1, 10, "b1", 100), 0);
            write.write(row(2, 20, "b2", 200), 0);
            write.write(row(3, 30, "b3", 300), 1);
            write.write(row(4, 40, "b4", 400), 1);
            batchCommit.commit(write.prepareCommit());
        }
        List<ManifestEntry> bases =
                files(table).stream()
                        .filter(entry -> entry.file().writeCols() == null)
                        .collect(Collectors.toList());
        assertThat(bases).isNotEmpty();
        for (ManifestEntry entry : bases) {
            commit(
                    table,
                    Collections.singletonList(
                            backfill(
                                    table,
                                    entry.partition(),
                                    entry.bucket(),
                                    entry.file(),
                                    Collections.singletonList("b"),
                                    i -> GenericRow.of(BinaryString.fromString("x")),
                                    Collections.emptyList())));
        }

        assertThat(columnFiles(table)).isNotEmpty();
        assertThat(readSorted(table))
                .containsExactly("1,10,x,100", "2,20,x,200", "3,30,x,300", "4,40,x,400");
    }

    @Test
    public void testReplaceAndMultipleColumnFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 3);
        DataFileMeta base = baseFiles(table).get(0);

        commit(
                table,
                Arrays.asList(
                        backfill(
                                table,
                                base,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("x" + (i + 1))),
                                Collections.emptyList()),
                        backfill(
                                table,
                                base,
                                Collections.singletonList("c"),
                                i -> GenericRow.of(-(i + 1)),
                                Collections.emptyList())));
        assertThat(readSorted(table)).containsExactly("1,10,x1,-1", "2,20,x2,-2", "3,30,x3,-3");

        DataFileMeta oldB =
                columnFiles(table).stream()
                        .filter(f -> f.writeCols().contains("b"))
                        .findFirst()
                        .get();
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                base,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("y" + (i + 1))),
                                Collections.singletonList(oldB))));

        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount()).isEqualTo(3L);
        assertThat(columnFiles(table)).hasSize(2).doesNotContain(oldB);
        assertThat(readSorted(table)).containsExactly("1,10,y1,-1", "2,20,y2,-2", "3,30,y3,-3");
    }

    @Test
    public void testNewerVersionWinsOverBackfilledRow() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 3);
        write(table, row(2, 2000, "update", 2000));
        DataFileMeta oldBase = baseFiles(table).get(0);

        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                oldBase,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("new" + (i + 1))),
                                Collections.emptyList())));

        assertThat(readSorted(table))
                .containsExactly("1,10,new1,100", "2,2000,update,2000", "3,30,new3,300");
    }

    @Test
    public void testCompactionStitchesColumnFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 3);
        writeRange(table, 4, 5);
        DataFileMeta base = baseFiles(table).get(0);
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                base,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("new" + (i + 1))),
                                Collections.emptyList())));
        List<String> expected =
                Arrays.asList(
                        "1,10,new1,100",
                        "2,20,new2,200",
                        "3,30,new3,300",
                        "4,40,b4,400",
                        "5,50,b5,500");
        assertThat(readSorted(table)).isEqualTo(expected);

        compact(table, BinaryRow.EMPTY_ROW, 0);

        assertThat(columnFiles(table)).isEmpty();
        List<DataFileMeta> bases = baseFiles(table);
        assertThat(bases).hasSize(1);
        assertThat(bases.get(0).firstRowId()).isGreaterThanOrEqualTo(5L);
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount()).isEqualTo(5L);
        assertThat(readSorted(table)).isEqualTo(expected);
    }

    @Test
    public void testDiscardedCompactionKeepsColumnFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 3);
        DataFileMeta base = baseFiles(table).get(0);
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                base,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("new" + (i + 1))),
                                Collections.emptyList())));
        List<DataFileMeta> files =
                files(table).stream().map(ManifestEntry::file).collect(Collectors.toList());
        DataFileMeta column = columnFiles(table).get(0);

        ColumnFileGroups groups = ColumnFileGroups.group(files);
        CompactRewriter rewriter =
                new AlignedCompactRewriter(
                        new CompactRewriter() {
                            @Override
                            public CompactResult rewrite(
                                    int outputLevel,
                                    boolean dropDelete,
                                    List<List<SortedRun>> sections) {
                                return new CompactResult(
                                        Collections.singletonList(base), Collections.emptyList());
                            }

                            @Override
                            public CompactResult upgrade(int outputLevel, DataFileMeta file) {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public void close() {}
                        },
                        groups);

        // the first result is discarded, the next compaction still has to remove the column file
        rewriter.rewrite(5, true, Collections.emptyList());
        assertThat(rewriter.rewrite(5, true, Collections.emptyList()).before())
                .containsExactly(base, column);
        assertThat(rewriter.upgrade(5, base).before()).containsExactly(base, column);

        groups.dropBaseFile(base.fileName());
        assertThat(groups.columnFilesOf(base)).isEmpty();
    }

    @Test
    public void testReadOptimizedTableIsRejected() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 3);

        assertThatThrownBy(() -> new ReadOptimizedTable(table).newScan().plan())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("data evolution");
    }

    @Test
    public void testAddedColumnStaysNullAfterCompaction() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 2);
        DataFileMeta base = baseFiles(table).get(0);
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                base,
                                Collections.singletonList("c"),
                                i -> GenericRow.of(-(i + 1)),
                                Collections.emptyList())));
        catalog.alterTable(identifier(), SchemaChange.addColumn("d", DataTypes.INT()), false);
        table = getTableDefault();

        List<InternalRow> rows = read(table);
        assertThat(rows).hasSize(2);
        for (InternalRow r : rows) {
            assertThat(r.getInt(3)).isEqualTo(-r.getInt(0));
            assertThat(r.isNullAt(4)).isTrue();
        }

        compact(table, BinaryRow.EMPTY_ROW, 0);
        rows = read(table);
        assertThat(rows).hasSize(2);
        for (InternalRow r : rows) {
            assertThat(r.getInt(1)).isEqualTo(r.getInt(0) * 10);
            assertThat(r.getInt(3)).isEqualTo(-r.getInt(0));
            assertThat(r.isNullAt(4)).isTrue();
        }
    }

    @Test
    public void testFilterPrunesWholeGroups() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 5);
        writeRange(table, 6, 10);
        DataFileMeta first = baseFiles(table).get(0);
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                first,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("x")),
                                Collections.emptyList())));

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        KeyValueFileStoreScan keyScan = (KeyValueFileStoreScan) table.store().newScan();
        keyScan.withKeyFilter(builder.lessOrEqual(0, 3)).dropStats();
        List<ManifestEntry> files = keyScan.plan().files();
        assertThat(files).hasSize(2);
        assertThat(files).allMatch(e -> e.file().firstRowId().equals(first.firstRowId()));
        assertThat(files).allMatch(e -> e.file().valueStats().minValues().getFieldCount() == 0);

        keyScan = (KeyValueFileStoreScan) table.store().newScan();
        keyScan.withKeyFilter(builder.greaterThan(0, 7)).dropStats();
        files = keyScan.plan().files();
        assertThat(files).hasSize(1);
        assertThat(files.get(0).file().writeCols()).isNull();
        assertThat(files.get(0).file().firstRowId()).isNotEqualTo(first.firstRowId());

        // the stale stats of the base file do not hold 'x', the group must still be kept
        KeyValueFileStoreScan valueScan = (KeyValueFileStoreScan) table.store().newScan();
        valueScan
                .withValueFilter(builder.equal(2, BinaryString.fromString("x")))
                .enableValueFilter()
                .dropStats();
        files = valueScan.plan().files();
        assertThat(files).hasSize(2);
        assertThat(files).allMatch(e -> e.file().firstRowId().equals(first.firstRowId()));

        ReadBuilder readBuilder =
                table.newReadBuilder().withFilter(builder.equal(2, BinaryString.fromString("x")));
        List<Split> splits = readBuilder.newScan().plan().splits();
        List<InternalRow> filtered = new ArrayList<>();
        readBuilder
                .newRead()
                .executeFilter()
                .createReader(splits)
                .forEachRemaining(r -> filtered.add(GenericRow.of(r.getInt(0))));
        assertThat(filtered).hasSize(5);
    }

    @Test
    public void testLimitPushdownSkipsBucketsWithColumnFiles() throws Exception {
        Map<String, String> options = dataEvolutionOptions();
        options.put(CoreOptions.BUCKET.key(), "2");
        catalog.createTable(identifier(), schema(options), false);
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 20);
        compact(table, BinaryRow.EMPTY_ROW, 0);
        compact(table, BinaryRow.EMPTY_ROW, 1);

        KeyValueFileStoreScan scan = (KeyValueFileStoreScan) table.store().newScan();
        scan.withLimit(1);
        assertThat(scan.limitPushdownEnabled()).isTrue();
        assertThat(scan.plan().files()).hasSize(1);

        DataFileMeta base =
                files(table).stream()
                        .filter(e -> e.bucket() == 0)
                        .map(ManifestEntry::file)
                        .findFirst()
                        .get();
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                base,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("x")),
                                Collections.emptyList())));

        scan = (KeyValueFileStoreScan) table.store().newScan();
        scan.withLimit(1);
        List<ManifestEntry> files = scan.plan().files();
        // bucket 0 is never cut by the limit, whichever bucket is visited first
        assertThat(files.stream().filter(e -> e.bucket() == 0).count()).isEqualTo(2L);
        assertThat(files.stream().filter(e -> e.bucket() == 1).count()).isLessThanOrEqualTo(1L);

        // the split plan keeps the backfilled bucket too: its merged row count is absent, and the
        // limit would otherwise drop that split once the other bucket fills it
        List<Split> limited = table.newReadBuilder().withLimit(1).newScan().plan().splits();
        assertThat(limited.stream().anyMatch(PrimaryKeyDataEvolutionTableTest::hasColumnFile))
                .isTrue();
        List<String> columnValues = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder().newRead().createReader(limited)) {
            reader.forEachRemaining(row -> columnValues.add(row.getString(2).toString()));
        }
        assertThat(columnValues.stream().filter("x"::equals).count()).isEqualTo(base.rowCount());
    }

    private static boolean hasColumnFile(Split split) {
        return ((DataSplit) split).dataFiles().stream().anyMatch(file -> file.writeCols() != null);
    }

    @Test
    public void testSplitRowCountIgnoresColumnFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 5);
        DataFileMeta base = baseFiles(table).get(0);
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                base,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("x")),
                                Collections.emptyList())));

        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        assertThat(splits.stream().anyMatch(s -> ((DataSplit) s).dataFiles().size() > 1)).isTrue();
        // engines weight splits by this count, the column file repeats the rows of its base file
        assertThat(splits.stream().mapToLong(Split::rowCount).sum()).isEqualTo(5L);
        assertThat(read(table)).hasSize(5);
    }

    @Test
    public void testRejectInvalidColumnFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 3);
        writeRange(table, 4, 6);
        DataFileMeta base = baseFiles(table).get(0);

        assertThatThrownBy(
                        () ->
                                backfill(
                                        table,
                                        base,
                                        Collections.singletonList("k"),
                                        i -> GenericRow.of(i),
                                        Collections.emptyList()))
                .hasMessageContaining("primary key or partition column");

        assertThatThrownBy(
                        () ->
                                ColumnFileWriter.write(
                                        table,
                                        BinaryRow.EMPTY_ROW,
                                        0,
                                        base,
                                        Collections.singletonList("b"),
                                        Collections.<InternalRow>singletonList(
                                                        GenericRow.of(BinaryString.fromString("x")))
                                                .iterator(),
                                        Collections.emptyList()))
                .hasMessageContaining("Column file must have 3 rows");

        // the same column written twice for one range in one commit
        assertThatThrownBy(
                        () ->
                                commit(
                                        table,
                                        Arrays.asList(
                                                backfill(
                                                        table,
                                                        base,
                                                        Collections.singletonList("b"),
                                                        i ->
                                                                GenericRow.of(
                                                                        BinaryString.fromString(
                                                                                "x")),
                                                        Collections.emptyList()),
                                                backfill(
                                                        table,
                                                        base,
                                                        Collections.singletonList("b"),
                                                        i ->
                                                                GenericRow.of(
                                                                        BinaryString.fromString(
                                                                                "y")),
                                                        Collections.emptyList()))))
                .hasStackTraceContaining("is written by both");

        // a column file aligned to no base file
        CommitMessageImpl valid =
                (CommitMessageImpl)
                        backfill(
                                table,
                                base,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("x")),
                                Collections.emptyList());
        DataFileMeta shifted =
                valid.compactIncrement().compactAfter().get(0).newFirstRowId(base.firstRowId() + 1);
        assertThatThrownBy(
                        () ->
                                commit(
                                        table,
                                        Collections.singletonList(
                                                compactMessage(
                                                        Collections.singletonList(shifted)))))
                .hasStackTraceContaining("Data evolution alignment conflicts");

        // column files must not be committed as appended files
        assertThatThrownBy(
                        () ->
                                commit(
                                        table,
                                        Collections.singletonList(
                                                new CommitMessageImpl(
                                                        BinaryRow.EMPTY_ROW,
                                                        0,
                                                        1,
                                                        new DataIncrement(
                                                                valid.compactIncrement()
                                                                        .compactAfter(),
                                                                Collections.emptyList(),
                                                                Collections.emptyList()),
                                                        CompactIncrement.emptyIncrement()))))
                .hasStackTraceContaining("must be committed as a compaction");

        assertThat(columnFiles(table)).isEmpty();
        assertThat(readSorted(table)).hasSize(6);
    }

    private static CommitMessage compactMessage(List<DataFileMeta> after) {
        return new CommitMessageImpl(
                BinaryRow.EMPTY_ROW,
                0,
                1,
                DataIncrement.emptyIncrement(),
                new CompactIncrement(Collections.emptyList(), after, Collections.emptyList()));
    }

    @Test
    public void testLegacyFilesRequireMigration() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        catalog.createTable(identifier(), schema(options), false);
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 3);
        assertThat(files(table).get(0).file().firstRowId()).isNull();

        catalog.alterTable(
                identifier(),
                SchemaChange.setOption(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true"),
                false);
        table = getTableDefault();
        writeRange(table, 4, 6);
        assertThat(readSorted(table)).hasSize(6);

        FileStoreTable finalTable = table;
        DataFileMeta aligned =
                files(table).stream()
                        .map(ManifestEntry::file)
                        .filter(f -> f.firstRowId() != null)
                        .findFirst()
                        .get();
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                finalTable,
                                aligned,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("new" + (i + 4))),
                                Collections.emptyList())));
        assertThatThrownBy(() -> readSorted(finalTable))
                .hasStackTraceContaining("Run a full compaction");

        compact(table, BinaryRow.EMPTY_ROW, 0);
        assertThat(files(table)).allMatch(e -> e.file().firstRowId() != null);
        assertThat(readSorted(table))
                .containsExactly(
                        "1,10,b1,100",
                        "2,20,b2,200",
                        "3,30,b3,300",
                        "4,40,new4,400",
                        "5,50,new5,500",
                        "6,60,new6,600");
    }

    @Test
    public void testStreamingReadSkipsBackfill() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRange(table, 1, 3);

        ReadBuilder readBuilder = table.newReadBuilder();
        StreamTableScan scan = readBuilder.newStreamScan();
        assertThat(scan.plan().splits()).isNotEmpty();

        DataFileMeta base = baseFiles(table).get(0);
        commit(
                table,
                Collections.singletonList(
                        backfill(
                                table,
                                base,
                                Collections.singletonList("b"),
                                i -> GenericRow.of(BinaryString.fromString("new")),
                                Collections.emptyList())));
        assertThat(scan.plan().splits()).isEmpty();

        write(table, row(9, 90, "b9", 900));
        List<Split> splits = scan.plan().splits();
        List<Integer> keys = new ArrayList<>();
        readBuilder.newRead().createReader(splits).forEachRemaining(r -> keys.add(r.getInt(0)));
        assertThat(keys).containsExactly(9);
    }
}
