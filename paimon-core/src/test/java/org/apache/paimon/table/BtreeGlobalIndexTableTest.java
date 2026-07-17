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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.globalindex.GlobalIndexCoverage;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexScanner;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.RowRangeIndex;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Test for BTree indexed batch scan. */
public class BtreeGlobalIndexTableTest extends DataEvolutionTestBase {

    @Test
    public void testBTreeGlobalIndex() throws Exception {
        write(100000L);
        createIndex("f1");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());

        Predicate predicate =
                new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("a100"));

        RoaringNavigableMap64 rowIds = globalIndexScan(table, predicate);
        assertNotNull(rowIds);
        assertThat(rowIds.getLongCardinality()).isEqualTo(1);
        assertThat(rowIds.toRangeList()).containsExactly(new Range(100L, 100L));

        Predicate predicate2 =
                new PredicateBuilder(table.rowType())
                        .in(
                                1,
                                Arrays.asList(
                                        BinaryString.fromString("a200"),
                                        BinaryString.fromString("a300"),
                                        BinaryString.fromString("a400")));

        rowIds = globalIndexScan(table, predicate2);
        assertNotNull(rowIds);
        assertThat(rowIds.getLongCardinality()).isEqualTo(3);
        assertThat(rowIds.toRangeList())
                .containsExactlyInAnyOrder(
                        new Range(200L, 200L), new Range(300L, 300L), new Range(400L, 400L));

        DataEvolutionBatchScan scan = (DataEvolutionBatchScan) table.newScan();
        RoaringNavigableMap64 finalRowIds = rowIds;
        scan.withGlobalIndexResult(GlobalIndexResult.create(finalRowIds));

        List<String> readF1 = new ArrayList<>();
        table.newRead()
                .createReader(scan.plan())
                .forEachRemaining(
                        row -> {
                            readF1.add(row.getString(1).toString());
                        });

        assertThat(readF1).containsExactly("a200", "a300", "a400");
    }

    @Test
    public void testBTreeGlobalIndexWithCoreScan() throws Exception {
        write(100000L);
        createIndex("f1");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());

        Predicate predicate =
                new PredicateBuilder(table.rowType())
                        .in(
                                1,
                                Arrays.asList(
                                        BinaryString.fromString("a200"),
                                        BinaryString.fromString("a300"),
                                        BinaryString.fromString("a400"),
                                        BinaryString.fromString("a56789")));

        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);

        List<String> readF1 = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row -> {
                            readF1.add(row.getString(1).toString());
                        });

        assertThat(readF1).containsExactly("a200", "a300", "a400", "a56789");
    }

    @Test
    public void testBTreeGlobalIndexPredicateTopNPushdown() throws Exception {
        write(1000L);
        createIndex("f0");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        RoaringNavigableMap64 candidateRows = new RoaringNavigableMap64();
        candidateRows.add(100L);
        candidateRows.add(900L);
        TopN topN = new TopN(new FieldRef(0, "f0", DataTypes.INT()), DESCENDING, NULLS_LAST, 1);

        ReadBuilder readBuilder =
                table.newReadBuilder().withTopN(topN).withTopNRowIdFilter(candidateRows);
        TableScan.Plan plan = readBuilder.newScan().plan();

        assertThat(plan.splits()).allMatch(split -> split instanceof IndexedSplit);
        assertThat(indexedRanges(plan)).containsExactly(new Range(900L, 900L));
        assertThat(readF1(readBuilder, plan)).containsExactly("a900");
    }

    @Test
    public void testBTreeTopNFallsBackForUnindexedTail() throws Exception {
        write(500L);
        createIndex("f0");
        appendRows(500, 1000);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        RoaringNavigableMap64 candidateRows = new RoaringNavigableMap64();
        candidateRows.add(100L);
        candidateRows.add(900L);
        TopN topN = new TopN(new FieldRef(0, "f0", DataTypes.INT()), DESCENDING, NULLS_LAST, 1);
        ReadBuilder readBuilder =
                table.newReadBuilder().withTopN(topN).withTopNRowIdFilter(candidateRows);
        TableScan.Plan plan = readBuilder.newScan().plan();

        assertThat(indexedRanges(plan))
                .containsExactlyInAnyOrder(new Range(100L, 100L), new Range(900L, 900L));
        assertThat(readF1(readBuilder, plan)).containsExactlyInAnyOrder("a100", "a900");
    }

    @Test
    public void testCandidateTopNDoesNotApplyWholeTableSplitPruning() throws Exception {
        write(500L);
        appendRows(500, 1000);
        createIndex("f0");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        RoaringNavigableMap64 candidateRows = new RoaringNavigableMap64();
        candidateRows.add(100L);
        TopN topN = new TopN(new FieldRef(0, "f0", DataTypes.INT()), DESCENDING, NULLS_LAST, 1);
        ReadBuilder readBuilder =
                table.newReadBuilder().withTopN(topN).withTopNRowIdFilter(candidateRows);
        TableScan.Plan plan = readBuilder.newScan().plan();

        assertThat(indexedRanges(plan)).containsExactly(new Range(100L, 100L));
        assertThat(readF1(readBuilder, plan)).containsExactly("a100");
    }

    @Test
    public void testBTreeTopNFallsBackForResidualPredicate() throws Exception {
        write(3L);
        createIndex("f0");
        createIndex("f1");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        Predicate predicate =
                new PredicateBuilder(table.rowType()).endsWith(1, BinaryString.fromString("0"));
        TopN topN = new TopN(new FieldRef(0, "f0", DataTypes.INT()), DESCENDING, NULLS_LAST, 1);
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate).withTopN(topN);
        TableScan.Plan plan = readBuilder.newScan().plan();

        assertThat(plan.splits()).allMatch(split -> split instanceof DataSplit);
        assertThat(readF1(readBuilder, plan)).contains("a0");
    }

    @Test
    public void testBTreeTopNWithZeroLimitReturnsEmptyPlan() throws Exception {
        write(3L);
        createIndex("f0");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        TopN topN = new TopN(new FieldRef(0, "f0", DataTypes.INT()), DESCENDING, NULLS_LAST, 0);
        ReadBuilder readBuilder = table.newReadBuilder().withTopN(topN);
        TableScan.Plan plan = readBuilder.newScan().plan();

        assertThat(plan.splits()).isEmpty();
        assertThat(readF1(readBuilder, plan)).isEmpty();
    }

    @Test
    public void testBTreeTopNWithEmptyCandidatesReturnsEmptyPlan() throws Exception {
        write(3L);
        createIndex("f0");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        TopN topN = new TopN(new FieldRef(0, "f0", DataTypes.INT()), DESCENDING, NULLS_LAST, 1);
        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withTopN(topN)
                        .withTopNRowIdFilter(new RoaringNavigableMap64());
        TableScan.Plan plan = readBuilder.newScan().plan();

        assertThat(plan.splits()).isEmpty();
        assertThat(readF1(readBuilder, plan)).isEmpty();
    }

    @Test
    public void testBTreeTopNSkipsScalarPushdownForLargeLimit() throws Exception {
        write(200L);
        createIndex("f0");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        TopN topN = new TopN(new FieldRef(0, "f0", DataTypes.INT()), DESCENDING, NULLS_LAST, 101);
        TableScan.Plan plan = table.newReadBuilder().withTopN(topN).newScan().plan();

        assertThat(plan.splits()).isNotEmpty();
        assertThat(plan.splits()).allMatch(split -> split instanceof DataSplit);
    }

    @Test
    public void testTopNRowIdFilterRequiresTopN() throws Exception {
        write(3L);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        RoaringNavigableMap64 candidateRows = new RoaringNavigableMap64();
        candidateRows.add(1L);
        ReadBuilder readBuilder = table.newReadBuilder().withTopNRowIdFilter(candidateRows);

        assertThatThrownBy(readBuilder::newScan)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires a TopN definition");
    }

    @Test
    public void testWrapToIndexSplitsPreservesQueryAuthSplit() throws Exception {
        write(3L);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        List<Split> authSplits =
                table.newScan().plan().splits().stream()
                        .map(split -> new QueryAuthSplit(split, null))
                        .collect(Collectors.toList());
        TableScan.Plan plan =
                DataEvolutionBatchScan.wrapToIndexSplits(
                        authSplits,
                        RowRangeIndex.create(Collections.singletonList(new Range(1L, 1L))),
                        null);

        assertThat(plan.splits()).hasSize(1);
        assertThat(plan.splits().get(0)).isInstanceOf(QueryAuthSplit.class);
        assertThat(((QueryAuthSplit) plan.splits().get(0)).split())
                .isInstanceOf(IndexedSplit.class);
    }

    @Test
    public void testMixedRowIdOrSkipsGlobalIndexScan() throws Exception {
        write(10L);
        createIndex("f1");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        PredicateBuilder builder =
                new PredicateBuilder(SpecialFields.rowTypeWithRowId(table.rowType()));
        int rowIdIndex = table.rowType().getFieldCount();
        Predicate predicate =
                PredicateBuilder.or(
                        builder.equal(rowIdIndex, 1L),
                        builder.equal(1, BinaryString.fromString("a7")));

        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        List<Split> splits = readBuilder.newScan().plan().splits();

        assertThat(splits).isNotEmpty();
        assertThat(splits).allMatch(split -> split instanceof DataSplit);
    }

    @Test
    public void testBTreeGlobalIndexSearchModeControlsUnindexedData() throws Exception {
        write(500L);
        createIndex("f1");
        appendRows(500, 1000);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        Predicate predicate =
                new PredicateBuilder(table.rowType())
                        .in(
                                1,
                                Arrays.asList(
                                        BinaryString.fromString("a100"),
                                        BinaryString.fromString("a700")));

        assertThat(readF1(table, predicate)).containsExactly("a100");

        assertThat(readF1(tableWithSearchMode(table, "full"), predicate))
                .containsExactly("a100", "a700");
        assertThat(readF1(tableWithSearchMode(table, "detail"), predicate))
                .containsExactly("a100", "a700");

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate andWithUnindexedField =
                PredicateBuilder.and(
                        builder.equal(1, BinaryString.fromString("a700")),
                        builder.equal(2, BinaryString.fromString("b700")));

        assertThat(readF1(table, andWithUnindexedField)).isEmpty();
        assertThat(readF1(tableWithSearchMode(table, "full"), andWithUnindexedField))
                .containsExactly("a700");
        assertThat(readF1(tableWithSearchMode(table, "detail"), andWithUnindexedField))
                .containsExactly("a700");
    }

    @Test
    public void testGlobalIndexScannerKeepsUnindexedRowsSeparate() throws Exception {
        write(500L);
        createIndex("f1");
        appendRows(500, 1000);

        FileStoreTable table =
                tableWithSearchMode((FileStoreTable) catalog.getTable(identifier()), "full");
        Predicate predicate =
                new PredicateBuilder(table.rowType())
                        .in(
                                1,
                                Arrays.asList(
                                        BinaryString.fromString("a100"),
                                        BinaryString.fromString("a700")));

        try (GlobalIndexScanner scanner =
                GlobalIndexScanner.create(table, PartitionPredicate.ALWAYS_TRUE, predicate).get()) {
            assertThat(scanner.scan(predicate).get().results().toRangeList())
                    .containsExactly(new Range(100L, 100L));
            assertThat(scanner.unindexedRows(predicate).results().toRangeList())
                    .containsExactly(new Range(500L, 999L));
        }
    }

    @Test
    public void testSourceBackedIndexIsExcludedFromGlobalRowIdScan() throws Exception {
        write(10L);
        FileStoreTable table =
                tableWithSearchMode((FileStoreTable) catalog.getTable(identifier()), "full");
        IndexFileMeta sourceBacked =
                new IndexFileMeta(
                        "btree",
                        "source-backed-index",
                        0,
                        10,
                        new GlobalIndexMeta(0, 9, 1, null, null, new byte[] {1}),
                        null);

        assertThat(GlobalIndexScanner.create(table, Collections.singletonList(sourceBacked)))
                .isEmpty();

        GlobalIndexCoverage coverage =
                new GlobalIndexCoverage(
                        table,
                        table.snapshotManager().latestSnapshot(),
                        PartitionPredicate.ALWAYS_TRUE,
                        Collections.singletonList(sourceBacked));
        assertThat(coverage.unindexedRanges(1)).containsExactly(new Range(0, 9));
    }

    @Test
    public void testOrdinaryAndSourceBackedBTreeIndexesCanCoexist() throws Exception {
        write(10L);
        createIndex("f1");
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        List<IndexFileMeta> mixedIndexes =
                table.store().newIndexFileHandler().scan(snapshot, "btree").stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(Collectors.toCollection(ArrayList::new));
        mixedIndexes.add(
                new IndexFileMeta(
                        "btree",
                        "source-backed-index",
                        0,
                        10,
                        new GlobalIndexMeta(0, 9, 1, null, null, new byte[] {1}),
                        null));
        Predicate predicate =
                new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("a7"));

        try (GlobalIndexScanner scanner =
                GlobalIndexScanner.create(table, mixedIndexes).orElseThrow(AssertionError::new)) {
            assertThat(scanner.scan(predicate).get().results().toRangeList())
                    .containsExactly(new Range(7, 7));
        }
    }

    @Test
    public void testBTreeGlobalIndexSearchModeUsesAllPredicateFieldCoverage() throws Exception {
        write(500L);
        createIndex("f1");
        appendRows(500, 1000);
        createIndex("f2");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate andPredicate =
                PredicateBuilder.and(
                        builder.equal(1, BinaryString.fromString("a700")),
                        builder.equal(2, BinaryString.fromString("b700")));

        assertThat(readF1(table, andPredicate)).isEmpty();
        assertThat(readF1(tableWithSearchMode(table, "full"), andPredicate))
                .containsExactly("a700");
        assertThat(readF1(tableWithSearchMode(table, "detail"), andPredicate))
                .containsExactly("a700");

        Predicate orPredicate =
                PredicateBuilder.or(
                        builder.equal(1, BinaryString.fromString("a700")),
                        builder.equal(2, BinaryString.fromString("b701")));

        assertThat(readF1(table, orPredicate)).containsExactly("a701");
        assertThat(readF1(tableWithSearchMode(table, "full"), orPredicate))
                .containsExactly("a700", "a701");
        assertThat(readF1(tableWithSearchMode(table, "detail"), orPredicate))
                .containsExactly("a700", "a701");
    }

    @Test
    public void testMultipleBTreeIndices() throws Exception {
        write(100000L);
        createIndex("f1");
        createIndex("f2");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        Predicate predicate1 =
                new PredicateBuilder(table.rowType())
                        .in(
                                1,
                                Arrays.asList(
                                        BinaryString.fromString("a200"),
                                        BinaryString.fromString("a300"),
                                        BinaryString.fromString("a56789")));

        Predicate predicate2 =
                new PredicateBuilder(table.rowType())
                        .in(
                                2,
                                Arrays.asList(
                                        BinaryString.fromString("b200"),
                                        BinaryString.fromString("b400"),
                                        BinaryString.fromString("b56789")));

        Predicate predicate = PredicateBuilder.and(predicate1, predicate2);
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);

        List<String> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row -> {
                            result.add(row.getString(1).toString());
                        });

        assertThat(result).containsExactly("a200", "a56789");
    }

    @Test
    public void testBTreeGlobalIndexOnAddedColumnContainsOldRowsAsNull() throws Exception {
        long oldRowCount = 10L;
        write(oldRowCount);

        catalog.alterTable(identifier(), SchemaChange.addColumn("f3", DataTypes.STRING()), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            write.write(
                    GenericRow.of(
                            100,
                            BinaryString.fromString("a-new"),
                            BinaryString.fromString("b-new"),
                            BinaryString.fromString("not-null")));
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }

        createIndex("f3");

        table = (FileStoreTable) catalog.getTable(identifier());
        Predicate predicate = new PredicateBuilder(table.rowType()).isNull(3);
        RoaringNavigableMap64 rowIds = globalIndexScan(table, predicate);
        assertNotNull(rowIds);
        assertThat(rowIds.getLongCardinality()).isEqualTo(oldRowCount);
        assertThat(rowIds.toRangeList()).containsExactly(new Range(0L, oldRowCount - 1));
    }

    private void createIndex(String fieldName) throws Exception {
        createIndex(fieldName, null);
    }

    private void createIndex(String fieldName, List<Range> rowRanges) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        SortedGlobalIndexBuilder builder =
                new SortedGlobalIndexBuilder(table, "btree").withIndexField(fieldName);
        List<DataSplit> dataSplits =
                builder.scan()
                        .map(org.apache.paimon.utils.Pair::getRight)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected scan result when building index."));
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (DataSplit dataSplit : indexSplits(table, rowRanges, dataSplits)) {
            commitMessages.addAll(builder.build(dataSplit, ioManager));
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commitMessages);
        }
    }

    private List<DataSplit> indexSplits(
            FileStoreTable table, List<Range> rowRanges, List<DataSplit> fallbackSplits) {
        if (rowRanges == null) {
            return fallbackSplits;
        }

        List<Split> splits =
                table.newReadBuilder().withRowRanges(rowRanges).newScan().plan().splits();
        return splits.stream()
                .map(split -> ((IndexedSplit) split).dataSplit())
                .collect(Collectors.toList());
    }

    private List<String> readF1(ReadBuilder readBuilder, TableScan.Plan plan) throws Exception {
        List<String> readF1 = new ArrayList<>();
        readBuilder
                .newRead()
                .executeFilter()
                .createReader(plan)
                .forEachRemaining(row -> readF1.add(row.getString(1).toString()));
        return readF1;
    }

    private List<Range> indexedRanges(TableScan.Plan plan) {
        return plan.splits().stream()
                .map(split -> (IndexedSplit) split)
                .flatMap(split -> split.rowRanges().stream())
                .distinct()
                .collect(Collectors.toList());
    }

    private List<String> readF1(FileStoreTable table, Predicate predicate) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        return readF1(readBuilder, readBuilder.newScan().plan());
    }

    private FileStoreTable tableWithSearchMode(FileStoreTable table, String searchMode) {
        return table.copy(
                Collections.singletonMap(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), searchMode));
    }

    private void appendRows(int fromInclusive, int toExclusive) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = fromInclusive; i < toExclusive; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                BinaryString.fromString("b" + i)));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private RoaringNavigableMap64 globalIndexScan(FileStoreTable table, Predicate predicate)
            throws Exception {
        try (GlobalIndexScanner scanner =
                GlobalIndexScanner.create(table, PartitionPredicate.ALWAYS_TRUE, predicate).get()) {
            return scanner.scan(predicate).get().results();
        }
    }
}
