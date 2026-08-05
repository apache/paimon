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
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexCoverage;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
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
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.OutputStreamAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;
import static org.assertj.core.api.Assertions.assertThat;
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
    public void testFullSearchIgnoresUnindexedAndResidualForCoverage() throws Exception {
        write(100L);
        createIndex("f1");

        FileStoreTable table =
                tableWithSearchMode((FileStoreTable) catalog.getTable(identifier()), "full");
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate predicate =
                PredicateBuilder.and(
                        builder.equal(1, BinaryString.fromString("a42")),
                        builder.equal(2, BinaryString.fromString("b42")));
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);

        TableScan.Plan plan = readBuilder.newScan().plan();

        assertThat(plan.splits()).allMatch(IndexedSplit.class::isInstance);
        assertThat(
                        plan.splits().stream()
                                .map(IndexedSplit.class::cast)
                                .flatMap(split -> split.rowRanges().stream())
                                .collect(Collectors.toList()))
                .containsExactly(new Range(42, 42));
        assertThat(readF1(readBuilder, plan)).containsExactly("a42");
    }

    @Test
    public void testBTreeGlobalIndexTopNCandidatesAcrossRanges() throws Exception {
        write(100L);
        createIndex("f1");
        appendRows(100, 200);
        createIndexIncremental("f1");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        TopN topN =
                new TopN(
                        new FieldRef(1, "f1", table.rowType().getTypeAt(1)),
                        DESCENDING,
                        NULLS_LAST,
                        5);

        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.createForTopN(
                                table, PartitionPredicate.ALWAYS_TRUE, topN)
                        .orElseThrow(AssertionError::new)) {
            assertThat(scanner.scan(topN).orElseThrow(AssertionError::new).results().toRangeList())
                    .containsExactly(new Range(95, 99));
        }

        ReadBuilder readBuilder = table.newReadBuilder().withTopN(topN);
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits()).allMatch(IndexedSplit.class::isInstance);
        assertThat(readF1(readBuilder, plan))
                .containsExactlyInAnyOrder("a95", "a96", "a97", "a98", "a99");

        TopN ascendingTopN =
                new TopN(
                        new FieldRef(1, "f1", table.rowType().getTypeAt(1)),
                        ASCENDING,
                        NULLS_LAST,
                        5);
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.createForTopN(
                                table, PartitionPredicate.ALWAYS_TRUE, ascendingTopN)
                        .orElseThrow(AssertionError::new)) {
            assertThat(
                            scanner.scan(ascendingTopN)
                                    .orElseThrow(AssertionError::new)
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(0, 1), new Range(10, 10), new Range(100, 101));
        }

        ReadBuilder ascendingReadBuilder = table.newReadBuilder().withTopN(ascendingTopN);
        TableScan.Plan ascendingPlan = ascendingReadBuilder.newScan().plan();
        assertThat(ascendingPlan.splits()).allMatch(IndexedSplit.class::isInstance);
        assertThat(readF1(ascendingReadBuilder, ascendingPlan))
                .containsExactlyInAnyOrder("a0", "a1", "a10", "a100", "a101");
    }

    @Test
    public void testBTreeGlobalIndexTopNCandidatesSkipSplitTopN() throws Exception {
        write(100L);
        createIndex("f0");
        appendRows(100, 200);
        createIndexIncremental("f0");

        FileStoreTable table =
                ((FileStoreTable) catalog.getTable(identifier()))
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), "1 b"));
        TopN topN =
                new TopN(
                        new FieldRef(0, "f0", table.rowType().getTypeAt(0)),
                        DESCENDING,
                        NULLS_LAST,
                        1);

        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.createForTopN(
                                table, PartitionPredicate.ALWAYS_TRUE, topN)
                        .orElseThrow(AssertionError::new)) {
            assertThat(scanner.scan(topN).orElseThrow(AssertionError::new).results().toRangeList())
                    .containsExactly(new Range(199, 199));
            // Index-file TopN pruning must not make the excluded indexed range look unindexed.
            assertThat(scanner.unindexedRows(topN).results()).isEmpty();
        }

        TableScan.Plan plan = table.newReadBuilder().withTopN(topN).newScan().plan();
        assertThat(plan.splits()).allMatch(IndexedSplit.class::isInstance);
        assertThat(
                        plan.splits().stream()
                                .map(IndexedSplit.class::cast)
                                .flatMap(split -> split.rowRanges().stream())
                                .collect(Collectors.toList()))
                .containsExactly(new Range(199, 199));
    }

    @Test
    public void testBTreeGlobalIndexTopNPartialCoverage() throws Exception {
        write(100L);
        createIndex("f1");
        appendRows(100, 110);

        FileStoreTable table =
                tableWithSearchMode((FileStoreTable) catalog.getTable(identifier()), "full");
        TopN topN =
                new TopN(
                        new FieldRef(1, "f1", table.rowType().getTypeAt(1)),
                        DESCENDING,
                        NULLS_LAST,
                        5);

        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.createForTopN(
                                table, PartitionPredicate.ALWAYS_TRUE, topN)
                        .orElseThrow(AssertionError::new)) {
            assertThat(scanner.scan(topN).orElseThrow(AssertionError::new).results().toRangeList())
                    .containsExactly(new Range(95, 99));
            assertThat(scanner.unindexedRows(topN).results().toRangeList())
                    .containsExactly(new Range(100, 109));
        }

        FileStoreTable fastTable = tableWithSearchMode(table, "fast");
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.createForTopN(
                                fastTable, PartitionPredicate.ALWAYS_TRUE, topN)
                        .orElseThrow(AssertionError::new)) {
            assertThat(scanner.unindexedRows(topN).results()).isEmpty();
        }
    }

    @Test
    public void testBTreeGlobalIndexTopNFallsBackForUnsafeReads() throws Exception {
        write(100L);
        createIndex("f1");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        TopN topN =
                new TopN(
                        new FieldRef(1, "f1", table.rowType().getTypeAt(1)),
                        DESCENDING,
                        NULLS_LAST,
                        5);
        Predicate filter = new PredicateBuilder(table.rowType()).lessThan(0, 10);
        ReadBuilder filtered = table.newReadBuilder().withFilter(filter).withTopN(topN);
        TableScan.Plan filteredPlan = filtered.newScan().plan();
        assertThat(filteredPlan.splits()).allMatch(DataSplit.class::isInstance);
        assertThat(readF1(filtered, filteredPlan))
                .containsExactlyInAnyOrder(
                        "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9");

        FileStoreTable modifiableDeletionVectorTable =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.DELETION_VECTORS_MODIFIABLE.key(), "true"));
        FileStoreTable deletionVectorTable =
                modifiableDeletionVectorTable.copy(
                        Collections.singletonMap(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(), "true"));
        ReadBuilder deletionVectorRead = deletionVectorTable.newReadBuilder().withTopN(topN);
        assertThat(deletionVectorRead.newScan().plan().splits())
                .allMatch(DataSplit.class::isInstance);
    }

    @Test
    public void testBTreeGlobalIndexTopNFallsBackForLargeLimit() throws Exception {
        write(200L);
        createIndex("f1");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        TopN maxSupportedTopN =
                new TopN(
                        new FieldRef(1, "f1", table.rowType().getTypeAt(1)),
                        DESCENDING,
                        NULLS_LAST,
                        100);
        assertThat(table.newReadBuilder().withTopN(maxSupportedTopN).newScan().plan().splits())
                .isNotEmpty()
                .allMatch(IndexedSplit.class::isInstance);

        TopN topN =
                new TopN(
                        new FieldRef(1, "f1", table.rowType().getTypeAt(1)),
                        DESCENDING,
                        NULLS_LAST,
                        101);

        assertThat(
                        DataEvolutionGlobalIndexScanner.createForTopN(
                                table, PartitionPredicate.ALWAYS_TRUE, topN))
                .isEmpty();

        TableScan.Plan plan = table.newReadBuilder().withTopN(topN).newScan().plan();
        assertThat(plan.splits()).isNotEmpty().allMatch(DataSplit.class::isInstance);
    }

    @Test
    public void testBTreeGlobalIndexTopNFallsBackForUnsupportedStartupModes() throws Exception {
        write(100L);
        createIndex("f1");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        long startSnapshot = table.snapshotManager().latestSnapshotId();
        appendRows(100, 110);
        table = (FileStoreTable) catalog.getTable(identifier());
        long endSnapshot = table.snapshotManager().latestSnapshotId();

        FileStoreTable incrementalTable =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.INCREMENTAL_BETWEEN.key(),
                                startSnapshot + "," + endSnapshot));
        TopN topN =
                new TopN(
                        new FieldRef(1, "f1", incrementalTable.rowType().getTypeAt(1)),
                        DESCENDING,
                        NULLS_LAST,
                        5);

        List<FileStoreTable> unsupportedTables =
                Arrays.asList(
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_MODE.key(),
                                        CoreOptions.StartupMode.COMPACTED_FULL.toString())),
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(), "0")),
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_CREATION_TIME_MILLIS.key(), "0")),
                        incrementalTable);
        for (FileStoreTable unsupportedTable : unsupportedTables) {
            ReadBuilder unsupportedReadBuilder = unsupportedTable.newReadBuilder().withTopN(topN);
            assertThat(unsupportedReadBuilder.newScan().plan().splits())
                    .isNotEmpty()
                    .allMatch(DataSplit.class::isInstance);
        }

        ReadBuilder readBuilder = incrementalTable.newReadBuilder().withTopN(topN);
        TableScan.Plan plan = readBuilder.newScan().plan();

        assertThat(plan.splits()).isNotEmpty().allMatch(DataSplit.class::isInstance);
        assertThat(readF1(readBuilder, plan))
                .containsExactlyInAnyOrder(
                        "a100", "a101", "a102", "a103", "a104", "a105", "a106", "a107", "a108",
                        "a109");
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
    public void testGlobalIndexDiagnosticLogs() throws Exception {
        write(10L);
        createIndex("f1");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        OutputStreamAppender appender =
                OutputStreamAppender.newBuilder()
                        .setName("global-index-diagnostic-test")
                        .setTarget(output)
                        .setLayout(PatternLayout.newBuilder().withPattern("%level %msg%n").build())
                        .build();
        Logger scanLogger = (Logger) LogManager.getLogger(DataEvolutionBatchScan.class);
        Logger scannerLogger = (Logger) LogManager.getLogger(DataEvolutionGlobalIndexScanner.class);
        Level previousScanLevel = scanLogger.getLevel();
        Level previousScannerLevel = scannerLogger.getLevel();

        appender.start();
        scanLogger.addAppender(appender);
        scannerLogger.addAppender(appender);
        scanLogger.setLevel(Level.INFO);
        scannerLogger.setLevel(Level.INFO);
        try {
            Predicate predicate =
                    new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("a7"));
            table.newReadBuilder().withFilter(predicate).newScan().plan();

            TopN topN =
                    new TopN(
                            new FieldRef(1, "f1", table.rowType().getTypeAt(1)),
                            DESCENDING,
                            NULLS_LAST,
                            1);
            table.newReadBuilder().withTopN(topN).newScan().plan();

            PredicateBuilder rowIdBuilder =
                    new PredicateBuilder(SpecialFields.rowTypeWithRowId(table.rowType()));
            int rowIdIndex = table.rowType().getFieldCount();
            Predicate mixedRowIdPredicate =
                    PredicateBuilder.or(
                            rowIdBuilder.equal(rowIdIndex, 1L),
                            rowIdBuilder.equal(1, BinaryString.fromString("a7")));
            table.newReadBuilder().withFilter(mixedRowIdPredicate).newScan().plan();

            String logs = new String(output.toByteArray(), StandardCharsets.UTF_8);
            assertThat(logs)
                    .containsPattern(
                            "INFO Scan table '[^']+' with global index\\. "
                                    + "searchMode='fast', total=\\d+ ms, metadata=\\d+ ms, "
                                    + "lookup=\\d+ ms, coverage=\\d+ ms\\.")
                    .containsPattern(
                            "INFO Scan table '[^']+' with BTree global index TopN\\. "
                                    + "searchMode='fast', topN='[^']+', total=\\d+ ms, "
                                    + "metadata=\\d+ ms, lookup=\\d+ ms, coverage=\\d+ ms\\.")
                    .containsPattern(
                            "INFO Global index lookup table='[^']+', type='btree', "
                                    + "fields='\\[f1\\]', lookup=\\d+ ms\\.")
                    .contains("INFO Scan table '" + table.name() + "' without global index.");
        } finally {
            scanLogger.setLevel(previousScanLevel);
            scannerLogger.setLevel(previousScannerLevel);
            scanLogger.removeAppender(appender);
            scannerLogger.removeAppender(appender);
            appender.stop();
        }
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

        // Default scalar-index.search-mode is 'fast': only indexed rows are
        // returned, so the unindexed a700 is dropped.
        assertThat(readF1(table, predicate)).containsExactly("a100");

        assertThat(readF1(tableWithSearchMode(table, "fast"), predicate)).containsExactly("a100");
        assertThat(readF1(tableWithSearchMode(table, "full"), predicate))
                .containsExactly("a100", "a700");
        assertThat(readF1(tableWithSearchMode(table, "detail"), predicate))
                .containsExactly("a100", "a700");

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate andWithUnindexedField =
                PredicateBuilder.and(
                        builder.equal(1, BinaryString.fromString("a700")),
                        builder.equal(2, BinaryString.fromString("b700")));

        // Default 'fast': a700 lives in the unindexed range, so it is dropped.
        assertThat(readF1(table, andWithUnindexedField)).isEmpty();
        assertThat(readF1(tableWithSearchMode(table, "fast"), andWithUnindexedField)).isEmpty();
        assertThat(readF1(tableWithSearchMode(table, "full"), andWithUnindexedField))
                .containsExactly("a700");
        assertThat(readF1(tableWithSearchMode(table, "detail"), andWithUnindexedField))
                .containsExactly("a700");
    }

    @Test
    public void testDataEvolutionGlobalIndexScannerKeepsUnindexedRowsSeparate() throws Exception {
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

        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.create(
                                table, PartitionPredicate.ALWAYS_TRUE, predicate)
                        .get()) {
            assertThat(scanner.scan(predicate).get().results().toRangeList())
                    .containsExactly(new Range(100L, 100L));
            assertThat(scanner.unindexedRows(predicate).results().toRangeList())
                    .containsExactly(new Range(500L, 999L));
        }
    }

    @Test
    public void testDataEvolutionSourceBackedIndexParticipatesInGlobalRowIdScan() throws Exception {
        write(10L);
        FileStoreTable table =
                tableWithSearchMode((FileStoreTable) catalog.getTable(identifier()), "full");
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        IndexFileMeta sourceBacked =
                new IndexFileMeta(
                        "btree",
                        "source-backed-index",
                        0,
                        10,
                        new GlobalIndexMeta(0, 9, 1, null, null, new byte[] {1}),
                        null);

        assertThat(
                        DataEvolutionGlobalIndexScanner.create(
                                table, Collections.singletonList(sourceBacked)))
                .isPresent();

        DataEvolutionGlobalIndexCoverage coverage =
                new DataEvolutionGlobalIndexCoverage(
                        table,
                        snapshot,
                        PartitionPredicate.ALWAYS_TRUE,
                        Collections.singletonList(sourceBacked),
                        table.coreOptions().scalarIndexSearchMode());
        assertThat(coverage.unindexedRanges(1)).isEmpty();
    }

    @Test
    public void testOrdinaryAndSourceBackedBTreeIndexCoverageCanCoexist() throws Exception {
        write(10L);
        FileStoreTable table =
                tableWithSearchMode((FileStoreTable) catalog.getTable(identifier()), "full");
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        List<IndexFileMeta> mixedIndexes = new ArrayList<>();
        mixedIndexes.add(
                new IndexFileMeta(
                        "btree",
                        "ordinary-index",
                        0,
                        5,
                        new GlobalIndexMeta(0, 4, 1, null, null),
                        null));
        mixedIndexes.add(
                new IndexFileMeta(
                        "btree",
                        "source-backed-index",
                        0,
                        5,
                        new GlobalIndexMeta(5, 9, 1, null, null, new byte[] {1}),
                        null));

        DataEvolutionGlobalIndexCoverage coverage =
                new DataEvolutionGlobalIndexCoverage(
                        table,
                        snapshot,
                        PartitionPredicate.ALWAYS_TRUE,
                        mixedIndexes,
                        table.coreOptions().scalarIndexSearchMode());
        assertThat(coverage.unindexedRanges(1)).isEmpty();
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

        // Default 'fast': a700 is in the unindexed range, so it is dropped.
        assertThat(readF1(table, andPredicate)).isEmpty();
        assertThat(readF1(tableWithSearchMode(table, "fast"), andPredicate)).isEmpty();
        assertThat(readF1(tableWithSearchMode(table, "full"), andPredicate))
                .containsExactly("a700");
        assertThat(readF1(tableWithSearchMode(table, "detail"), andPredicate))
                .containsExactly("a700");

        Predicate orPredicate =
                PredicateBuilder.or(
                        builder.equal(1, BinaryString.fromString("a700")),
                        builder.equal(2, BinaryString.fromString("b701")));

        // Default 'fast': a700 is unindexed and dropped; a701 (f2 indexed) stays.
        assertThat(readF1(table, orPredicate)).containsExactly("a701");
        assertThat(readF1(tableWithSearchMode(table, "fast"), orPredicate)).containsExactly("a701");
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

    @Test
    public void testUnionAcrossRangesWithMixedFallbackAnswers() throws Exception {
        write(100L);
        createIndex("f0");

        appendRows(100, 20100);
        createIndexIncremental("f0");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());

        long firstRangeSize = 0;
        long secondRangeSize = 0;
        for (IndexManifestEntry entry : table.store().newIndexFileHandler().scanEntries()) {
            IndexFileMeta file = entry.indexFile();
            if (!"btree".equals(file.indexType())) {
                continue;
            }
            if (file.globalIndexMeta().rowRangeStart() == 0) {
                firstRangeSize += file.fileSize();
            } else {
                secondRangeSize += file.fileSize();
            }
        }
        assertThat(firstRangeSize).isGreaterThan(0);
        assertThat(secondRangeSize).isGreaterThan(firstRangeSize);

        long fallbackScanMaxSize = (firstRangeSize + secondRangeSize) / 2;
        FileStoreTable capped =
                table.copy(
                        Collections.singletonMap(
                                BTreeIndexOptions.BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE.key(),
                                String.valueOf(fallbackScanMaxSize)));

        Predicate predicate = new PredicateBuilder(capped.rowType()).lessThan(0, 150);
        List<String> result = readF1(capped, predicate);

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            expected.add("a" + i);
        }
        assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
    }

    private void createIndexIncremental(String fieldName) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        SortedGlobalIndexBuilder builder =
                new SortedGlobalIndexBuilder(table, "btree").withIndexField(fieldName);
        List<DataSplit> dataSplits =
                builder.incrementalScan()
                        .map(org.apache.paimon.utils.Pair::getRight)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected incremental scan result when building index."));
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (DataSplit dataSplit : dataSplits) {
            commitMessages.addAll(builder.build(dataSplit, ioManager));
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commitMessages);
        }
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
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.create(
                                table, PartitionPredicate.ALWAYS_TRUE, predicate)
                        .get()) {
            return scanner.scan(predicate).get().results();
        }
    }
}
