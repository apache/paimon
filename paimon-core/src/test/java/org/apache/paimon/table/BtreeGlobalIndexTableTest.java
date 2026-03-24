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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexScanBuilder;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.RowRangeGlobalIndexScanner;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
        scan.withGlobalIndexResult(GlobalIndexResult.create(() -> finalRowIds));

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
    public void testBtreeWithNonIndexedRowRange() throws Exception {
        write(10L);
        append(0, 10);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        createIndex("f1", Collections.singletonList(new Range(0L, 9L)));

        assertThat(table.store().newGlobalIndexScanBuilder().shardList())
                .containsExactly(new Range(0L, 9L));

        Predicate predicate =
                new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("a5"));
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);

        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits).hasSize(1);

        IndexedSplit indexedSplit = (IndexedSplit) splits.get(0);
        assertThat(indexedSplit.rowRanges())
                .containsExactly(new Range(5L, 5L), new Range(10L, 19L));
        assertThat(
                        indexedSplit.dataSplit().dataFiles().stream()
                                .map(DataFileMeta::firstRowId)
                                .distinct()
                                .sorted()
                                .collect(Collectors.toList()))
                .containsExactly(0L, 10L);

        List<String> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(splits)
                .forEachRemaining(row -> result.add(row.getString(1).toString()));
        assertThat(result)
                .containsExactly("a5", "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9");
    }

    private void createIndex(String fieldName) throws Exception {
        createIndex(fieldName, null);
    }

    private void createIndex(String fieldName, List<Range> rowRanges) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        BTreeGlobalIndexBuilder builder =
                new BTreeGlobalIndexBuilder(table).withIndexField(fieldName);
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (DataSplit dataSplit : indexSplits(table, rowRanges, builder.scan())) {
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

    private void append(int startInclusive, int endExclusive) throws Exception {
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        RowType writeType = schemaDefault().rowType();
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType)) {
            for (int i = startInclusive; i < endExclusive; i++) {
                write0.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                BinaryString.fromString("b" + i)));
            }
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write0.prepareCommit());
            }
        }
    }

    private RoaringNavigableMap64 globalIndexScan(FileStoreTable table, Predicate predicate)
            throws Exception {
        GlobalIndexScanBuilder indexScanBuilder = table.store().newGlobalIndexScanBuilder();
        List<Range> ranges = indexScanBuilder.shardList();
        GlobalIndexResult globalFileIndexResult = GlobalIndexResult.createEmpty();
        for (Range range : ranges) {
            try (RowRangeGlobalIndexScanner scanner =
                    indexScanBuilder.withRowRange(range).build()) {
                Optional<GlobalIndexResult> globalIndexResult = scanner.scan(predicate, null);
                if (!globalIndexResult.isPresent()) {
                    throw new RuntimeException("Can't find index result by scan");
                }
                globalFileIndexResult = globalFileIndexResult.or(globalIndexResult.get());
            }
        }

        return globalFileIndexResult.results();
    }
}
