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

package org.apache.paimon.globalindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.AppendBatchTableScan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.RowRangeIndex;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.apache.paimon.table.SpecialFields.ROW_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link DataEvolutionBatchScan}. */
public class DataEvolutionBatchScanTest {

    @Test
    public void testWithFilterKeepsMixedOrWhenRowRangeExtractionFails() {
        PredicateBuilder builder = new PredicateBuilder(rowTypeWithRowId());
        Predicate predicate = PredicateBuilder.or(builder.equal(2, 1L), builder.greaterThan(0, 5));

        AppendBatchTableScan batchScan = mock(AppendBatchTableScan.class);
        SnapshotReader snapshotReader = mockSnapshotReader(batchScan);
        new DataEvolutionBatchScan(null, batchScan).withFilter(predicate);

        verify(snapshotReader).withFilter(predicate, null);
        verify(batchScan, never()).withFilter(any(Predicate.class));
    }

    @Test
    public void testWithFilterRemovesRowIdAfterRowRangeExtractionSucceeds() {
        PredicateBuilder builder = new PredicateBuilder(rowTypeWithRowId());
        Predicate nonRowIdPredicate = builder.greaterThan(0, 5);
        Predicate predicate = PredicateBuilder.and(builder.equal(2, 1L), nonRowIdPredicate);

        AppendBatchTableScan batchScan = mock(AppendBatchTableScan.class);
        SnapshotReader snapshotReader = mockSnapshotReader(batchScan);
        new DataEvolutionBatchScan(null, batchScan).withFilter(predicate);

        ArgumentCaptor<Predicate> captor = ArgumentCaptor.forClass(Predicate.class);
        verify(snapshotReader).withFilter(same(predicate), captor.capture());
        assertThat(captor.getValue()).isSameAs(nonRowIdPredicate);
    }

    @Test
    public void testWithFilterDropsNestedMixedOrFromStatsResidual() {
        PredicateBuilder builder = new PredicateBuilder(rowTypeWithRowId());
        Predicate nonRowIdPredicate = builder.lessThan(0, 100);
        Predicate mixedOr = PredicateBuilder.or(builder.equal(2, 1L), builder.greaterThan(1, 5));
        Predicate predicate =
                PredicateBuilder.and(builder.between(2, 0L, 10L), nonRowIdPredicate, mixedOr);

        AppendBatchTableScan batchScan = mock(AppendBatchTableScan.class);
        SnapshotReader snapshotReader = mockSnapshotReader(batchScan);
        new DataEvolutionBatchScan(null, batchScan).withFilter(predicate);

        ArgumentCaptor<Predicate> captor = ArgumentCaptor.forClass(Predicate.class);
        verify(snapshotReader).withFilter(same(predicate), captor.capture());
        assertThat(captor.getValue()).isSameAs(nonRowIdPredicate);
    }

    @Test
    public void testWithShardKeepsDataEvolutionWrapper() {
        AppendBatchTableScan batchScan = mock(AppendBatchTableScan.class);
        when(batchScan.withShard(0, 2)).thenReturn(batchScan);

        DataEvolutionBatchScan scan = new DataEvolutionBatchScan(null, batchScan);
        DataTableScan returned = scan.withShard(0, 2);

        assertThat(returned).isSameAs(scan);
        verify(batchScan).withShard(0, 2);
    }

    @Test
    public void testTopNFallsBackWhenQueryAuthEnabled() {
        CoreOptions options = mock(CoreOptions.class);
        when(options.globalIndexEnabled()).thenReturn(true);
        when(options.queryAuthEnabled()).thenReturn(true);

        FileStoreTable table = mock(FileStoreTable.class);
        when(table.coreOptions()).thenReturn(options);

        AppendBatchTableScan batchScan = mock(AppendBatchTableScan.class);
        TableScan.Plan fallbackPlan = mock(TableScan.Plan.class);
        when(batchScan.plan()).thenReturn(fallbackPlan);

        TopN topN = new TopN(new FieldRef(0, "f0", DataTypes.INT()), DESCENDING, NULLS_LAST, 1);
        DataEvolutionBatchScan scan = new DataEvolutionBatchScan(table, batchScan);
        scan.withTopN(topN);

        assertThat(scan.plan()).isSameAs(fallbackPlan);
        verify(batchScan).withTopN(topN);
    }

    @Test
    public void testTopNFallsBackWhenLimitIsAlsoPushed() {
        CoreOptions options = mock(CoreOptions.class);
        when(options.globalIndexEnabled()).thenReturn(true);

        FileStoreTable table = mock(FileStoreTable.class);
        when(table.coreOptions()).thenReturn(options);

        AppendBatchTableScan batchScan = mock(AppendBatchTableScan.class);
        TableScan.Plan fallbackPlan = mock(TableScan.Plan.class);
        when(batchScan.plan()).thenReturn(fallbackPlan);

        TopN topN = new TopN(new FieldRef(0, "f0", DataTypes.INT()), DESCENDING, NULLS_LAST, 1);
        DataEvolutionBatchScan scan = new DataEvolutionBatchScan(table, batchScan);
        scan.withLimit(1).withTopN(topN);

        assertThat(scan.plan()).isSameAs(fallbackPlan);
        verify(batchScan).withLimit(1);
        verify(batchScan).withTopN(topN);
    }

    @Test
    public void testSmallExplicitCandidateSetSkipsIndexEvaluationWithFilter() {
        AppendBatchTableScan batchScan = mock(AppendBatchTableScan.class);
        mockSnapshotReader(batchScan);
        when(batchScan.withRowRangeIndex(any(RowRangeIndex.class))).thenReturn(batchScan);
        when(batchScan.plan()).thenReturn(() -> Collections.emptyList());

        Predicate filter = new PredicateBuilder(rowTypeWithRowId()).greaterThan(0, 5);
        TopN topN = new TopN(new FieldRef(0, "f0", DataTypes.INT()), DESCENDING, NULLS_LAST, 1);
        RoaringNavigableMap64 candidates = new RoaringNavigableMap64();
        candidates.add(1L);

        DataEvolutionBatchScan scan = new DataEvolutionBatchScan(null, batchScan);
        scan.withFilter(filter).withTopN(topN).withTopNRowIdFilter(candidates);

        assertThat(scan.plan().splits()).isEmpty();
        ArgumentCaptor<RowRangeIndex> captor = ArgumentCaptor.forClass(RowRangeIndex.class);
        verify(batchScan).withRowRangeIndex(captor.capture());
        assertThat(captor.getValue().ranges()).containsExactly(new Range(1L, 1L));
    }

    @Test
    public void testWrapToIndexSplitsRandomly() {
        Random random = new Random();
        for (int round = 0; round < 2000; round++) {
            int splitNum = 1 + random.nextInt(20);
            List<Split> splits = new ArrayList<>(splitNum);
            List<Range> splitRanges = new ArrayList<>(splitNum);

            long cursor = random.nextInt(10);
            for (int i = 0; i < splitNum; i++) {
                long start = cursor + random.nextInt(4);
                long rowCount = 30 + random.nextInt(31);
                long end = start + rowCount - 1;

                DataSplit split =
                        DataSplit.builder()
                                .withSnapshot(1L)
                                .withPartition(BinaryRow.EMPTY_ROW)
                                .withBucket(i)
                                .withBucketPath("bucket-" + i)
                                .withDataFiles(
                                        Collections.singletonList(
                                                newAppendFile(
                                                        start,
                                                        rowCount,
                                                        "round-" + round + "-split-" + i)))
                                .build();
                splits.add(split);
                splitRanges.add(new Range(start, end));
                cursor = end + 2 + random.nextInt(4);
            }

            List<Range> candidateRanges = new ArrayList<>();
            for (Range splitRange : splitRanges) {
                int fragmentNum = 5 + random.nextInt(6);
                candidateRanges.add(new Range(splitRange.from, splitRange.from));
                for (int i = 0; i < fragmentNum - 2; i++) {
                    long rowId = splitRange.from + 2L * (i + 1);
                    candidateRanges.add(new Range(rowId, rowId));
                }
                candidateRanges.add(new Range(splitRange.to, splitRange.to));
            }

            List<Range> rowRanges = Range.sortAndMergeOverlap(candidateRanges, true);
            List<Split> indexedSplits =
                    DataEvolutionBatchScan.wrapToIndexSplits(
                                    splits, RowRangeIndex.create(rowRanges), null)
                            .splits();

            assertThat(indexedSplits).hasSize(splits.size());
            for (int i = 0; i < indexedSplits.size(); i++) {
                DataSplit split = (DataSplit) splits.get(i);
                IndexedSplit indexedSplit = (IndexedSplit) indexedSplits.get(i);

                List<DataFileMeta> files = split.dataFiles();
                long min = files.get(0).nonNullFirstRowId();
                long max =
                        files.get(files.size() - 1).nonNullFirstRowId()
                                + files.get(files.size() - 1).rowCount()
                                - 1;
                List<Range> expected = expectedRanges(min, max, rowRanges);

                assertThat(expected).isNotEmpty();
                assertThat(expected.size()).isBetween(5, 10);
                assertThat(indexedSplit.dataSplit()).isEqualTo(split);
                assertThat(indexedSplit.rowRanges()).containsExactlyElementsOf(expected);
            }
        }
    }

    @Test
    public void testWrapToIndexSplitsWithUnorderedAndDiscontiguousDataFiles() {
        DataFileMeta file1 = newAppendFile(4650L, 51L, "file-1");
        DataFileMeta file2 = newAppendFile(4300L, 151L, "file-2");
        DataFileMeta file3 = newAppendFile(4200L, 208L, "file-3");
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Arrays.asList(file1, file2, file3))
                        .build();

        List<Split> indexedSplits =
                DataEvolutionBatchScan.wrapToIndexSplits(
                                Collections.singletonList(split),
                                RowRangeIndex.create(Collections.singletonList(new Range(0, 5000))),
                                null)
                        .splits();

        assertThat(indexedSplits).hasSize(1);
        IndexedSplit indexedSplit = (IndexedSplit) indexedSplits.get(0);
        assertThat(indexedSplit.dataSplit()).isEqualTo(split);
        assertThat(indexedSplit.rowRanges())
                .containsExactly(new Range(4200, 4450), new Range(4650, 4700));
    }

    private static RowType rowTypeWithRowId() {
        return RowType.of(
                new DataField(0, "f0", DataTypes.INT()),
                new DataField(1, "f1", DataTypes.INT()),
                new DataField(2, ROW_ID.name(), DataTypes.BIGINT()));
    }

    private static SnapshotReader mockSnapshotReader(AppendBatchTableScan batchScan) {
        SnapshotReader snapshotReader = mock(SnapshotReader.class);
        when(batchScan.snapshotReader()).thenReturn(snapshotReader);
        return snapshotReader;
    }

    private static List<Range> expectedRanges(long min, long max, List<Range> rowRanges) {
        List<Range> expected = new ArrayList<>();
        for (Range range : rowRanges) {
            if (range.to < min) {
                continue;
            }
            if (range.from > max) {
                break;
            }
            expected.add(new Range(Math.max(min, range.from), Math.min(max, range.to)));
        }
        return expected;
    }

    private static DataFileMeta newAppendFile(long firstRowId, long rowCount, String name) {
        return DataFileMeta.forAppend(
                name,
                1024L,
                rowCount,
                EMPTY_STATS,
                0L,
                firstRowId + rowCount - 1,
                1L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                firstRowId,
                null);
    }
}
