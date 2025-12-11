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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.globalindex.GlobalIndexScanBuilder.parallelScan;

/** Scan for data evolution table. */
public class DataEvolutionBatchScan implements DataTableScan {

    private final FileStoreTable table;
    private final DataTableBatchScan batchScan;

    private Predicate filter;
    private List<Range> rowRanges;
    private ScoreGetter scoreGetter;

    public DataEvolutionBatchScan(FileStoreTable wrapped, DataTableBatchScan batchScan) {
        this.table = wrapped;
        this.batchScan = batchScan;
    }

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        return batchScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        this.filter = predicate;
        batchScan.withFilter(predicate);
        return this;
    }

    @Override
    public InnerTableScan withReadType(@Nullable RowType readType) {
        batchScan.withReadType(readType);
        return this;
    }

    @Override
    public InnerTableScan withBucket(int bucket) {
        batchScan.withBucket(bucket);
        return this;
    }

    @Override
    public InnerTableScan withTopN(TopN topN) {
        batchScan.withTopN(topN);
        return this;
    }

    @Override
    public InnerTableScan dropStats() {
        batchScan.dropStats();
        return this;
    }

    @Override
    public InnerTableScan withMetricRegistry(MetricRegistry metricsRegistry) {
        batchScan.withMetricRegistry(metricsRegistry);
        return this;
    }

    @Override
    public InnerTableScan withLimit(int limit) {
        batchScan.withLimit(limit);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        batchScan.withPartitionFilter(partitionSpec);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(List<BinaryRow> partitions) {
        batchScan.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
        batchScan.withPartitionsFilter(partitions);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        batchScan.withPartitionFilter(partitionPredicate);
        return this;
    }

    @Override
    public InnerTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        batchScan.withBucketFilter(bucketFilter);
        return this;
    }

    @Override
    public InnerTableScan withLevelFilter(Filter<Integer> levelFilter) {
        batchScan.withLevelFilter(levelFilter);
        return this;
    }

    @Override
    public InnerTableScan withRowRanges(List<Range> rowRanges) {
        this.rowRanges = rowRanges;
        return this;
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        return batchScan.listPartitionEntries();
    }

    @Override
    public Plan plan() {
        generateRowRanges();
        if (rowRanges != null) {
            batchScan.withRowRanges(rowRanges);
        }
        List<Split> splits = batchScan.plan().splits();
        return tryWrapToIndexSplits(splits);
    }

    private void generateRowRanges() {
        if (rowRanges != null) {
            return;
        }
        if (filter == null) {
            return;
        }
        if (!table.coreOptions().globalIndexEnabled()) {
            return;
        }

        PartitionPredicate partitionPredicate =
                batchScan.snapshotReader().manifestsReader().partitionFilter();
        GlobalIndexScanBuilder indexScanBuilder = table.store().newGlobalIndexScanBuilder();
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        indexScanBuilder.withPartitionPredicate(partitionPredicate).withSnapshot(snapshot);
        List<Range> indexedRowRanges = indexScanBuilder.shardList();
        if (!indexedRowRanges.isEmpty()) {
            Long nextRowId = Objects.requireNonNull(snapshot.nextRowId());
            List<Range> nonIndexedRowRanges = new Range(0, nextRowId - 1).exclude(indexedRowRanges);
            Optional<GlobalIndexResult> combined =
                    parallelScan(
                            indexedRowRanges,
                            indexScanBuilder,
                            filter,
                            table.coreOptions().globalIndexThreadNum());
            if (combined.isPresent()) {
                GlobalIndexResult result = combined.get();
                if (!nonIndexedRowRanges.isEmpty()) {
                    for (Range range : nonIndexedRowRanges) {
                        result.or(GlobalIndexResult.fromRange(range));
                    }
                }

                rowRanges = result.results().toRangeList();
                if (result instanceof TopkGlobalIndexResult) {
                    scoreGetter = ((TopkGlobalIndexResult) result).scoreGetter();
                }
            }
        }
    }

    private Plan tryWrapToIndexSplits(List<Split> splits) {
        if (rowRanges == null) {
            return () -> splits;
        }

        List<Split> indexedSplits = new ArrayList<>();
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            List<Range> fromDataFile = new ArrayList<>();
            for (DataFileMeta d : dataSplit.dataFiles()) {
                fromDataFile.add(
                        new Range(d.nonNullFirstRowId(), d.nonNullFirstRowId() + d.rowCount() - 1));
            }

            fromDataFile = Range.mergeSortedAsPossible(fromDataFile);

            List<Range> expected = Range.and(fromDataFile, rowRanges);

            float[] scores = null;
            if (scoreGetter != null) {
                int size = expected.stream().mapToInt(r -> (int) (r.to - r.from + 1)).sum();
                scores = new float[size];

                int index = 0;
                for (Range range : expected) {
                    for (long i = range.from; i <= range.to; i++) {
                        scores[index++] = scoreGetter.score(i);
                    }
                }
            }

            indexedSplits.add(new IndexedSplit(dataSplit, expected, scores));
        }
        return () -> indexedSplits;
    }
}
