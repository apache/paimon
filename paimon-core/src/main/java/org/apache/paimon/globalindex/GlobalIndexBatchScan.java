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
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
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
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.globalindex.GlobalIndexScanBuilder.parallelScan;

/** Scan with global index. */
public class GlobalIndexBatchScan implements DataTableScan {
    private final FileStoreTable wrapped;
    private final DataTableScan batchScan;
    private GlobalIndexResult globalIndexResult;
    private Predicate filter;

    public GlobalIndexBatchScan(FileStoreTable wrapped, DataTableScan batchScan) {
        this.wrapped = wrapped;
        this.batchScan = batchScan;
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

    @Nullable
    @Override
    public PartitionPredicate partitionFilter() {
        return batchScan.partitionFilter();
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
        if (rowRanges != null) {
            this.globalIndexResult = GlobalIndexResult.fromRanges(rowRanges);
        }
        return this;
    }

    public InnerTableScan withGlobalIndexResult(GlobalIndexResult globalIndexResult) {
        this.globalIndexResult = globalIndexResult;
        return this;
    }

    private void configureGlobalIndex(InnerTableScan scan) {
        if (globalIndexResult == null && filter != null) {
            PartitionPredicate partitionPredicate = scan.partitionFilter();
            GlobalIndexScanBuilder globalIndexScanBuilder = wrapped.newIndexScanBuilder();
            Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(wrapped);
            globalIndexScanBuilder
                    .withPartitionPredicate(partitionPredicate)
                    .withSnapshot(snapshot);
            List<Range> indexedRowRanges = globalIndexScanBuilder.shardList();
            if (!indexedRowRanges.isEmpty()) {
                List<Range> nonIndexedRowRanges =
                        new Range(0, snapshot.nextRowId() - 1).exclude(indexedRowRanges);
                Optional<GlobalIndexResult> combined =
                        parallelScan(indexedRowRanges, globalIndexScanBuilder, filter);
                if (combined.isPresent()) {
                    GlobalIndexResult globalIndexResultTemp = combined.get();
                    if (!nonIndexedRowRanges.isEmpty()) {
                        for (Range range : nonIndexedRowRanges) {
                            globalIndexResultTemp.or(GlobalIndexResult.fromRange(range));
                        }
                    }

                    this.globalIndexResult = globalIndexResultTemp;
                }
            }
        }

        if (this.globalIndexResult != null) {
            scan.withRowRanges(this.globalIndexResult.results().toRangeList());
        }
    }

    @Override
    public Plan plan() {
        configureGlobalIndex(batchScan);
        List<Split> splits = batchScan.plan().splits();
        return wrap(splits);
    }

    private Plan wrap(List<Split> splits) {
        if (globalIndexResult == null) {
            return () -> splits;
        }
        List<Split> indexedSplits = new ArrayList<>();
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            List<Range> fromDataFile =
                    Range.mergeSortedAsPossible(
                            dataSplit.dataFiles().stream()
                                    .map(
                                            d ->
                                                    new Range(
                                                            d.nonNullFirstRowId(),
                                                            d.nonNullFirstRowId()
                                                                    + d.rowCount()
                                                                    - 1))
                                    .collect(Collectors.toList()));

            List<Range> expected =
                    Range.and(fromDataFile, globalIndexResult.results().toRangeList());

            Float[] scores = null;
            ScoreFunction scoreFunction = globalIndexResult.scoreFunction();
            if (scoreFunction != null) {
                int size = expected.stream().mapToInt(r -> (int) (r.to - r.from + 1)).sum();
                scores = new Float[size];

                int index = 0;
                for (Range range : expected) {
                    for (long i = range.from; i <= range.to; i++) {
                        scores[index++] = scoreFunction.score(i);
                    }
                }
            }

            indexedSplits.add(new IndexedSplit(dataSplit, expected, scores));
        }
        return () -> indexedSplits;
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        return batchScan.listPartitionEntries();
    }

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        batchScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }
}
