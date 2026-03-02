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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.RowIdPredicateVisitor;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
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
import org.apache.paimon.utils.RowRangeIndex;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.paimon.globalindex.GlobalIndexScanBuilder.parallelScan;
import static org.apache.paimon.table.SpecialFields.ROW_ID;
import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;

/** Scan for data evolution table. */
public class DataEvolutionBatchScan implements DataTableScan {

    private final FileStoreTable table;
    private final DataTableBatchScan batchScan;

    private Predicate filter;
    private VectorSearch vectorSearch;
    private RowRangeIndex pushedRowRangeIndex;
    private GlobalIndexResult globalIndexResult;

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
        if (predicate == null) {
            return this;
        }

        predicate.visit(new RowIdPredicateVisitor()).ifPresent(this::withRowRanges);
        predicate = removeRowIdFilter(predicate);
        this.filter = predicate;
        batchScan.withFilter(predicate);
        return this;
    }

    private Predicate removeRowIdFilter(Predicate filter) {
        if (filter instanceof LeafPredicate
                && ((LeafPredicate) filter).fieldNames().contains(ROW_ID.name())) {
            return null;
        } else if (filter instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) filter;

            List<Predicate> newChildren = new ArrayList<>();
            for (Predicate child : compoundPredicate.children()) {
                Predicate newChild = removeRowIdFilter(child);
                if (newChild != null) {
                    newChildren.add(newChild);
                }
            }

            if (newChildren.isEmpty()) {
                return null;
            } else if (newChildren.size() == 1) {
                return newChildren.get(0);
            } else {
                return new CompoundPredicate(compoundPredicate.function(), newChildren);
            }
        }
        return filter;
    }

    @Override
    public InnerTableScan withVectorSearch(VectorSearch vectorSearch) {
        this.vectorSearch = vectorSearch;
        batchScan.withVectorSearch(vectorSearch);
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
        if (rowRanges == null) {
            return this;
        }

        this.pushedRowRangeIndex = RowRangeIndex.create(rowRanges);
        if (globalIndexResult != null) {
            throw new IllegalStateException("Cannot push row ranges after global index eval.");
        }
        return this;
    }

    @Override
    public InnerTableScan withRowRangeIndex(RowRangeIndex rowRangeIndex) {
        if (rowRangeIndex == null) {
            return this;
        }

        this.pushedRowRangeIndex = rowRangeIndex;
        if (globalIndexResult != null) {
            throw new IllegalStateException("Cannot push row ranges after global index eval.");
        }
        return this;
    }

    // To enable other system computing index result by their own.
    public InnerTableScan withGlobalIndexResult(GlobalIndexResult globalIndexResult) {
        this.globalIndexResult = globalIndexResult;
        if (pushedRowRangeIndex != null) {
            throw new IllegalStateException(
                    "Can't set global index result after pushing down row ranges.");
        }
        return this;
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        return batchScan.listPartitionEntries();
    }

    @Override
    public Plan plan() {
        RowRangeIndex rowRangeIndex = this.pushedRowRangeIndex;
        ScoreGetter scoreGetter = null;

        if (rowRangeIndex == null) {
            Optional<GlobalIndexResult> indexResult = evalGlobalIndex();
            if (indexResult.isPresent()) {
                GlobalIndexResult result = indexResult.get();
                rowRangeIndex = RowRangeIndex.create(result.results().toRangeList());
                if (result instanceof ScoredGlobalIndexResult) {
                    scoreGetter = ((ScoredGlobalIndexResult) result).scoreGetter();
                }
            }
        }

        if (rowRangeIndex == null) {
            return batchScan.plan();
        }

        List<Split> splits = batchScan.withRowRangeIndex(rowRangeIndex).plan().splits();
        return wrapToIndexSplits(splits, rowRangeIndex, scoreGetter);
    }

    private Optional<GlobalIndexResult> evalGlobalIndex() {
        if (this.globalIndexResult != null) {
            return Optional.of(globalIndexResult);
        }
        if (filter == null && vectorSearch == null) {
            return Optional.empty();
        }
        if (!table.coreOptions().globalIndexEnabled()) {
            return Optional.empty();
        }
        PartitionPredicate partitionPredicate =
                batchScan.snapshotReader().manifestsReader().partitionFilter();
        GlobalIndexScanBuilder indexScanBuilder = table.store().newGlobalIndexScanBuilder();
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        indexScanBuilder.withPartitionPredicate(partitionPredicate).withSnapshot(snapshot);
        List<Range> indexedRowRanges = indexScanBuilder.shardList();
        if (indexedRowRanges.isEmpty()) {
            return Optional.empty();
        }

        Long nextRowId = Objects.requireNonNull(snapshot.nextRowId());
        List<Range> nonIndexedRowRanges = new Range(0, nextRowId - 1).exclude(indexedRowRanges);
        Optional<GlobalIndexResult> resultOptional =
                parallelScan(
                        indexedRowRanges,
                        indexScanBuilder,
                        filter,
                        vectorSearch,
                        table.coreOptions().globalIndexThreadNum());
        if (!resultOptional.isPresent()) {
            return Optional.empty();
        }

        GlobalIndexResult result = resultOptional.get();
        if (!nonIndexedRowRanges.isEmpty()) {
            for (Range range : nonIndexedRowRanges) {
                result.or(GlobalIndexResult.fromRange(range));
            }
        }

        return Optional.of(result);
    }

    @VisibleForTesting
    static Plan wrapToIndexSplits(
            List<Split> splits, RowRangeIndex rowRangeIndex, ScoreGetter scoreGetter) {
        List<Split> indexedSplits = new ArrayList<>();
        Function<Split, List<IndexedSplit>> process =
                split ->
                        Collections.singletonList(
                                wrap((DataSplit) split, rowRangeIndex, scoreGetter));
        randomlyExecuteSequentialReturn(process, splits, null).forEachRemaining(indexedSplits::add);
        return () -> indexedSplits;
    }

    private static IndexedSplit wrap(
            DataSplit dataSplit, final RowRangeIndex rowRangeIndex, ScoreGetter scoreGetter) {
        List<DataFileMeta> files = dataSplit.dataFiles();
        long min = files.get(0).nonNullFirstRowId();
        long max =
                files.get(files.size() - 1).nonNullFirstRowId()
                        + files.get(files.size() - 1).rowCount()
                        - 1;

        List<Range> expected = rowRangeIndex.intersectedRanges(min, max);
        if (expected.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "This is a bug, there should be intersected ranges for split with min row id %d and max row id %d.",
                            min, max));
        }

        float[] scores = null;
        if (scoreGetter != null) {
            int size = expected.stream().mapToInt(r -> (int) (r.count())).sum();
            scores = new float[size];

            int index = 0;
            for (Range range : expected) {
                for (long i = range.from; i <= range.to; i++) {
                    scores[index++] = scoreGetter.score(i);
                }
            }
        }

        return new IndexedSplit(dataSplit, expected, scores);
    }
}
