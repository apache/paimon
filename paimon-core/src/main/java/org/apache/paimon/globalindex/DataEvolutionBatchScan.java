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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.RowIdPredicateVisitor;
import org.apache.paimon.predicate.ScalarSearch;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.AppendBatchTableScan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.RowRangeIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.paimon.table.SpecialFields.ROW_ID;
import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Scan for data evolution table. */
public class DataEvolutionBatchScan implements DataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionBatchScan.class);
    private static final int MAX_SCALAR_TOPN_LIMIT = 100;
    private static final long MAX_SCALAR_TOPN_RESULT_SIZE = 10_000L;
    private static final long MAX_SCALAR_TOPN_SCANNED_ROW_IDS = 100_000L;

    private final FileStoreTable table;
    private final AppendBatchTableScan batchScan;

    private Predicate filter;
    private TopN topN;
    private RoaringNavigableMap64 topNRowIdFilter;
    private boolean pushedLimit;
    private RowRangeIndex pushedRowRangeIndex;
    private GlobalIndexResult globalIndexResult;

    public DataEvolutionBatchScan(FileStoreTable wrapped, AppendBatchTableScan batchScan) {
        this.table = wrapped;
        this.batchScan = batchScan;
    }

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        batchScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        if (predicate == null) {
            return this;
        }

        Optional<List<Range>> rowRanges = predicate.visit(new RowIdPredicateVisitor());
        if (rowRanges.isPresent()) {
            withRowRanges(rowRanges.get());
        }
        this.filter = predicate;

        batchScan.snapshotReader().withFilter(predicate, rowIdSafeResidualFilter(predicate));
        return this;
    }

    private Predicate rowIdSafeResidualFilter(Predicate filter) {
        if (filter instanceof LeafPredicate
                && ((LeafPredicate) filter).fieldNames().contains(ROW_ID.name())) {
            return null;
        } else if (filter instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) filter;
            if (compoundPredicate.function() instanceof Or) {
                return containsRowId(compoundPredicate) ? null : filter;
            }

            List<Predicate> newChildren = new ArrayList<>();
            for (Predicate child : compoundPredicate.children()) {
                Predicate newChild = rowIdSafeResidualFilter(child);
                if (newChild != null) {
                    newChildren.add(newChild);
                }
            }

            return PredicateBuilder.andNullable(newChildren);
        }
        return filter;
    }

    private boolean containsRowId(Predicate filter) {
        if (filter instanceof LeafPredicate) {
            return ((LeafPredicate) filter).fieldNames().contains(ROW_ID.name());
        } else if (filter instanceof CompoundPredicate) {
            for (Predicate child : ((CompoundPredicate) filter).children()) {
                if (containsRowId(child)) {
                    return true;
                }
            }
        }
        return false;
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
        this.topN = topN;
        return this;
    }

    @Override
    public InnerTableScan withTopNRowIdFilter(RoaringNavigableMap64 rowIds) {
        this.topNRowIdFilter = rowIds;
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
        this.pushedLimit = true;
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

    @Override
    public DataEvolutionBatchScan withGlobalIndexResult(GlobalIndexResult globalIndexResult) {
        if (globalIndexResult == null) {
            return this;
        }

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
        checkState(
                topNRowIdFilter == null || topN != null,
                "TopN row ID filter requires a TopN definition.");
        checkState(topN == null || topN.limit() >= 0, "TopN limit must not be negative.");
        if (topN != null && topN.limit() == 0) {
            return () -> Collections.emptyList();
        }

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
            if (rowRangeIndex == null && topNRowIdFilter != null) {
                rowRangeIndex = RowRangeIndex.create(topNRowIdFilter.toRangeList());
            }
        }

        if (rowRangeIndex != null && rowRangeIndex.ranges().isEmpty()) {
            return () -> Collections.emptyList();
        }

        if (rowRangeIndex == null) {
            batchScan.withTopN(topN);
            return batchScan.plan();
        }

        List<Split> splits = batchScan.withRowRangeIndex(rowRangeIndex).plan().splits();
        return wrapToIndexSplits(splits, rowRangeIndex, scoreGetter);
    }

    private Optional<GlobalIndexResult> evalGlobalIndex() {
        if (this.globalIndexResult != null) {
            return Optional.of(globalIndexResult);
        }
        if (filter == null && topN == null) {
            return Optional.empty();
        }
        if (topN != null
                && topNRowIdFilter != null
                && topNRowIdFilter.getLongCardinality() <= topN.limit()) {
            return Optional.of(GlobalIndexResult.create(copy(topNRowIdFilter)));
        }
        CoreOptions options = table.coreOptions();
        if (!options.globalIndexEnabled()) {
            return Optional.empty();
        }
        if (topN != null && !canUseScalarTopN(options)) {
            return Optional.empty();
        }
        Predicate globalIndexFilter = filter == null ? null : rowIdSafeResidualFilter(filter);
        if (filter != null && globalIndexFilter == null) {
            return Optional.empty();
        }
        PartitionPredicate partitionFilter =
                batchScan.snapshotReader().manifestsReader().partitionFilter();
        Optional<GlobalIndexScanner> optionalScanner =
                GlobalIndexScanner.create(table, partitionFilter, globalIndexFilter, topN);
        if (!optionalScanner.isPresent()) {
            return Optional.empty();
        }

        try (GlobalIndexScanner scanner = optionalScanner.get()) {
            if (topN != null) {
                RoaringNavigableMap64 includeRowIds = null;
                if (topNRowIdFilter != null) {
                    includeRowIds = new RoaringNavigableMap64();
                    includeRowIds.or(topNRowIdFilter);
                }
                if (globalIndexFilter != null) {
                    Optional<GlobalIndexResult> filterResult = scanner.scan(globalIndexFilter);
                    if (!filterResult.isPresent()
                            || !filterResult.get().isExact()
                            || intersectsUnindexedRows(
                                    includeRowIds,
                                    scanner.unindexedRowsForCorrectness(globalIndexFilter))) {
                        return Optional.empty();
                    }
                    if (includeRowIds == null) {
                        includeRowIds = filterResult.get().results();
                    } else {
                        includeRowIds.and(filterResult.get().results());
                    }
                }

                if (includeRowIds != null && includeRowIds.getLongCardinality() <= topN.limit()) {
                    return Optional.of(GlobalIndexResult.create(includeRowIds));
                }

                String orderFieldName = topN.orders().get(0).field().name();
                int orderFieldId = table.rowType().getField(orderFieldName).id();
                if (intersectsUnindexedRows(
                        includeRowIds, scanner.unindexedRowsForCorrectness(orderFieldId))) {
                    return Optional.empty();
                }

                Optional<GlobalIndexResult> result =
                        scanner.scan(
                                new ScalarSearch(topN)
                                        .withIncludeRowIds(includeRowIds)
                                        .withMaxResultSize(MAX_SCALAR_TOPN_RESULT_SIZE)
                                        .withMaxScannedRowIds(MAX_SCALAR_TOPN_SCANNED_ROW_IDS));
                if (result.isPresent()) {
                    LOG.info("Scan table '{}' with scalar global index TopN.", table.name());
                }
                return result;
            }

            Optional<GlobalIndexResult> result = scanner.scan(globalIndexFilter);
            if (result.isPresent()) {
                LOG.info("Scan table '{}' with global index.", table.name());
                return Optional.of(result.get().or(scanner.unindexedRows(globalIndexFilter)));
            }
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean canUseScalarTopN(CoreOptions options) {
        return topN.orders().size() == 1
                && topN.limit() <= MAX_SCALAR_TOPN_LIMIT
                && !pushedLimit
                && !options.queryAuthEnabled()
                && !options.deletionVectorsEnabled();
    }

    private RoaringNavigableMap64 copy(RoaringNavigableMap64 rowIds) {
        RoaringNavigableMap64 copy = new RoaringNavigableMap64();
        copy.or(rowIds);
        return copy;
    }

    private boolean intersectsUnindexedRows(
            @Nullable RoaringNavigableMap64 includeRowIds, GlobalIndexResult unindexedRows) {
        if (unindexedRows.results().isEmpty()) {
            return false;
        }
        if (includeRowIds == null) {
            return true;
        }
        RoaringNavigableMap64 intersection = new RoaringNavigableMap64();
        intersection.or(unindexedRows.results());
        intersection.and(includeRowIds);
        return !intersection.isEmpty();
    }

    @VisibleForTesting
    public static Plan wrapToIndexSplits(
            List<Split> splits, RowRangeIndex rowRangeIndex, ScoreGetter scoreGetter) {
        List<Split> indexedSplits = new ArrayList<>();
        Function<Split, List<Split>> process =
                split -> Collections.singletonList(wrapSplit(split, rowRangeIndex, scoreGetter));
        randomlyExecuteSequentialReturn(process, splits, null).forEachRemaining(indexedSplits::add);
        return () -> indexedSplits;
    }

    private static Split wrapSplit(
            Split split, RowRangeIndex rowRangeIndex, ScoreGetter scoreGetter) {
        if (split instanceof QueryAuthSplit) {
            QueryAuthSplit authSplit = (QueryAuthSplit) split;
            return new QueryAuthSplit(
                    wrapSplit(authSplit.split(), rowRangeIndex, scoreGetter),
                    authSplit.authResult());
        }
        if (split instanceof IndexedSplit) {
            return split;
        }
        return wrap((DataSplit) split, rowRangeIndex, scoreGetter);
    }

    private static IndexedSplit wrap(
            DataSplit dataSplit, final RowRangeIndex rowRangeIndex, ScoreGetter scoreGetter) {
        List<DataFileMeta> files = dataSplit.dataFiles();

        List<Range> expected = new ArrayList<>();
        for (DataFileMeta file : files) {
            Range fileRange = file.nonNullRowIdRange();
            expected.addAll(rowRangeIndex.intersectedRanges(fileRange.from, fileRange.to));
        }
        expected = Range.sortAndMergeOverlap(expected, true);
        if (expected.isEmpty()) {
            long min = files.stream().mapToLong(f -> f.nonNullRowIdRange().from).min().orElse(-1L);
            long max = files.stream().mapToLong(f -> f.nonNullRowIdRange().to).max().orElse(-1L);
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
