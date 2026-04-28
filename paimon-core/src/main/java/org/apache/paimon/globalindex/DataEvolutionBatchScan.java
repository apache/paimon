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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.RowIdPredicateVisitor;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.table.SpecialFields.ROW_ID;
import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;

/** Scan for data evolution table. */
public class DataEvolutionBatchScan implements DataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionBatchScan.class);

    private final FileStoreTable table;
    private final DataTableBatchScan batchScan;

    private Predicate filter;
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
        RowRangeIndex rowRangeIndex = this.pushedRowRangeIndex;
        ScoreGetter scoreGetter = null;

        if (rowRangeIndex == null) {
            if (this.globalIndexResult != null) {
                // Pre-set global index result pushed down from outer layer
                GlobalIndexResult result = this.globalIndexResult;
                rowRangeIndex = RowRangeIndex.create(result.results().toRangeList());
                if (result instanceof ScoredGlobalIndexResult) {
                    scoreGetter = ((ScoredGlobalIndexResult) result).scoreGetter();
                }
            } else if (filter != null && table.coreOptions().globalIndexEnabled()) {
                PartitionPredicate partitionFilter =
                        batchScan.snapshotReader().manifestsReader().partitionFilter();
                Optional<GlobalIndexScanner> optScanner =
                        GlobalIndexScanner.create(table, partitionFilter, filter);
                if (optScanner.isPresent()) {
                    try (GlobalIndexScanner scanner = optScanner.get()) {
                        // Fast path: btree_file_meta (zero manifest reads)
                        if (scanner.hasFilePathIndex()) {
                            Optional<FilePathGlobalIndexResult> fp =
                                    scanner.scanWithFilePath(filter);
                            if (fp.isPresent()) {
                                Plan fastPlan = buildPlanFromFilePathIndex(fp.get());
                                if (fastPlan != null) {
                                    return fastPlan;
                                }
                                // Stale index detected; fall through to regular btree or batchScan
                            }
                        }
                        // Regular btree fallback (same scanner, no extra IO)
                        Optional<GlobalIndexResult> idx = scanner.scan(filter);
                        if (idx.isPresent()) {
                            GlobalIndexResult result = idx.get();
                            rowRangeIndex = RowRangeIndex.create(result.results().toRangeList());
                            if (result instanceof ScoredGlobalIndexResult) {
                                scoreGetter = ((ScoredGlobalIndexResult) result).scoreGetter();
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        if (rowRangeIndex == null) {
            return batchScan.plan();
        }

        List<Split> splits = batchScan.withRowRangeIndex(rowRangeIndex).plan().splits();
        return wrapToIndexSplits(splits, rowRangeIndex, scoreGetter);
    }

    /**
     * Builds a query plan using the pre-fetched {@link ManifestEntry} rows stored in
     * btree_file_meta index. This method does NOT call {@link #batchScan} at all, so no manifest
     * HDFS reads occur.
     *
     * <p>Returns {@code null} if the index is stale (i.e., one or more referenced data files no
     * longer exist, typically due to compaction after the index was built). The caller should fall
     * back to a regular manifest scan in that case.
     */
    @Nullable
    private Plan buildPlanFromFilePathIndex(FilePathGlobalIndexResult result) throws IOException {
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();

        List<ManifestEntry> entries = new ArrayList<>();
        for (byte[] bytes : result.manifestEntryBytes()) {
            entries.add(serializer.deserializeFromBytes(bytes));
        }

        // Build rowRangeIndex once; use it for both planning-time file pruning and
        // reading-time row-level filtering (IndexedSplit).
        RowRangeIndex rowRangeIndex = null;
        if (result.hasRowLevelFilter()) {
            rowRangeIndex = RowRangeIndex.create(result.rowIndexResult().results().toRangeList());
            // Planning-time file pruning: discard files whose row range has no intersection
            // with the matched row IDs, equivalent to what batchScan does during manifest scan.
            final RowRangeIndex index = rowRangeIndex;
            entries =
                    entries.stream()
                            .filter(
                                    e -> {
                                        long from = e.file().nonNullFirstRowId();
                                        long to = from + e.file().rowCount() - 1;
                                        return !index.intersectedRanges(from, to).isEmpty();
                                    })
                            .collect(Collectors.toList());
        }

        // Guard against stale index caused by compaction after the index was built.
        // For typical point/narrow queries this is O(1~3) fileIO.exists() calls.
        for (ManifestEntry entry : entries) {
            if (!table.fileIO()
                    .exists(
                            table.store()
                                    .pathFactory()
                                    .createDataFilePathFactory(entry.partition(), entry.bucket())
                                    .toPath(entry.file()))) {
                LOG.warn(
                        "btree_file_meta index is stale: file {} no longer exists "
                                + "(possibly removed by compaction). Falling back to manifest scan.",
                        entry.file().fileName());
                return null;
            }
        }

        List<Split> splits = buildSplitsFromEntries(entries);
        if (splits.isEmpty()) {
            return Collections::emptyList;
        }

        if (rowRangeIndex != null) {
            return wrapToIndexSplits(splits, rowRangeIndex, null);
        }
        return () -> splits;
    }

    /**
     * Reconstructs {@link DataSplit} objects from a list of {@link ManifestEntry} instances.
     *
     * <p>Groups entries by {@code partition + bucket}, then builds one {@link DataSplit} per group.
     * Uses {@link BinaryRow} directly as map key so that grouping relies on its content-based
     * {@code equals}/{@code hashCode}, which is collision-free.
     */
    private List<Split> buildSplitsFromEntries(List<ManifestEntry> entries) {
        // partition -> bucket -> files
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> grouped = new HashMap<>();
        Map<BinaryRow, Map<Integer, ManifestEntry>> representative = new HashMap<>();

        for (ManifestEntry entry : entries) {
            grouped.computeIfAbsent(entry.partition(), k -> new HashMap<>())
                    .computeIfAbsent(entry.bucket(), k -> new ArrayList<>())
                    .add(entry.file());
            representative
                    .computeIfAbsent(entry.partition(), k -> new HashMap<>())
                    .put(entry.bucket(), entry);
        }

        List<Split> splits = new ArrayList<>();
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> partEntry :
                grouped.entrySet()) {
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry :
                    partEntry.getValue().entrySet()) {
                ManifestEntry rep =
                        representative.get(partEntry.getKey()).get(bucketEntry.getKey());
                List<DataFileMeta> files = bucketEntry.getValue();
                // Sort by firstRowId for consistent ordering
                files.sort((a, b) -> Long.compare(a.nonNullFirstRowId(), b.nonNullFirstRowId()));

                Long latestSnapshotId =
                        batchScan.snapshotReader().snapshotManager().latestSnapshotId();
                DataSplit split =
                        DataSplit.builder()
                                .withSnapshot(latestSnapshotId != null ? latestSnapshotId : -1L)
                                .withPartition(rep.partition())
                                .withBucket(rep.bucket())
                                .withBucketPath("")
                                .withTotalBuckets(rep.totalBuckets())
                                .isStreaming(false)
                                .rawConvertible(false)
                                .withDataFiles(files)
                                .build();
                splits.add(split);
            }
        }
        return splits;
    }

    @VisibleForTesting
    public static Plan wrapToIndexSplits(
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
