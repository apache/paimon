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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.RowRangeIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for global index build. */
public class GlobalIndexBuilderUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexBuilderUtils.class);

    public static List<IndexFileMeta> toIndexFileMetas(
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            CoreOptions options,
            Range range,
            int indexFieldId,
            String indexType,
            List<ResultEntry> entries)
            throws IOException {
        return toIndexFileMetas(
                fileIO,
                indexPathFactory,
                options,
                range,
                indexFieldId,
                null,
                indexType,
                entries,
                null);
    }

    /**
     * Builds the index file metas. The first column in {@code fields} is treated as the primary
     * index column (e.g. the first column in {@code CREATE ... INDEX ON (a, b, c)}) and is stored
     * as {@code indexFieldId}; the remaining columns go into {@code extraFieldIds}. Callers must
     * pass {@code fields} in the intended column order.
     */
    public static List<IndexFileMeta> toIndexFileMetas(
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            CoreOptions options,
            Range range,
            List<DataField> fields,
            String indexType,
            List<ResultEntry> entries,
            @Nullable byte[] sourceMeta)
            throws IOException {
        return toIndexFileMetas(
                fileIO,
                indexPathFactory,
                options,
                range,
                fields.get(0).id(),
                extraFieldIds(fields),
                indexType,
                entries,
                sourceMeta);
    }

    public static List<Range> unindexedRowRanges(
            @Nullable Snapshot snapshot, List<IndexManifestEntry> currentIndexes) {
        if (snapshot == null || snapshot.nextRowId() == null || snapshot.nextRowId() <= 0) {
            return Collections.emptyList();
        }

        Range dataRange = new Range(0, snapshot.nextRowId() - 1);
        List<Range> indexedRanges = new ArrayList<>(currentIndexes.size());
        for (IndexManifestEntry entry : currentIndexes) {
            GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
            if (meta != null) {
                indexedRanges.add(meta.rowRange());
            }
        }
        indexedRanges = Range.sortAndMergeOverlap(indexedRanges, true);
        return Range.sortAndMergeOverlap(dataRange.exclude(indexedRanges), true);
    }

    public static List<IndexManifestEntry> currentIndexEntries(
            FileStoreTable table,
            Snapshot snapshot,
            String indexType,
            List<DataField> fields,
            @Nullable PartitionPredicate partitionPredicate) {
        if (fields.isEmpty()) {
            return Collections.emptyList();
        }

        int indexFieldId = fields.get(0).id();
        int[] extraFieldIds = extraFieldIds(fields);
        List<IndexManifestEntry> entries = new ArrayList<>();
        for (IndexManifestEntry entry :
                table.store().newIndexFileHandler().scan(snapshot, indexType)) {
            if (partitionPredicate != null && !partitionPredicate.test(entry.partition())) {
                continue;
            }
            GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
            if (meta == null) {
                continue;
            }
            if (meta.indexFieldId() != indexFieldId) {
                continue;
            }
            if (!sameExtraFieldIds(meta.extraFieldIds(), extraFieldIds)) {
                continue;
            }
            entries.add(entry);
        }
        return entries;
    }

    public static List<Pair<Range, Split>> splitByRowRangeIndex(
            RowRangeIndex rowRangeIndex, DataSplit dataSplit) {
        if (rowRangeIndex == null) {
            Range range = calcRowRange(dataSplit);
            return range == null
                    ? Collections.emptyList()
                    : Collections.singletonList(Pair.of(range, dataSplit));
        }

        List<Pair<Range, Split>> result = new ArrayList<>();
        for (Split split :
                DataEvolutionBatchScan.wrapToIndexSplits(
                                Collections.singletonList(dataSplit), rowRangeIndex, null)
                        .splits()) {
            IndexedSplit indexedSplit = (IndexedSplit) split;
            for (Range rowRange : indexedSplit.rowRanges()) {
                result.add(
                        Pair.of(
                                rowRange,
                                new IndexedSplit(
                                        indexedSplit.dataSplit(),
                                        Collections.singletonList(rowRange),
                                        null)));
            }
        }
        return result;
    }

    @Nullable
    public static Range calcRowRange(DataSplit dataSplit) {
        List<Range> ranges = calcRowRanges(Collections.singletonList(dataSplit));
        if (ranges.isEmpty()) {
            return null;
        }
        return new Range(ranges.get(0).from, ranges.get(ranges.size() - 1).to);
    }

    public static List<Range> calcRowRanges(List<DataSplit> dataSplits) {
        List<Range> ranges = new ArrayList<>();
        for (DataSplit dataSplit : dataSplits) {
            for (DataFileMeta file : dataSplit.dataFiles()) {
                ranges.add(file.nonNullRowIdRange());
            }
        }
        return Range.sortAndMergeOverlap(ranges, true);
    }

    public static List<DataSplit> splitByContiguousRowRange(List<DataSplit> splits) {
        List<DataSplit> result = new ArrayList<>();
        for (DataSplit split : splits) {
            result.addAll(splitByContiguousRowRange(split));
        }
        return result;
    }

    public static Map<BinaryRow, Map<Range, List<Split>>> groupSplitsByRange(
            RowRangeIndex rowRangeIndex, List<DataSplit> splits) {
        Map<BinaryRow, List<Pair<Range, Split>>> partitionSplitRanges = new HashMap<>();
        for (DataSplit split : splits) {
            for (Pair<Range, Split> keyPair : splitByRowRangeIndex(rowRangeIndex, split)) {
                Range splitRange = keyPair.getKey();
                Split splitWithRange = keyPair.getValue();
                if (splitRange == null) {
                    continue;
                }
                BinaryRow partition = split.partition();
                partitionSplitRanges
                        .computeIfAbsent(partition, p -> new ArrayList<>())
                        .add(Pair.of(splitRange, splitWithRange));
            }
        }

        Map<BinaryRow, Map<Range, List<Split>>> result = new HashMap<>();
        for (Map.Entry<BinaryRow, List<Pair<Range, Split>>> partitionEntry :
                partitionSplitRanges.entrySet()) {
            List<Pair<Range, Split>> splitRanges = partitionEntry.getValue();
            splitRanges.sort(
                    Comparator.comparingLong((Pair<Range, Split> e) -> e.getKey().from)
                            .thenComparingLong(e -> e.getKey().to));

            Map<Range, List<Split>> partitionRanges = new LinkedHashMap<>();
            Range current = null;
            List<Split> currentSplits = new ArrayList<>();
            for (Map.Entry<Range, Split> entry : splitRanges) {
                Range splitRange = entry.getKey();
                if (current == null) {
                    current = splitRange;
                    currentSplits.add(entry.getValue());
                    continue;
                }
                Range merged = Range.union(current, splitRange);
                if (merged != null) {
                    current = merged;
                    currentSplits.add(entry.getValue());
                } else {
                    partitionRanges.put(current, currentSplits);
                    current = splitRange;
                    currentSplits = new ArrayList<>();
                    currentSplits.add(entry.getValue());
                }
            }
            if (current != null) {
                partitionRanges.put(current, currentSplits);
            }
            result.put(partitionEntry.getKey(), partitionRanges);
        }

        return result;
    }

    /** Splits contiguous build ranges into globally aligned, source-row bounded shards. */
    public static Map<Range, List<Split>> shardSplitsByRowRange(
            Map<Range, List<Split>> rangeSplits, long rowsPerShard) {
        checkArgument(rowsPerShard > 0, "Rows per sorted-index shard must be positive.");
        Map<Range, List<Split>> result = new LinkedHashMap<>();
        for (Map.Entry<Range, List<Split>> entry : rangeSplits.entrySet()) {
            Range buildRange = entry.getKey();
            checkArgument(
                    buildRange.from >= 0,
                    "Sorted-index row IDs must be non-negative, but range starts at %s.",
                    buildRange.from);
            long shardStart = (buildRange.from / rowsPerShard) * rowsPerShard;
            while (shardStart <= buildRange.to) {
                long unboundedShardEnd = shardStart + rowsPerShard - 1;
                long shardEnd = unboundedShardEnd < shardStart ? Long.MAX_VALUE : unboundedShardEnd;
                Range shardRange =
                        new Range(
                                Math.max(buildRange.from, shardStart),
                                Math.min(buildRange.to, shardEnd));
                List<Split> shardSplits = new ArrayList<>();
                for (Split split : entry.getValue()) {
                    DataSplit dataSplit;
                    List<Range> splitRanges;
                    if (split instanceof IndexedSplit) {
                        IndexedSplit indexedSplit = (IndexedSplit) split;
                        dataSplit = indexedSplit.dataSplit();
                        splitRanges = indexedSplit.rowRanges();
                    } else {
                        dataSplit = (DataSplit) split;
                        splitRanges = calcRowRanges(Collections.singletonList(dataSplit));
                    }
                    List<Range> intersections =
                            Range.and(splitRanges, Collections.singletonList(shardRange));
                    if (!intersections.isEmpty()) {
                        shardSplits.add(new IndexedSplit(dataSplit, intersections, null));
                    }
                }
                if (!shardSplits.isEmpty()) {
                    result.put(shardRange, shardSplits);
                }
                if (shardEnd == Long.MAX_VALUE) {
                    break;
                }
                shardStart = shardEnd + 1;
            }
        }
        return result;
    }

    private static List<DataSplit> splitByContiguousRowRange(DataSplit split) {
        List<DataFileMeta> input = split.dataFiles();
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        List<List<DataFileMeta>> ranges = rangeHelper.mergeOverlappingRanges(input);

        Supplier<DataSplit.Builder> builderSupplier =
                () ->
                        DataSplit.builder()
                                .withSnapshot(split.snapshotId())
                                .withPartition(split.partition())
                                .withBucket(split.bucket())
                                .withBucketPath(split.bucketPath())
                                .withTotalBuckets(split.totalBuckets())
                                .isStreaming(split.isStreaming())
                                .rawConvertible(split.rawConvertible());
        return packByContiguousRanges(builderSupplier, ranges);
    }

    private static List<DataSplit> packByContiguousRanges(
            Supplier<DataSplit.Builder> builderFactory, List<List<DataFileMeta>> ranges) {
        if (ranges.isEmpty()) {
            return new ArrayList<>();
        }

        List<DataSplit> result = new ArrayList<>();
        List<DataFileMeta> currentSegment = new ArrayList<>();
        long currentMaxRowId = Long.MIN_VALUE;

        for (List<DataFileMeta> rangeFiles : ranges) {
            long minRowId = minRowId(rangeFiles);
            long maxRowId = maxRowId(rangeFiles);
            if (currentSegment.isEmpty() || areContiguous(currentMaxRowId, minRowId)) {
                currentSegment.addAll(rangeFiles);
                currentMaxRowId = maxRowId;
            } else {
                DataSplit.Builder builder = builderFactory.get();
                builder.withDataFiles(currentSegment);
                result.add(builder.build());
                currentSegment = new ArrayList<>(rangeFiles);
                currentMaxRowId = maxRowId;
            }
        }

        DataSplit.Builder builder = builderFactory.get();
        builder.withDataFiles(currentSegment);
        result.add(builder.build());
        return result;
    }

    private static long minRowId(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(f -> f.nonNullRowIdRange().from)
                .min()
                .orElse(Long.MAX_VALUE);
    }

    private static long maxRowId(List<DataFileMeta> files) {
        return files.stream().mapToLong(f -> f.nonNullRowIdRange().to).max().orElse(Long.MIN_VALUE);
    }

    private static boolean areContiguous(long previousMaxRowId, long currentMinRowId) {
        return previousMaxRowId >= currentMinRowId - 1;
    }

    public static List<IndexedSplit> createShardIndexedSplits(
            FileStoreTable table, List<ManifestEntry> entries, long rowsPerShard) {
        return createShardIndexedSplits(table, entries, rowsPerShard, null);
    }

    public static List<IndexedSplit> createShardIndexedSplits(
            FileStoreTable table,
            List<ManifestEntry> entries,
            long rowsPerShard,
            @Nullable List<Range> rowRangesToBuild) {
        return createShardIndexedSplits(
                entries,
                rowsPerShard,
                (partition, bucket) ->
                        table.store().pathFactory().bucketPath(partition, bucket).toString(),
                rowRangesToBuild);
    }

    public static List<IndexedSplit> createShardIndexedSplits(
            List<ManifestEntry> entries,
            long rowsPerShard,
            BiFunction<BinaryRow, Integer, String> bucketPathFactory,
            @Nullable List<Range> rowRangesToBuild) {
        checkArgument(
                rowsPerShard > 0,
                "Option 'global-index.row-count-per-shard' must be greater than 0.");
        if (rowRangesToBuild != null) {
            rowRangesToBuild = Range.sortAndMergeOverlap(rowRangesToBuild, true);
            if (rowRangesToBuild.isEmpty()) {
                return Collections.emptyList();
            }
        }

        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> entriesByPartitionAndBucket =
                FileStoreScan.Plan.groupByPartFiles(entries);

        List<IndexedSplit> result = new ArrayList<>();
        for (Map.Entry<BinaryRow, Map<Integer, List<ManifestEntry>>> partitionEntry :
                entriesByPartitionAndBucket.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            for (Map.Entry<Integer, List<ManifestEntry>> bucketEntry :
                    partitionEntry.getValue().entrySet()) {
                addShardIndexedSplits(
                        result,
                        partition,
                        bucketEntry.getKey(),
                        bucketEntry.getValue(),
                        rowsPerShard,
                        bucketPathFactory,
                        rowRangesToBuild);
            }
        }
        return result;
    }

    private static void addShardIndexedSplits(
            List<IndexedSplit> result,
            BinaryRow partition,
            int bucket,
            List<ManifestEntry> entries,
            long rowsPerShard,
            BiFunction<BinaryRow, Integer, String> bucketPathFactory,
            @Nullable List<Range> rowRangesToBuild) {
        Map<Long, List<DataFileMeta>> filesByShard = new LinkedHashMap<>();
        for (ManifestEntry entry : entries) {
            DataFileMeta file = entry.file();
            if (file.firstRowId() == null) {
                LOG.warn(
                        "Skipping file '{}' in partition {} bucket {} because it has no row ID.",
                        file.fileName(),
                        partition,
                        bucket);
                continue;
            }
            Range fileRange = file.nonNullRowIdRange();
            long startShardId = fileRange.from / rowsPerShard;
            long endShardId = fileRange.to / rowsPerShard;
            for (long shardId = startShardId; shardId <= endShardId; shardId++) {
                long shardStartRowId = shardId * rowsPerShard;
                filesByShard.computeIfAbsent(shardStartRowId, key -> new ArrayList<>()).add(file);
            }
        }

        for (Map.Entry<Long, List<DataFileMeta>> shardEntry : filesByShard.entrySet()) {
            long shardStart = shardEntry.getKey();
            long shardEnd = shardStart + rowsPerShard - 1;
            List<DataFileMeta> shardFiles = shardEntry.getValue();
            if (shardFiles.isEmpty()) {
                continue;
            }

            shardFiles.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));
            List<DataFileMeta> currentGroup = new ArrayList<>();
            long currentGroupEnd = -1;

            for (DataFileMeta file : shardFiles) {
                long fileStart = file.nonNullFirstRowId();
                long fileEnd = file.nonNullRowIdRange().to;
                if (currentGroup.isEmpty()) {
                    currentGroup.add(file);
                    currentGroupEnd = fileEnd;
                } else if (fileStart <= currentGroupEnd + 1) {
                    currentGroup.add(file);
                    currentGroupEnd = Math.max(currentGroupEnd, fileEnd);
                } else {
                    addIndexedSplitForFileGroup(
                            result,
                            currentGroup,
                            shardStart,
                            shardEnd,
                            partition,
                            bucket,
                            entries.get(0).totalBuckets(),
                            bucketPathFactory.apply(partition, bucket),
                            rowRangesToBuild);
                    currentGroup = new ArrayList<>();
                    currentGroup.add(file);
                    currentGroupEnd = fileEnd;
                }
            }
            if (!currentGroup.isEmpty()) {
                addIndexedSplitForFileGroup(
                        result,
                        currentGroup,
                        shardStart,
                        shardEnd,
                        partition,
                        bucket,
                        entries.get(0).totalBuckets(),
                        bucketPathFactory.apply(partition, bucket),
                        rowRangesToBuild);
            }
        }
    }

    private static void addIndexedSplitForFileGroup(
            List<IndexedSplit> result,
            List<DataFileMeta> files,
            long shardStart,
            long shardEnd,
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            String bucketPath,
            @Nullable List<Range> rowRangesToBuild) {
        long groupMinRowId = files.get(0).nonNullFirstRowId();
        long groupMaxRowId =
                files.stream().mapToLong(file -> file.nonNullRowIdRange().to).max().getAsLong();
        Range groupRange =
                new Range(Math.max(groupMinRowId, shardStart), Math.min(groupMaxRowId, shardEnd));
        List<Range> taskRanges =
                rowRangesToBuild == null
                        ? Collections.singletonList(groupRange)
                        : Range.and(Collections.singletonList(groupRange), rowRangesToBuild);
        if (taskRanges.isEmpty()) {
            return;
        }

        // Deliberately omit deletion files. Global indexes cover the stable physical row-id
        // range, while query-side live-row filtering applies deletion vectors for the pinned
        // snapshot. Passing deletion files here would create row-id gaps in index writers.
        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withTotalBuckets(totalBuckets)
                        .withDataFiles(files)
                        .withBucketPath(bucketPath)
                        .rawConvertible(false)
                        .build();
        for (Range taskRange : taskRanges) {
            result.add(new IndexedSplit(dataSplit, Collections.singletonList(taskRange), null));
        }
    }

    private static List<IndexFileMeta> toIndexFileMetas(
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            CoreOptions options,
            Range range,
            int indexFieldId,
            @Nullable int[] extraFieldIds,
            String indexType,
            List<ResultEntry> entries,
            @Nullable byte[] sourceMeta)
            throws IOException {
        List<IndexFileMeta> results = new ArrayList<>();
        for (ResultEntry entry : entries) {
            String fileName = entry.fileName();
            long fileSize = fileIO.getFileSize(indexPathFactory.toPath(fileName));
            GlobalIndexMeta globalIndexMeta =
                    new GlobalIndexMeta(
                            range.from,
                            range.to,
                            indexFieldId,
                            extraFieldIds,
                            entry.meta(),
                            sourceMeta);

            Path externalPathDir = options.globalIndexExternalPath();
            String externalPathString = null;
            if (externalPathDir != null) {
                Path externalPath = new Path(externalPathDir, fileName);
                externalPathString = externalPath.toString();
            }
            IndexFileMeta indexFileMeta =
                    new IndexFileMeta(
                            indexType,
                            fileName,
                            fileSize,
                            entry.rowCount(),
                            globalIndexMeta,
                            externalPathString);
            results.add(indexFileMeta);
        }
        return results;
    }

    public static GlobalIndexWriter createIndexWriter(
            FileStoreTable table, String indexType, DataField indexField, Options options)
            throws IOException {
        GlobalIndexer globalIndexer = GlobalIndexer.create(indexType, indexField, options);
        return globalIndexer.createWriter(createGlobalIndexFileReadWrite(table));
    }

    public static GlobalIndexWriter createIndexWriter(
            FileStoreTable table,
            String indexType,
            DataField indexField,
            List<DataField> extraFields,
            Options options)
            throws IOException {
        GlobalIndexer globalIndexer =
                GlobalIndexer.create(indexType, indexField, extraFields, options);
        return globalIndexer.createWriter(createGlobalIndexFileReadWrite(table));
    }

    private static GlobalIndexFileReadWrite createGlobalIndexFileReadWrite(FileStoreTable table) {
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        return new GlobalIndexFileReadWrite(table.fileIO(), indexPathFactory);
    }

    @Nullable
    private static int[] extraFieldIds(List<DataField> fields) {
        return fields.size() > 1
                ? fields.subList(1, fields.size()).stream().mapToInt(DataField::id).toArray()
                : null;
    }

    private static boolean sameExtraFieldIds(@Nullable int[] left, @Nullable int[] right) {
        if (left == null || left.length == 0) {
            return right == null || right.length == 0;
        }
        if (right == null || right.length == 0) {
            return false;
        }
        return Arrays.equals(left, right);
    }
}
