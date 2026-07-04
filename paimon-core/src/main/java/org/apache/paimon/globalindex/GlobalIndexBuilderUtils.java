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
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;

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
                fileIO, indexPathFactory, options, range, indexFieldId, null, indexType, entries);
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
            List<ResultEntry> entries)
            throws IOException {
        // The first column is the primary index column and is stored as indexFieldId; the
        // remaining columns (if any) go into extraFieldIds.
        int indexFieldId = fields.get(0).id();
        int[] extraFieldIds = extraFieldIds(fields);
        return toIndexFileMetas(
                fileIO,
                indexPathFactory,
                options,
                range,
                indexFieldId,
                extraFieldIds,
                indexType,
                entries);
    }

    public static List<Range> unindexedRowRanges(
            FileStoreTable table,
            @Nullable Snapshot snapshot,
            String indexType,
            List<DataField> fields,
            @Nullable PartitionPredicate partitionPredicate) {
        if (snapshot == null || snapshot.nextRowId() == null || snapshot.nextRowId() <= 0) {
            return Collections.emptyList();
        }

        Range dataRange = new Range(0, snapshot.nextRowId() - 1);
        List<Range> indexedRanges =
                indexedRowRanges(table, snapshot, indexType, fields, partitionPredicate);
        return Range.sortAndMergeOverlap(dataRange.exclude(indexedRanges), true);
    }

    public static List<Range> indexedRowRanges(
            FileStoreTable table,
            @Nullable Snapshot snapshot,
            String indexType,
            List<DataField> fields,
            @Nullable PartitionPredicate partitionPredicate) {
        if (snapshot == null || fields.isEmpty()) {
            return Collections.emptyList();
        }

        int indexFieldId = fields.get(0).id();
        int[] extraFieldIds = extraFieldIds(fields);
        List<Range> ranges = new ArrayList<>();
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
            ranges.add(meta.rowRange());
        }
        return Range.sortAndMergeOverlap(ranges, true);
    }

    @Nullable
    public static List<Range> rowRangesAfter(long maxIndexedRowId) {
        if (maxIndexedRowId < 0) {
            return null;
        }
        if (maxIndexedRowId == Long.MAX_VALUE) {
            return Collections.emptyList();
        }
        return Collections.singletonList(new Range(maxIndexedRowId + 1, Long.MAX_VALUE));
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
                new LinkedHashMap<>();
        for (ManifestEntry entry : entries) {
            entriesByPartitionAndBucket
                    .computeIfAbsent(entry.partition(), key -> new LinkedHashMap<>())
                    .computeIfAbsent(entry.bucket(), key -> new ArrayList<>())
                    .add(entry);
        }

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
            List<ResultEntry> entries)
            throws IOException {
        List<IndexFileMeta> results = new ArrayList<>();
        for (ResultEntry entry : entries) {
            String fileName = entry.fileName();
            long fileSize = fileIO.getFileSize(indexPathFactory.toPath(fileName));
            GlobalIndexMeta globalIndexMeta =
                    new GlobalIndexMeta(
                            range.from, range.to, indexFieldId, extraFieldIds, entry.meta());

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

    /**
     * Find the minimum firstRowId among files whose schema does not contain all index columns.
     * Files at or beyond this rowId cannot be indexed because the column was added later via ALTER
     * TABLE.
     *
     * @return the boundary rowId, or {@link Long#MAX_VALUE} if all files contain the columns
     */
    public static long findMinNonIndexableRowId(
            SchemaManager schemaManager, List<ManifestEntry> entries, List<String> indexColumns) {
        Map<Long, Boolean> schemaContainsColumns = new HashMap<>();
        long minRowId = Long.MAX_VALUE;
        long minSchemaId = -1;
        for (ManifestEntry entry : entries) {
            long sid = entry.file().schemaId();
            boolean contains =
                    schemaContainsColumns.computeIfAbsent(
                            sid,
                            id -> schemaManager.schema(id).fieldNames().containsAll(indexColumns));
            if (!contains && entry.file().firstRowId() != null) {
                long rowId = entry.file().nonNullFirstRowId();
                if (rowId < minRowId) {
                    minRowId = rowId;
                    minSchemaId = sid;
                }
            }
        }
        if (minRowId != Long.MAX_VALUE) {
            List<String> schemaFields = schemaManager.schema(minSchemaId).fieldNames();
            List<String> missingColumns = new ArrayList<>();
            for (String col : indexColumns) {
                if (!schemaFields.contains(col)) {
                    missingColumns.add(col);
                }
            }
            LOG.info(
                    "Found non-indexable files: schemaId={} missing columns {}, boundaryRowId={}.",
                    minSchemaId,
                    missingColumns,
                    minRowId);
        }
        return minRowId;
    }

    /** Keep only entries whose firstRowId is strictly less than the given boundary. */
    public static List<ManifestEntry> filterEntriesBefore(
            List<ManifestEntry> entries, long boundaryRowId) {
        if (boundaryRowId == Long.MAX_VALUE) {
            return entries;
        }
        List<ManifestEntry> result = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            if (entry.file().firstRowId() != null
                    && entry.file().nonNullFirstRowId() < boundaryRowId) {
                result.add(entry);
            }
        }
        LOG.info(
                "Filtered {} files to {} indexable files (boundaryRowId={}).",
                entries.size(),
                result.size(),
                boundaryRowId);
        return result;
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
