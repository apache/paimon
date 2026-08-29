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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.DataEvolutionArray;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.manifest.ManifestFileMeta.allContainsRowId;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.fileFieldIds;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.apache.paimon.utils.InternalRowUtils.compare;
import static org.apache.paimon.utils.InternalRowUtils.get;

/** {@link FileStoreScan} for data-evolution enabled table. */
public class DataEvolutionFileStoreScan extends AppendOnlyFileStoreScan {

    private boolean dropStats = false;
    @Nullable private RowType readType;
    private final boolean deletionVectorsEnabled;

    // Cache file's physical field id set per (schemaId, writeCols) to avoid recomputing during
    // per-file column pruning in postFilterManifestEntries.
    private final ConcurrentMap<Pair<Long, List<String>>, Set<Integer>> fileFieldIdsCache =
            new ConcurrentHashMap<>();
    private final EvolutionStatsCache evolutionStatsCache = new EvolutionStatsCache();

    public DataEvolutionFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            Integer scanManifestParallelism,
            boolean deletionVectorsEnabled) {
        super(
                manifestsReader,
                bucketSelectConverter,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism,
                false,
                deletionVectorsEnabled,
                true);
        this.deletionVectorsEnabled = deletionVectorsEnabled;
    }

    @Override
    public FileStoreScan dropStats() {
        // overwrite to keep stats here
        // TODO refactor this hacky
        this.dropStats = true;
        return this;
    }

    @Override
    public FileStoreScan keepStats() {
        // overwrite to keep stats here
        // TODO refactor this hacky
        this.dropStats = false;
        return this;
    }

    @Override
    public DataEvolutionFileStoreScan withFilter(Predicate predicate) {
        // overwrite to keep all filter here
        // TODO refactor this hacky
        this.inputFilter = predicate;
        return this;
    }

    @Override
    public FileStoreScan withReadType(RowType readType) {
        // a type without user columns prunes nothing; assign unconditionally, this method
        // may be recalled
        if (readType != null
                && readType.getFields().stream()
                        .anyMatch(f -> !SpecialFields.isSystemField(f.id()))) {
            this.readType = readType;
        } else {
            this.readType = null;
        }
        return this;
    }

    @Override
    public Iterator<ManifestEntry> readManifestEntries(
            List<ManifestFileMeta> manifestFiles, boolean useSequential) {
        if (inputFilter != null
                || limit == null
                || limit <= 0
                || deletionVectorsEnabled
                || !allContainsRowId(manifestFiles)) {
            return super.readManifestEntries(manifestFiles, useSequential);
        }

        List<ManifestEntry> filtered = new ArrayList<>();
        RangeHelper<ManifestFileMeta> rangeHelper =
                new RangeHelper<>(meta -> new Range(meta.minRowId(), meta.maxRowId()));
        Queue<List<ManifestFileMeta>> queue =
                new ArrayDeque<>(rangeHelper.mergeOverlappingRanges(manifestFiles));

        long accumulatedRowCount = 0;
        while (!queue.isEmpty()) {
            List<ManifestFileMeta> groupMetas = queue.poll();
            List<ManifestEntry> entries = new ArrayList<>();
            super.readManifestEntries(groupMetas, useSequential).forEachRemaining(entries::add);
            RangeHelper<ManifestEntry> rangeHelper2 =
                    new RangeHelper<>(e -> e.file().nonNullRowIdRange());
            List<List<ManifestEntry>> splitByRowId = rangeHelper2.mergeOverlappingRanges(entries);

            for (List<ManifestEntry> group : splitByRowId) {
                filtered.addAll(group);
                long groupRowCount =
                        group.stream()
                                .mapToLong(e -> e.file().rowCount())
                                .reduce(Long::max)
                                .orElse(0L);
                accumulatedRowCount += groupRowCount;
                if (accumulatedRowCount >= limit) {
                    return filtered.iterator();
                }
            }
        }
        return filtered.iterator();
    }

    @Override
    protected boolean postFilterManifestEntriesEnabled() {
        return true;
    }

    @Override
    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> entries) {
        if (inputFilter != null || readType != null) {
            // group by row id range
            RangeHelper<ManifestEntry> rangeHelper =
                    new RangeHelper<>(e -> e.file().nonNullRowIdRange());
            List<List<ManifestEntry>> splitByRowId = rangeHelper.mergeOverlappingRanges(entries);

            return splitByRowId.stream()
                    .filter(group -> inputFilter == null || filterByStats(group))
                    .flatMap(group -> pruneByReadType(group).stream())
                    .map(entry -> dropStats ? dropStats(entry) : entry)
                    .collect(Collectors.toList());
        } else if (dropStats) {
            return entries.stream().map(this::dropStats).collect(Collectors.toList());
        } else {
            return entries;
        }
    }

    private boolean filterByStats(List<ManifestEntry> entries) {
        EvolutionStats stats =
                evolutionStats(schema, this::scanTableSchema, entries, evolutionStatsCache);
        return inputFilter.test(
                stats.rowCount(), stats.minValues(), stats.maxValues(), stats.nullCounts());
    }

    /**
     * Per-file column pruning within a row-id-range group: drop files whose physical columns have
     * no overlap with the query's {@code readType}. Necessary for columnar-split DE scenarios where
     * a logical row is reconstructed from multiple files in the same row id range — a query that
     * does not reference a file's columns has no reason to read it.
     *
     * <p>When every file in the group lacks a requested column (e.g. an ADD COLUMN projection over
     * a row-disjoint pre-ALTER group), one file is kept as a row-count representative so the reader
     * can emit the right number of NULL-filled rows.
     *
     * <p>If Deletion-Vector is enabled, we always keep the oldest normal file for each group as the
     * anchor file to lookup corresponding Deletion Files.
     */
    private List<ManifestEntry> pruneByReadType(List<ManifestEntry> group) {
        if (readType == null || group.size() <= 1) {
            return group;
        }
        ManifestEntry anchor =
                deletionVectorsEnabled ? retrieveAnchorFile(group, ManifestEntry::file) : null;
        Set<Integer> readFieldIds = new HashSet<>();
        for (DataField f : readType.getFields()) {
            readFieldIds.add(f.id());
        }
        List<ManifestEntry> kept = new ArrayList<>(group.size());
        for (ManifestEntry entry : group) {
            Set<Integer> fileIds = fileFieldIdsForEntry(entry);
            for (int id : readFieldIds) {
                if (fileIds.contains(id)) {
                    kept.add(entry);
                    break;
                }
            }
        }
        if (anchor != null && !kept.contains(anchor)) {
            kept.add(anchor);
        }
        if (kept.stream()
                .map(ManifestEntry::file)
                .anyMatch(
                        file ->
                                isBlobFile(file.fileName())
                                        || isVectorStoreFile(file.fileName()))) {
            for (ManifestEntry entry : group) {
                DataFileMeta file = entry.file();
                if (!isBlobFile(file.fileName())
                        && !isVectorStoreFile(file.fileName())
                        && !kept.contains(entry)) {
                    kept.add(entry);
                }
            }
        }
        // Group must contribute at least one file so the reader sees rowCount and can NULL-fill
        // missing columns for the projection's rows.
        return kept.isEmpty() ? Collections.singletonList(group.get(0)) : kept;
    }

    private Set<Integer> fileFieldIdsForEntry(ManifestEntry entry) {
        return fileFieldIdsCache.computeIfAbsent(
                Pair.of(entry.file().schemaId(), entry.file().writeCols()),
                pair -> fileFieldIds(this::scanTableSchema, entry.file()));
    }

    @VisibleForTesting
    static EvolutionStats evolutionStats(
            TableSchema schema,
            Function<Long, TableSchema> scanTableSchema,
            List<ManifestEntry> metas,
            EvolutionStatsCache evolutionStatsCache) {
        long groupStart =
                metas.stream()
                        .map(ManifestEntry::file)
                        .map(DataFileMeta::nonNullRowIdRange)
                        .mapToLong(range -> range.from)
                        .min()
                        .orElseThrow(() -> new IllegalArgumentException("Empty evolution group."));
        long groupEnd =
                metas.stream()
                        .map(ManifestEntry::file)
                        .map(DataFileMeta::nonNullRowIdRange)
                        .mapToLong(range -> range.to)
                        .max()
                        .orElseThrow(() -> new IllegalArgumentException("Empty evolution group."));
        long groupRowCount = groupEnd - groupStart + 1;
        Set<Integer> excludedFileFieldIds =
                metas.stream()
                        .filter(
                                entry ->
                                        isBlobFile(entry.file().fileName())
                                                || isVectorStoreFile(entry.file().fileName()))
                        .flatMap(
                                entry ->
                                        evolutionStatsCache.get(scanTableSchema, entry.file())
                                                .dataFileSchema().fields().stream()
                                                .map(DataField::id))
                        .collect(Collectors.toSet());
        // exclude blob and vector-store files, useless for predicate eval
        List<ManifestEntry> normalMetas =
                metas.stream()
                        .filter(entry -> !isBlobFile(entry.file().fileName()))
                        .filter(entry -> !isVectorStoreFile(entry.file().fileName()))
                        .collect(Collectors.toList());

        int[] allFields = schema.fields().stream().mapToInt(DataField::id).toArray();
        DataType[] targetTypes =
                schema.fields().stream().map(DataField::type).toArray(DataType[]::new);
        int fieldsCount = schema.fields().size();
        int[] rowOffsets = new int[fieldsCount];
        int[] fieldOffsets = new int[fieldsCount];
        long[] latestSequences = new long[fieldsCount];
        boolean[] tiedLatestProviders = new boolean[fieldsCount];
        Arrays.fill(rowOffsets, -1);
        Arrays.fill(fieldOffsets, -1);
        Arrays.fill(latestSequences, Long.MIN_VALUE);

        InternalRow[] min = new InternalRow[normalMetas.size()];
        InternalRow[] max = new InternalRow[normalMetas.size()];
        BinaryArray[] nullCounts = new BinaryArray[normalMetas.size()];
        EvolutionStatsCache.ProjectedFileSchema[] projectedSchemas =
                new EvolutionStatsCache.ProjectedFileSchema[normalMetas.size()];

        for (int i = 0; i < normalMetas.size(); i++) {
            DataFileMeta file = normalMetas.get(i).file();
            SimpleStats stats = file.valueStats();
            min[i] = stats.minValues();
            max[i] = stats.maxValues();
            nullCounts[i] = stats.nullCounts();
            EvolutionStatsCache.ProjectedFileSchema projected =
                    evolutionStatsCache.get(scanTableSchema, file);
            projectedSchemas[i] = projected;
            for (int j = 0; j < fieldsCount; j++) {
                if (projected.fieldStats(allFields[j]) == null) {
                    continue;
                }
                long sequence = file.maxSequenceNumber();
                if (sequence > latestSequences[j]) {
                    latestSequences[j] = sequence;
                    rowOffsets[j] = i;
                    tiedLatestProviders[j] = false;
                } else if (sequence == latestSequences[j]) {
                    tiedLatestProviders[j] = true;
                }
            }
        }

        for (int j = 0; j < fieldsCount; j++) {
            if (rowOffsets[j] == -1) {
                if (excludedFileFieldIds.contains(allFields[j])) {
                    rowOffsets[j] = -2;
                }
                continue;
            }
            int provider = rowOffsets[j];
            DataFileMeta file = normalMetas.get(provider).file();
            EvolutionStatsCache.FileFieldStats fileStats =
                    projectedSchemas[provider].fieldStats(allFields[j]);
            Range fileRange = file.nonNullRowIdRange();
            if (tiedLatestProviders[j]
                    || !fileStats.hasStats()
                    || !fileStats.type().equalsIgnoreFieldId(targetTypes[j])
                    || fileRange.from != groupStart
                    || fileRange.to != groupEnd) {
                rowOffsets[j] = -2;
                continue;
            }
            int fieldOffset = fileStats.index();
            if (!isValidStats(file.valueStats(), fieldOffset, targetTypes[j], groupRowCount)) {
                rowOffsets[j] = -2;
                continue;
            }
            fieldOffsets[j] = fieldOffset;
        }
        DataEvolutionRow finalMin =
                new DataEvolutionRow(normalMetas.size(), rowOffsets, fieldOffsets);
        DataEvolutionRow finalMax =
                new DataEvolutionRow(normalMetas.size(), rowOffsets, fieldOffsets);
        // For null-count specifically, a field absent from every file in the group means every
        // logical row is null for that field — encode as groupRowCount so stats predicates can
        // prune non-null comparisons (e.g. `extra2 = 'x'`) instead of falling back to
        // "unknown stats -> keep" in LeafPredicate.test.
        DataEvolutionArray finalNullCounts =
                new DataEvolutionArray(normalMetas.size(), rowOffsets, fieldOffsets, groupRowCount);

        finalMin.setRows(min);
        finalMax.setRows(max);
        finalNullCounts.setRows(nullCounts);
        return new EvolutionStats(groupRowCount, finalMin, finalMax, finalNullCounts);
    }

    private static boolean isValidStats(
            SimpleStats stats, int fieldOffset, DataType type, long rowCount) {
        try {
            Object min = get(stats.minValues(), fieldOffset, type);
            Object max = get(stats.maxValues(), fieldOffset, type);
            BinaryArray nullCounts = stats.nullCounts();
            Long nullCount =
                    nullCounts.isNullAt(fieldOffset) ? null : nullCounts.getLong(fieldOffset);
            if (nullCount != null && (nullCount < 0 || nullCount > rowCount)) {
                return false;
            }
            if ((min == null) != (max == null)) {
                return false;
            }
            if (min == null) {
                return true;
            }
            return (nullCount == null || nullCount != rowCount)
                    && compare(min, max, type.getTypeRoot()) <= 0;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        DataFileMeta file = entry.file();

        // Do not drop a file based on read-column intersection. For data-evolution
        // tables a field absent from a file is an implicit NULL across rowCount()
        // rows, and predicates such as `new_col IS NULL` should still match those
        // rows. Predicate-based stats pruning runs in
        // filterByStats(List<ManifestEntry>), which evolves stats per file via
        // DataEvolutionRow / DataEvolutionArray and correctly reports missing
        // fields as null.

        // If rowRanges is null, all entries should be kept
        if (this.rowRangeIndex == null) {
            return true;
        }

        // If entry.firstRowId does not exist, keep the entry
        Long firstRowId = file.firstRowId();
        if (firstRowId == null) {
            return true;
        }

        // Check if any value in indices is in the range [firstRowId, firstRowId + rowCount - 1]
        long rowCount = file.rowCount();
        long endRowId = firstRowId + rowCount - 1;
        return rowRangeIndex.intersects(firstRowId, endRowId);
    }

    /** Statistics for data evolution. */
    public static class EvolutionStats {

        private final long rowCount;
        private final InternalRow minValues;
        private final InternalRow maxValues;
        private final InternalArray nullCounts;

        public EvolutionStats(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            this.rowCount = rowCount;
            this.minValues = minValues;
            this.maxValues = maxValues;
            this.nullCounts = nullCounts;
        }

        public long rowCount() {
            return rowCount;
        }

        public InternalRow minValues() {
            return minValues;
        }

        public InternalRow maxValues() {
            return maxValues;
        }

        public InternalArray nullCounts() {
            return nullCounts;
        }
    }
}
