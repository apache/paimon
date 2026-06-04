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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.manifest.ManifestFileMeta.allContainsRowId;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;

/** {@link FileStoreScan} for data-evolution enabled table. */
public class DataEvolutionFileStoreScan extends AppendOnlyFileStoreScan {

    private boolean dropStats = false;
    @Nullable private RowType readType;

    // Cache file's physical field id set per (schemaId, writeCols) to avoid recomputing during
    // per-file column pruning in postFilterManifestEntries.
    private final ConcurrentMap<Pair<Long, List<String>>, Set<Integer>> fileFieldIdsCache =
            new ConcurrentHashMap<>();

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
        if (readType != null) {
            List<DataField> nonSystemFields =
                    readType.getFields().stream()
                            .filter(f -> !SpecialFields.isSystemField(f.id()))
                            .collect(Collectors.toList());
            if (!nonSystemFields.isEmpty()) {
                this.readType = readType;
            }
        }
        return this;
    }

    @Override
    public Iterator<ManifestEntry> readManifestEntries(
            List<ManifestFileMeta> manifestFiles, boolean useSequential) {
        if (inputFilter != null
                || limit == null
                || limit <= 0
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
        // Always enable post-filtering. The list filterByStats handles predicate-based pruning
        // and pruneByReadType strips per-file columns that are not requested — both
        // need row-id-range grouping that single filterByStats(ManifestEntry) cannot see.
        return inputFilter != null || readType != null;
    }

    @Override
    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> entries) {
        // group by row id range
        RangeHelper<ManifestEntry> rangeHelper =
                new RangeHelper<>(e -> e.file().nonNullRowIdRange());
        List<List<ManifestEntry>> splitByRowId = rangeHelper.mergeOverlappingRanges(entries);

        return splitByRowId.stream()
                .filter(group -> inputFilter == null || filterByStats(group))
                .flatMap(group -> pruneByReadType(group).stream())
                .map(entry -> dropStats ? dropStats(entry) : entry)
                .collect(Collectors.toList());
    }

    private boolean filterByStats(List<ManifestEntry> entries) {
        EvolutionStats stats = evolutionStats(schema, this::scanTableSchema, entries);
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
     */
    private List<ManifestEntry> pruneByReadType(List<ManifestEntry> group) {
        if (readType == null || group.size() <= 1) {
            return group;
        }
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
        // Group must contribute at least one file so the reader sees rowCount and can NULL-fill
        // missing columns for the projection's rows.
        return kept.isEmpty() ? Collections.singletonList(group.get(0)) : kept;
    }

    private Set<Integer> fileFieldIdsForEntry(ManifestEntry entry) {
        return fileFieldIdsCache.computeIfAbsent(
                Pair.of(entry.file().schemaId(), entry.file().writeCols()),
                pair -> computeFileFieldIds(this::scanTableSchema, entry.file()));
    }

    /**
     * Field ids of the columns physically present in {@code file}, resolved through the file's own
     * schema (i.e. the schema the file was written under). Field id, not field name, is the stable
     * identity across schemas — necessary so a renamed column matches an old file written under the
     * pre-rename name.
     */
    @VisibleForTesting
    static Set<Integer> computeFileFieldIds(
            Function<Long, TableSchema> scanTableSchema, DataFileMeta file) {
        Set<Integer> ids = new HashSet<>();
        for (DataField f :
                scanTableSchema.apply(file.schemaId()).project(file.writeCols()).fields()) {
            ids.add(f.id());
        }
        return ids;
    }

    /** TODO: Optimize implementation of this method. */
    @VisibleForTesting
    static EvolutionStats evolutionStats(
            TableSchema schema,
            Function<Long, TableSchema> scanTableSchema,
            List<ManifestEntry> metas) {
        // exclude blob and vector-store files, useless for predicate eval
        metas =
                metas.stream()
                        .filter(entry -> !isBlobFile(entry.file().fileName()))
                        .filter(entry -> !isVectorStoreFile(entry.file().fileName()))
                        .collect(Collectors.toList());

        ToLongFunction<ManifestEntry> maxSeqFunc = e -> e.file().maxSequenceNumber();
        metas.sort(Comparator.comparingLong(maxSeqFunc).reversed());

        int[] allFields = schema.fields().stream().mapToInt(DataField::id).toArray();
        int fieldsCount = schema.fields().size();
        int[] rowOffsets = new int[fieldsCount];
        int[] fieldOffsets = new int[fieldsCount];
        Arrays.fill(rowOffsets, -1);
        Arrays.fill(fieldOffsets, -1);

        InternalRow[] min = new InternalRow[metas.size()];
        InternalRow[] max = new InternalRow[metas.size()];
        BinaryArray[] nullCounts = new BinaryArray[metas.size()];

        for (int i = 0; i < metas.size(); i++) {
            SimpleStats stats = metas.get(i).file().valueStats();
            min[i] = stats.minValues();
            max[i] = stats.maxValues();
            nullCounts[i] = stats.nullCounts();
        }

        for (int i = 0; i < metas.size(); i++) {
            DataFileMeta fileMeta = metas.get(i).file();

            TableSchema dataFileSchema =
                    scanTableSchema.apply(fileMeta.schemaId()).project(fileMeta.writeCols());

            TableSchema dataFileSchemaWithStats = dataFileSchema.project(fileMeta.valueStatsCols());

            int[] fieldIds =
                    dataFileSchema.logicalRowType().getFields().stream()
                            .mapToInt(DataField::id)
                            .toArray();

            int[] fieldIdsWithStats =
                    dataFileSchemaWithStats.logicalRowType().getFields().stream()
                            .mapToInt(DataField::id)
                            .toArray();

            loop1:
            for (int j = 0; j < fieldsCount; j++) {
                if (rowOffsets[j] != -1) {
                    continue;
                }
                int targetFieldId = allFields[j];
                DataType targetType = schema.fields().get(j).type();
                for (int fieldId : fieldIds) {
                    if (targetFieldId == fieldId) {
                        for (int k = 0; k < fieldIdsWithStats.length; k++) {
                            if (fieldId == fieldIdsWithStats[k]) {
                                DataType fileType = dataFileSchemaWithStats.fields().get(k).type();
                                if (!fileType.equalsIgnoreFieldId(targetType)) {
                                    continue loop1;
                                }
                                rowOffsets[j] = i;
                                fieldOffsets[j] = k;
                                continue loop1;
                            }
                        }
                        rowOffsets[j] = -2;
                        continue loop1;
                    }
                }
            }
        }

        long groupRowCount = metas.get(0).file().rowCount();
        DataEvolutionRow finalMin = new DataEvolutionRow(metas.size(), rowOffsets, fieldOffsets);
        DataEvolutionRow finalMax = new DataEvolutionRow(metas.size(), rowOffsets, fieldOffsets);
        // For null-count specifically, a field absent from every file in the group means every
        // logical row is null for that field — encode as groupRowCount so stats predicates can
        // prune non-null comparisons (e.g. `extra2 = 'x'`) instead of falling back to
        // "unknown stats -> keep" in LeafPredicate.test.
        DataEvolutionArray finalNullCounts =
                new DataEvolutionArray(metas.size(), rowOffsets, fieldOffsets, groupRowCount);

        finalMin.setRows(min);
        finalMax.setRows(max);
        finalNullCounts.setRows(nullCounts);
        return new EvolutionStats(groupRowCount, finalMin, finalMax, finalNullCounts);
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
