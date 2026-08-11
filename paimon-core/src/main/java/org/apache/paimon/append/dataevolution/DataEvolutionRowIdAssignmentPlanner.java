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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.io.BinaryDataFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BinaryManifestEntry;
import org.apache.paimon.manifest.BinaryManifestEntry.Projection;
import org.apache.paimon.manifest.CompactFileIdentifierSet;
import org.apache.paimon.manifest.FileEntry.ReusableIdentifier;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ByteArrayKey;
import org.apache.paimon.utils.ByteArrayLookupKey;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PrimitiveRowRanges;
import org.apache.paimon.utils.SerializationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.append.dataevolution.LiveFileRowIdRangeCollector.FileRole.DEDICATED;
import static org.apache.paimon.append.dataevolution.LiveFileRowIdRangeCollector.FileRole.NORMAL;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Low-memory planner for row-id reassignment.
 *
 * <p>Each pass projects only the manifest fields it needs. Planner state is scoped to one
 * invocation and the hot entry/range paths retain primitive arrays instead of manifest objects.
 */
final class DataEvolutionRowIdAssignmentPlanner {

    private static final Logger LOG =
            LoggerFactory.getLogger(DataEvolutionRowIdAssignmentPlanner.class);
    private static final int EXCLUDED_PARTITION_CACHE_SIZE = 1024;
    private static final int MAX_INITIAL_LIVE_FILE_RANGES = 1 << 24;
    private static final BinaryString ROW_ID_FIELD =
            BinaryString.fromString(SpecialFields.ROW_ID.name());
    private static final BinaryString BLOB_FILE_SUFFIX = BinaryString.fromString(".blob");
    private static final BinaryString VECTOR_FILE_MARKER = BinaryString.fromString(".vector.");
    private static final Projection ADD_IDENTIFIER_PROJECTION =
            manifestProjection(
                    true,
                    DataFileMeta.FILE_NAME,
                    DataFileMeta.ROW_COUNT,
                    DataFileMeta.LEVEL,
                    DataFileMeta.EXTRA_FILES,
                    DataFileMeta.EMBEDDED_FILE_INDEX,
                    DataFileMeta.EXTERNAL_PATH,
                    DataFileMeta.FIRST_ROW_ID,
                    DataFileMeta.WRITE_COLS);
    private static final Projection COMPACT_ADD_PROJECTION =
            manifestProjection(
                    false,
                    DataFileMeta.FILE_NAME,
                    DataFileMeta.ROW_COUNT,
                    DataFileMeta.FIRST_ROW_ID,
                    DataFileMeta.WRITE_COLS);
    private static final Projection REWRITE_PROJECTION =
            manifestProjection(
                    false,
                    DataFileMeta.ROW_COUNT,
                    DataFileMeta.FIRST_ROW_ID,
                    DataFileMeta.WRITE_COLS);

    private final FileStoreTable table;
    private final ManifestFile manifestFile;
    private final @Nullable PartitionPredicate partitionPredicate;
    private final List<ManifestFileMeta> manifestMetas;
    private final Map<String, Integer> manifestOrdinals;
    private final boolean[] plannedManifests;
    private final boolean[] rewrittenManifests;
    private final Map<ByteArrayKey, SelectedPartition> selectedPartitions;
    private final long skipContiguousRowCount;

    DataEvolutionRowIdAssignmentPlanner(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            List<ManifestFileMeta> manifestMetas) {
        this.table = table;
        this.manifestFile = table.store().manifestFileFactory().create();
        this.partitionPredicate = partitionPredicate;
        this.manifestMetas = manifestMetas;
        this.manifestOrdinals = manifestOrdinals(manifestMetas);
        this.plannedManifests = new boolean[manifestMetas.size()];
        this.rewrittenManifests = new boolean[manifestMetas.size()];
        this.selectedPartitions = new LinkedHashMap<>();
        this.skipContiguousRowCount =
                table.coreOptions().dataEvolutionReassignSkipContiguousRowCount();
    }

    private static Projection manifestProjection(
            boolean includeBucket, String... projectedFileFields) {
        List<DataField> fields = new ArrayList<>();
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.KIND));
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.PARTITION));
        if (includeBucket) {
            fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.BUCKET));
        }
        fields.add(
                ManifestEntry.MANIFEST_ROW_TYPE
                        .getField(ManifestEntry.FILE)
                        .newType(DataFileMeta.SCHEMA.project(projectedFileFields)));
        return Projection.create(new RowType(false, fields));
    }

    Result plan(List<List<ManifestFileMeta>> groups) {
        validateGroups(groups);
        for (List<ManifestFileMeta> group : groups) {
            planGroup(group);
        }
        return buildResult();
    }

    private void planGroup(List<ManifestFileMeta> manifestGroup) {
        GroupState group =
                new GroupState(
                        table.schema().logicalPartitionType().getFieldCount(),
                        partitionPredicate,
                        EXCLUDED_PARTITION_CACHE_SIZE,
                        partitionPredicate == null
                                ? initialLiveFileRangeCapacity(manifestGroup)
                                : 0);
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            plannedManifests[ordinal(manifestMeta)] = true;
        }
        ReusableIdentifier identifier = new ReusableIdentifier();
        long[] rowRangeScratch = new long[2];

        try {
            collectDeletedIdentifiers(manifestGroup, group, identifier);
            collectLiveFileRanges(manifestGroup, group, identifier, rowRangeScratch);
            identifier.release();
            group.releaseDeletedIdentifiers();

            List<PartitionState> selections = group.selectFragmentedPartitions();
            for (PartitionState selection : selections) {
                mergeSelectedPartition(selection);
            }
        } catch (RuntimeException | Error e) {
            identifier.release();
            group.abort();
            throw e;
        }
    }

    private void collectDeletedIdentifiers(
            List<ManifestFileMeta> manifestGroup, GroupState group, ReusableIdentifier identifier) {
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            if (manifestMeta.numDeletedFiles() <= 0) {
                continue;
            }
            try (CloseableIterator<BinaryManifestEntry> entries =
                    manifestFile.scan(
                            manifestMeta.fileName(), BinaryManifestEntry.DELETE_ENTRY_PROJECTION)) {
                while (entries.hasNext()) {
                    BinaryManifestEntry entry = entries.next();
                    if (!entry.isDelete()) {
                        continue;
                    }
                    PartitionState partition = group.internPartition(entry.partitionBytes());
                    if (partition == null) {
                        continue;
                    }
                    identifier.replace(entry);
                    group.deletedIdentifiers.add(partition.id, identifier);
                }
            } catch (Exception e) {
                throw scanException(manifestMeta, e);
            }
        }
    }

    private void collectLiveFileRanges(
            List<ManifestFileMeta> manifestGroup,
            GroupState group,
            ReusableIdentifier identifier,
            long[] rowRangeScratch) {
        Projection addProjection =
                group.deletedIdentifiers.isEmpty()
                        ? COMPACT_ADD_PROJECTION
                        : ADD_IDENTIFIER_PROJECTION;
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            if (manifestMeta.numAddedFiles() <= 0) {
                continue;
            }
            int manifestOrdinal = ordinal(manifestMeta);
            try (CloseableIterator<BinaryManifestEntry> entries =
                    manifestFile.scan(manifestMeta.fileName(), addProjection)) {
                while (entries.hasNext()) {
                    BinaryManifestEntry entry = entries.next();
                    if (!entry.isAdd()) {
                        continue;
                    }
                    PartitionState partition = group.internPartition(entry.partitionBytes());
                    if (partition == null) {
                        continue;
                    }
                    if (!group.deletedIdentifiers.isEmpty()) {
                        identifier.replace(entry);
                        if (group.deletedIdentifiers.contains(partition.id, identifier)) {
                            continue;
                        }
                    }

                    BinaryDataFileMeta file = entry.file();
                    BinaryString fileName = file.fileNameBinary();
                    readRowRange(file, manifestOrdinal, fileName, rowRangeScratch);
                    checkState(
                            !file.containsWriteColumn(ROW_ID_FIELD),
                            "Cannot reassign row IDs for file '%s' because it physically stores the row-id field.",
                            fileName);
                    int fileOrder = fileOrder(fileName);
                    partition.setMinFirstRowId(rowRangeScratch[0]);
                    group.liveFileRanges.add(
                            partition.id,
                            fileOrder == 0 ? NORMAL : DEDICATED,
                            rowRangeScratch[0],
                            inclusiveRangeCount(rowRangeScratch[0], rowRangeScratch[1]));
                }
            } catch (Exception e) {
                throw scanException(manifestMeta, e);
            }
        }
    }

    private void markRewrittenManifests() {
        ByteArrayLookupKey lookup = new ByteArrayLookupKey();
        long[] rowRangeScratch = new long[2];
        for (int manifestOrdinal = 0; manifestOrdinal < manifestMetas.size(); manifestOrdinal++) {
            if (!plannedManifests[manifestOrdinal]) {
                continue;
            }
            ManifestFileMeta manifestMeta = manifestMetas.get(manifestOrdinal);
            if (!manifestMayContainSelectedRange(manifestMeta)) {
                continue;
            }
            try (CloseableIterator<BinaryManifestEntry> entries =
                    manifestFile.scan(manifestMeta.fileName(), REWRITE_PROJECTION)) {
                while (entries.hasNext()) {
                    BinaryManifestEntry entry = entries.next();
                    lookup.reset(entry.partitionBytes());
                    SelectedPartition selection;
                    try {
                        selection = selectedPartitions.get(lookup);
                    } finally {
                        lookup.clear();
                    }
                    if (selection == null) {
                        continue;
                    }
                    BinaryDataFileMeta file = entry.file();
                    readRowRange(file, manifestOrdinal, null, rowRangeScratch);
                    if (!selection.logicalRanges.covers(rowRangeScratch[0], rowRangeScratch[1])) {
                        continue;
                    }
                    checkState(
                            !file.containsWriteColumn(ROW_ID_FIELD),
                            "Cannot reassign an entry in manifest '%s' because it physically stores the row-id field.",
                            manifestMeta.fileName());
                    rewrittenManifests[manifestOrdinal] = true;
                    break;
                }
            } catch (Exception e) {
                throw scanException(manifestMeta, e);
            }
        }
    }

    private boolean manifestMayContainSelectedRange(ManifestFileMeta manifestMeta) {
        Long minimum = manifestMeta.minRowId();
        Long maximum = manifestMeta.maxRowId();
        if (minimum == null || maximum == null) {
            return true;
        }
        for (SelectedPartition partition : selectedPartitions.values()) {
            if (partition.logicalRanges.overlaps(minimum, maximum)) {
                return true;
            }
        }
        return false;
    }

    private Result buildResult() {
        if (selectedPartitions.isEmpty()) {
            return new Result(new int[0], Collections.emptyMap(), 0L);
        }

        long skippedRangeCount = 0L;
        long skippedRowCount = 0L;
        Iterator<Map.Entry<ByteArrayKey, SelectedPartition>> selectedIterator =
                selectedPartitions.entrySet().iterator();
        while (selectedIterator.hasNext()) {
            SelectedPartition partition = selectedIterator.next().getValue();
            partition.logicalRanges.normalizeOverlapping();
            if (skipContiguousRowCount > 0) {
                Pair<Long, Long> skipped =
                        partition.removeLargeContiguousRuns(skipContiguousRowCount);
                skippedRangeCount = Math.addExact(skippedRangeCount, skipped.getLeft());
                skippedRowCount = Math.addExact(skippedRowCount, skipped.getRight());
            }
            if (!partition.hasFragmentedLogicalRanges()) {
                selectedIterator.remove();
            }
        }
        if (skippedRangeCount > 0) {
            LOG.info(
                    "Excluded {} logical ranges containing {} rows from row-id reassignment "
                            + "because their strictly contiguous same-partition runs exceed {} rows.",
                    skippedRangeCount,
                    skippedRowCount,
                    skipContiguousRowCount);
        }
        if (selectedPartitions.isEmpty()) {
            return new Result(new int[0], Collections.emptyMap(), 0L);
        }

        markRewrittenManifests();
        List<SelectedPartition> partitions = new ArrayList<>(selectedPartitions.values());
        RecordComparator typedComparator =
                CodeGenUtils.newRecordComparator(
                        table.schema().logicalPartitionType().getFieldTypes());
        partitions.sort(
                (left, right) -> {
                    int comparison = typedComparator.compare(left.partition, right.partition);
                    // Row IDs are globally unique, so this also orders binary-distinct
                    // partitions which compare equal, for example different NaN payloads.
                    return comparison != 0
                            ? comparison
                            : Long.compare(left.minFirstRowId, right.minFirstRowId);
                });

        Map<BinaryRow, RowRangeMappingIndex> mappings = new LinkedHashMap<>();
        long nextOffset = 0L;
        for (SelectedPartition partition : partitions) {
            int rangeCount = partition.logicalRanges.size();
            checkState(rangeCount > 0, "Selected partition has no logical row-id ranges.");
            PrimitiveRowRanges.Owned ownedRanges = partition.logicalRanges.takeOwned();
            long[] oldStarts = ownedRanges.starts();
            long[] oldEnds = ownedRanges.ends();
            long[] newStarts = new long[rangeCount];
            for (int i = 0; i < rangeCount; i++) {
                newStarts[i] = nextOffset;
                nextOffset =
                        Math.addExact(nextOffset, inclusiveRangeCount(oldStarts[i], oldEnds[i]));
            }
            mappings.put(
                    partition.partition,
                    RowRangeMappingIndex.createFromOwnedArrays(oldStarts, oldEnds, newStarts));
        }

        int rewrittenCount = 0;
        for (boolean rewritten : rewrittenManifests) {
            if (rewritten) {
                rewrittenCount++;
            }
        }
        int[] ordinals = new int[rewrittenCount];
        int position = 0;
        for (int i = 0; i < rewrittenManifests.length; i++) {
            if (rewrittenManifests[i]) {
                ordinals[position++] = i;
            }
        }
        checkState(
                ordinals.length > 0,
                "Selected row-id mappings do not reference any manifest file.");
        return new Result(ordinals, mappings, nextOffset);
    }

    private void mergeSelectedPartition(PartitionState selection) {
        PrimitiveRowRanges logicalRanges = selection.requiredLogicalRanges();
        ByteArrayLookupKey lookup = new ByteArrayLookupKey(selection.serialized);
        SelectedPartition selected = selectedPartitions.get(lookup);
        if (selected == null) {
            selected =
                    new SelectedPartition(
                            selection.partition, selection.minFirstRowId, logicalRanges);
            selectedPartitions.put(new ByteArrayKey(selection.serialized), selected);
            return;
        }
        selected.minFirstRowId = Math.min(selected.minFirstRowId, selection.minFirstRowId);
        selected.logicalRanges.append(logicalRanges);
        selected.logicalRanges.normalizeOverlapping();
    }

    private static RuntimeException scanException(ManifestFileMeta manifestMeta, Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new RuntimeException("Failed to scan manifest file " + manifestMeta.fileName(), e);
    }

    private static void readRowRange(
            BinaryDataFileMeta file,
            int manifestOrdinal,
            @Nullable BinaryString fileName,
            long[] result) {
        checkState(
                file.hasFirstRowId(),
                fileName == null
                        ? "Manifest %s contains a file without first row id."
                        : "File '%s' does not have first row id.",
                fileName == null ? manifestOrdinal : fileName);
        long firstRowId = file.nonNullFirstRowId();
        long rowCount = file.rowCount();
        checkState(
                rowCount > 0,
                "Manifest %s contains a file with non-positive row count %s.",
                manifestOrdinal,
                rowCount);
        result[0] = firstRowId;
        result[1] = Math.addExact(firstRowId, rowCount - 1L);
    }

    private static int fileOrder(BinaryString fileName) {
        if (fileName.endsWith(BLOB_FILE_SUFFIX)) {
            return 1;
        }
        if (fileName.contains(VECTOR_FILE_MARKER)) {
            return 2;
        }
        return 0;
    }

    private int ordinal(ManifestFileMeta manifestMeta) {
        Integer ordinal = manifestOrdinals.get(manifestMeta.fileName());
        checkArgument(
                ordinal != null,
                "Planning group references unknown manifest '%s'.",
                manifestMeta.fileName());
        return ordinal;
    }

    private static int initialLiveFileRangeCapacity(List<ManifestFileMeta> manifestGroup) {
        long addedCount = 0L;
        long deletedCount = 0L;
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            checkState(
                    manifestMeta.numAddedFiles() >= 0 && manifestMeta.numDeletedFiles() >= 0,
                    "Manifest file counts cannot be negative.");
            addedCount = Math.addExact(addedCount, manifestMeta.numAddedFiles());
            deletedCount = Math.addExact(deletedCount, manifestMeta.numDeletedFiles());
        }
        return initialLiveFileRangeCapacity(addedCount, deletedCount);
    }

    static int initialLiveFileRangeCapacity(long addedCount, long deletedCount) {
        checkArgument(addedCount >= 0, "Added entry count cannot be negative.");
        checkArgument(deletedCount >= 0, "Deleted entry count cannot be negative.");
        // Counts are only a sizing hint: DELETE entries may be duplicated or may not match an ADD
        // in this group. Estimate the live set, cap the eager allocation, and let
        // LiveFileRowIdRangeCollector grow if the actual number of retained ADD entries is larger.
        long estimatedLiveCount = addedCount > deletedCount ? addedCount - deletedCount : 0L;
        return (int) Math.min(estimatedLiveCount, MAX_INITIAL_LIVE_FILE_RANGES);
    }

    private void validateGroups(List<List<ManifestFileMeta>> groups) {
        boolean[] seen = new boolean[manifestMetas.size()];
        for (List<ManifestFileMeta> group : groups) {
            checkArgument(group != null && !group.isEmpty(), "Manifest group cannot be empty.");
            for (ManifestFileMeta manifestMeta : group) {
                checkArgument(manifestMeta != null, "Manifest meta cannot be null.");
                int ordinal = ordinal(manifestMeta);
                checkArgument(
                        !seen[ordinal],
                        "Manifest '%s' occurs in more than one planning group.",
                        manifestMeta.fileName());
                seen[ordinal] = true;
            }
        }
    }

    private static Map<String, Integer> manifestOrdinals(List<ManifestFileMeta> manifestMetas) {
        Map<String, Integer> result = new HashMap<>();
        for (int i = 0; i < manifestMetas.size(); i++) {
            ManifestFileMeta manifestMeta = manifestMetas.get(i);
            checkArgument(manifestMeta != null, "Manifest meta cannot be null.");
            checkArgument(
                    result.put(manifestMeta.fileName(), i) == null,
                    "Duplicate manifest file '%s'.",
                    manifestMeta.fileName());
        }
        return result;
    }

    private static long inclusiveRangeCount(long start, long end) {
        return Math.addExact(Math.subtractExact(end, start), 1L);
    }

    /** Compact planner result. All new row starts are relative to the snapshot's next row ID. */
    static final class Result {

        final int[] manifestOrdinals;
        final Map<BinaryRow, RowRangeMappingIndex> rowIdMappings;
        final long totalOffset;

        private Result(
                int[] manifestOrdinals,
                Map<BinaryRow, RowRangeMappingIndex> rowIdMappings,
                long totalOffset) {
            this.manifestOrdinals = manifestOrdinals;
            this.rowIdMappings = Collections.unmodifiableMap(rowIdMappings);
            this.totalOffset = totalOffset;
        }

        boolean isEmpty() {
            return rowIdMappings.isEmpty();
        }
    }

    private static final class GroupState {

        private final GroupPartitionDictionary partitions;
        private final CompactFileIdentifierSet deletedIdentifiers = new CompactFileIdentifierSet();
        private final LiveFileRowIdRangeCollector liveFileRanges;

        private GroupState(
                int partitionArity,
                @Nullable PartitionPredicate partitionPredicate,
                int excludedPartitionCacheSize,
                int expectedLiveFileCount) {
            this.partitions =
                    new GroupPartitionDictionary(
                            partitionArity, partitionPredicate, excludedPartitionCacheSize);
            this.liveFileRanges = new LiveFileRowIdRangeCollector(expectedLiveFileCount);
        }

        private @Nullable PartitionState internPartition(byte[] serialized) {
            return partitions.intern(serialized);
        }

        private void releaseDeletedIdentifiers() {
            deletedIdentifiers.release();
        }

        private List<PartitionState> selectFragmentedPartitions() {
            List<PartitionState> selections = new ArrayList<>();
            liveFileRanges.finish(
                    (partitionId, logicalRanges) -> {
                        PartitionState partition = partitions.partition(partitionId);
                        partition.select(logicalRanges);
                        selections.add(partition);
                    });
            return selections;
        }

        private void abort() {
            deletedIdentifiers.release();
            liveFileRanges.abort();
        }
    }

    private static final class GroupPartitionDictionary {

        private final int expectedArity;
        private final @Nullable PartitionPredicate partitionPredicate;
        private final int excludedCacheSize;
        private final Map<ByteArrayKey, PartitionState> included = new HashMap<>();
        private final LinkedHashMap<ByteArrayKey, Boolean> excluded;
        private final ByteArrayLookupKey lookup = new ByteArrayLookupKey();
        private final List<PartitionState> partitions = new ArrayList<>();

        private GroupPartitionDictionary(
                int expectedArity,
                @Nullable PartitionPredicate partitionPredicate,
                int excludedCacheSize) {
            checkArgument(excludedCacheSize >= 0, "Excluded cache size cannot be negative.");
            this.expectedArity = expectedArity;
            this.partitionPredicate = partitionPredicate;
            this.excludedCacheSize = excludedCacheSize;
            this.excluded =
                    new LinkedHashMap<ByteArrayKey, Boolean>(16, 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(
                                Map.Entry<ByteArrayKey, Boolean> eldest) {
                            return size() > GroupPartitionDictionary.this.excludedCacheSize;
                        }
                    };
        }

        private @Nullable PartitionState intern(byte[] serialized) {
            checkArgument(serialized != null, "Serialized partition cannot be null.");
            lookup.reset(serialized);
            try {
                PartitionState existing = included.get(lookup);
                if (existing != null) {
                    return existing;
                }
                if (partitionPredicate != null && excluded.get(lookup) != null) {
                    return null;
                }

                byte[] canonical = copyValidatedPartition(expectedArity, serialized);
                BinaryRow partition = SerializationUtils.deserializeBinaryRow(canonical);
                if (partitionPredicate != null && !partitionPredicate.test(partition)) {
                    if (excludedCacheSize > 0) {
                        excluded.put(new ByteArrayKey(canonical), Boolean.TRUE);
                    }
                    return null;
                }

                checkState(
                        partitions.size() < Integer.MAX_VALUE,
                        "Too many partitions in one manifest group.");
                PartitionState created =
                        new PartitionState(partitions.size(), canonical, partition);
                partitions.add(created);
                included.put(new ByteArrayKey(canonical), created);
                return created;
            } finally {
                lookup.clear();
            }
        }

        private PartitionState partition(int partitionId) {
            return partitions.get(partitionId);
        }
    }

    private static byte[] copyValidatedPartition(int expectedArity, byte[] serialized) {
        checkArgument(serialized.length >= 4, "Serialized partition is truncated.");
        int arity =
                ((serialized[0] & 0xFF) << 24)
                        | ((serialized[1] & 0xFF) << 16)
                        | ((serialized[2] & 0xFF) << 8)
                        | (serialized[3] & 0xFF);
        checkArgument(
                arity == expectedArity,
                "Serialized partition has arity %s, expected %s.",
                arity,
                expectedArity);
        int fixedLength = BinaryRow.calculateFixPartSizeInBytes(arity);
        checkArgument(
                serialized.length - 4 >= fixedLength, "Serialized partition payload is truncated.");
        return Arrays.copyOf(serialized, serialized.length);
    }

    private static final class PartitionState {

        private final int id;
        private final byte[] serialized;
        private final BinaryRow partition;
        private long minFirstRowId = Long.MAX_VALUE;
        private @Nullable PrimitiveRowRanges logicalRanges;

        private PartitionState(int id, byte[] serialized, BinaryRow partition) {
            this.id = id;
            this.serialized = serialized;
            this.partition = partition;
        }

        private void setMinFirstRowId(long firstRowId) {
            minFirstRowId = Math.min(minFirstRowId, firstRowId);
        }

        private void select(PrimitiveRowRanges logicalRanges) {
            checkState(this.logicalRanges == null, "Partition is already selected.");
            this.logicalRanges = logicalRanges;
        }

        private PrimitiveRowRanges requiredLogicalRanges() {
            checkState(logicalRanges != null, "Selected partition has no logical row-id ranges.");
            return logicalRanges;
        }
    }

    private static final class SelectedPartition {

        private final BinaryRow partition;
        private long minFirstRowId;
        private PrimitiveRowRanges logicalRanges;

        private SelectedPartition(
                BinaryRow partition, long minFirstRowId, PrimitiveRowRanges logicalRanges) {
            this.partition = partition;
            this.minFirstRowId = minFirstRowId;
            this.logicalRanges = logicalRanges;
        }

        private Pair<Long, Long> removeLargeContiguousRuns(long threshold) {
            checkArgument(threshold > 0, "Skip threshold must be positive.");
            int originalRangeCount = logicalRanges.size();
            int retainedRangeCount = 0;
            long skippedRangeCount = 0L;
            long skippedRowCount = 0L;

            int index = 0;
            while (index < originalRangeCount) {
                int runEnd = contiguousRunEnd(index);
                long start = logicalRanges.start(index);
                long end = logicalRanges.end(runEnd);
                if (rangeCountExceeds(start, end, threshold)) {
                    skippedRangeCount =
                            Math.addExact(skippedRangeCount, (long) runEnd - index + 1L);
                    skippedRowCount =
                            Math.addExact(skippedRowCount, inclusiveRangeCount(start, end));
                } else {
                    retainedRangeCount = Math.addExact(retainedRangeCount, runEnd - index + 1);
                }
                index = runEnd + 1;
            }

            if (skippedRangeCount == 0L) {
                return Pair.of(0L, 0L);
            }

            PrimitiveRowRanges retained = new PrimitiveRowRanges(retainedRangeCount);
            index = 0;
            while (index < originalRangeCount) {
                int runEnd = contiguousRunEnd(index);
                long start = logicalRanges.start(index);
                long end = logicalRanges.end(runEnd);
                if (!rangeCountExceeds(start, end, threshold)) {
                    for (int rangeIndex = index; rangeIndex <= runEnd; rangeIndex++) {
                        retained.add(
                                logicalRanges.start(rangeIndex), logicalRanges.end(rangeIndex));
                    }
                }
                index = runEnd + 1;
            }
            logicalRanges = retained;
            return Pair.of(skippedRangeCount, skippedRowCount);
        }

        private boolean hasFragmentedLogicalRanges() {
            for (int index = 1; index < logicalRanges.size(); index++) {
                if (!adjacent(logicalRanges.end(index - 1), logicalRanges.start(index))) {
                    return true;
                }
            }
            return false;
        }

        private int contiguousRunEnd(int runStart) {
            int runEnd = runStart;
            while (runEnd + 1 < logicalRanges.size()
                    && adjacent(logicalRanges.end(runEnd), logicalRanges.start(runEnd + 1))) {
                runEnd++;
            }
            return runEnd;
        }

        private static boolean adjacent(long leftEnd, long rightStart) {
            return leftEnd != Long.MAX_VALUE && rightStart == leftEnd + 1L;
        }

        private static boolean rangeCountExceeds(long start, long end, long threshold) {
            if (start > Long.MAX_VALUE - threshold) {
                return false;
            }
            return end >= start + threshold;
        }
    }
}
