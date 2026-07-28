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
import org.apache.paimon.manifest.BinaryManifestEntry.ReusableIdentifier;
import org.apache.paimon.manifest.DeletedIdentifierSet;
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
import org.apache.paimon.utils.PrimitiveRowRanges;
import org.apache.paimon.utils.SerializationUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
                    DataFileMeta.WRITE_COLS,
                    DataFileMeta.MAX_SEQUENCE_NUMBER);
    private static final Projection COMPACT_ADD_PROJECTION =
            manifestProjection(
                    false,
                    DataFileMeta.FILE_NAME,
                    DataFileMeta.ROW_COUNT,
                    DataFileMeta.FIRST_ROW_ID,
                    DataFileMeta.WRITE_COLS,
                    DataFileMeta.MAX_SEQUENCE_NUMBER);
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
    private final boolean[] rewrittenManifests;
    private final Map<ByteArrayKey, SelectedPartition> selectedPartitions;
    private long nextManifestGroupOrdinal;
    private long nextRetainedAddScanOrdinal;

    DataEvolutionRowIdAssignmentPlanner(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            List<ManifestFileMeta> manifestMetas) {
        this.table = table;
        this.manifestFile = table.store().manifestFileFactory().create();
        this.partitionPredicate = partitionPredicate;
        this.manifestMetas = manifestMetas;
        this.manifestOrdinals = manifestOrdinals(manifestMetas);
        this.rewrittenManifests = new boolean[manifestMetas.size()];
        this.selectedPartitions = new LinkedHashMap<>();
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
        long manifestGroupOrdinal = nextManifestGroupOrdinal;
        nextManifestGroupOrdinal = Math.addExact(nextManifestGroupOrdinal, 1L);
        GroupState group =
                new GroupState(
                        table.schema().logicalPartitionType().getFieldCount(),
                        partitionPredicate,
                        EXCLUDED_PARTITION_CACHE_SIZE,
                        partitionPredicate == null
                                ? initialLiveFileRangeCapacity(manifestGroup)
                                : 0);
        ReusableIdentifier identifier = new ReusableIdentifier();
        long[] rowRangeScratch = new long[2];

        collectDeletedIdentifiers(manifestGroup, group, identifier);
        collectLiveFileRanges(
                manifestGroup, group, identifier, manifestGroupOrdinal, rowRangeScratch);
        identifier.release();
        group.releaseDeletedIdentifiers();

        List<PartitionState> selections = group.selectFragmentedPartitions();
        for (PartitionState selection : selections) {
            mergeSelectedPartition(selection);
        }
        if (!selections.isEmpty()) {
            markRewrittenManifests(manifestGroup, group, rowRangeScratch);
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
                            manifestMeta.fileName(),
                            manifestMeta.fileSize(),
                            BinaryManifestEntry.DELETE_ENTRY_PROJECTION)) {
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
            long manifestGroupOrdinal,
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
                    manifestFile.scan(
                            manifestMeta.fileName(), manifestMeta.fileSize(), addProjection)) {
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
                    long maxSequenceNumber = file.maxSequenceNumber();
                    long retainedAddScanOrdinal = nextRetainedAddScanOrdinal;
                    nextRetainedAddScanOrdinal = Math.addExact(nextRetainedAddScanOrdinal, 1L);
                    int fileOrder = fileOrder(fileName);
                    partition.considerLegacyOrderKey(
                            manifestGroupOrdinal,
                            rowRangeScratch[0],
                            fileOrder,
                            maxSequenceNumber,
                            fileName,
                            retainedAddScanOrdinal);
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

    private void markRewrittenManifests(
            List<ManifestFileMeta> manifestGroup, GroupState group, long[] rowRangeScratch) {
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            int manifestOrdinal = ordinal(manifestMeta);
            try (CloseableIterator<BinaryManifestEntry> entries =
                    manifestFile.scan(
                            manifestMeta.fileName(), manifestMeta.fileSize(), REWRITE_PROJECTION)) {
                while (entries.hasNext()) {
                    BinaryManifestEntry entry = entries.next();
                    PartitionState partition = group.internPartition(entry.partitionBytes());
                    if (partition == null || partition.logicalRanges == null) {
                        continue;
                    }
                    BinaryDataFileMeta file = entry.file();
                    readRowRange(file, manifestOrdinal, null, rowRangeScratch);
                    if (!partition.logicalRanges.covers(rowRangeScratch[0], rowRangeScratch[1])) {
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

    private Result buildResult() {
        if (selectedPartitions.isEmpty()) {
            return new Result(new int[0], Collections.emptyMap(), 0L);
        }

        List<SelectedPartition> partitions = new ArrayList<>(selectedPartitions.values());
        RecordComparator typedComparator =
                CodeGenUtils.newRecordComparator(
                        table.schema().logicalPartitionType().getFieldTypes());
        partitions.sort(
                (left, right) -> {
                    int comparison = typedComparator.compare(left.partition, right.partition);
                    return comparison != 0
                            ? comparison
                            : left.legacyOrderKey.compareTo(right.legacyOrderKey);
                });

        Map<BinaryRow, RowRangeMappingIndex> mappings = new LinkedHashMap<>();
        long nextOffset = 0L;
        for (SelectedPartition partition : partitions) {
            partition.logicalRanges.normalizeOverlapping();
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
            LegacyPartitionOrderKey legacyOrderKey = selection.requiredLegacyOrderKey();
            selected = new SelectedPartition(selection.partition, legacyOrderKey, logicalRanges);
            selectedPartitions.put(new ByteArrayKey(selection.serialized), selected);
            return;
        }
        LegacyPartitionOrderKey incomingOrderKey = selection.requiredLegacyOrderKey();
        if (incomingOrderKey.compareTo(selected.legacyOrderKey) < 0) {
            selected.legacyOrderKey = incomingOrderKey;
        }
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
        private final DeletedIdentifierSet deletedIdentifiers = new DeletedIdentifierSet();
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
        private @Nullable LegacyPartitionOrderKey legacyOrderKey;
        private @Nullable PrimitiveRowRanges logicalRanges;

        private PartitionState(int id, byte[] serialized, BinaryRow partition) {
            this.id = id;
            this.serialized = serialized;
            this.partition = partition;
        }

        private void considerLegacyOrderKey(
                long manifestGroupOrdinal,
                long firstRowId,
                int fileOrder,
                long maxSequenceNumber,
                BinaryString fileName,
                long retainedAddScanOrdinal) {
            if (legacyOrderKey == null) {
                legacyOrderKey =
                        new LegacyPartitionOrderKey(
                                manifestGroupOrdinal,
                                firstRowId,
                                fileOrder,
                                maxSequenceNumber,
                                fileName.toString(),
                                retainedAddScanOrdinal);
                return;
            }

            int comparison =
                    Long.compare(manifestGroupOrdinal, legacyOrderKey.manifestGroupOrdinal);
            if (comparison == 0) {
                comparison = Long.compare(firstRowId, legacyOrderKey.firstRowId);
            }
            if (comparison == 0) {
                comparison = Integer.compare(fileOrder, legacyOrderKey.fileOrder);
            }
            if (comparison == 0) {
                comparison = Long.compare(legacyOrderKey.maxSequenceNumber, maxSequenceNumber);
            }

            String stableFileName = null;
            if (comparison == 0) {
                stableFileName = fileName.toString();
                comparison = stableFileName.compareTo(legacyOrderKey.fileName);
            }
            if (comparison == 0) {
                comparison =
                        Long.compare(retainedAddScanOrdinal, legacyOrderKey.retainedAddScanOrdinal);
            }
            if (comparison < 0) {
                if (stableFileName == null) {
                    stableFileName = fileName.toString();
                }
                legacyOrderKey =
                        new LegacyPartitionOrderKey(
                                manifestGroupOrdinal,
                                firstRowId,
                                fileOrder,
                                maxSequenceNumber,
                                stableFileName,
                                retainedAddScanOrdinal);
            }
        }

        private LegacyPartitionOrderKey requiredLegacyOrderKey() {
            checkState(
                    legacyOrderKey != null,
                    "Selected partition does not have a retained ADD ordering key.");
            return legacyOrderKey;
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

    private static final class LegacyPartitionOrderKey
            implements Comparable<LegacyPartitionOrderKey> {

        private final long manifestGroupOrdinal;
        private final long firstRowId;
        private final int fileOrder;
        private final long maxSequenceNumber;
        private final String fileName;
        private final long retainedAddScanOrdinal;

        private LegacyPartitionOrderKey(
                long manifestGroupOrdinal,
                long firstRowId,
                int fileOrder,
                long maxSequenceNumber,
                String fileName,
                long retainedAddScanOrdinal) {
            this.manifestGroupOrdinal = manifestGroupOrdinal;
            this.firstRowId = firstRowId;
            this.fileOrder = fileOrder;
            this.maxSequenceNumber = maxSequenceNumber;
            this.fileName = fileName;
            this.retainedAddScanOrdinal = retainedAddScanOrdinal;
        }

        @Override
        public int compareTo(LegacyPartitionOrderKey other) {
            int comparison = Long.compare(manifestGroupOrdinal, other.manifestGroupOrdinal);
            if (comparison != 0) {
                return comparison;
            }
            comparison = Long.compare(firstRowId, other.firstRowId);
            if (comparison != 0) {
                return comparison;
            }
            comparison = Integer.compare(fileOrder, other.fileOrder);
            if (comparison != 0) {
                return comparison;
            }
            comparison = Long.compare(other.maxSequenceNumber, maxSequenceNumber);
            if (comparison != 0) {
                return comparison;
            }
            comparison = fileName.compareTo(other.fileName);
            return comparison != 0
                    ? comparison
                    : Long.compare(retainedAddScanOrdinal, other.retainedAddScanOrdinal);
        }
    }

    private static final class SelectedPartition {

        private final BinaryRow partition;
        private LegacyPartitionOrderKey legacyOrderKey;
        private final PrimitiveRowRanges logicalRanges;

        private SelectedPartition(
                BinaryRow partition,
                LegacyPartitionOrderKey legacyOrderKey,
                PrimitiveRowRanges logicalRanges) {
            this.partition = partition;
            this.legacyOrderKey = legacyOrderKey;
            this.logicalRanges = logicalRanges;
        }
    }
}
