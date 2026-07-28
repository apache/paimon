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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.io.BinaryDataFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BinaryManifestEntry;
import org.apache.paimon.manifest.BinaryManifestEntry.Projection;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    private static final int CURRENT_ENTRY_WORDS = 3;
    private static final int MAX_INITIAL_CURRENT_ENTRIES = 1 << 24;
    private static final long CURRENT_SPECIAL = 1L << 32;
    private static final BinaryString ROW_ID_FIELD =
            BinaryString.fromString(SpecialFields.ROW_ID.name());
    private static final BinaryString BLOB_FILE_SUFFIX = BinaryString.fromString(".blob");
    private static final BinaryString VECTOR_FILE_MARKER = BinaryString.fromString(".vector.");

    private final FileStoreTable table;
    private final @Nullable PartitionPredicate partitionPredicate;
    private final List<ManifestFileMeta> manifestMetas;
    private final Map<String, Integer> manifestOrdinals;
    private final boolean[] rewrittenManifests;
    private final Projection deleteProjection;
    private final Projection addIdentifierProjection;
    private final Projection compactAddProjection;
    private final Projection rewriteProjection;
    private final Map<ByteArrayKey, SelectedPartition> selectedPartitions;
    private long nextManifestGroupOrdinal;
    private long nextRetainedAddScanOrdinal;

    DataEvolutionRowIdAssignmentPlanner(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            List<ManifestFileMeta> manifestMetas) {
        this.table = table;
        this.partitionPredicate = partitionPredicate;
        this.manifestMetas = manifestMetas;
        this.manifestOrdinals = manifestOrdinals(manifestMetas);
        this.rewrittenManifests = new boolean[manifestMetas.size()];
        FileFormat format = FileFormat.manifestFormat(table.coreOptions());
        this.deleteProjection =
                manifestProjection(
                        format,
                        true,
                        DataFileMeta.FILE_NAME,
                        DataFileMeta.LEVEL,
                        DataFileMeta.EXTRA_FILES,
                        DataFileMeta.EMBEDDED_FILE_INDEX,
                        DataFileMeta.EXTERNAL_PATH);
        this.addIdentifierProjection =
                manifestProjection(
                        format,
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
        this.compactAddProjection =
                manifestProjection(
                        format,
                        false,
                        DataFileMeta.FILE_NAME,
                        DataFileMeta.ROW_COUNT,
                        DataFileMeta.FIRST_ROW_ID,
                        DataFileMeta.WRITE_COLS,
                        DataFileMeta.MAX_SEQUENCE_NUMBER);
        this.rewriteProjection =
                manifestProjection(
                        format,
                        false,
                        DataFileMeta.ROW_COUNT,
                        DataFileMeta.FIRST_ROW_ID,
                        DataFileMeta.WRITE_COLS);
        this.selectedPartitions = new LinkedHashMap<>();
    }

    private static Projection manifestProjection(
            FileFormat format, boolean includeBucket, String... projectedFileFields) {
        RowType manifestType = VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);
        List<DataField> fields = new ArrayList<>();
        fields.add(manifestType.getField(ManifestEntry.KIND));
        fields.add(manifestType.getField(ManifestEntry.PARTITION));
        if (includeBucket) {
            fields.add(manifestType.getField(ManifestEntry.BUCKET));
        }
        fields.add(
                manifestType
                        .getField(ManifestEntry.FILE)
                        .newType(DataFileMeta.SCHEMA.project(projectedFileFields)));
        return Projection.create(format, new RowType(false, fields));
    }

    void planGroup(List<ManifestFileMeta> manifestGroup) {
        long manifestGroupOrdinal = nextManifestGroupOrdinal;
        nextManifestGroupOrdinal = Math.addExact(nextManifestGroupOrdinal, 1L);
        GroupState group =
                new GroupState(
                        table.schema().logicalPartitionType().getFieldCount(),
                        partitionPredicate,
                        EXCLUDED_PARTITION_CACHE_SIZE,
                        partitionPredicate == null
                                ? initialCurrentEntryCapacity(manifestGroup)
                                : 0);
        IdentifierScratch identifier = new IdentifierScratch();
        long[] rowRangeScratch = new long[2];

        for (ManifestFileMeta manifestMeta : manifestGroup) {
            if (manifestMeta.numDeletedFiles() <= 0) {
                continue;
            }
            scan(
                    manifestMeta,
                    deleteProjection,
                    entry -> {
                        if (!entry.isDelete()) {
                            return true;
                        }
                        PartitionState partition = group.internPartition(entry.partitionBytes());
                        if (partition == null) {
                            return true;
                        }
                        identifier.encode(entry);
                        group.deletedIdentifiers.add(
                                partition.id, identifier.bytes(), identifier.length());
                        return true;
                    });
        }

        Projection addProjection =
                group.deletedIdentifiers.isEmpty() ? compactAddProjection : addIdentifierProjection;
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            if (manifestMeta.numAddedFiles() <= 0) {
                continue;
            }
            int manifestOrdinal = ordinal(manifestMeta);
            scan(
                    manifestMeta,
                    addProjection,
                    entry -> {
                        if (!entry.isAdd()) {
                            return true;
                        }
                        PartitionState partition = group.internPartition(entry.partitionBytes());
                        if (partition == null) {
                            return true;
                        }
                        if (!group.deletedIdentifiers.isEmpty()) {
                            identifier.encode(entry);
                            if (group.deletedIdentifiers.contains(
                                    partition.id, identifier.bytes(), identifier.length())) {
                                return true;
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
                        partition.considerLegacyOrderKey(
                                manifestGroupOrdinal,
                                rowRangeScratch[0],
                                fileOrder(fileName),
                                maxSequenceNumber,
                                fileName,
                                retainedAddScanOrdinal);
                        group.currentEntries.add(
                                partition.id,
                                isSpecialFile(fileName),
                                rowRangeScratch[0],
                                inclusiveRangeCount(rowRangeScratch[0], rowRangeScratch[1]));
                        return true;
                    });
        }
        identifier.release();
        group.releaseDeletedIdentifiers();

        GroupSelection[] groupSelections = group.finishAddPass();
        boolean selected = false;
        for (GroupSelection selection : groupSelections) {
            if (selection == null) {
                continue;
            }
            selected = true;
            mergeSelectedPartition(selection);
        }
        if (!selected) {
            return;
        }

        for (ManifestFileMeta manifestMeta : manifestGroup) {
            int manifestOrdinal = ordinal(manifestMeta);
            scan(
                    manifestMeta,
                    rewriteProjection,
                    entry -> {
                        PartitionState partition = group.internPartition(entry.partitionBytes());
                        if (partition == null) {
                            return true;
                        }
                        if (partition.id >= groupSelections.length) {
                            return true;
                        }
                        GroupSelection selection = groupSelections[partition.id];
                        if (selection == null) {
                            return true;
                        }
                        BinaryDataFileMeta file = entry.file();
                        readRowRange(file, manifestOrdinal, null, rowRangeScratch);
                        if (!rangesFullyCover(
                                rowRangeScratch[0], rowRangeScratch[1], selection.logicalRanges)) {
                            return true;
                        }
                        checkState(
                                !file.containsWriteColumn(ROW_ID_FIELD),
                                "Cannot reassign an entry in manifest '%s' because it physically stores the row-id field.",
                                manifestMeta.fileName());
                        rewrittenManifests[manifestOrdinal] = true;
                        return false;
                    });
        }
    }

    Result buildResult() {
        if (selectedPartitions.isEmpty()) {
            return new Result(new int[0], Collections.emptyList(), 0L);
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

        List<PartitionMapping> mappings = new ArrayList<>(partitions.size());
        long nextOffset = 0L;
        for (SelectedPartition partition : partitions) {
            partition.logicalRanges.normalizeOverlapping();
            int rangeCount = partition.logicalRanges.size();
            checkState(rangeCount > 0, "Selected partition has no logical row-id ranges.");
            OwnedPrimitiveRanges ownedRanges = partition.logicalRanges.takeOwned();
            long[] oldStarts = ownedRanges.starts;
            long[] oldEnds = ownedRanges.ends;
            long[] newStarts = new long[rangeCount];
            for (int i = 0; i < rangeCount; i++) {
                newStarts[i] = nextOffset;
                nextOffset =
                        Math.addExact(nextOffset, inclusiveRangeCount(oldStarts[i], oldEnds[i]));
            }
            mappings.add(new PartitionMapping(partition.partition, oldStarts, oldEnds, newStarts));
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

    private void mergeSelectedPartition(GroupSelection selection) {
        ByteArrayLookupKey lookup = new ByteArrayLookupKey(selection.partition.serialized);
        SelectedPartition selected = selectedPartitions.get(lookup);
        if (selected == null) {
            LegacyPartitionOrderKey legacyOrderKey = selection.partition.requiredLegacyOrderKey();
            selected =
                    new SelectedPartition(
                            selection.partition.serialized,
                            selection.partition.partition,
                            legacyOrderKey,
                            selection.logicalRanges);
            selectedPartitions.put(new ByteArrayKey(selection.partition.serialized), selected);
            return;
        }
        LegacyPartitionOrderKey incomingOrderKey = selection.partition.requiredLegacyOrderKey();
        if (incomingOrderKey.compareTo(selected.legacyOrderKey) < 0) {
            selected.legacyOrderKey = incomingOrderKey;
        }
        selected.logicalRanges.append(selection.logicalRanges);
        selected.logicalRanges.normalizeOverlapping();
    }

    private void scan(
            ManifestFileMeta manifestMeta, Projection projection, ProjectedRowVisitor visitor) {
        BinaryManifestEntry entry = projection.createEntry();
        try (RecordReader<InternalRow> reader =
                FileUtils.createFormatReader(
                        table.fileIO(),
                        projection.readerFactory(),
                        table.store().pathFactory().toManifestFilePath(manifestMeta.fileName()),
                        manifestMeta.fileSize())) {
            boolean keepReading = true;
            while (keepReading) {
                RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
                if (batch == null) {
                    break;
                }
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        if (!visitor.visit(entry.replace(row))) {
                            keepReading = false;
                            break;
                        }
                    }
                } finally {
                    entry.clear();
                    batch.releaseBatch();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read manifest file " + manifestMeta.fileName(), e);
        }
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

    private static boolean isSpecialFile(BinaryString fileName) {
        return fileOrder(fileName) != 0;
    }

    private int ordinal(ManifestFileMeta manifestMeta) {
        Integer ordinal = manifestOrdinals.get(manifestMeta.fileName());
        checkArgument(
                ordinal != null,
                "Planning group references unknown manifest '%s'.",
                manifestMeta.fileName());
        return ordinal;
    }

    private static int initialCurrentEntryCapacity(List<ManifestFileMeta> manifestGroup) {
        long addedCount = 0L;
        long deletedCount = 0L;
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            checkState(
                    manifestMeta.numAddedFiles() >= 0 && manifestMeta.numDeletedFiles() >= 0,
                    "Manifest file counts cannot be negative.");
            addedCount = Math.addExact(addedCount, manifestMeta.numAddedFiles());
            deletedCount = Math.addExact(deletedCount, manifestMeta.numDeletedFiles());
        }
        return initialCurrentEntryCapacity(addedCount, deletedCount);
    }

    static int initialCurrentEntryCapacity(long addedCount, long deletedCount) {
        checkArgument(addedCount >= 0, "Added entry count cannot be negative.");
        checkArgument(deletedCount >= 0, "Deleted entry count cannot be negative.");
        // Counts are only a sizing hint: DELETE entries may be duplicated or may not match an
        // ADD
        // in this group. Estimate the live set, cap the eager allocation, and let
        // CurrentEntries
        // grow if the actual number of retained ADD entries is larger.
        long estimatedLiveCount = addedCount > deletedCount ? addedCount - deletedCount : 0L;
        return (int) Math.min(estimatedLiveCount, MAX_INITIAL_CURRENT_ENTRIES);
    }

    void validateGroups(List<List<ManifestFileMeta>> groups) {
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

    private static boolean rangesFullyCover(
            long rangeStart, long rangeEnd, PrimitiveRangeBuffer mappings) {
        long cursor = rangeStart;
        for (int i = 0; i < mappings.size(); i++) {
            long mappingStart = mappings.start(i);
            long mappingEnd = mappings.end(i);
            if (mappingEnd < cursor) {
                continue;
            }
            if (mappingStart > cursor) {
                return false;
            }
            long segmentEnd = Math.min(mappingEnd, rangeEnd);
            if (segmentEnd == rangeEnd) {
                return true;
            }
            if (segmentEnd == Long.MAX_VALUE) {
                return false;
            }
            cursor = segmentEnd + 1L;
        }
        return false;
    }

    private static long inclusiveRangeCount(long start, long end) {
        return Math.addExact(Math.subtractExact(end, start), 1L);
    }

    /** Compact planner result. All new row starts are relative to the snapshot's next row ID. */
    static final class Result {

        final int[] manifestOrdinals;
        final List<PartitionMapping> partitionMappings;
        final long totalOffset;

        private Result(
                int[] manifestOrdinals,
                List<PartitionMapping> partitionMappings,
                long totalOffset) {
            this.manifestOrdinals = manifestOrdinals;
            this.partitionMappings =
                    Collections.unmodifiableList(new ArrayList<>(partitionMappings));
            this.totalOffset = totalOffset;
        }

        boolean isEmpty() {
            return partitionMappings.isEmpty();
        }
    }

    /** Mapping arrays for one partition. Elements at the same index form one mapping. */
    static final class PartitionMapping {

        final BinaryRow partition;
        final long[] oldStarts;
        final long[] oldEnds;
        final long[] newRelativeStarts;

        private PartitionMapping(
                BinaryRow partition, long[] oldStarts, long[] oldEnds, long[] newRelativeStarts) {
            this.partition = partition;
            this.oldStarts = oldStarts;
            this.oldEnds = oldEnds;
            this.newRelativeStarts = newRelativeStarts;
        }
    }

    private interface ProjectedRowVisitor {

        boolean visit(BinaryManifestEntry entry);
    }

    private static final class GroupState {

        private final GroupPartitionDictionary partitions;
        private final DeletedIdentifierSet deletedIdentifiers = new DeletedIdentifierSet();
        private final CurrentEntries currentEntries;

        private GroupState(
                int partitionArity,
                @Nullable PartitionPredicate partitionPredicate,
                int excludedPartitionCacheSize,
                int expectedAddEntryCount) {
            this.partitions =
                    new GroupPartitionDictionary(
                            partitionArity, partitionPredicate, excludedPartitionCacheSize);
            this.currentEntries = new CurrentEntries(expectedAddEntryCount);
        }

        private @Nullable PartitionState internPartition(byte[] serialized) {
            return partitions.intern(serialized);
        }

        private void releaseDeletedIdentifiers() {
            deletedIdentifiers.release();
        }

        private GroupSelection[] finishAddPass() {
            currentEntries.sort();
            GroupSelection[] selections = new GroupSelection[partitions.partitionCount()];
            long[] rangeScratch = new long[2];
            int groupStart = 0;
            while (groupStart < currentEntries.size()) {
                int partitionId = currentEntries.partitionId(groupStart);
                int groupEnd = groupStart + 1;
                while (groupEnd < currentEntries.size()
                        && currentEntries.partitionId(groupEnd) == partitionId) {
                    groupEnd++;
                }
                int rangeScan =
                        currentEntries.scanLogicalRanges(groupStart, groupEnd, rangeScratch);
                if (rangeScan > 0) {
                    PrimitiveRangeBuffer logicalRanges =
                            currentEntries.materializeLogicalRanges(
                                    groupStart, groupEnd, rangeScan, rangeScratch);
                    selections[partitionId] =
                            new GroupSelection(partitions.partition(partitionId), logicalRanges);
                }
                groupStart = groupEnd;
            }
            currentEntries.release();
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

        private int partitionCount() {
            return partitions.size();
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

    private static final class GroupSelection {

        private final PartitionState partition;
        private final PrimitiveRangeBuffer logicalRanges;

        private GroupSelection(PartitionState partition, PrimitiveRangeBuffer logicalRanges) {
            this.partition = partition;
            this.logicalRanges = logicalRanges;
        }
    }

    private static final class SelectedPartition {

        private final byte[] serializedPartition;
        private final BinaryRow partition;
        private LegacyPartitionOrderKey legacyOrderKey;
        private final PrimitiveRangeBuffer logicalRanges;

        private SelectedPartition(
                byte[] serializedPartition,
                BinaryRow partition,
                LegacyPartitionOrderKey legacyOrderKey,
                PrimitiveRangeBuffer logicalRanges) {
            this.serializedPartition = serializedPartition;
            this.partition = partition;
            this.legacyOrderKey = legacyOrderKey;
            this.logicalRanges = logicalRanges;
        }
    }

    /** Object-free storage for current entries. */
    static final class CurrentEntries {

        private long[] words;
        private int size;

        CurrentEntries() {
            this(0);
        }

        CurrentEntries(int expectedEntries) {
            checkArgument(expectedEntries >= 0, "Expected current entry count cannot be negative.");
            this.words = new long[Math.multiplyExact(expectedEntries, CURRENT_ENTRY_WORDS)];
        }

        void add(int partitionId, boolean special, long firstRowId, long rowCount) {
            checkArgument(partitionId >= 0, "Partition id cannot be negative.");
            checkArgument(rowCount > 0, "Row count must be positive.");
            Math.addExact(firstRowId, rowCount - 1L);
            ensureCapacity(Math.addExact(size, 1));
            int offset = size * CURRENT_ENTRY_WORDS;
            words[offset] = Integer.toUnsignedLong(partitionId) | (special ? CURRENT_SPECIAL : 0L);
            words[offset + 1] = firstRowId;
            words[offset + 2] = rowCount;
            size++;
        }

        int size() {
            return size;
        }

        int retainedWordCount() {
            return words.length;
        }

        int usedWordCount() {
            return size * CURRENT_ENTRY_WORDS;
        }

        private void release() {
            words = new long[0];
            size = 0;
        }

        int partitionId(int index) {
            return (int) words[offset(index)];
        }

        private boolean special(int index) {
            return (words[offset(index)] & CURRENT_SPECIAL) != 0;
        }

        private long firstRowId(int index) {
            return words[offset(index) + 1];
        }

        private long rowCount(int index) {
            return words[offset(index) + 2];
        }

        private long lastRowId(int index) {
            return firstRowId(index) + rowCount(index) - 1L;
        }

        private int offset(int index) {
            checkArgument(index >= 0 && index < size, "Current entry index is out of bounds.");
            return index * CURRENT_ENTRY_WORDS;
        }

        private void ensureCapacity(int requiredEntries) {
            long requiredWords = (long) requiredEntries * CURRENT_ENTRY_WORDS;
            checkState(
                    requiredWords <= Integer.MAX_VALUE,
                    "Too many current entries in one manifest group.");
            if (requiredWords <= words.length) {
                return;
            }
            int newLength = Math.max(48, words.length);
            while (newLength < requiredWords) {
                int grown = newLength + (newLength >>> 1);
                if (grown <= newLength || grown > Integer.MAX_VALUE) {
                    newLength = (int) requiredWords;
                    break;
                }
                newLength = grown;
            }
            words = Arrays.copyOf(words, newLength);
        }

        private void sort() {
            if (size > 1) {
                sort(0, size - 1);
            }
        }

        private void sort(int left, int right) {
            while (left < right) {
                int middle = left + ((right - left) >>> 1);
                long pivotPartition = words[middle * CURRENT_ENTRY_WORDS];
                long pivotFirst = words[middle * CURRENT_ENTRY_WORDS + 1];
                long pivotCount = words[middle * CURRENT_ENTRY_WORDS + 2];
                int lower = left;
                int current = left;
                int upper = right;
                while (current <= upper) {
                    int comparison = compare(current, pivotPartition, pivotFirst, pivotCount);
                    if (comparison < 0) {
                        swap(lower++, current++);
                    } else if (comparison > 0) {
                        swap(current, upper--);
                    } else {
                        current++;
                    }
                }

                if (lower - left < right - upper) {
                    if (left < lower - 1) {
                        sort(left, lower - 1);
                    }
                    left = upper + 1;
                } else {
                    if (upper + 1 < right) {
                        sort(upper + 1, right);
                    }
                    right = lower - 1;
                }
            }
        }

        private int compare(int index, long pivotPartition, long pivotFirst, long pivotCount) {
            int offset = index * CURRENT_ENTRY_WORDS;
            int result = Long.compare(words[offset] & 0xFFFF_FFFFL, pivotPartition & 0xFFFF_FFFFL);
            if (result != 0) {
                return result;
            }
            result = Long.compare(words[offset + 1], pivotFirst);
            if (result != 0) {
                return result;
            }
            long end = words[offset + 1] + words[offset + 2] - 1L;
            long pivotEnd = pivotFirst + pivotCount - 1L;
            return Long.compare(end, pivotEnd);
        }

        private void swap(int left, int right) {
            if (left == right) {
                return;
            }
            int leftOffset = left * CURRENT_ENTRY_WORDS;
            int rightOffset = right * CURRENT_ENTRY_WORDS;
            for (int i = 0; i < CURRENT_ENTRY_WORDS; i++) {
                long value = words[leftOffset + i];
                words[leftOffset + i] = words[rightOffset + i];
                words[rightOffset + i] = value;
            }
        }

        /**
         * Scans logical ranges without retaining one object (or even one primitive pair) per range.
         *
         * <p>The absolute return value is the number of logical ranges. A negative result means
         * that all logical ranges are contiguous and therefore this partition does not need a plan.
         * A positive result means that the ranges are fragmented and need materialization.
         */
        private int scanLogicalRanges(int from, int to, long[] rangeScratch) {
            checkArgument(from >= 0 && from < to && to <= size, "Invalid entry slice.");
            int overlapStart = from;
            long currentEnd = lastRowId(from);
            int rangeCount = 0;
            boolean contiguous = true;
            boolean hasPrevious = false;
            long previousEnd = 0L;
            for (int i = from + 1; i < to; i++) {
                if (firstRowId(i) <= currentEnd) {
                    currentEnd = Math.max(currentEnd, lastRowId(i));
                } else {
                    computeLogicalRange(overlapStart, i, rangeScratch);
                    rangeCount++;
                    if (hasPrevious
                            && (previousEnd == Long.MAX_VALUE
                                    || rangeScratch[0] != previousEnd + 1L)) {
                        contiguous = false;
                    }
                    previousEnd = rangeScratch[1];
                    hasPrevious = true;
                    overlapStart = i;
                    currentEnd = lastRowId(i);
                }
            }
            computeLogicalRange(overlapStart, to, rangeScratch);
            rangeCount++;
            if (hasPrevious
                    && (previousEnd == Long.MAX_VALUE || rangeScratch[0] != previousEnd + 1L)) {
                contiguous = false;
            }
            return contiguous ? -rangeCount : rangeCount;
        }

        private PrimitiveRangeBuffer materializeLogicalRanges(
                int from, int to, int expectedRangeCount, long[] rangeScratch) {
            checkArgument(
                    from >= 0 && from < to && to <= size && expectedRangeCount > 0,
                    "Invalid fragmented entry slice.");
            PrimitiveRangeBuffer ranges = new PrimitiveRangeBuffer(expectedRangeCount);
            int overlapStart = from;
            long currentEnd = lastRowId(from);
            for (int i = from + 1; i < to; i++) {
                if (firstRowId(i) <= currentEnd) {
                    currentEnd = Math.max(currentEnd, lastRowId(i));
                } else {
                    computeLogicalRange(overlapStart, i, rangeScratch);
                    ranges.add(rangeScratch[0], rangeScratch[1]);
                    overlapStart = i;
                    currentEnd = lastRowId(i);
                }
            }
            computeLogicalRange(overlapStart, to, rangeScratch);
            ranges.add(rangeScratch[0], rangeScratch[1]);
            checkState(
                    ranges.size() == expectedRangeCount,
                    "Logical range count changed between scan and materialization.");
            return ranges;
        }

        private void computeLogicalRange(int from, int to, long[] result) {
            boolean hasOrdinary = false;
            long ordinaryStart = 0L;
            long ordinaryEnd = 0L;
            long spanningStart = Long.MAX_VALUE;
            long spanningEnd = Long.MIN_VALUE;
            for (int i = from; i < to; i++) {
                long start = firstRowId(i);
                long end = lastRowId(i);
                spanningStart = Math.min(spanningStart, start);
                spanningEnd = Math.max(spanningEnd, end);
                if (!special(i)) {
                    checkState(
                            !hasOrdinary || (ordinaryStart == start && ordinaryEnd == end),
                            "Data files in one overlapping row-id group must have the same row-id range.");
                    ordinaryStart = start;
                    ordinaryEnd = end;
                    hasOrdinary = true;
                }
            }
            long logicalStart = hasOrdinary ? ordinaryStart : spanningStart;
            long logicalEnd = hasOrdinary ? ordinaryEnd : spanningEnd;
            for (int i = from; i < to; i++) {
                checkState(
                        firstRowId(i) >= logicalStart && lastRowId(i) <= logicalEnd,
                        "File row-id range is outside its logical row-id range.");
            }
            result[0] = logicalStart;
            result[1] = logicalEnd;
        }

        @Nullable
        PrimitiveRangeBuffer selectedRangesForTesting() {
            checkState(size > 0, "Cannot inspect an empty current-entry buffer.");
            sort();
            int partitionId = partitionId(0);
            for (int i = 1; i < size; i++) {
                checkState(
                        partitionId(i) == partitionId,
                        "The structural range test helper requires one partition.");
            }
            long[] rangeScratch = new long[2];
            int rangeScan = scanLogicalRanges(0, size, rangeScratch);
            return rangeScan < 0
                    ? null
                    : materializeLogicalRanges(0, size, rangeScan, rangeScratch);
        }
    }

    /** Compact, collision-safe set backed by primitive arrays and one identifier byte arena. */
    static final class DeletedIdentifierSet {

        private static final float LOAD_FACTOR = 0.75f;

        private int[] buckets = filledWithMinusOne(16);
        private long[] hashes = new long[16];
        private int[] partitionIds = new int[16];
        private int[] offsets = new int[16];
        private int[] lengths = new int[16];
        private int[] next = new int[16];
        private byte[] arena = new byte[256];
        private int arenaSize;
        private int size;

        boolean isEmpty() {
            return size == 0;
        }

        int size() {
            return size;
        }

        int retainedIdentifierBytes() {
            return arenaSize;
        }

        private void release() {
            buckets = filledWithMinusOne(16);
            hashes = new long[0];
            partitionIds = new int[0];
            offsets = new int[0];
            lengths = new int[0];
            next = new int[0];
            arena = new byte[0];
            arenaSize = 0;
            size = 0;
        }

        void add(int partitionId, byte[] identifier, int length) {
            checkIdentifier(identifier, length);
            long hash = hash(partitionId, identifier, length);
            if (contains(partitionId, identifier, length, hash)) {
                return;
            }
            if (size + 1 > (int) (buckets.length * LOAD_FACTOR)) {
                growBuckets();
            }
            ensureEntryCapacity(size + 1);
            ensureArenaCapacity(length);
            int offset = arenaSize;
            System.arraycopy(identifier, 0, arena, offset, length);
            arenaSize = Math.addExact(arenaSize, length);

            int bucket = bucket(hash);
            hashes[size] = hash;
            partitionIds[size] = partitionId;
            offsets[size] = offset;
            lengths[size] = length;
            next[size] = buckets[bucket];
            buckets[bucket] = size;
            size++;
        }

        boolean contains(int partitionId, byte[] identifier, int length) {
            checkIdentifier(identifier, length);
            return contains(partitionId, identifier, length, hash(partitionId, identifier, length));
        }

        private boolean contains(int partitionId, byte[] identifier, int length, long hash) {
            for (int entry = buckets[bucket(hash)]; entry >= 0; entry = next[entry]) {
                if (hashes[entry] == hash
                        && partitionIds[entry] == partitionId
                        && lengths[entry] == length
                        && bytesEqual(arena, offsets[entry], identifier, length)) {
                    return true;
                }
            }
            return false;
        }

        private void growBuckets() {
            checkState(
                    buckets.length < (1 << 30),
                    "Too many deleted identifiers in one manifest group.");
            int[] grown = filledWithMinusOne(buckets.length << 1);
            for (int entry = 0; entry < size; entry++) {
                int bucket = bucket(hashes[entry], grown.length);
                next[entry] = grown[bucket];
                grown[bucket] = entry;
            }
            buckets = grown;
        }

        private void ensureEntryCapacity(int required) {
            if (required <= hashes.length) {
                return;
            }
            int grown = Math.max(required, hashes.length + (hashes.length >>> 1));
            hashes = Arrays.copyOf(hashes, grown);
            partitionIds = Arrays.copyOf(partitionIds, grown);
            offsets = Arrays.copyOf(offsets, grown);
            lengths = Arrays.copyOf(lengths, grown);
            next = Arrays.copyOf(next, grown);
        }

        private void ensureArenaCapacity(int additional) {
            int required;
            try {
                required = Math.addExact(arenaSize, additional);
            } catch (ArithmeticException e) {
                throw new IllegalStateException(
                        "Deleted identifier arena exceeds the Java array limit.", e);
            }
            if (required <= arena.length) {
                return;
            }
            int grown = Math.max(required, arena.length + (arena.length >>> 1));
            if (grown < 0) {
                grown = required;
            }
            arena = Arrays.copyOf(arena, grown);
        }

        private int bucket(long hash) {
            return bucket(hash, buckets.length);
        }

        private static int bucket(long hash, int bucketCount) {
            return ((int) (hash ^ (hash >>> 32))) & (bucketCount - 1);
        }

        private static long hash(int partitionId, byte[] bytes, int length) {
            long hash = 0xcbf29ce484222325L;
            hash ^= Integer.toUnsignedLong(partitionId);
            hash *= 0x100000001b3L;
            for (int i = 0; i < length; i++) {
                hash ^= bytes[i] & 0xFFL;
                hash *= 0x100000001b3L;
            }
            return hash;
        }

        private static boolean bytesEqual(byte[] left, int leftOffset, byte[] right, int length) {
            for (int i = 0; i < length; i++) {
                if (left[leftOffset + i] != right[i]) {
                    return false;
                }
            }
            return true;
        }

        private static void checkIdentifier(byte[] identifier, int length) {
            checkArgument(identifier != null, "Identifier bytes cannot be null.");
            checkArgument(
                    length >= 0 && length <= identifier.length,
                    "Invalid identifier length %s.",
                    length);
        }

        private static int[] filledWithMinusOne(int length) {
            int[] values = new int[length];
            Arrays.fill(values, -1);
            return values;
        }
    }

    private static final class IdentifierScratch {

        private byte[] bytes = new byte[256];
        private int length;

        private void encode(BinaryManifestEntry entry) {
            length = 0;
            putInt(entry.bucket());
            BinaryDataFileMeta file = entry.file();
            putInt(file.level());
            putString(file.fileNameBinary());

            int extraFileCount = file.extraFileCount();
            putInt(extraFileCount);
            for (int i = 0; i < extraFileCount; i++) {
                putString(file.extraFile(i));
            }

            if (!file.hasEmbeddedIndex()) {
                putInt(-1);
            } else {
                putBytes(file.embeddedIndex());
            }
            if (!file.hasExternalPath()) {
                putInt(-1);
            } else {
                putString(file.externalPathBinary());
            }
        }

        private byte[] bytes() {
            return bytes;
        }

        private int length() {
            return length;
        }

        private void release() {
            bytes = new byte[0];
            length = 0;
        }

        private void putString(BinaryString value) {
            checkState(value != null, "Manifest string field cannot be null.");
            int valueLength = value.getSizeInBytes();
            putInt(valueLength);
            ensureCapacity(valueLength);
            MemorySegmentUtils.copyToBytes(
                    value.getSegments(), value.getOffset(), bytes, length, valueLength);
            length += valueLength;
        }

        private void putBytes(byte[] value) {
            checkState(value != null, "Manifest binary field cannot be null.");
            putInt(value.length);
            ensureCapacity(value.length);
            System.arraycopy(value, 0, bytes, length, value.length);
            length += value.length;
        }

        private void putInt(int value) {
            ensureCapacity(Integer.BYTES);
            bytes[length++] = (byte) (value >>> 24);
            bytes[length++] = (byte) (value >>> 16);
            bytes[length++] = (byte) (value >>> 8);
            bytes[length++] = (byte) value;
        }

        private void ensureCapacity(int additional) {
            int required = Math.addExact(length, additional);
            if (required <= bytes.length) {
                return;
            }
            int grown = Math.max(required, bytes.length + (bytes.length >>> 1));
            bytes = Arrays.copyOf(bytes, grown);
        }
    }

    /**
     * Object-free logical range storage.
     *
     * <p>Starts and ends are kept in separate primitive arrays so that the planner can transfer
     * ownership of both arrays directly into the final mapping. The common one-group path allocates
     * the exact range count and requires no copy during that transfer.
     */
    static final class PrimitiveRangeBuffer {

        private long[] starts;
        private long[] ends;
        private int size;
        private boolean sorted = true;

        private PrimitiveRangeBuffer(int expectedRanges) {
            checkArgument(expectedRanges >= 0, "Expected range count cannot be negative.");
            starts = new long[expectedRanges];
            ends = new long[expectedRanges];
        }

        int size() {
            return size;
        }

        int retainedWordCount() {
            return Math.addExact(starts.length, ends.length);
        }

        long start(int index) {
            checkIndex(index);
            return starts[index];
        }

        long end(int index) {
            checkIndex(index);
            return ends[index];
        }

        private void add(long start, long end) {
            checkArgument(start <= end, "Invalid row-id range [%s, %s].", start, end);
            ensureCapacity(Math.addExact(size, 1));
            if (size > 0 && compare(starts[size - 1], ends[size - 1], start, end) > 0) {
                sorted = false;
            }
            starts[size] = start;
            ends[size] = end;
            size++;
        }

        private void append(PrimitiveRangeBuffer other) {
            checkArgument(other != null, "Ranges to append cannot be null.");
            if (other.size == 0) {
                return;
            }
            int oldSize = size;
            int combinedSize = Math.addExact(size, other.size);
            ensureCapacity(combinedSize);
            if (oldSize > 0
                    && compare(
                                    starts[oldSize - 1],
                                    ends[oldSize - 1],
                                    other.starts[0],
                                    other.ends[0])
                            > 0) {
                sorted = false;
            }
            sorted &= other.sorted;
            System.arraycopy(other.starts, 0, starts, oldSize, other.size);
            System.arraycopy(other.ends, 0, ends, oldSize, other.size);
            size = combinedSize;
        }

        private void normalizeOverlapping() {
            if (size <= 1) {
                sorted = true;
                return;
            }
            if (!sorted) {
                sort(0, size - 1);
                sorted = true;
            }
            int writeIndex = 0;
            for (int readIndex = 1; readIndex < size; readIndex++) {
                if (starts[readIndex] <= ends[writeIndex]) {
                    ends[writeIndex] = Math.max(ends[writeIndex], ends[readIndex]);
                } else {
                    writeIndex++;
                    starts[writeIndex] = starts[readIndex];
                    ends[writeIndex] = ends[readIndex];
                }
            }
            size = writeIndex + 1;
        }

        private OwnedPrimitiveRanges takeOwned() {
            long[] ownedStarts = starts.length == size ? starts : Arrays.copyOf(starts, size);
            long[] ownedEnds = ends.length == size ? ends : Arrays.copyOf(ends, size);
            starts = new long[0];
            ends = new long[0];
            size = 0;
            sorted = true;
            return new OwnedPrimitiveRanges(ownedStarts, ownedEnds);
        }

        private void ensureCapacity(int required) {
            if (required <= starts.length) {
                return;
            }
            int grown = Math.max(16, starts.length);
            while (grown < required) {
                int next = grown + (grown >>> 1);
                if (next <= grown || next < 0) {
                    grown = required;
                    break;
                }
                grown = next;
            }
            starts = Arrays.copyOf(starts, grown);
            ends = Arrays.copyOf(ends, grown);
        }

        private void sort(int left, int right) {
            while (left < right) {
                int middle = left + ((right - left) >>> 1);
                long pivotStart = starts[middle];
                long pivotEnd = ends[middle];
                int lower = left;
                int current = left;
                int upper = right;
                while (current <= upper) {
                    int comparison = compare(starts[current], ends[current], pivotStart, pivotEnd);
                    if (comparison < 0) {
                        swap(lower++, current++);
                    } else if (comparison > 0) {
                        swap(current, upper--);
                    } else {
                        current++;
                    }
                }

                if (lower - left < right - upper) {
                    if (left < lower - 1) {
                        sort(left, lower - 1);
                    }
                    left = upper + 1;
                } else {
                    if (upper + 1 < right) {
                        sort(upper + 1, right);
                    }
                    right = lower - 1;
                }
            }
        }

        private void swap(int left, int right) {
            if (left == right) {
                return;
            }
            long start = starts[left];
            long end = ends[left];
            starts[left] = starts[right];
            ends[left] = ends[right];
            starts[right] = start;
            ends[right] = end;
        }

        private void checkIndex(int index) {
            checkArgument(index >= 0 && index < size, "Logical range index is out of bounds.");
        }

        private static int compare(long leftStart, long leftEnd, long rightStart, long rightEnd) {
            int result = Long.compare(leftStart, rightStart);
            return result == 0 ? Long.compare(leftEnd, rightEnd) : result;
        }
    }

    private static final class OwnedPrimitiveRanges {

        private final long[] starts;
        private final long[] ends;

        private OwnedPrimitiveRanges(long[] starts, long[] ends) {
            this.starts = starts;
            this.ends = ends;
        }
    }

    private static final class ByteArrayKey {

        private final byte[] bytes;
        private final int hash;

        private ByteArrayKey(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this
                    || (obj instanceof ByteArrayKey
                            && Arrays.equals(bytes, ((ByteArrayKey) obj).bytes))
                    || (obj instanceof ByteArrayLookupKey
                            && Arrays.equals(bytes, ((ByteArrayLookupKey) obj).bytes));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private static final class ByteArrayLookupKey {

        private @Nullable byte[] bytes;
        private int hash;

        private ByteArrayLookupKey() {}

        private ByteArrayLookupKey(byte[] bytes) {
            reset(bytes);
        }

        private void reset(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        private void clear() {
            bytes = null;
            hash = 0;
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this
                    || (bytes != null
                            && obj instanceof ByteArrayKey
                            && Arrays.equals(bytes, ((ByteArrayKey) obj).bytes))
                    || (bytes != null
                            && obj instanceof ByteArrayLookupKey
                            && Arrays.equals(bytes, ((ByteArrayLookupKey) obj).bytes));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
