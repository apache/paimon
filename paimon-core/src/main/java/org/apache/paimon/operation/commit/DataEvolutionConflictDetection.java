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

package org.apache.paimon.operation.commit;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.errors.ErrorMessages;
import org.apache.paimon.globalindex.CentroidIndexMeta;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.FileEntryMergeBuffer;
import org.apache.paimon.operation.commit.RetryCommitResult.CommitFailRetryResult;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.ThrowingConsumer;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.operation.commit.ManifestEntryChanges.changedPartitions;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.fileFieldIds;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Conflict detection for Data Evolution tables. */
public class DataEvolutionConflictDetection extends ConflictDetection {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionConflictDetection.class);

    private final String tableName;
    private final String commitUser;
    private final SnapshotManager snapshotManager;
    private final IndexFileHandler indexFileHandler;
    private final boolean nestedFieldEnabled;
    private final boolean streamingConflictsSupported;

    private @Nullable Long rowIdCheckFromSnapshot;
    private @Nullable RowIdConflictCheckStrategy rowIdConflictCheckStrategy;

    public DataEvolutionConflictDetection(
            String tableName,
            String commitUser,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            BucketMode bucketMode,
            boolean deletionVectorsEnabled,
            boolean nestedFieldEnabled,
            IndexFileHandler indexFileHandler,
            SnapshotManager snapshotManager,
            CommitScanner commitScanner) {
        super(
                tableName,
                commitUser,
                partitionType,
                pathFactory,
                bucketMode,
                deletionVectorsEnabled,
                indexFileHandler,
                commitScanner);
        this.tableName = tableName;
        this.commitUser = commitUser;
        this.snapshotManager = snapshotManager;
        this.indexFileHandler = indexFileHandler;
        this.nestedFieldEnabled = nestedFieldEnabled;
        this.streamingConflictsSupported = !deletionVectorsEnabled;
    }

    @Override
    public void setRowIdCheckFromSnapshot(@Nullable Long rowIdCheckFromSnapshot) {
        setRowIdCheckFromSnapshot(
                rowIdCheckFromSnapshot, DataEvolutionDmlRowIdConflictCheck.INSTANCE);
    }

    @Override
    public void setRowIdCheckFromSnapshotForMaterializeDvCompaction(
            @Nullable Long rowIdCheckFromSnapshot) {
        setRowIdCheckFromSnapshot(rowIdCheckFromSnapshot, MaterializeDvRowIdConflictCheck.INSTANCE);
    }

    private void setRowIdCheckFromSnapshot(
            @Nullable Long rowIdCheckFromSnapshot,
            RowIdConflictCheckStrategy conflictCheckStrategy) {
        this.rowIdCheckFromSnapshot = rowIdCheckFromSnapshot;
        this.rowIdConflictCheckStrategy =
                rowIdCheckFromSnapshot == null ? null : conflictCheckStrategy;
    }

    @Override
    public boolean shouldCheckRowIdFromSnapshot(CommitKind commitKind) {
        return rowIdCheckFromSnapshot != null && rowIdConflictCheckStrategy().appliesTo(commitKind);
    }

    @Override
    @Nullable
    public RowIdConflictChecker createRowIdConflictChecker(
            SchemaManager schemaManager, List<ManifestEntry> deltaFiles, CommitKind commitKind) {
        if (!shouldCheckRowIdFromSnapshot(commitKind)) {
            return null;
        }
        return rowIdConflictCheckStrategy()
                .createChecker(schemaManager, deltaFiles, nestedFieldEnabled);
    }

    private RowIdConflictCheckStrategy rowIdConflictCheckStrategy() {
        checkState(
                rowIdConflictCheckStrategy != null, "Row ID conflict check strategy is not set.");
        return rowIdConflictCheckStrategy;
    }

    @Override
    public boolean canSkipDataFileConflictDetection(
            Snapshot latestSnapshot,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            CommitKind commitKind) {
        if (commitKind != CommitKind.COMPACT
                || !deltaFiles.isEmpty()
                || indexFiles.isEmpty()
                || rowIdCheckFromSnapshot != null
                || latestSnapshot.indexManifest() == null) {
            return false;
        }

        Map<String, IndexManifestEntry> removedFiles = new HashMap<>();
        Set<String> addedFiles = new HashSet<>();
        Map<IndexCompactionKey, List<Range>> removedRanges = new HashMap<>();
        Map<IndexCompactionKey, List<Range>> addedRanges = new HashMap<>();
        for (IndexManifestEntry entry : indexFiles) {
            IndexFileMeta file = entry.indexFile();
            GlobalIndexMeta meta = file.globalIndexMeta();
            if (meta == null || file.dvRanges() != null) {
                return false;
            }
            Map<IndexCompactionKey, List<Range>> ranges;
            if (entry.kind() == FileKind.DELETE) {
                if (removedFiles.put(file.fileName(), entry) != null) {
                    return false;
                }
                ranges = removedRanges;
            } else {
                if (!addedFiles.add(file.fileName())) {
                    return false;
                }
                ranges = addedRanges;
            }
            ranges.computeIfAbsent(new IndexCompactionKey(entry), key -> new ArrayList<>())
                    .add(meta.rowRange());
        }
        if (removedFiles.isEmpty()
                || addedFiles.isEmpty()
                || !Collections.disjoint(removedFiles.keySet(), addedFiles)
                || !removedRanges.keySet().equals(addedRanges.keySet())) {
            return false;
        }
        for (Map.Entry<IndexCompactionKey, List<Range>> removed : removedRanges.entrySet()) {
            if (!Range.sortAndMergeOverlap(removed.getValue(), true)
                    .equals(Range.sortAndMergeOverlap(addedRanges.get(removed.getKey()), true))) {
                return false;
            }
        }

        for (IndexManifestEntry current :
                indexFileHandler.readManifest(latestSnapshot.indexManifest())) {
            IndexManifestEntry removed = removedFiles.get(current.indexFile().fileName());
            if (removed == null) {
                continue;
            }
            if (current.kind() != FileKind.ADD
                    || !current.partition().equals(removed.partition())
                    || current.bucket() != removed.bucket()
                    || !current.indexFile().equals(removed.indexFile())) {
                return false;
            }
            removedFiles.remove(current.indexFile().fileName());
        }
        return removedFiles.isEmpty();
    }

    private static class IndexCompactionKey {

        private final BinaryRow partition;
        private final int bucket;
        private final String indexType;
        private final List<Integer> fieldIds;

        private IndexCompactionKey(IndexManifestEntry entry) {
            this.partition = entry.partition();
            this.bucket = entry.bucket();
            this.indexType = entry.indexFile().indexType();
            this.fieldIds = entry.indexFile().globalIndexMeta().getIndexedFieldIds();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof IndexCompactionKey)) {
                return false;
            }
            IndexCompactionKey that = (IndexCompactionKey) other;
            return bucket == that.bucket
                    && partition.equals(that.partition)
                    && indexType.equals(that.indexType)
                    && fieldIds.equals(that.fieldIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, indexType, fieldIds);
        }
    }

    @Override
    public List<SimpleFileEntry> scanBaseDataFiles(
            Snapshot latestSnapshot,
            List<BinaryRow> changedPartitions,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            CommitKind commitKind,
            @Nullable CommitFailRetryResult previousAttempt,
            boolean hasOverwriteSincePreviousAttempt) {
        List<Range> changedRowRanges = changedRowRanges(deltaFiles, indexFiles);
        Set<String> referencedDataFiles = referencedDataFiles(deltaFiles, indexFiles);
        if (!changedRowRanges.isEmpty()) {
            return scanChangedRowRanges(
                    latestSnapshot, changedPartitions, changedRowRanges, referencedDataFiles);
        }
        if (!referencedDataFiles.isEmpty()) {
            return commitScanner()
                    .readAllEntriesFromDataFiles(
                            latestSnapshot, changedPartitions, referencedDataFiles);
        }
        return Collections.emptyList();
    }

    @Override
    public void checkRetainedGlobalIndexes(
            Snapshot snapshot,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            CommitKind commitKind) {
        if (commitKind == CommitKind.COMPACT || snapshot.indexManifest() == null) {
            return;
        }
        Map<BinaryRow, List<DataFileMeta>> updates = new HashMap<>();
        for (ManifestEntry change : deltaFiles) {
            if (change.kind() == FileKind.ADD && change.file().firstRowId() != null) {
                updates.computeIfAbsent(change.partition(), ignored -> new ArrayList<>())
                        .add(change.file());
            }
        }
        if (updates.isEmpty()) {
            return;
        }
        Set<String> deletedIndexes =
                indexFiles.stream()
                        .filter(entry -> entry.kind() == FileKind.DELETE)
                        .map(entry -> entry.indexFile().fileName())
                        .collect(Collectors.toSet());
        Map<Long, TableSchema> schemas = new HashMap<>();
        for (IndexManifestEntry index : indexFileHandler.readManifest(snapshot.indexManifest())) {
            IndexFileMeta file = index.indexFile();
            GlobalIndexMeta meta = file.globalIndexMeta();
            List<DataFileMeta> partitionUpdates = updates.get(index.partition());
            if (partitionUpdates == null
                    || meta == null
                    || !CentroidIndexMeta.hasBitmapFields(meta)
                    || !CentroidIndexMeta.isRoutingModel(meta.indexMeta())
                    || deletedIndexes.contains(file.fileName())) {
                continue;
            }
            for (DataFileMeta update : partitionUpdates) {
                Range changed = update.nonNullRowIdRange();
                if (changed.to < meta.rowRangeStart() || changed.from > meta.rowRangeEnd()) {
                    continue;
                }
                TableSchema schema =
                        schemas.computeIfAbsent(update.schemaId(), commitScanner()::schema);
                if (!Collections.disjoint(
                        meta.getIndexedFieldIds(), fileFieldIds(schema, update))) {
                    throw new IllegalStateException(
                            "Update affects an active centroid index with Bitmap fields. "
                                    + "Drop the index or use DROP_PARTITION_INDEX before updating.");
                }
            }
        }
    }

    private List<SimpleFileEntry> scanChangedRowRanges(
            Snapshot latestSnapshot,
            List<BinaryRow> changedPartitions,
            List<Range> changedRowRanges,
            Set<String> referencedDataFiles) {
        List<SimpleFileEntry> baseDataFiles =
                new ArrayList<>(
                        commitScanner()
                                .readAllEntriesFromChangedRowRanges(
                                        latestSnapshot, changedPartitions, changedRowRanges));
        referencedDataFiles.removeAll(
                baseDataFiles.stream().map(SimpleFileEntry::fileName).collect(Collectors.toSet()));
        if (referencedDataFiles.isEmpty()) {
            return baseDataFiles;
        }

        baseDataFiles.addAll(
                commitScanner()
                        .readAllEntriesFromDataFiles(
                                latestSnapshot, changedPartitions, referencedDataFiles));
        return new ArrayList<>(
                baseDataFiles.stream()
                        .collect(
                                Collectors.toMap(
                                        FileEntry::identifier,
                                        entry -> entry,
                                        (left, right) -> left,
                                        LinkedHashMap::new))
                        .values());
    }

    /**
     * Produces the ordinary DE base scan's input without retaining its complete metadata. Returns
     * whether the caller must keep the first base record per identity, matching the supplemental
     * scan branch's left-first deduplication. Range and referenced-name state still grow with the
     * changes; the consumer must not retain the entire decoded base.
     */
    public boolean forEachBaseDataFile(
            Snapshot latestSnapshot,
            List<BinaryRow> changedPartitions,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            ThrowingConsumer<SimpleFileEntry, Exception> consumer)
            throws Exception {
        List<Range> changedRowRanges = changedRowRanges(deltaFiles, indexFiles);
        Set<String> referencedDataFiles = referencedDataFiles(deltaFiles, indexFiles);
        if (changedRowRanges.isEmpty()) {
            if (!referencedDataFiles.isEmpty()) {
                commitScanner()
                        .forEachEntryFromDataFiles(
                                latestSnapshot, changedPartitions, referencedDataFiles, consumer);
            }
            return false;
        }

        commitScanner()
                .forEachEntryFromChangedRowRanges(
                        latestSnapshot,
                        changedPartitions,
                        changedRowRanges,
                        entry -> {
                            referencedDataFiles.remove(entry.fileName());
                            consumer.accept(entry);
                        });
        if (referencedDataFiles.isEmpty()) {
            return false;
        }
        commitScanner()
                .forEachEntryFromDataFiles(
                        latestSnapshot, changedPartitions, referencedDataFiles, consumer);
        // Selecting this branch triggers deduplication even when the second scan emits no rows.
        return true;
    }

    public boolean supportsStreamingConflicts() {
        return streamingConflictsSupported;
    }

    /**
     * Checks the no-DV successful path without complete base/delta/merged metadata collections.
     * Possible conflicts fall back to the original checker after closing spill resources,
     * preserving its diagnostics, error priority and throw/return contract. That diagnostic path
     * retains the original memory boundary. Buffers too small for both sorters also use that
     * boundary instead of increasing the configured sort-buffer budget. Callback and DV callers
     * keep the ordinary path.
     */
    public Optional<RuntimeException> checkConflictsStreaming(
            CoreOptions options,
            @Nullable IOManager ioManager,
            Snapshot latestSnapshot,
            List<BinaryRow> changedPartitions,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            @Nullable RowIdConflictChecker rowIdConflictChecker,
            CommitKind commitKind) {
        boolean ordinary =
                !supportsStreamingConflicts() || !FileEntryMergeBuffer.supportsBufferSize(options);
        if (!ordinary) {
            boolean globalIndexes =
                    indexFiles.stream()
                            .anyMatch(
                                    entry ->
                                            entry.kind() == FileKind.ADD
                                                    && entry.indexFile().globalIndexMeta() != null);
            DataEvolutionStreamingChecks checks =
                    new DataEvolutionStreamingChecks(
                            commitKind,
                            rowIdCheckFromSnapshot != null || commitKind == CommitKind.COMPACT,
                            globalIndexes);
            try (FileEntryMergeBuffer buffer = new FileEntryMergeBuffer(options, ioManager)) {
                boolean deduplicate =
                        forEachBaseDataFile(
                                latestSnapshot,
                                changedPartitions,
                                deltaFiles,
                                indexFiles,
                                entry -> {
                                    checks.addRawBase(entry);
                                    buffer.writeBase(entry);
                                });
                for (ManifestEntry entry : deltaFiles) {
                    buffer.writeDelta(SimpleFileEntry.from(entry));
                }
                try {
                    buffer.forEachMerged(deduplicate, checks::addBase, checks::addMerged);
                } catch (IllegalStateException invalidIdentity) {
                    // The original checker selects bucket errors before identity errors and
                    // distinguishes delta exceptions from combined conflict return values.
                    ordinary = true;
                }
                ordinary =
                        ordinary
                                || checks.mayConflict(
                                        deduplicate,
                                        deltaFiles,
                                        indexFiles,
                                        latestSnapshot.nextRowId());
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed streaming data evolution conflict check.", e);
            } finally {
                checks.clear();
            }
        }
        if (ordinary) {
            List<SimpleFileEntry> base =
                    scanBaseDataFiles(
                            latestSnapshot,
                            changedPartitions,
                            deltaFiles,
                            indexFiles,
                            commitKind,
                            null,
                            false);
            return checkConflicts(
                    latestSnapshot,
                    base,
                    SimpleFileEntry.from(deltaFiles),
                    indexFiles,
                    rowIdConflictChecker,
                    commitKind);
        }
        Optional<RuntimeException> exception =
                checkCentroidSourceChanges(latestSnapshot, indexFiles);
        if (exception.isPresent()) {
            return exception;
        }
        return checkForRowIdFromSnapshot(
                latestSnapshot,
                Lists.transform(deltaFiles, SimpleFileEntry::from),
                indexFiles,
                rowIdConflictChecker);
    }

    private Set<String> referencedDataFiles(
            List<ManifestEntry> deltaFiles, List<IndexManifestEntry> indexFiles) {
        // Include ADD files to detect replay even if their row IDs have changed or are unassigned.
        Set<String> referencedDataFiles =
                deltaFiles.stream()
                        .map(entry -> entry.file().fileName())
                        .collect(Collectors.toSet());
        for (IndexManifestEntry indexFile : indexFiles) {
            if (indexFile.indexFile().dvRanges() != null) {
                referencedDataFiles.addAll(indexFile.indexFile().dvRanges().keySet());
            }
        }
        return referencedDataFiles;
    }

    private List<Range> changedRowRanges(
            List<ManifestEntry> deltaFiles, List<IndexManifestEntry> indexFiles) {
        List<Range> ranges =
                deltaFiles.stream()
                        .map(ManifestEntry::file)
                        .filter(file -> file.firstRowId() != null)
                        .map(DataFileMeta::nonNullRowIdRange)
                        .collect(Collectors.toList());
        indexFiles.stream()
                .filter(entry -> entry.kind() == FileKind.ADD)
                .map(IndexManifestEntry::indexFile)
                .map(IndexFileMeta::globalIndexMeta)
                .filter(Objects::nonNull)
                .map(GlobalIndexMeta::rowRange)
                .forEach(ranges::add);
        return Range.sortAndMergeOverlap(ranges, true);
    }

    @Override
    protected Optional<RuntimeException> checkTableSpecificConflicts(
            Snapshot latestSnapshot,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            List<IndexManifestEntry> deltaIndexEntries,
            Collection<SimpleFileEntry> mergedEntries,
            @Nullable RowIdConflictChecker rowIdConflictChecker,
            CommitKind commitKind,
            String baseCommitUser) {
        Optional<RuntimeException> exception =
                checkRowIdExistence(
                        baseEntries, deltaEntries, latestSnapshot.nextRowId(), commitKind);
        if (exception.isPresent()) {
            return exception;
        }

        exception = checkRowIdRangeConflicts(commitKind, mergedEntries);
        if (exception.isPresent()) {
            return exception;
        }

        exception = checkGlobalIndexRowIdExistence(baseEntries, deltaIndexEntries);
        if (exception.isPresent()) {
            return exception;
        }

        exception = checkCentroidSourceChanges(latestSnapshot, deltaIndexEntries);
        if (exception.isPresent()) {
            return exception;
        }

        return checkForRowIdFromSnapshot(
                latestSnapshot, deltaEntries, deltaIndexEntries, rowIdConflictChecker);
    }

    private Optional<RuntimeException> checkRowIdRangeConflicts(
            CommitKind commitKind, Collection<SimpleFileEntry> mergedEntries) {
        if (rowIdCheckFromSnapshot == null && commitKind != CommitKind.COMPACT) {
            return Optional.empty();
        }

        List<SimpleFileEntry> entries =
                mergedEntries.stream()
                        .filter(file -> file.firstRowId() != null)
                        .collect(Collectors.toList());

        RangeHelper<SimpleFileEntry> rangeHelper =
                new RangeHelper<>(SimpleFileEntry::nonNullRowIdRange);
        List<SimpleFileEntry> dataFiles =
                entries.stream()
                        .filter(file -> !dedicatedStorageFile(file.fileName()))
                        .collect(Collectors.toList());

        Optional<RuntimeException> exception =
                checkDataFileRowIdRangeConflicts(rangeHelper, dataFiles);
        if (exception.isPresent()) {
            return exception;
        }

        List<SimpleFileEntry> dedicatedFiles =
                entries.stream()
                        .filter(file -> dedicatedStorageFile(file.fileName()))
                        .collect(Collectors.toList());
        return checkDedicatedFileRowIdRangeConflicts(dataFiles, dedicatedFiles);
    }

    private Optional<RuntimeException> checkDataFileRowIdRangeConflicts(
            RangeHelper<SimpleFileEntry> rangeHelper, List<SimpleFileEntry> dataFiles) {
        for (List<SimpleFileEntry> dataFileGroup : rangeHelper.mergeOverlappingRanges(dataFiles)) {
            if (!rangeHelper.areAllRangesSame(dataFileGroup)) {
                return Optional.of(
                        new DataEvolutionRowRangeConflictException(
                                "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' "
                                        + "operations "
                                        + "have encountered conflicts, data files: "
                                        + dataFileGroup.stream()
                                                .map(this::fileEntryToString)
                                                .collect(Collectors.toList()),
                                dataFileGroup.stream()
                                        .map(FileEntry::identifier)
                                        .collect(Collectors.toList())));
            }
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkDedicatedFileRowIdRangeConflicts(
            List<SimpleFileEntry> dataFiles, List<SimpleFileEntry> dedicatedFiles) {
        if (dedicatedFiles.isEmpty()) {
            return Optional.empty();
        }

        RowRangeIndex dataFileRowRangeIndex = rowRangeIndex(dataFiles, false);
        for (SimpleFileEntry dedicatedFile : dedicatedFiles) {
            Range dedicatedRange = dedicatedFile.nonNullRowIdRange();
            if (dataFileRowRangeIndex.contains(dedicatedRange)) {
                continue;
            }

            List<Range> intersectingRanges =
                    dataFileRowRangeIndex.intersectedRanges(dedicatedRange.from, dedicatedRange.to);
            List<SimpleFileEntry> intersectingDataFiles =
                    dataFiles.stream()
                            .filter(
                                    dataFile ->
                                            dataFile.nonNullRowIdRange()
                                                    .hasIntersection(dedicatedRange))
                            .collect(Collectors.toList());
            String conflictReason =
                    intersectingRanges.size() > 1
                            ? "spans multiple data file ranges"
                            : "is not covered by one data file range";
            return Optional.of(
                    new RuntimeException(
                            String.format(
                                    "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' "
                                            + "operations have encountered conflicts, dedicated "
                                            + "file %s %s %s: %s",
                                    fileEntryToString(dedicatedFile),
                                    dedicatedRange,
                                    conflictReason,
                                    intersectingDataFiles.stream()
                                            .map(this::fileEntryToString)
                                            .collect(Collectors.toList()))));
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkForRowIdFromSnapshot(
            Snapshot latestSnapshot,
            List<SimpleFileEntry> deltaEntries,
            List<IndexManifestEntry> deltaIndexEntries,
            @Nullable RowIdConflictChecker conflictChecker) {
        if (rowIdCheckFromSnapshot == null
                || conflictChecker == null
                || conflictChecker.isEmpty()) {
            return Optional.empty();
        }

        List<BinaryRow> changedPartitions = changedPartitions(deltaEntries, deltaIndexEntries);
        Long checkNextRowId = snapshotManager.snapshot(rowIdCheckFromSnapshot).nextRowId();
        checkState(
                checkNextRowId != null,
                "Next row id cannot be null for snapshot %s.",
                rowIdCheckFromSnapshot);
        for (long i = rowIdCheckFromSnapshot + 1; i <= latestSnapshot.id(); i++) {
            Snapshot snapshot = snapshotManager.snapshot(i);
            if (snapshot.commitKind() == CommitKind.COMPACT) {
                continue;
            }
            List<ManifestEntry> changes =
                    commitScanner().readIncrementalEntries(snapshot, changedPartitions);
            for (ManifestEntry entry : changes) {
                if (!shouldCheckHistoricalRowIdEntry(entry.kind())) {
                    continue;
                }
                DataFileMeta file = entry.file();
                if (file.firstRowId() != null
                        && file.nonNullRowIdRange().from < checkNextRowId
                        && conflictChecker.conflictsWith(file)) {
                    LOG.debug(
                            "Data evolution row id conflict detected for table {}, commit user {}, "
                                    + "snapshot {}, file {}.",
                            tableName,
                            commitUser,
                            snapshot.id(),
                            file);
                    return Optional.of(
                            new RuntimeException(
                                    ErrorMessages.DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE));
                }
            }
        }
        return Optional.empty();
    }

    boolean shouldCheckHistoricalRowIdEntry(FileKind kind) {
        return rowIdConflictCheckStrategy().shouldCheckHistoricalEntry(kind);
    }

    private interface RowIdConflictCheckStrategy {

        boolean appliesTo(CommitKind commitKind);

        RowIdConflictChecker createChecker(
                SchemaManager schemaManager,
                List<ManifestEntry> deltaFiles,
                boolean nestedFieldEnabled);

        boolean shouldCheckHistoricalEntry(FileKind kind);
    }

    private static class DataEvolutionDmlRowIdConflictCheck implements RowIdConflictCheckStrategy {

        private static final DataEvolutionDmlRowIdConflictCheck INSTANCE =
                new DataEvolutionDmlRowIdConflictCheck();

        @Override
        public boolean appliesTo(CommitKind commitKind) {
            return true;
        }

        @Override
        public RowIdConflictChecker createChecker(
                SchemaManager schemaManager,
                List<ManifestEntry> deltaFiles,
                boolean nestedFieldEnabled) {
            return RowIdColumnConflictChecker.fromDataFiles(
                    schemaManager,
                    Lists.transform(deltaFiles, ManifestEntry::file),
                    nestedFieldEnabled);
        }

        @Override
        public boolean shouldCheckHistoricalEntry(FileKind kind) {
            return true;
        }
    }

    private static class MaterializeDvRowIdConflictCheck implements RowIdConflictCheckStrategy {

        private static final MaterializeDvRowIdConflictCheck INSTANCE =
                new MaterializeDvRowIdConflictCheck();

        @Override
        public boolean appliesTo(CommitKind commitKind) {
            return commitKind == CommitKind.COMPACT;
        }

        @Override
        public RowIdConflictChecker createChecker(
                SchemaManager schemaManager,
                List<ManifestEntry> deltaFiles,
                boolean nestedFieldEnabled) {
            // Materializing deletion vectors rewrites complete row ranges. A concurrent ADD in a
            // deleted normal-file range can otherwise restore logically deleted rows.
            List<DataFileMeta> deletedNormalFiles =
                    deltaFiles.stream()
                            .filter(entry -> entry.kind() == FileKind.DELETE)
                            .map(ManifestEntry::file)
                            .filter(file -> file.firstRowId() != null)
                            .filter(
                                    file ->
                                            !isBlobFile(file.fileName())
                                                    && !isVectorStoreFile(file.fileName()))
                            .collect(Collectors.toList());
            return RowIdRangeConflictChecker.fromDataFiles(deletedNormalFiles);
        }

        @Override
        public boolean shouldCheckHistoricalEntry(FileKind kind) {
            return kind == FileKind.ADD;
        }
    }

    private Optional<RuntimeException> checkGlobalIndexRowIdExistence(
            List<SimpleFileEntry> baseEntries, List<IndexManifestEntry> deltaIndexEntries) {
        List<IndexManifestEntry> indexesToCheck = globalIndexFileAdditions(deltaIndexEntries);
        if (indexesToCheck.isEmpty()) {
            return Optional.empty();
        }

        Map<Pair<BinaryRow, Integer>, List<Range>> dataRanges = new HashMap<>();
        for (SimpleFileEntry entry : baseEntries) {
            if (entry.kind() == FileKind.ADD && entry.firstRowId() != null) {
                dataRanges
                        .computeIfAbsent(
                                Pair.of(entry.partition(), entry.bucket()), k -> new ArrayList<>())
                        .add(entry.nonNullRowIdRange());
            }
        }
        Map<Pair<BinaryRow, Integer>, RowRangeIndex> rowRangeIndexes =
                dataRanges.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> RowRangeIndex.create(entry.getValue())));

        for (IndexManifestEntry indexEntry : indexesToCheck) {
            GlobalIndexMeta globalIndex = indexEntry.indexFile().globalIndexMeta();
            checkState(globalIndex != null, "Global index meta must not be null.");
            RowRangeIndex rowRangeIndex =
                    rowRangeIndexes.get(Pair.of(indexEntry.partition(), indexEntry.bucket()));
            Range indexRange = globalIndex.rowRange();
            if (rowRangeIndex == null || !rowRangeIndex.contains(indexRange)) {
                return Optional.of(
                        new RuntimeException(
                                String.format(
                                        "Global index row ID existence conflict: index file '%s' "
                                                + "references row range %s, but this range "
                                                + "is not fully covered by current data "
                                                + "files. The referenced row IDs may have been "
                                                + "reassigned or removed by a concurrent commit.",
                                        indexEntry.indexFile().fileName(), indexRange)));
            }
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkCentroidSourceChanges(
            Snapshot latestSnapshot, List<IndexManifestEntry> indexEntries) {
        Map<Long, List<IndexManifestEntry>> byBaseline = new HashMap<>();
        long firstBaseline = latestSnapshot.id();
        for (IndexManifestEntry index : indexEntries) {
            IndexFileMeta file = index.indexFile();
            if (index.kind() == FileKind.ADD
                    && file.globalIndexMeta() != null
                    && CentroidIndexMeta.isRoutingModel(file.globalIndexMeta().indexMeta())) {
                long baseline =
                        DataEvolutionIndexSourceMeta.fromIndexFile(index.indexFile())
                                .scanSnapshotId();
                byBaseline.computeIfAbsent(baseline, ignored -> new ArrayList<>()).add(index);
                firstBaseline = Math.min(firstBaseline, baseline);
            }
        }

        Map<BinaryRow, Map<Integer, RowRangeIndex>> active = new HashMap<>();
        Map<Long, TableSchema> schemas = new HashMap<>();
        Map<Pair<Long, List<String>>, Set<Integer>> fileFieldIds = new HashMap<>();
        for (long baseline = firstBaseline; baseline < latestSnapshot.id(); baseline++) {
            List<IndexManifestEntry> starting = byBaseline.get(baseline);
            if (starting != null) {
                Map<Pair<BinaryRow, Integer>, List<Range>> additions = new HashMap<>();
                for (IndexManifestEntry index : starting) {
                    GlobalIndexMeta meta = index.indexFile().globalIndexMeta();
                    for (int fieldId : meta.getIndexedFieldIds()) {
                        additions
                                .computeIfAbsent(
                                        Pair.of(index.partition(), fieldId),
                                        ignored -> new ArrayList<>())
                                .add(meta.rowRange());
                    }
                }
                additions.forEach(
                        (key, ranges) -> {
                            Map<Integer, RowRangeIndex> fields =
                                    active.computeIfAbsent(
                                            key.getLeft(), ignored -> new HashMap<>());
                            RowRangeIndex existing = fields.get(key.getRight());
                            if (existing != null) {
                                ranges.addAll(existing.ranges());
                            }
                            fields.put(key.getRight(), RowRangeIndex.create(ranges));
                        });
            }
            Snapshot snapshot = snapshotManager.snapshot(baseline + 1);
            for (ManifestEntry change :
                    commitScanner()
                            .readIncrementalEntries(snapshot, new ArrayList<>(active.keySet()))) {
                DataFileMeta file = change.file();
                Map<Integer, RowRangeIndex> fields = active.get(change.partition());
                if (fields == null || file.firstRowId() == null) {
                    continue;
                }
                Range changed = file.nonNullRowIdRange();
                for (Map.Entry<Integer, RowRangeIndex> field : fields.entrySet()) {
                    if (!field.getValue().intersects(changed.from, changed.to)) {
                        continue;
                    }
                    Set<Integer> written =
                            fileFieldIds.computeIfAbsent(
                                    Pair.of(file.schemaId(), file.writeCols()),
                                    key ->
                                            fileFieldIds(
                                                    schemas.computeIfAbsent(
                                                            key.getLeft(), commitScanner()::schema),
                                                    file));
                    if (written.contains(field.getKey())) {
                        return Optional.of(
                                new RuntimeException(
                                        "Centroid index source rows changed in snapshot "
                                                + snapshot.id()
                                                + "; rebuild the index against the latest snapshot."));
                    }
                }
            }
        }
        return Optional.empty();
    }

    private List<IndexManifestEntry> globalIndexFileAdditions(
            List<IndexManifestEntry> indexFileChanges) {
        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry entry : indexFileChanges) {
            if (entry.kind() == FileKind.ADD && entry.indexFile().globalIndexMeta() != null) {
                result.add(entry);
            }
        }
        return result;
    }

    Optional<RuntimeException> checkRowIdExistence(
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            @Nullable Long nextRowId,
            CommitKind commitKind) {
        Optional<RuntimeException> exception =
                checkDeletedFileRowIdExistence(baseEntries, deltaEntries);
        if (exception.isPresent()) {
            return exception;
        }

        List<SimpleFileEntry> existingDataFiles =
                baseEntries.stream()
                        .filter(
                                base ->
                                        base.firstRowId() != null
                                                && !dedicatedStorageFile(base.fileName()))
                        .collect(Collectors.toList());

        if (commitKind == CommitKind.COMPACT) {
            return checkCompactRowIdExistence(existingDataFiles, deltaEntries);
        }
        return checkNonCompactRowIdExistence(existingDataFiles, deltaEntries, nextRowId);
    }

    private Optional<RuntimeException> checkDeletedFileRowIdExistence(
            List<SimpleFileEntry> baseEntries, List<SimpleFileEntry> deltaEntries) {
        // FileEntry.Identifier deliberately excludes RowID metadata because reassignment does not
        // create a new physical data file. Reject a DELETE planned before reassignment by comparing
        // it with the current ADD before identifier-based manifest merging cancels the two entries.
        Map<FileEntry.Identifier, SimpleFileEntry> deletedFiles = new HashMap<>();
        for (SimpleFileEntry entry : deltaEntries) {
            if (entry.kind() == FileKind.DELETE) {
                deletedFiles.put(entry.identifier(), entry);
            }
        }
        if (deletedFiles.isEmpty()) {
            return Optional.empty();
        }

        for (SimpleFileEntry current : baseEntries) {
            if (current.kind() != FileKind.ADD) {
                continue;
            }
            SimpleFileEntry deleted = deletedFiles.get(current.identifier());
            if (deleted != null
                    && (!Objects.equals(current.firstRowId(), deleted.firstRowId())
                            || current.rowCount() != deleted.rowCount())) {
                return Optional.of(
                        new RowIdExistenceConflictException(
                                deleted.fileName(),
                                deleted.firstRowId(),
                                deleted.rowCount(),
                                current.firstRowId(),
                                current.rowCount(),
                                deleted.bucket()));
            }
        }
        return Optional.empty();
    }

    /**
     * Checks conflicts between compaction and concurrent Row ID reassignment, which may otherwise
     * cause reassigned Row IDs to fall back. For example, compaction produces a file with Row IDs
     * [0, 9], then reassignment moves the current range to [10, 19]. Committing the stale
     * compaction would move the Row IDs back to [0, 9].
     */
    private Optional<RuntimeException> checkCompactRowIdExistence(
            List<SimpleFileEntry> existingDataFiles, List<SimpleFileEntry> deltaEntries) {
        Map<Pair<BinaryRow, Integer>, List<SimpleFileEntry>> dataFilesByBucket =
                existingDataFiles.stream()
                        .collect(
                                Collectors.groupingBy(
                                        file -> Pair.of(file.partition(), file.bucket())));
        Map<Pair<BinaryRow, Integer>, RowRangeIndex> existingIndexes =
                dataFilesByBucket.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> rowRangeIndex(entry.getValue(), true)));

        for (SimpleFileEntry entry : deltaEntries) {
            if (entry.kind() != FileKind.ADD || entry.firstRowId() == null) {
                continue;
            }
            RowRangeIndex existingIndex =
                    existingIndexes.get(Pair.of(entry.partition(), entry.bucket()));
            if (existingIndex == null || !existingIndex.contains(entry.nonNullRowIdRange())) {
                return Optional.of(rowIdExistenceConflict(entry));
            }
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkNonCompactRowIdExistence(
            List<SimpleFileEntry> existingDataFiles,
            List<SimpleFileEntry> deltaEntries,
            @Nullable Long nextRowId) {
        List<SimpleFileEntry> filesToCheck =
                deltaEntries.stream()
                        .filter(
                                entry ->
                                        entry.kind() == FileKind.ADD
                                                && entry.firstRowId() != null
                                                && nextRowId != null
                                                && entry.firstRowId() < nextRowId)
                        .collect(Collectors.toList());
        if (filesToCheck.isEmpty()) {
            return Optional.empty();
        }

        RowRangeIndex existingIndex = rowRangeIndex(existingDataFiles, false);
        for (SimpleFileEntry entry : filesToCheck) {
            Range rowRange = entry.nonNullRowIdRange();
            boolean exists =
                    dedicatedStorageFile(entry.fileName())
                            ? existingIndex.contains(rowRange)
                            : existingIndex.containsExactly(rowRange);
            if (!exists) {
                return Optional.of(rowIdExistenceConflict(entry));
            }
        }
        return Optional.empty();
    }

    private RowIdExistenceConflictException rowIdExistenceConflict(SimpleFileEntry entry) {
        return new RowIdExistenceConflictException(
                entry.fileName(), entry.firstRowId(), entry.rowCount(), entry.bucket());
    }

    private static RowRangeIndex rowRangeIndex(
            Collection<SimpleFileEntry> files, boolean mergeAdjacent) {
        return RowRangeIndex.create(
                files.stream().map(SimpleFileEntry::nonNullRowIdRange).collect(Collectors.toList()),
                mergeAdjacent);
    }

    static boolean dedicatedStorageFile(String fileName) {
        return isBlobFile(fileName) || isVectorStoreFile(fileName);
    }
}
