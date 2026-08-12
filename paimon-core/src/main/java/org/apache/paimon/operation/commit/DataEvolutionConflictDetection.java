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

import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.errors.ErrorMessages;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.commit.RetryCommitResult.CommitFailRetryResult;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.operation.commit.ManifestEntryChanges.changedPartitions;
import static org.apache.paimon.operation.commit.RowIdConflictChecker.TriggerSource.MATERIALIZE_DV_COMPACTION;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Conflict detection for Data Evolution tables. */
public class DataEvolutionConflictDetection extends ConflictDetection {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionConflictDetection.class);

    private final String tableName;
    private final String commitUser;
    private final SnapshotManager snapshotManager;

    private @Nullable Long rowIdCheckFromSnapshot;
    private @Nullable RowIdConflictChecker.TriggerSource rowIdConflictCheckTriggerSource;

    public DataEvolutionConflictDetection(
            String tableName,
            String commitUser,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            BucketMode bucketMode,
            boolean deletionVectorsEnabled,
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
    }

    @Override
    public void setRowIdCheckFromSnapshot(@Nullable Long rowIdCheckFromSnapshot) {
        setRowIdCheckFromSnapshot(
                rowIdCheckFromSnapshot, RowIdConflictChecker.TriggerSource.DATA_EVOLUTION_DML);
    }

    @Override
    public void setRowIdCheckFromSnapshotForMaterializeDvCompaction(
            @Nullable Long rowIdCheckFromSnapshot) {
        setRowIdCheckFromSnapshot(rowIdCheckFromSnapshot, MATERIALIZE_DV_COMPACTION);
    }

    private void setRowIdCheckFromSnapshot(
            @Nullable Long rowIdCheckFromSnapshot,
            RowIdConflictChecker.TriggerSource triggerSource) {
        this.rowIdCheckFromSnapshot = rowIdCheckFromSnapshot;
        this.rowIdConflictCheckTriggerSource =
                rowIdCheckFromSnapshot == null ? null : triggerSource;
    }

    @Override
    public boolean shouldCheckRowIdFromSnapshot(CommitKind commitKind) {
        return rowIdCheckFromSnapshot != null
                && (rowIdConflictCheckTriggerSource != MATERIALIZE_DV_COMPACTION
                        || commitKind == CommitKind.COMPACT);
    }

    @Override
    public RowIdConflictChecker.TriggerSource rowIdConflictCheckTriggerSource() {
        checkState(
                rowIdConflictCheckTriggerSource != null,
                "Row ID conflict check trigger source is not set.");
        return rowIdConflictCheckTriggerSource;
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
        List<Range> changedRowRanges = changedRowRanges(deltaFiles, indexFiles, commitKind);
        Set<String> referencedDataFiles = referencedDataFiles(deltaFiles, indexFiles);
        if (changedRowRanges.isEmpty()) {
            if (commitKind == CommitKind.OVERWRITE && !referencedDataFiles.isEmpty()) {
                return commitScanner()
                        .readAllEntriesFromDataFiles(
                                latestSnapshot, changedPartitions, referencedDataFiles);
            }
            return scanChangedPartitions(
                    latestSnapshot,
                    changedPartitions,
                    previousAttempt,
                    hasOverwriteSincePreviousAttempt);
        }

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

    private Set<String> referencedDataFiles(
            List<ManifestEntry> deltaFiles, List<IndexManifestEntry> indexFiles) {
        Set<String> referencedDataFiles =
                deltaFiles.stream()
                        .filter(entry -> entry.kind() == FileKind.DELETE)
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
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            CommitKind commitKind) {
        if (commitKind != CommitKind.COMPACT && commitKind != CommitKind.OVERWRITE) {
            return Collections.emptyList();
        }

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
                        new RuntimeException(
                                "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' "
                                        + "operations "
                                        + "have encountered conflicts, data files: "
                                        + dataFileGroup));
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
                                    dedicatedFile,
                                    dedicatedRange,
                                    conflictReason,
                                    intersectingDataFiles)));
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
        return rowIdConflictCheckTriggerSource != MATERIALIZE_DV_COMPACTION || kind == FileKind.ADD;
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
            Range indexRange = globalIndex.rowRange();
            RowRangeIndex rowRangeIndex =
                    rowRangeIndexes.get(Pair.of(indexEntry.partition(), indexEntry.bucket()));
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

    private static boolean dedicatedStorageFile(String fileName) {
        return isBlobFile(fileName) || isVectorStoreFile(fileName);
    }
}
