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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.manifest.SimpleFileEntryWithDV;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.operation.commit.ManifestEntryChanges.changedPartitions;
import static org.apache.paimon.utils.InternalRowPartitionComputer.partToSimpleString;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Util class for detecting conflicts between base and delta files. */
public class ConflictDetection {

    private static final Logger LOG = LoggerFactory.getLogger(ConflictDetection.class);

    private final String tableName;
    private final String commitUser;
    private final RowType partitionType;
    private final FileStorePathFactory pathFactory;
    private final @Nullable Comparator<InternalRow> keyComparator;
    private final BucketMode bucketMode;
    private final boolean deletionVectorsEnabled;
    private final boolean dataEvolutionEnabled;
    private final IndexFileHandler indexFileHandler;
    private final SnapshotManager snapshotManager;
    private final CommitScanner commitScanner;

    private @Nullable PartitionExpire partitionExpire;
    private @Nullable Long rowIdCheckFromSnapshot = null;

    public ConflictDetection(
            String tableName,
            String commitUser,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            @Nullable Comparator<InternalRow> keyComparator,
            BucketMode bucketMode,
            boolean deletionVectorsEnabled,
            boolean dataEvolutionEnabled,
            IndexFileHandler indexFileHandler,
            SnapshotManager snapshotManager,
            CommitScanner commitScanner) {
        this.tableName = tableName;
        this.commitUser = commitUser;
        this.partitionType = partitionType;
        this.pathFactory = pathFactory;
        this.keyComparator = keyComparator;
        this.bucketMode = bucketMode;
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.dataEvolutionEnabled = dataEvolutionEnabled;
        this.indexFileHandler = indexFileHandler;
        this.snapshotManager = snapshotManager;
        this.commitScanner = commitScanner;
    }

    public void setRowIdCheckFromSnapshot(@Nullable Long rowIdCheckFromSnapshot) {
        this.rowIdCheckFromSnapshot = rowIdCheckFromSnapshot;
    }

    @Nullable
    public Comparator<InternalRow> keyComparator() {
        return keyComparator;
    }

    public void withPartitionExpire(PartitionExpire partitionExpire) {
        this.partitionExpire = partitionExpire;
    }

    public <T extends FileEntry> boolean shouldBeOverwriteCommit(
            List<T> appendFileEntries, List<IndexManifestEntry> appendIndexFiles) {
        for (T appendFileEntry : appendFileEntries) {
            if (appendFileEntry.kind().equals(FileKind.DELETE)) {
                return true;
            }
        }
        for (IndexManifestEntry appendIndexFile : appendIndexFiles) {
            if (appendIndexFile.indexFile().indexType().equals(DELETION_VECTORS_INDEX)) {
                return true;
            }
        }
        return rowIdCheckFromSnapshot != null;
    }

    public Optional<RuntimeException> checkConflicts(
            Snapshot latestSnapshot,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            List<IndexManifestEntry> deltaIndexEntries,
            CommitKind commitKind) {
        String baseCommitUser = latestSnapshot.commitUser();
        if (deletionVectorsEnabled && bucketMode.equals(BucketMode.BUCKET_UNAWARE)) {
            // Enrich dvName in fileEntry to checker for base ADD dv and delta DELETE dv.
            // For example:
            // If the base file is <ADD baseFile1, ADD dv1>,
            // then the delta file must be <DELETE deltaFile1, DELETE dv1>; and vice versa,
            // If the delta file is <DELETE deltaFile2, DELETE dv2>,
            // then the base file must be <ADD baseFile2, ADD dv2>.
            try {
                baseEntries =
                        buildBaseEntriesWithDV(
                                baseEntries,
                                latestSnapshot.indexManifest() == null
                                        ? Collections.emptyList()
                                        : indexFileHandler.readManifest(
                                                latestSnapshot.indexManifest()));
                deltaEntries =
                        buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries);
            } catch (Throwable e) {
                return Optional.of(
                        conflictException(commitUser, baseEntries, deltaEntries).apply(e));
            }
        }

        List<SimpleFileEntry> allEntries = new ArrayList<>(baseEntries);
        allEntries.addAll(deltaEntries);

        Optional<RuntimeException> exception =
                checkBucketKeepSame(
                        baseEntries, deltaEntries, commitKind, allEntries, baseCommitUser);
        if (exception.isPresent()) {
            return exception;
        }

        Function<Throwable, RuntimeException> conflictException =
                conflictException(baseCommitUser, baseEntries, deltaEntries);

        try {
            // check the delta, it is important not to delete and add the same file. Since scan
            // relies on map for deduplication, this may result in the loss of this file
            FileEntry.mergeEntries(deltaEntries);
        } catch (Throwable e) {
            throw conflictException.apply(e);
        }

        Collection<SimpleFileEntry> mergedEntries;
        try {
            // merge manifest entries and also check if the files we want to delete are still there
            mergedEntries = FileEntry.mergeEntries(allEntries);
        } catch (Throwable e) {
            return Optional.of(conflictException.apply(e));
        }

        exception = checkDeleteInEntries(mergedEntries, conflictException);
        if (exception.isPresent()) {
            return exception;
        }
        exception = checkKeyRange(baseEntries, deltaEntries, mergedEntries, baseCommitUser);
        if (exception.isPresent()) {
            return exception;
        }

        exception = checkRowIdRangeConflicts(commitKind, mergedEntries);
        if (exception.isPresent()) {
            return exception;
        }

        return checkForRowIdFromSnapshot(latestSnapshot, deltaEntries, deltaIndexEntries);
    }

    private Optional<RuntimeException> checkBucketKeepSame(
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            CommitKind commitKind,
            List<SimpleFileEntry> allEntries,
            String baseCommitUser) {
        if (commitKind == CommitKind.OVERWRITE) {
            return Optional.empty();
        }

        // total buckets within the same partition should remain the same
        Map<BinaryRow, Integer> totalBuckets = new HashMap<>();
        for (SimpleFileEntry entry : allEntries) {
            if (entry.totalBuckets() <= 0) {
                continue;
            }

            if (!totalBuckets.containsKey(entry.partition())) {
                totalBuckets.put(entry.partition(), entry.totalBuckets());
                continue;
            }

            int old = totalBuckets.get(entry.partition());
            if (old == entry.totalBuckets()) {
                continue;
            }

            Pair<RuntimeException, RuntimeException> conflictException =
                    createConflictException(
                            "Total buckets of partition "
                                    + entry.partition()
                                    + " changed from "
                                    + old
                                    + " to "
                                    + entry.totalBuckets()
                                    + " without overwrite. Give up committing.",
                            baseCommitUser,
                            baseEntries,
                            deltaEntries,
                            null);
            LOG.warn("", conflictException.getLeft());
            return Optional.of(conflictException.getRight());
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkKeyRange(
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            Collection<SimpleFileEntry> mergedEntries,
            String baseCommitUser) {
        // fast exit for file store without keys
        if (keyComparator == null) {
            return Optional.empty();
        }

        // group entries by partitions, buckets and levels
        Map<LevelIdentifier, List<SimpleFileEntry>> levels = new HashMap<>();
        for (SimpleFileEntry entry : mergedEntries) {
            int level = entry.level();
            if (level >= 1) {
                levels.computeIfAbsent(
                                new LevelIdentifier(entry.partition(), entry.bucket(), level),
                                lv -> new ArrayList<>())
                        .add(entry);
            }
        }

        // check for all LSM level >= 1, key ranges of files do not intersect
        for (List<SimpleFileEntry> entries : levels.values()) {
            entries.sort((a, b) -> keyComparator.compare(a.minKey(), b.minKey()));
            for (int i = 0; i + 1 < entries.size(); i++) {
                SimpleFileEntry a = entries.get(i);
                SimpleFileEntry b = entries.get(i + 1);
                if (keyComparator.compare(a.maxKey(), b.minKey()) >= 0) {
                    Pair<RuntimeException, RuntimeException> conflictException =
                            createConflictException(
                                    "LSM conflicts detected! Give up committing. Conflict files are:\n"
                                            + a.identifier().toString(pathFactory)
                                            + "\n"
                                            + b.identifier().toString(pathFactory),
                                    baseCommitUser,
                                    baseEntries,
                                    deltaEntries,
                                    null);

                    LOG.warn("", conflictException.getLeft());
                    return Optional.of(conflictException.getRight());
                }
            }
        }
        return Optional.empty();
    }

    private Function<Throwable, RuntimeException> conflictException(
            String baseCommitUser,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries) {
        return e -> {
            Pair<RuntimeException, RuntimeException> conflictException =
                    createConflictException(
                            "File deletion conflicts detected! Give up committing.",
                            baseCommitUser,
                            baseEntries,
                            deltaEntries,
                            e);
            LOG.warn("", conflictException.getLeft());
            return conflictException.getRight();
        };
    }

    private Optional<RuntimeException> checkDeleteInEntries(
            Collection<SimpleFileEntry> mergedEntries,
            Function<Throwable, RuntimeException> exceptionFunction) {
        try {
            for (SimpleFileEntry entry : mergedEntries) {
                checkState(
                        entry.kind() != FileKind.DELETE,
                        "Trying to delete file %s for table %s which is not previously added.",
                        entry.fileName(),
                        tableName);
            }
        } catch (Throwable e) {
            Optional<RuntimeException> exception = checkConflictForPartitionExpire(mergedEntries);
            if (exception.isPresent()) {
                return exception;
            }
            return Optional.of(exceptionFunction.apply(e));
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkConflictForPartitionExpire(
            Collection<SimpleFileEntry> mergedEntries) {
        if (partitionExpire != null && partitionExpire.isValueExpiration()) {
            Set<BinaryRow> deletedPartitions = new HashSet<>();
            for (SimpleFileEntry entry : mergedEntries) {
                if (entry.kind() == FileKind.DELETE) {
                    deletedPartitions.add(entry.partition());
                }
            }
            if (partitionExpire.isValueAllExpired(deletedPartitions)) {
                List<String> expiredPartitions =
                        deletedPartitions.stream()
                                .map(
                                        partition ->
                                                partToSimpleString(
                                                        partitionType, partition, "-", 200))
                                .collect(Collectors.toList());
                return Optional.of(
                        new RuntimeException(
                                "You are writing data to expired partitions, and you can filter this data to avoid job failover."
                                        + " Otherwise, continuous expired records will cause the job to failover restart continuously."
                                        + " Expired partitions are: "
                                        + expiredPartitions));
            }
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkRowIdRangeConflicts(
            CommitKind commitKind, Collection<SimpleFileEntry> mergedEntries) {
        if (!dataEvolutionEnabled) {
            return Optional.empty();
        }
        if (rowIdCheckFromSnapshot == null && commitKind != CommitKind.COMPACT) {
            return Optional.empty();
        }

        List<SimpleFileEntry> entries =
                mergedEntries.stream()
                        .filter(file -> file.firstRowId() != null)
                        .collect(Collectors.toList());

        RangeHelper<SimpleFileEntry> rangeHelper =
                new RangeHelper<>(
                        SimpleFileEntry::nonNullFirstRowId,
                        f -> f.nonNullFirstRowId() + f.rowCount() - 1);
        List<List<SimpleFileEntry>> merged = rangeHelper.mergeOverlappingRanges(entries);
        for (List<SimpleFileEntry> group : merged) {
            List<SimpleFileEntry> dataFiles = new ArrayList<>();
            for (SimpleFileEntry f : group) {
                if (!isBlobFile(f.fileName())) {
                    dataFiles.add(f);
                }
            }
            if (!rangeHelper.areAllRangesSame(dataFiles)) {
                return Optional.of(
                        new RuntimeException(
                                "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' operations "
                                        + "have encountered conflicts, data files: "
                                        + dataFiles));
            }
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkForRowIdFromSnapshot(
            Snapshot latestSnapshot,
            List<SimpleFileEntry> deltaEntries,
            List<IndexManifestEntry> deltaIndexEntries) {
        if (!dataEvolutionEnabled) {
            return Optional.empty();
        }
        if (rowIdCheckFromSnapshot == null) {
            return Optional.empty();
        }

        List<BinaryRow> changedPartitions = changedPartitions(deltaEntries, deltaIndexEntries);
        // collect history row id ranges
        List<Range> historyIdRanges = new ArrayList<>();
        for (SimpleFileEntry entry : deltaEntries) {
            Long firstRowId = entry.firstRowId();
            long rowCount = entry.rowCount();
            if (firstRowId != null) {
                historyIdRanges.add(new Range(firstRowId, firstRowId + rowCount - 1));
            }
        }

        // check history row id ranges
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
                    commitScanner.readIncrementalEntries(snapshot, changedPartitions);
            for (ManifestEntry entry : changes) {
                DataFileMeta file = entry.file();
                long firstRowId = file.nonNullFirstRowId();
                if (firstRowId < checkNextRowId) {
                    Range fileRange = new Range(firstRowId, firstRowId + file.rowCount() - 1);
                    for (Range range : historyIdRanges) {
                        if (range.hasIntersection(fileRange)) {
                            return Optional.of(
                                    new RuntimeException(
                                            "For Data Evolution table, multiple 'MERGE INTO' operations have encountered conflicts,"
                                                    + " updating the same file, which can render some updates ineffective."));
                        }
                    }
                }
            }
        }

        return Optional.empty();
    }

    static List<SimpleFileEntry> buildBaseEntriesWithDV(
            List<SimpleFileEntry> baseEntries, List<IndexManifestEntry> baseIndexEntries) {
        if (baseEntries.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, String> fileNameToDVFileName = new HashMap<>();
        for (IndexManifestEntry indexManifestEntry : baseIndexEntries) {
            // Should not attach DELETE type dv index for base file.
            if (!indexManifestEntry.kind().equals(FileKind.DELETE)) {
                IndexFileMeta indexFile = indexManifestEntry.indexFile();
                LinkedHashMap<String, DeletionVectorMeta> dvRanges = indexFile.dvRanges();
                if (dvRanges != null) {
                    for (DeletionVectorMeta value : dvRanges.values()) {
                        checkState(
                                !fileNameToDVFileName.containsKey(value.dataFileName()),
                                "One file should correspond to only one dv entry.");
                        fileNameToDVFileName.put(value.dataFileName(), indexFile.fileName());
                    }
                }
            }
        }

        // Attach dv name to file entries.
        List<SimpleFileEntry> entriesWithDV = new ArrayList<>(baseEntries.size());
        for (SimpleFileEntry fileEntry : baseEntries) {
            entriesWithDV.add(
                    new SimpleFileEntryWithDV(
                            fileEntry, fileNameToDVFileName.get(fileEntry.fileName())));
        }
        return entriesWithDV;
    }

    static List<SimpleFileEntry> buildDeltaEntriesWithDV(
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            List<IndexManifestEntry> deltaIndexEntries) {
        if (deltaEntries.isEmpty() && deltaIndexEntries.isEmpty()) {
            return Collections.emptyList();
        }

        List<SimpleFileEntry> entriesWithDV = new ArrayList<>(deltaEntries.size());

        // One file may correspond to more than one dv entries, for example, delete the old dv, and
        // create a new one.
        Map<String, List<IndexManifestEntry>> fileNameToDVEntry = new HashMap<>();
        for (IndexManifestEntry deltaIndexEntry : deltaIndexEntries) {
            LinkedHashMap<String, DeletionVectorMeta> dvRanges =
                    deltaIndexEntry.indexFile().dvRanges();
            if (dvRanges != null) {
                for (DeletionVectorMeta meta : dvRanges.values()) {
                    fileNameToDVEntry.putIfAbsent(meta.dataFileName(), new ArrayList<>());
                    fileNameToDVEntry.get(meta.dataFileName()).add(deltaIndexEntry);
                }
            }
        }

        Set<String> fileNotInDeltaEntries = new HashSet<>(fileNameToDVEntry.keySet());
        // 1. Attach dv name to delta file entries.
        for (SimpleFileEntry fileEntry : deltaEntries) {
            if (fileNameToDVEntry.containsKey(fileEntry.fileName())) {
                List<IndexManifestEntry> dvs = fileNameToDVEntry.get(fileEntry.fileName());
                checkState(dvs.size() == 1, "Delta entry only can have one dv file");
                entriesWithDV.add(
                        new SimpleFileEntryWithDV(fileEntry, dvs.get(0).indexFile().fileName()));
                fileNotInDeltaEntries.remove(fileEntry.fileName());
            } else {
                entriesWithDV.add(new SimpleFileEntryWithDV(fileEntry, null));
            }
        }

        // 2. For file not in delta entries, build entry with dv with baseEntries.
        if (!fileNotInDeltaEntries.isEmpty()) {
            Map<String, SimpleFileEntry> fileNameToFileEntry = new HashMap<>();
            for (SimpleFileEntry baseEntry : baseEntries) {
                if (baseEntry.kind().equals(FileKind.ADD)) {
                    fileNameToFileEntry.put(baseEntry.fileName(), baseEntry);
                }
            }

            for (String fileName : fileNotInDeltaEntries) {
                SimpleFileEntryWithDV simpleFileEntry =
                        (SimpleFileEntryWithDV) fileNameToFileEntry.get(fileName);
                checkState(
                        simpleFileEntry != null,
                        String.format(
                                "Trying to create deletion vector on file %s which is not previously added.",
                                fileName));
                List<IndexManifestEntry> dvEntries = fileNameToDVEntry.get(fileName);
                // If dv entry's type id DELETE, add DELETE<f, dv>
                // If dv entry's type id ADD, add ADD<f, dv>
                for (IndexManifestEntry dvEntry : dvEntries) {
                    entriesWithDV.add(
                            new SimpleFileEntryWithDV(
                                    dvEntry.kind().equals(FileKind.ADD)
                                            ? simpleFileEntry
                                            : simpleFileEntry.toDelete(),
                                    dvEntry.indexFile().fileName()));
                }

                // If one file correspond to only one dv entry and the type is ADD,
                // we need to add a DELETE<f, null>.
                // This happens when create a dv for a file that doesn't have dv before.
                if (dvEntries.size() == 1 && dvEntries.get(0).kind().equals(FileKind.ADD)) {
                    entriesWithDV.add(new SimpleFileEntryWithDV(simpleFileEntry.toDelete(), null));
                }
            }
        }

        return entriesWithDV;
    }

    /**
     * Construct detailed conflict exception. The returned exception is formed of (full exception,
     * simplified exception), The simplified exception is generated when the entry length is larger
     * than the max limit.
     */
    private Pair<RuntimeException, RuntimeException> createConflictException(
            String message,
            String baseCommitUser,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> changes,
            Throwable cause) {
        String possibleCauses =
                String.join(
                        "\n",
                        "Don't panic!",
                        "Conflicts during commits are normal and this failure is intended to resolve the conflicts.",
                        "Conflicts are mainly caused by the following scenarios:",
                        "1. Multiple jobs are writing into the same partition at the same time, "
                                + "or you use STATEMENT SET to execute multiple INSERT statements into the same Paimon table.",
                        "   You'll probably see different base commit user and current commit user below.",
                        "   You can use "
                                + "https://paimon.apache.org/docs/master/maintenance/dedicated-compaction#dedicated-compaction-job"
                                + " to support multiple writing.",
                        "2. You're recovering from an old savepoint, or you're creating multiple jobs from a savepoint.",
                        "   The job will fail continuously in this scenario to protect metadata from corruption.",
                        "   You can either recover from the latest savepoint, "
                                + "or you can revert the table to the snapshot corresponding to the old savepoint.");
        String commitUserString =
                "Base commit user is: "
                        + baseCommitUser
                        + "; Current commit user is: "
                        + commitUser;
        String baseEntriesString =
                "Base entries are:\n"
                        + baseEntries.stream()
                                .map(Object::toString)
                                .collect(Collectors.joining("\n"));
        String changesString =
                "Changes are:\n"
                        + changes.stream().map(Object::toString).collect(Collectors.joining("\n"));

        RuntimeException fullException =
                new RuntimeException(
                        message
                                + "\n\n"
                                + possibleCauses
                                + "\n\n"
                                + commitUserString
                                + "\n\n"
                                + baseEntriesString
                                + "\n\n"
                                + changesString,
                        cause);

        RuntimeException simplifiedException;
        int maxEntry = 50;
        if (baseEntries.size() > maxEntry || changes.size() > maxEntry) {
            baseEntriesString =
                    "Base entries are:\n"
                            + baseEntries.subList(0, Math.min(baseEntries.size(), maxEntry))
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining("\n"));
            changesString =
                    "Changes are:\n"
                            + changes.subList(0, Math.min(changes.size(), maxEntry)).stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining("\n"));
            simplifiedException =
                    new RuntimeException(
                            message
                                    + "\n\n"
                                    + possibleCauses
                                    + "\n\n"
                                    + commitUserString
                                    + "\n\n"
                                    + baseEntriesString
                                    + "\n\n"
                                    + changesString
                                    + "\n\n"
                                    + "The entry list above are not fully displayed, please refer to taskmanager.log for more information.",
                            cause);
            return Pair.of(fullException, simplifiedException);
        } else {
            return Pair.of(fullException, fullException);
        }
    }

    private static class LevelIdentifier {

        private final BinaryRow partition;
        private final int bucket;
        private final int level;

        private LevelIdentifier(BinaryRow partition, int bucket, int level) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LevelIdentifier)) {
                return false;
            }
            LevelIdentifier that = (LevelIdentifier) o;
            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && level == that.level;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, level);
        }
    }

    /** Factory to create {@link ConflictDetection}. */
    public interface Factory {
        ConflictDetection create(CommitScanner scanner);
    }
}
