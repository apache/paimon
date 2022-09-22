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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.manifest.FileKind;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.file.utils.AtomicFileWriter;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.utils.RowDataToObjectArrayConverter;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link FileStoreCommit}.
 *
 * <p>This class provides an atomic commit method to the user.
 *
 * <ol>
 *   <li>Before calling {@link FileStoreCommitImpl#commit}, if user cannot determine if this commit
 *       is done before, user should first call {@link FileStoreCommitImpl#filterCommitted}.
 *   <li>Before committing, it will first check for conflicts by checking if all files to be removed
 *       currently exists, and if modified files have overlapping key ranges with existing files.
 *   <li>After that it use the external {@link FileStoreCommitImpl#lock} (if provided) or the atomic
 *       rename of the file system to ensure atomicity.
 *   <li>If commit fails due to conflicts or exception it tries its best to clean up and aborts.
 *   <li>If atomic rename fails it tries again after reading the latest snapshot from step 2.
 * </ol>
 *
 * <p>NOTE: If you want to modify this class, any exception during commit MUST NOT BE IGNORED. They
 * must be thrown to restart the job. It is recommended to run FileStoreCommitTest thousands of
 * times to make sure that your changes is correct.
 */
public class FileStoreCommitImpl implements FileStoreCommit {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitImpl.class);

    private final long schemaId;
    private final String commitUser;
    private final RowType partitionType;
    private final RowDataToObjectArrayConverter partitionObjectConverter;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;
    private final FileStoreScan scan;
    private final int numBucket;
    private final MemorySize manifestTargetSize;
    private final int manifestMergeMinCount;
    @Nullable private final Comparator<RowData> keyComparator;

    @Nullable private Lock lock;
    private boolean createEmptyCommit;

    public FileStoreCommitImpl(
            long schemaId,
            String commitUser,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            FileStoreScan scan,
            int numBucket,
            MemorySize manifestTargetSize,
            int manifestMergeMinCount,
            @Nullable Comparator<RowData> keyComparator) {
        this.schemaId = schemaId;
        this.commitUser = commitUser;
        this.partitionType = partitionType;
        this.partitionObjectConverter = new RowDataToObjectArrayConverter(partitionType);
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.manifestFile = manifestFileFactory.create();
        this.manifestList = manifestListFactory.create();
        this.scan = scan;
        this.numBucket = numBucket;
        this.manifestTargetSize = manifestTargetSize;
        this.manifestMergeMinCount = manifestMergeMinCount;
        this.keyComparator = keyComparator;

        this.lock = null;
        this.createEmptyCommit = false;
    }

    @Override
    public FileStoreCommit withLock(Lock lock) {
        this.lock = lock;
        return this;
    }

    @Override
    public FileStoreCommit withCreateEmptyCommit(boolean createEmptyCommit) {
        this.createEmptyCommit = createEmptyCommit;
        return this;
    }

    @Override
    public List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committableList) {
        // nothing to filter, fast exit
        if (committableList.isEmpty()) {
            return committableList;
        }

        // if there is no previous snapshots then nothing should be filtered
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            return committableList;
        }

        // check if a committable is already committed by its identifier
        Map<String, ManifestCommittable> identifiers = new LinkedHashMap<>();
        for (ManifestCommittable committable : committableList) {
            identifiers.put(committable.identifier(), committable);
        }

        for (long id = latestSnapshotId; id >= Snapshot.FIRST_SNAPSHOT_ID; id--) {
            if (!snapshotManager.snapshotExists(id)) {
                // snapshots before this are expired
                break;
            }
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (commitUser.equals(snapshot.commitUser())) {
                if (identifiers.containsKey(snapshot.commitIdentifier())) {
                    identifiers.remove(snapshot.commitIdentifier());
                } else {
                    // early exit, because committableList must be the latest commits by this
                    // commit user
                    break;
                }
            }
        }

        return new ArrayList<>(identifiers.values());
    }

    @Override
    public void commit(ManifestCommittable committable, Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit\n" + committable.toString());
        }

        Long safeLatestSnapshotId = null;
        List<ManifestEntry> baseEntries = new ArrayList<>();

        List<ManifestEntry> appendChanges = collectChanges(committable.newFiles(), FileKind.ADD);
        List<ManifestEntry> compactChanges = new ArrayList<>();
        compactChanges.addAll(collectChanges(committable.compactBefore(), FileKind.DELETE));
        compactChanges.addAll(collectChanges(committable.compactAfter(), FileKind.ADD));

        if (createEmptyCommit || !appendChanges.isEmpty()) {
            // Optimization for common path.
            // Step 1:
            // Read manifest entries from changed partitions here and check for conflicts.
            // If there are no other jobs committing at the same time,
            // we can skip conflict checking in tryCommit method.
            // This optimization is mainly used to decrease the number of times we read from files.
            Long latestSnapshotId = snapshotManager.latestSnapshotId();
            if (latestSnapshotId != null) {
                // it is possible that some partitions only have compact changes,
                // so we need to contain all changes
                baseEntries.addAll(
                        readAllEntriesFromChangedPartitions(
                                latestSnapshotId, appendChanges, compactChanges));
                noConflictsOrFail(baseEntries, appendChanges);
                safeLatestSnapshotId = latestSnapshotId;
            }

            tryCommit(
                    appendChanges,
                    committable.identifier(),
                    committable.logOffsets(),
                    Snapshot.CommitKind.APPEND,
                    safeLatestSnapshotId);
        }

        if (!compactChanges.isEmpty()) {
            // Optimization for common path.
            // Step 2:
            // Add appendChanges to the manifest entries read above and check for conflicts.
            // If there are no other jobs committing at the same time,
            // we can skip conflict checking in tryCommit method.
            // This optimization is mainly used to decrease the number of times we read from files.
            if (safeLatestSnapshotId != null) {
                baseEntries.addAll(appendChanges);
                noConflictsOrFail(baseEntries, compactChanges);
                // assume this compact commit follows just after the append commit created above
                safeLatestSnapshotId += 1;
            }

            tryCommit(
                    compactChanges,
                    committable.identifier(),
                    committable.logOffsets(),
                    Snapshot.CommitKind.COMPACT,
                    safeLatestSnapshotId);
        }
    }

    @Override
    public void overwrite(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ready to overwrite partition "
                            + partition.toString()
                            + "\n"
                            + committable.toString());
        }

        List<ManifestEntry> appendChanges = collectChanges(committable.newFiles(), FileKind.ADD);
        // sanity check, all changes must be done within the given partition
        Predicate partitionFilter = PredicateConverter.fromMap(partition, partitionType);
        if (partitionFilter != null) {
            for (ManifestEntry entry : appendChanges) {
                if (!partitionFilter.test(partitionObjectConverter.convert(entry.partition()))) {
                    throw new IllegalArgumentException(
                            "Trying to overwrite partition "
                                    + partition
                                    + ", but the changes in "
                                    + pathFactory.getPartitionString(entry.partition())
                                    + " does not belong to this partition");
                }
            }
        }
        // overwrite new files
        tryOverwrite(
                partitionFilter, appendChanges, committable.identifier(), committable.logOffsets());

        List<ManifestEntry> compactChanges = new ArrayList<>();
        compactChanges.addAll(collectChanges(committable.compactBefore(), FileKind.DELETE));
        compactChanges.addAll(collectChanges(committable.compactAfter(), FileKind.ADD));
        if (!compactChanges.isEmpty()) {
            tryCommit(
                    compactChanges,
                    committable.identifier(),
                    committable.logOffsets(),
                    Snapshot.CommitKind.COMPACT,
                    null);
        }
    }

    private void tryCommit(
            List<ManifestEntry> changes,
            String hash,
            Map<Integer, Long> logOffsets,
            Snapshot.CommitKind commitKind,
            Long safeLatestSnapshotId) {
        while (true) {
            Long latestSnapshotId = snapshotManager.latestSnapshotId();
            if (tryCommitOnce(
                    changes,
                    hash,
                    logOffsets,
                    commitKind,
                    latestSnapshotId,
                    safeLatestSnapshotId)) {
                break;
            }
        }
    }

    private void tryOverwrite(
            Predicate partitionFilter,
            List<ManifestEntry> changes,
            String identifier,
            Map<Integer, Long> logOffsets) {
        while (true) {
            Long latestSnapshotId = snapshotManager.latestSnapshotId();

            List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
            if (latestSnapshotId != null) {
                List<ManifestEntry> currentEntries =
                        scan.withSnapshot(latestSnapshotId)
                                .withPartitionFilter(partitionFilter)
                                .plan()
                                .files();
                for (ManifestEntry entry : currentEntries) {
                    changesWithOverwrite.add(
                            new ManifestEntry(
                                    FileKind.DELETE,
                                    entry.partition(),
                                    entry.bucket(),
                                    entry.totalBuckets(),
                                    entry.file()));
                }
            }
            changesWithOverwrite.addAll(changes);

            if (tryCommitOnce(
                    changesWithOverwrite,
                    identifier,
                    logOffsets,
                    Snapshot.CommitKind.OVERWRITE,
                    latestSnapshotId,
                    null)) {
                break;
            }
        }
    }

    private List<ManifestEntry> collectChanges(
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> map, FileKind kind) {
        List<ManifestEntry> changes = new ArrayList<>();
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> entryWithPartition :
                map.entrySet()) {
            for (Map.Entry<Integer, List<DataFileMeta>> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                changes.addAll(
                        entryWithBucket.getValue().stream()
                                .map(
                                        file ->
                                                new ManifestEntry(
                                                        kind,
                                                        entryWithPartition.getKey(),
                                                        entryWithBucket.getKey(),
                                                        numBucket,
                                                        file))
                                .collect(Collectors.toList()));
            }
        }
        return changes;
    }

    private boolean tryCommitOnce(
            List<ManifestEntry> changes,
            String identifier,
            Map<Integer, Long> logOffsets,
            Snapshot.CommitKind commitKind,
            Long latestSnapshotId,
            Long safeLatestSnapshotId) {
        long newSnapshotId =
                latestSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID : latestSnapshotId + 1;
        Path newSnapshotPath = snapshotManager.snapshotPath(newSnapshotId);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit changes to snapshot #" + newSnapshotId);
            for (ManifestEntry entry : changes) {
                LOG.debug("  * " + entry.toString());
            }
        }

        Snapshot latestSnapshot = null;
        if (latestSnapshotId != null) {
            if (!latestSnapshotId.equals(safeLatestSnapshotId)) {
                // latestSnapshotId is different from the snapshot id we've checked for conflicts,
                // so we have to check again
                noConflictsOrFail(latestSnapshotId, changes);
            }
            latestSnapshot = snapshotManager.snapshot(latestSnapshotId);
        }

        Snapshot newSnapshot;
        String previousChangesListName = null;
        String newChangesListName = null;
        List<ManifestFileMeta> oldMetas = new ArrayList<>();
        List<ManifestFileMeta> newMetas = new ArrayList<>();
        try {
            if (latestSnapshot != null) {
                // read all previous manifest files
                oldMetas.addAll(latestSnapshot.readAllManifests(manifestList));
                // read the last snapshot to complete the bucket's offsets when logOffsets does not
                // contain all buckets
                latestSnapshot.getLogOffsets().forEach(logOffsets::putIfAbsent);
            }
            // merge manifest files with changes
            newMetas.addAll(
                    ManifestFileMeta.merge(
                            oldMetas,
                            manifestFile,
                            manifestTargetSize.getBytes(),
                            manifestMergeMinCount));
            previousChangesListName = manifestList.write(newMetas);

            // write new changes into manifest files
            List<ManifestFileMeta> newChangesManifests = manifestFile.write(changes);
            newMetas.addAll(newChangesManifests);
            newChangesListName = manifestList.write(newChangesManifests);

            // prepare snapshot file
            newSnapshot =
                    new Snapshot(
                            newSnapshotId,
                            schemaId,
                            previousChangesListName,
                            newChangesListName,
                            commitUser,
                            identifier,
                            commitKind,
                            System.currentTimeMillis(),
                            logOffsets);
        } catch (Throwable e) {
            // fails when preparing for commit, we should clean up
            cleanUpTmpManifests(previousChangesListName, newChangesListName, oldMetas, newMetas);
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when preparing snapshot #%d (path %s) by user %s "
                                    + "with hash %s and kind %s. Clean up.",
                            newSnapshotId,
                            newSnapshotPath.toString(),
                            commitUser,
                            identifier,
                            commitKind.name()),
                    e);
        }

        boolean success;
        try {
            FileSystem fs = newSnapshotPath.getFileSystem();
            Callable<Boolean> callable =
                    () -> {
                        if (fs.exists(newSnapshotPath)) {
                            return false;
                        }

                        boolean committed =
                                AtomicFileWriter.writeFileUtf8(
                                        newSnapshotPath, newSnapshot.toJson());
                        if (committed) {
                            snapshotManager.commitLatestHint(newSnapshotId);
                        }
                        return committed;
                    };
            if (lock != null) {
                success =
                        lock.runWithLock(
                                () ->
                                        // fs.rename may not returns false if target file
                                        // already exists, or even not atomic
                                        // as we're relying on external locking, we can first
                                        // check if file exist then rename to work around this
                                        // case
                                        !fs.exists(newSnapshotPath) && callable.call());
            } else {
                success = callable.call();
            }
        } catch (Throwable e) {
            // exception when performing the atomic rename,
            // we cannot clean up because we can't determine the success
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when committing snapshot #%d (path %s) by user %s "
                                    + "with identifier %s and kind %s. "
                                    + "Cannot clean up because we can't determine the success.",
                            newSnapshotId,
                            newSnapshotPath,
                            commitUser,
                            identifier,
                            commitKind.name()),
                    e);
        }

        if (success) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        String.format(
                                "Successfully commit snapshot #%d (path %s) by user %s "
                                        + "with identifier %s and kind %s.",
                                newSnapshotId,
                                newSnapshotPath,
                                commitUser,
                                identifier,
                                commitKind.name()));
            }
            return true;
        }

        // atomic rename fails, clean up and try again
        LOG.warn(
                String.format(
                        "Atomic commit failed for snapshot #%d (path %s) by user %s "
                                + "with identifier %s and kind %s. "
                                + "Clean up and try again.",
                        newSnapshotId, newSnapshotPath, commitUser, identifier, commitKind.name()));
        cleanUpTmpManifests(previousChangesListName, newChangesListName, oldMetas, newMetas);
        return false;
    }

    @SafeVarargs
    private final List<ManifestEntry> readAllEntriesFromChangedPartitions(
            long snapshotId, List<ManifestEntry>... changes) {
        List<BinaryRowData> changedPartitions =
                Arrays.stream(changes)
                        .flatMap(Collection::stream)
                        .map(ManifestEntry::partition)
                        .distinct()
                        .collect(Collectors.toList());
        try {
            return scan.withSnapshot(snapshotId)
                    .withPartitionFilter(changedPartitions)
                    .plan()
                    .files();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read manifest entries from changed partitions.", e);
        }
    }

    private void noConflictsOrFail(long snapshotId, List<ManifestEntry> changes) {
        noConflictsOrFail(readAllEntriesFromChangedPartitions(snapshotId, changes), changes);
    }

    private void noConflictsOrFail(List<ManifestEntry> baseEntries, List<ManifestEntry> changes) {
        List<ManifestEntry> allEntries = new ArrayList<>(baseEntries);
        allEntries.addAll(changes);

        Collection<ManifestEntry> mergedEntries;
        try {
            // merge manifest entries and also check if the files we want to delete are still there
            mergedEntries = ManifestEntry.mergeManifestEntries(allEntries);
        } catch (Throwable e) {
            LOG.warn("File deletion conflicts detected! Give up committing.", e);
            throw createConflictException(
                    "File deletion conflicts detected! Give up committing.", baseEntries, changes);
        }

        // fast exit for file store without keys
        if (keyComparator == null) {
            return;
        }

        // group entries by partitions, buckets and levels
        Map<LevelIdentifier, List<ManifestEntry>> levels = new HashMap<>();
        for (ManifestEntry entry : mergedEntries) {
            int level = entry.file().level();
            if (level >= 1) {
                levels.computeIfAbsent(
                                new LevelIdentifier(entry.partition(), entry.bucket(), level),
                                lv -> new ArrayList<>())
                        .add(entry);
            }
        }

        // check for all LSM level >= 1, key ranges of files do not intersect
        for (List<ManifestEntry> entries : levels.values()) {
            entries.sort((a, b) -> keyComparator.compare(a.file().minKey(), b.file().minKey()));
            for (int i = 0; i + 1 < entries.size(); i++) {
                ManifestEntry a = entries.get(i);
                ManifestEntry b = entries.get(i + 1);
                if (keyComparator.compare(a.file().maxKey(), b.file().minKey()) >= 0) {
                    throw createConflictException(
                            "LSM conflicts detected! Give up committing. Conflict files are:\n"
                                    + a.identifier().toString(pathFactory)
                                    + "\n"
                                    + b.identifier().toString(pathFactory),
                            baseEntries,
                            changes);
                }
            }
        }
    }

    private RuntimeException createConflictException(
            String message, List<ManifestEntry> baseEntries, List<ManifestEntry> changes) {
        String possibleCauses =
                String.join(
                        "\n",
                        "Conflicts during commits are normal and this failure is intended to resolve the conflicts.",
                        "Conflicts are mainly caused by the following scenarios:",
                        "1. Multiple jobs are writing into the same partition at the same time.",
                        "2. You're recovering from an old savepoint, or you're creating multiple jobs from a savepoint.",
                        "   The job will fail continuously in this scenario to protect metadata from corruption.",
                        "   You can either recover from the latest savepoint, "
                                + "or you can revert the table to the snapshot corresponding to the old savepoint.");
        String baseEntriesString =
                "Base entries are:\n"
                        + baseEntries.stream()
                                .map(ManifestEntry::toString)
                                .collect(Collectors.joining("\n"));
        String changesString =
                "Changes are:\n"
                        + changes.stream()
                                .map(ManifestEntry::toString)
                                .collect(Collectors.joining("\n"));
        return new RuntimeException(
                message
                        + "\n\n"
                        + possibleCauses
                        + "\n\n"
                        + baseEntriesString
                        + "\n\n"
                        + changesString);
    }

    private void cleanUpTmpManifests(
            String previousChangesListName,
            String newChangesListName,
            List<ManifestFileMeta> oldMetas,
            List<ManifestFileMeta> newMetas) {
        // clean up newly created manifest list
        if (previousChangesListName != null) {
            manifestList.delete(previousChangesListName);
        }
        if (newChangesListName != null) {
            manifestList.delete(newChangesListName);
        }
        // clean up newly merged manifest files
        Set<ManifestFileMeta> oldMetaSet = new HashSet<>(oldMetas); // for faster searching
        for (ManifestFileMeta suspect : newMetas) {
            if (!oldMetaSet.contains(suspect)) {
                manifestList.delete(suspect.fileName());
            }
        }
    }

    private static class LevelIdentifier {

        private final BinaryRowData partition;
        private final int bucket;
        private final int level;

        private LevelIdentifier(BinaryRowData partition, int bucket, int level) {
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
}
