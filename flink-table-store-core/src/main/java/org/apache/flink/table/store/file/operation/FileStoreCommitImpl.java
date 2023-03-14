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

import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.manifest.FileKind;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.options.MemorySize;
import org.apache.flink.table.store.table.sink.CommitMessage;
import org.apache.flink.table.store.table.sink.CommitMessageImpl;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.RowDataToObjectArrayConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
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
    @Nullable private final Comparator<InternalRow> keyComparator;

    @Nullable private Lock lock;
    private boolean ignoreEmptyCommit;

    public FileStoreCommitImpl(
            FileIO fileIO,
            SchemaManager schemaManager,
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
            @Nullable Comparator<InternalRow> keyComparator) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
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
        this.ignoreEmptyCommit = true;
    }

    @Override
    public FileStoreCommit withLock(Lock lock) {
        this.lock = lock;
        return this;
    }

    @Override
    public FileStoreCommit ignoreEmptyCommit(boolean ignoreEmptyCommit) {
        this.ignoreEmptyCommit = ignoreEmptyCommit;
        return this;
    }

    @Override
    public Set<Long> filterCommitted(Set<Long> commitIdentifiers) {
        // nothing to filter, fast exit
        if (commitIdentifiers.isEmpty()) {
            return commitIdentifiers;
        }

        Optional<Snapshot> latestSnapshot = snapshotManager.latestSnapshotOfUser(commitUser);
        if (latestSnapshot.isPresent()) {
            Set<Long> result = new HashSet<>();
            for (Long identifier : commitIdentifiers) {
                // if committable is newer than latest snapshot, then it hasn't been committed
                if (identifier > latestSnapshot.get().commitIdentifier()) {
                    result.add(identifier);
                }
            }
            return result;
        } else {
            // if there is no previous snapshots then nothing should be filtered
            return commitIdentifiers;
        }
    }

    @Override
    public void commit(ManifestCommittable committable, Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit\n" + committable.toString());
        }

        Long safeLatestSnapshotId = null;
        List<ManifestEntry> baseEntries = new ArrayList<>();

        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        collectChanges(
                committable.fileCommittables(),
                appendTableFiles,
                appendChangelog,
                compactTableFiles,
                compactChangelog);

        if (!ignoreEmptyCommit || !appendTableFiles.isEmpty() || !appendChangelog.isEmpty()) {
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
                                latestSnapshotId, appendTableFiles, compactTableFiles));
                noConflictsOrFail(baseEntries, appendTableFiles);
                safeLatestSnapshotId = latestSnapshotId;
            }

            tryCommit(
                    appendTableFiles,
                    appendChangelog,
                    committable.identifier(),
                    committable.watermark(),
                    committable.logOffsets(),
                    Snapshot.CommitKind.APPEND,
                    safeLatestSnapshotId);
        }

        if (!compactTableFiles.isEmpty() || !compactChangelog.isEmpty()) {
            // Optimization for common path.
            // Step 2:
            // Add appendChanges to the manifest entries read above and check for conflicts.
            // If there are no other jobs committing at the same time,
            // we can skip conflict checking in tryCommit method.
            // This optimization is mainly used to decrease the number of times we read from files.
            if (safeLatestSnapshotId != null) {
                baseEntries.addAll(appendTableFiles);
                noConflictsOrFail(baseEntries, compactTableFiles);
                // assume this compact commit follows just after the append commit created above
                safeLatestSnapshotId += 1;
            }

            tryCommit(
                    compactTableFiles,
                    compactChangelog,
                    committable.identifier(),
                    committable.watermark(),
                    committable.logOffsets(),
                    Snapshot.CommitKind.COMPACT,
                    safeLatestSnapshotId);
        }
    }

    @Override
    public void overwrite(
            List<Map<String, String>> partitions,
            ManifestCommittable committable,
            Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ready to overwrite partition "
                            + partitions.stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining(","))
                            + "\n"
                            + committable.toString());
        }

        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        collectChanges(
                committable.fileCommittables(),
                appendTableFiles,
                appendChangelog,
                compactTableFiles,
                compactChangelog);

        if (!appendChangelog.isEmpty() || !compactChangelog.isEmpty()) {
            StringBuilder warnMessage =
                    new StringBuilder(
                            "Overwrite mode currently does not commit any changelog.\n"
                                    + "Please make sure that the partition you're overwriting "
                                    + "is not being consumed by a streaming reader.\n"
                                    + "Ignored changelog files are:\n");
            for (ManifestEntry entry : appendChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            for (ManifestEntry entry : compactChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            LOG.warn(warnMessage.toString());
        }

        // sanity check, all changes must be done within the given partition
        List<Predicate> partitionFilters = new ArrayList<>();
        for (Map<String, String> partition : partitions) {
            Predicate partitionFilter = PredicateBuilder.partition(partition, partitionType);
            if (partitionFilter != null) {
                for (ManifestEntry entry : appendTableFiles) {
                    if (!partitionFilter.test(
                            partitionObjectConverter.convert(entry.partition()))) {
                        throw new IllegalArgumentException(
                                "Trying to overwrite partition "
                                        + partition
                                        + ", but the changes in "
                                        + pathFactory.getPartitionString(entry.partition())
                                        + " does not belong to this partition");
                    }
                }

                partitionFilters.add(partitionFilter);
            }
        }

        Predicate partitionFilter;
        if (partitionFilters.size() == 0) {
            partitionFilter = null;
        } else if (partitionFilters.size() == 1) {
            partitionFilter = partitionFilters.get(0);
        } else {
            partitionFilter = PredicateBuilder.or(partitionFilters);
        }

        // overwrite new files
        tryOverwrite(
                partitionFilter,
                appendTableFiles,
                committable.identifier(),
                committable.watermark(),
                committable.logOffsets());

        if (!compactTableFiles.isEmpty()) {
            tryCommit(
                    compactTableFiles,
                    Collections.emptyList(),
                    committable.identifier(),
                    committable.watermark(),
                    committable.logOffsets(),
                    Snapshot.CommitKind.COMPACT,
                    null);
        }
    }

    private void collectChanges(
            List<CommitMessage> commitMessages,
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelog,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelog) {
        for (CommitMessage message : commitMessages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            commitMessage
                    .newFilesIncrement()
                    .newFiles()
                    .forEach(m -> appendTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
            commitMessage
                    .newFilesIncrement()
                    .changelogFiles()
                    .forEach(m -> appendChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));
            commitMessage
                    .compactIncrement()
                    .compactBefore()
                    .forEach(
                            m ->
                                    compactTableFiles.add(
                                            makeEntry(FileKind.DELETE, commitMessage, m)));
            commitMessage
                    .compactIncrement()
                    .compactAfter()
                    .forEach(m -> compactTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
            commitMessage
                    .compactIncrement()
                    .changelogFiles()
                    .forEach(m -> compactChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));
        }
    }

    private ManifestEntry makeEntry(FileKind kind, CommitMessage commitMessage, DataFileMeta file) {
        return new ManifestEntry(
                kind, commitMessage.partition(), commitMessage.bucket(), numBucket, file);
    }

    private void tryCommit(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Snapshot.CommitKind commitKind,
            Long safeLatestSnapshotId) {
        while (true) {
            Long latestSnapshotId = snapshotManager.latestSnapshotId();
            if (tryCommitOnce(
                    tableFiles,
                    changelogFiles,
                    identifier,
                    watermark,
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
            long identifier,
            @Nullable Long watermark,
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
                    Collections.emptyList(),
                    identifier,
                    watermark,
                    logOffsets,
                    Snapshot.CommitKind.OVERWRITE,
                    latestSnapshotId,
                    null)) {
                break;
            }
        }
    }

    private boolean tryCommitOnce(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Snapshot.CommitKind commitKind,
            Long latestSnapshotId,
            Long safeLatestSnapshotId) {
        long newSnapshotId =
                latestSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID : latestSnapshotId + 1;
        Path newSnapshotPath = snapshotManager.snapshotPath(newSnapshotId);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit table files to snapshot #" + newSnapshotId);
            for (ManifestEntry entry : tableFiles) {
                LOG.debug("  * " + entry.toString());
            }
            LOG.debug("Ready to commit changelog to snapshot #" + newSnapshotId);
            for (ManifestEntry entry : changelogFiles) {
                LOG.debug("  * " + entry.toString());
            }
        }

        Snapshot latestSnapshot = null;
        if (latestSnapshotId != null) {
            if (!latestSnapshotId.equals(safeLatestSnapshotId)) {
                // latestSnapshotId is different from the snapshot id we've checked for conflicts,
                // so we have to check again
                noConflictsOrFail(latestSnapshotId, tableFiles);
            }
            latestSnapshot = snapshotManager.snapshot(latestSnapshotId);
        }

        Snapshot newSnapshot;
        String previousChangesListName = null;
        String newChangesListName = null;
        String changelogListName = null;
        List<ManifestFileMeta> oldMetas = new ArrayList<>();
        List<ManifestFileMeta> newMetas = new ArrayList<>();
        List<ManifestFileMeta> changelogMetas = new ArrayList<>();
        try {
            long previousTotalRecordCount = 0L;
            Long currentWatermark = watermark;
            if (latestSnapshot != null) {
                previousTotalRecordCount = latestSnapshot.totalRecordCount(scan);
                List<ManifestFileMeta> previousManifests =
                        latestSnapshot.dataManifests(manifestList);
                // read all previous manifest files
                oldMetas.addAll(previousManifests);
                // read the last snapshot to complete the bucket's offsets when logOffsets does not
                // contain all buckets
                latestSnapshot.logOffsets().forEach(logOffsets::putIfAbsent);
                Long latestWatermark = latestSnapshot.watermark();
                if (latestWatermark != null) {
                    currentWatermark =
                            currentWatermark == null
                                    ? latestWatermark
                                    : Math.max(currentWatermark, latestWatermark);
                }
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
            long deltaRecordCount = Snapshot.recordCount(tableFiles);
            List<ManifestFileMeta> newChangesManifests = manifestFile.write(tableFiles);
            newMetas.addAll(newChangesManifests);
            newChangesListName = manifestList.write(newChangesManifests);

            // write changelog into manifest files
            if (!changelogFiles.isEmpty()) {
                changelogMetas.addAll(manifestFile.write(changelogFiles));
                changelogListName = manifestList.write(changelogMetas);
            }

            // prepare snapshot file
            newSnapshot =
                    new Snapshot(
                            newSnapshotId,
                            schemaManager.latest().get().id(),
                            previousChangesListName,
                            newChangesListName,
                            changelogListName,
                            commitUser,
                            identifier,
                            commitKind,
                            System.currentTimeMillis(),
                            logOffsets,
                            previousTotalRecordCount + deltaRecordCount,
                            deltaRecordCount,
                            Snapshot.recordCount(changelogFiles),
                            currentWatermark);
        } catch (Throwable e) {
            // fails when preparing for commit, we should clean up
            cleanUpTmpManifests(
                    previousChangesListName,
                    newChangesListName,
                    changelogListName,
                    oldMetas,
                    newMetas,
                    changelogMetas);
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
            Callable<Boolean> callable =
                    () -> {
                        boolean committed =
                                fileIO.writeFileUtf8(newSnapshotPath, newSnapshot.toJson());
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
                                        !fileIO.exists(newSnapshotPath) && callable.call());
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
        cleanUpTmpManifests(
                previousChangesListName,
                newChangesListName,
                changelogListName,
                oldMetas,
                newMetas,
                changelogMetas);
        return false;
    }

    @SafeVarargs
    private final List<ManifestEntry> readAllEntriesFromChangedPartitions(
            long snapshotId, List<ManifestEntry>... changes) {
        List<BinaryRow> changedPartitions =
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
            mergedEntries = ManifestEntry.mergeEntries(allEntries);
            ManifestEntry.assertNoDelete(mergedEntries);
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
            String changelogListName,
            List<ManifestFileMeta> oldMetas,
            List<ManifestFileMeta> newMetas,
            List<ManifestFileMeta> changelogMetas) {
        // clean up newly created manifest list
        if (previousChangesListName != null) {
            manifestList.delete(previousChangesListName);
        }
        if (newChangesListName != null) {
            manifestList.delete(newChangesListName);
        }
        if (changelogListName != null) {
            manifestList.delete(changelogListName);
        }
        // clean up newly merged manifest files
        Set<ManifestFileMeta> oldMetaSet = new HashSet<>(oldMetas); // for faster searching
        for (ManifestFileMeta suspect : newMetas) {
            if (!oldMetaSet.contains(suspect)) {
                manifestList.delete(suspect.fileName());
            }
        }
        // clean up changelog manifests
        for (ManifestFileMeta meta : changelogMetas) {
            manifestList.delete(meta.fileName());
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
}
