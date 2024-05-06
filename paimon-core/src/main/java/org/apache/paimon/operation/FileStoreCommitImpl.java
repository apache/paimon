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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.operation.metrics.CommitStats;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

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

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;

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
 * times to make sure that your changes are correct.
 */
public class FileStoreCommitImpl implements FileStoreCommit {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitImpl.class);

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final String commitUser;
    private final RowType partitionType;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;
    private final IndexManifestFile indexManifestFile;
    private final FileStoreScan scan;
    private final int numBucket;
    private final MemorySize manifestTargetSize;
    private final MemorySize manifestFullCompactionSize;
    private final int manifestMergeMinCount;
    private final boolean dynamicPartitionOverwrite;
    @Nullable private final Comparator<InternalRow> keyComparator;
    private final String branchName;

    @Nullable private Lock lock;
    private boolean ignoreEmptyCommit;

    private CommitMetrics commitMetrics;

    private final StatsFileHandler statsFileHandler;

    public FileStoreCommitImpl(
            FileIO fileIO,
            SchemaManager schemaManager,
            String commitUser,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            IndexManifestFile.Factory indexManifestFileFactory,
            FileStoreScan scan,
            int numBucket,
            MemorySize manifestTargetSize,
            MemorySize manifestFullCompactionSize,
            int manifestMergeMinCount,
            boolean dynamicPartitionOverwrite,
            @Nullable Comparator<InternalRow> keyComparator,
            String branchName,
            StatsFileHandler statsFileHandler) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.commitUser = commitUser;
        this.partitionType = partitionType;
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.manifestFile = manifestFileFactory.create();
        this.manifestList = manifestListFactory.create();
        this.indexManifestFile = indexManifestFileFactory.create();
        this.scan = scan;
        this.numBucket = numBucket;
        this.manifestTargetSize = manifestTargetSize;
        this.manifestFullCompactionSize = manifestFullCompactionSize;
        this.manifestMergeMinCount = manifestMergeMinCount;
        this.dynamicPartitionOverwrite = dynamicPartitionOverwrite;
        this.keyComparator = keyComparator;
        this.branchName = branchName;

        this.lock = null;
        this.ignoreEmptyCommit = true;
        this.commitMetrics = null;
        this.statsFileHandler = statsFileHandler;
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
        commit(committable, properties, false);
    }

    @Override
    public void commit(
            ManifestCommittable committable,
            Map<String, String> properties,
            boolean checkAppendFiles) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit\n" + committable.toString());
        }

        long started = System.nanoTime();
        int generatedSnapshot = 0;
        int attempts = 0;
        Snapshot latestSnapshot = null;
        Long safeLatestSnapshotId = null;
        List<SimpleFileEntry> baseEntries = new ArrayList<>();

        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        List<IndexManifestEntry> appendHashIndexFiles = new ArrayList<>();
        List<IndexManifestEntry> compactDvIndexFiles = new ArrayList<>();
        collectChanges(
                committable.fileCommittables(),
                appendTableFiles,
                appendChangelog,
                compactTableFiles,
                compactChangelog,
                appendHashIndexFiles,
                compactDvIndexFiles);
        try {
            List<SimpleFileEntry> appendSimpleEntries = SimpleFileEntry.from(appendTableFiles);
            if (!ignoreEmptyCommit
                    || !appendTableFiles.isEmpty()
                    || !appendChangelog.isEmpty()
                    || !appendHashIndexFiles.isEmpty()) {
                // Optimization for common path.
                // Step 1:
                // Read manifest entries from changed partitions here and check for conflicts.
                // If there are no other jobs committing at the same time,
                // we can skip conflict checking in tryCommit method.
                // This optimization is mainly used to decrease the number of times we read from
                // files.
                latestSnapshot = snapshotManager.latestSnapshot(branchName);
                if (latestSnapshot != null && checkAppendFiles) {
                    // it is possible that some partitions only have compact changes,
                    // so we need to contain all changes
                    baseEntries.addAll(
                            readAllEntriesFromChangedPartitions(
                                    latestSnapshot, appendTableFiles, compactTableFiles));
                    noConflictsOrFail(
                            latestSnapshot.commitUser(), baseEntries, appendSimpleEntries);
                    safeLatestSnapshotId = latestSnapshot.id();
                }

                attempts +=
                        tryCommit(
                                appendTableFiles,
                                appendChangelog,
                                appendHashIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                Snapshot.CommitKind.APPEND,
                                noConflictCheck(),
                                branchName,
                                null);
                generatedSnapshot += 1;
            }

            if (!compactTableFiles.isEmpty()
                    || !compactChangelog.isEmpty()
                    || !compactDvIndexFiles.isEmpty()) {
                // Optimization for common path.
                // Step 2:
                // Add appendChanges to the manifest entries read above and check for conflicts.
                // If there are no other jobs committing at the same time,
                // we can skip conflict checking in tryCommit method.
                // This optimization is mainly used to decrease the number of times we read from
                // files.
                if (safeLatestSnapshotId != null) {
                    baseEntries.addAll(appendSimpleEntries);
                    noConflictsOrFail(
                            latestSnapshot.commitUser(),
                            baseEntries,
                            SimpleFileEntry.from(compactTableFiles));
                    // assume this compact commit follows just after the append commit created above
                    safeLatestSnapshotId += 1;
                }

                attempts +=
                        tryCommit(
                                compactTableFiles,
                                compactChangelog,
                                compactDvIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                Snapshot.CommitKind.COMPACT,
                                hasConflictChecked(safeLatestSnapshotId),
                                branchName,
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            if (this.commitMetrics != null) {
                reportCommit(
                        appendTableFiles,
                        appendChangelog,
                        compactTableFiles,
                        compactChangelog,
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
    }

    private void reportCommit(
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelogFiles,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelogFiles,
            long commitDuration,
            int generatedSnapshots,
            int attempts) {
        CommitStats commitStats =
                new CommitStats(
                        appendTableFiles,
                        appendChangelogFiles,
                        compactTableFiles,
                        compactChangelogFiles,
                        commitDuration,
                        generatedSnapshots,
                        attempts);
        commitMetrics.reportCommit(commitStats);
    }

    @Override
    public void overwrite(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ready to overwrite partition {}\nManifestCommittable: {}\nProperties: {}",
                    partition,
                    committable,
                    properties);
        }

        long started = System.nanoTime();
        int generatedSnapshot = 0;
        int attempts = 0;
        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        List<IndexManifestEntry> appendHashIndexFiles = new ArrayList<>();
        List<IndexManifestEntry> compactDvIndexFiles = new ArrayList<>();
        collectChanges(
                committable.fileCommittables(),
                appendTableFiles,
                appendChangelog,
                compactTableFiles,
                compactChangelog,
                appendHashIndexFiles,
                compactDvIndexFiles);

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

        try {
            boolean skipOverwrite = false;
            // partition filter is built from static or dynamic partition according to properties
            Predicate partitionFilter = null;
            if (dynamicPartitionOverwrite) {
                if (appendTableFiles.isEmpty()) {
                    // in dynamic mode, if there is no changes to commit, no data will be deleted
                    skipOverwrite = true;
                } else {
                    partitionFilter =
                            appendTableFiles.stream()
                                    .map(ManifestEntry::partition)
                                    .distinct()
                                    // partition filter is built from new data's partitions
                                    .map(p -> PredicateBuilder.equalPartition(p, partitionType))
                                    .reduce(PredicateBuilder::or)
                                    .orElseThrow(
                                            () ->
                                                    new RuntimeException(
                                                            "Failed to get dynamic partition filter. This is unexpected."));
                }
            } else {
                partitionFilter = PredicateBuilder.partition(partition, partitionType);
                // sanity check, all changes must be done within the given partition
                if (partitionFilter != null) {
                    for (ManifestEntry entry : appendTableFiles) {
                        if (!partitionFilter.test(entry.partition())) {
                            throw new IllegalArgumentException(
                                    "Trying to overwrite partition "
                                            + partition
                                            + ", but the changes in "
                                            + pathFactory.getPartitionString(entry.partition())
                                            + " does not belong to this partition");
                        }
                    }
                }
            }

            // overwrite new files
            if (!skipOverwrite) {
                attempts +=
                        tryOverwrite(
                                partitionFilter,
                                appendTableFiles,
                                appendHashIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets());
                generatedSnapshot += 1;
            }

            if (!compactTableFiles.isEmpty() || !compactDvIndexFiles.isEmpty()) {
                attempts +=
                        tryCommit(
                                compactTableFiles,
                                Collections.emptyList(),
                                compactDvIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                Snapshot.CommitKind.COMPACT,
                                mustConflictCheck(),
                                branchName,
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            if (this.commitMetrics != null) {
                reportCommit(
                        appendTableFiles,
                        Collections.emptyList(),
                        compactTableFiles,
                        Collections.emptyList(),
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
    }

    @Override
    public void dropPartitions(List<Map<String, String>> partitions, long commitIdentifier) {
        Preconditions.checkArgument(!partitions.isEmpty(), "Partitions list cannot be empty.");

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ready to drop partitions {}",
                    partitions.stream().map(Objects::toString).collect(Collectors.joining(",")));
        }

        Predicate partitionFilter =
                partitions.stream()
                        .map(partition -> PredicateBuilder.partition(partition, partitionType))
                        .reduce(PredicateBuilder::or)
                        .orElseThrow(() -> new RuntimeException("Failed to get partition filter."));

        tryOverwrite(
                partitionFilter,
                Collections.emptyList(),
                Collections.emptyList(),
                commitIdentifier,
                null,
                new HashMap<>());
    }

    @Override
    public void truncateTable(long commitIdentifier) {
        tryOverwrite(
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                commitIdentifier,
                null,
                new HashMap<>());
    }

    @Override
    public void abort(List<CommitMessage> commitMessages) {
        Map<Pair<BinaryRow, Integer>, DataFilePathFactory> factoryMap = new HashMap<>();
        for (CommitMessage message : commitMessages) {
            DataFilePathFactory pathFactory =
                    factoryMap.computeIfAbsent(
                            Pair.of(message.partition(), message.bucket()),
                            k ->
                                    this.pathFactory.createDataFilePathFactory(
                                            k.getKey(), k.getValue()));
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            List<DataFileMeta> toDelete = new ArrayList<>();
            toDelete.addAll(commitMessage.newFilesIncrement().newFiles());
            toDelete.addAll(commitMessage.newFilesIncrement().changelogFiles());
            toDelete.addAll(commitMessage.compactIncrement().compactAfter());
            toDelete.addAll(commitMessage.compactIncrement().changelogFiles());

            for (DataFileMeta file : toDelete) {
                fileIO.deleteQuietly(pathFactory.toPath(file.fileName()));
            }
        }
    }

    @Override
    public FileStoreCommit withMetrics(CommitMetrics metrics) {
        this.commitMetrics = metrics;
        return this;
    }

    @Override
    public void commitStatistics(Statistics stats, long commitIdentifier) {
        String statsFileName = statsFileHandler.writeStats(stats);
        tryCommit(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                commitIdentifier,
                null,
                Collections.emptyMap(),
                Snapshot.CommitKind.ANALYZE,
                noConflictCheck(),
                branchName,
                statsFileName);
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory;
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    private void collectChanges(
            List<CommitMessage> commitMessages,
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelog,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelog,
            List<IndexManifestEntry> appendHashIndexFiles,
            List<IndexManifestEntry> compactDvIndexFiles) {
        for (CommitMessage message : commitMessages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            commitMessage
                    .newFilesIncrement()
                    .newFiles()
                    .forEach(m -> appendTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
            commitMessage
                    .newFilesIncrement()
                    .deletedFiles()
                    .forEach(
                            m ->
                                    appendTableFiles.add(
                                            makeEntry(FileKind.DELETE, commitMessage, m)));
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
            commitMessage
                    .indexIncrement()
                    .newIndexFiles()
                    .forEach(
                            f -> {
                                switch (f.indexType()) {
                                    case HASH_INDEX:
                                        appendHashIndexFiles.add(
                                                new IndexManifestEntry(
                                                        FileKind.ADD,
                                                        commitMessage.partition(),
                                                        commitMessage.bucket(),
                                                        f));
                                        break;
                                    case DELETION_VECTORS_INDEX:
                                        compactDvIndexFiles.add(
                                                new IndexManifestEntry(
                                                        FileKind.ADD,
                                                        commitMessage.partition(),
                                                        commitMessage.bucket(),
                                                        f));
                                        break;
                                    default:
                                        throw new RuntimeException(
                                                "Unknown index type: " + f.indexType());
                                }
                            });
        }
    }

    private ManifestEntry makeEntry(FileKind kind, CommitMessage commitMessage, DataFileMeta file) {
        return new ManifestEntry(
                kind, commitMessage.partition(), commitMessage.bucket(), numBucket, file);
    }

    private int tryCommit(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Snapshot.CommitKind commitKind,
            ConflictCheck conflictCheck,
            String branchName,
            @Nullable String statsFileName) {
        int cnt = 0;
        while (true) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot(branchName);
            cnt++;
            if (tryCommitOnce(
                    tableFiles,
                    changelogFiles,
                    indexFiles,
                    identifier,
                    watermark,
                    logOffsets,
                    commitKind,
                    latestSnapshot,
                    conflictCheck,
                    branchName,
                    statsFileName)) {
                break;
            }
        }
        return cnt;
    }

    private int tryOverwrite(
            Predicate partitionFilter,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets) {
        int cnt = 0;
        while (true) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();

            cnt++;
            List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
            List<IndexManifestEntry> indexChangesWithOverwrite = new ArrayList<>();
            if (latestSnapshot != null) {
                List<ManifestEntry> currentEntries =
                        scan.withSnapshot(latestSnapshot)
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

                // collect index files
                if (latestSnapshot.indexManifest() != null) {
                    List<IndexManifestEntry> entries =
                            indexManifestFile.read(latestSnapshot.indexManifest());
                    for (IndexManifestEntry entry : entries) {
                        if (partitionFilter == null || partitionFilter.test(entry.partition())) {
                            indexChangesWithOverwrite.add(entry.toDeleteEntry());
                        }
                    }
                }
            }
            changesWithOverwrite.addAll(changes);
            indexChangesWithOverwrite.addAll(indexFiles);

            if (tryCommitOnce(
                    changesWithOverwrite,
                    Collections.emptyList(),
                    indexChangesWithOverwrite,
                    identifier,
                    watermark,
                    logOffsets,
                    Snapshot.CommitKind.OVERWRITE,
                    latestSnapshot,
                    mustConflictCheck(),
                    branchName,
                    null)) {
                break;
            }
        }
        return cnt;
    }

    @VisibleForTesting
    public boolean tryCommitOnce(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Snapshot.CommitKind commitKind,
            @Nullable Snapshot latestSnapshot,
            ConflictCheck conflictCheck,
            String branchName,
            @Nullable String newStatsFileName) {
        long newSnapshotId =
                latestSnapshot == null ? Snapshot.FIRST_SNAPSHOT_ID : latestSnapshot.id() + 1;
        Path newSnapshotPath =
                branchName.equals(DEFAULT_MAIN_BRANCH)
                        ? snapshotManager.snapshotPath(newSnapshotId)
                        : snapshotManager.branchSnapshotPath(branchName, newSnapshotId);

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

        if (latestSnapshot != null && conflictCheck.shouldCheck(latestSnapshot.id())) {
            // latestSnapshotId is different from the snapshot id we've checked for conflicts,
            // so we have to check again
            noConflictsOrFail(latestSnapshot.commitUser(), latestSnapshot, tableFiles);
        }

        Snapshot newSnapshot;
        String previousChangesListName = null;
        String newChangesListName = null;
        String changelogListName = null;
        String newIndexManifest = null;
        List<ManifestFileMeta> oldMetas = new ArrayList<>();
        List<ManifestFileMeta> newMetas = new ArrayList<>();
        List<ManifestFileMeta> changelogMetas = new ArrayList<>();
        try {
            long previousTotalRecordCount = 0L;
            Long currentWatermark = watermark;
            String previousIndexManifest = null;
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
                previousIndexManifest = latestSnapshot.indexManifest();
            }
            // merge manifest files with changes
            newMetas.addAll(
                    ManifestFileMeta.merge(
                            oldMetas,
                            manifestFile,
                            manifestTargetSize.getBytes(),
                            manifestMergeMinCount,
                            manifestFullCompactionSize.getBytes(),
                            partitionType));
            previousChangesListName = manifestList.write(newMetas);

            // the added records subtract the deleted records from
            long deltaRecordCount =
                    Snapshot.recordCountAdd(tableFiles) - Snapshot.recordCountDelete(tableFiles);
            long totalRecordCount = previousTotalRecordCount + deltaRecordCount;

            // write new changes into manifest files
            List<ManifestFileMeta> newChangesManifests = manifestFile.write(tableFiles);
            newMetas.addAll(newChangesManifests);
            newChangesListName = manifestList.write(newChangesManifests);

            // write changelog into manifest files
            if (!changelogFiles.isEmpty()) {
                changelogMetas.addAll(manifestFile.write(changelogFiles));
                changelogListName = manifestList.write(changelogMetas);
            }

            // write new index manifest
            String indexManifest = indexManifestFile.merge(previousIndexManifest, indexFiles);
            if (!Objects.equals(indexManifest, previousIndexManifest)) {
                newIndexManifest = indexManifest;
            }

            long latestSchemaId = schemaManager.latest(branchName).get().id();

            // write new stats or inherit from the previous snapshot
            String statsFileName = null;
            if (newStatsFileName != null) {
                statsFileName = newStatsFileName;
            } else if (latestSnapshot != null) {
                Optional<Statistics> previousStatistic = statsFileHandler.readStats(latestSnapshot);
                if (previousStatistic.isPresent()) {
                    if (previousStatistic.get().schemaId() != latestSchemaId) {
                        LOG.warn("Schema changed, stats will not be inherited");
                    } else {
                        statsFileName = latestSnapshot.statistics();
                    }
                }
            }

            // prepare snapshot file
            newSnapshot =
                    new Snapshot(
                            newSnapshotId,
                            latestSchemaId,
                            previousChangesListName,
                            newChangesListName,
                            changelogListName,
                            indexManifest,
                            commitUser,
                            identifier,
                            commitKind,
                            System.currentTimeMillis(),
                            logOffsets,
                            totalRecordCount,
                            deltaRecordCount,
                            Snapshot.recordCount(changelogFiles),
                            currentWatermark,
                            statsFileName);
        } catch (Throwable e) {
            // fails when preparing for commit, we should clean up
            cleanUpTmpManifests(
                    previousChangesListName,
                    newChangesListName,
                    changelogListName,
                    newIndexManifest,
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
                            snapshotManager.commitLatestHint(newSnapshotId, branchName);
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
                newIndexManifest,
                oldMetas,
                newMetas,
                changelogMetas);
        return false;
    }

    @SafeVarargs
    private final List<SimpleFileEntry> readAllEntriesFromChangedPartitions(
            Snapshot snapshot, List<ManifestEntry>... changes) {
        List<BinaryRow> changedPartitions =
                Arrays.stream(changes)
                        .flatMap(Collection::stream)
                        .map(ManifestEntry::partition)
                        .distinct()
                        .collect(Collectors.toList());
        try {
            return scan.withSnapshot(snapshot)
                    .withPartitionFilter(changedPartitions)
                    .readSimpleEntries();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read manifest entries from changed partitions.", e);
        }
    }

    private void noConflictsOrFail(
            String baseCommitUser, Snapshot latestSnapshot, List<ManifestEntry> changes) {
        noConflictsOrFail(
                baseCommitUser,
                readAllEntriesFromChangedPartitions(latestSnapshot, changes),
                SimpleFileEntry.from(changes));
    }

    private void noConflictsOrFail(
            String baseCommitUser,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> changes) {
        List<SimpleFileEntry> allEntries = new ArrayList<>(baseEntries);
        allEntries.addAll(changes);

        Collection<SimpleFileEntry> mergedEntries;
        try {
            // merge manifest entries and also check if the files we want to delete are still there
            mergedEntries = FileEntry.mergeEntries(allEntries);
            FileEntry.assertNoDelete(mergedEntries);
        } catch (Throwable e) {
            Pair<RuntimeException, RuntimeException> conflictException =
                    createConflictException(
                            "File deletion conflicts detected! Give up committing.",
                            baseCommitUser,
                            baseEntries,
                            changes,
                            e,
                            50);
            LOG.warn("", conflictException.getLeft());
            throw conflictException.getRight();
        }

        // fast exit for file store without keys
        if (keyComparator == null) {
            return;
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
                                    changes,
                                    null,
                                    50);

                    LOG.warn("", conflictException.getLeft());
                    throw conflictException.getRight();
                }
            }
        }
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
            Throwable cause,
            int maxEntry) {
        String possibleCauses =
                String.join(
                        "\n",
                        "Don't panic!",
                        "Conflicts during commits are normal and this failure is intended to resolve the conflicts.",
                        "Conflicts are mainly caused by the following scenarios:",
                        "1. Your job is suffering from back-pressuring.",
                        "   There are too many snapshots waiting to be committed "
                                + "and an exception occurred during the commit procedure "
                                + "(most probably due to checkpoint timeout).",
                        "   See https://paimon.apache.org/docs/master/maintenance/write-performance/ "
                                + "for how to improve writing performance.",
                        "2. Multiple jobs are writing into the same partition at the same time, "
                                + "or you use STATEMENT SET to execute multiple INSERT statements into the same Paimon table.",
                        "   You'll probably see different base commit user and current commit user below.",
                        "   You can use "
                                + "https://paimon.apache.org/docs/master/maintenance/dedicated-compaction#dedicated-compaction-job"
                                + " to support multiple writing.",
                        "3. You're recovering from an old savepoint, or you're creating multiple jobs from a savepoint.",
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

    private void cleanUpTmpManifests(
            String previousChangesListName,
            String newChangesListName,
            String changelogListName,
            String newIndexManifest,
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
        if (newIndexManifest != null) {
            indexManifestFile.delete(newIndexManifest);
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

    /** Should do conflict check. */
    interface ConflictCheck {
        boolean shouldCheck(long latestSnapshot);
    }

    static ConflictCheck hasConflictChecked(@Nullable Long checkedLatestSnapshotId) {
        return latestSnapshot -> !Objects.equals(latestSnapshot, checkedLatestSnapshotId);
    }

    static ConflictCheck noConflictCheck() {
        return latestSnapshot -> false;
    }

    public static ConflictCheck mustConflictCheck() {
        return latestSnapshot -> true;
    }
}
