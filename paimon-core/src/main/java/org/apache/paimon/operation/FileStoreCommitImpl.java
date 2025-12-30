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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
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
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.commit.CommitChanges;
import org.apache.paimon.operation.commit.CommitChangesProvider;
import org.apache.paimon.operation.commit.CommitCleaner;
import org.apache.paimon.operation.commit.CommitKindProvider;
import org.apache.paimon.operation.commit.CommitResult;
import org.apache.paimon.operation.commit.CommitScanner;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.operation.commit.ConflictDetection.ConflictCheck;
import org.apache.paimon.operation.commit.ManifestEntryChanges;
import org.apache.paimon.operation.commit.RetryCommitResult;
import org.apache.paimon.operation.commit.RowTrackingCommitUtils.RowTrackingAssigned;
import org.apache.paimon.operation.commit.SuccessCommitResult;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.operation.metrics.CommitStats;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.manifest.ManifestEntry.nullableRecordCount;
import static org.apache.paimon.manifest.ManifestEntry.recordCountAdd;
import static org.apache.paimon.manifest.ManifestEntry.recordCountDelete;
import static org.apache.paimon.operation.commit.ConflictDetection.hasConflictChecked;
import static org.apache.paimon.operation.commit.ConflictDetection.mustConflictCheck;
import static org.apache.paimon.operation.commit.ConflictDetection.noConflictCheck;
import static org.apache.paimon.operation.commit.ManifestEntryChanges.changedPartitions;
import static org.apache.paimon.operation.commit.RowTrackingCommitUtils.assignRowTracking;
import static org.apache.paimon.partition.PartitionPredicate.createBinaryPartitions;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Default implementation of {@link FileStoreCommit}.
 *
 * <p>This class provides an atomic commit method to the user.
 *
 * <ol>
 *   <li>Before calling {@link #commit}, if user cannot determine if this commit is done before,
 *       user should first call {@link #filterCommitted}.
 *   <li>Before committing, it will first check for conflicts by checking if all files to be removed
 *       currently exists, and if modified files have overlapping key ranges with existing files.
 *   <li>After that it use the external {@link SnapshotCommit} (if provided) or the atomic rename of
 *       the file system to ensure atomicity.
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

    private final SnapshotCommit snapshotCommit;
    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final String tableName;
    private final String commitUser;
    private final RowType partitionType;
    private final CoreOptions options;
    private final String partitionDefaultName;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;
    private final IndexManifestFile indexManifestFile;
    private final CommitScanner scanner;
    private final int numBucket;
    private final MemorySize manifestTargetSize;
    private final MemorySize manifestFullCompactionSize;
    private final int manifestMergeMinCount;
    private final boolean dynamicPartitionOverwrite;
    private final String branchName;
    @Nullable private final Integer manifestReadParallelism;
    private final List<CommitCallback> commitCallbacks;
    private final StatsFileHandler statsFileHandler;
    private final BucketMode bucketMode;
    private final long commitTimeout;
    private final long commitMinRetryWait;
    private final long commitMaxRetryWait;
    private final int commitMaxRetries;
    @Nullable private Long strictModeLastSafeSnapshot;
    private final InternalRowPartitionComputer partitionComputer;
    private final boolean rowTrackingEnabled;
    private final boolean discardDuplicateFiles;
    private final ConflictDetection conflictDetection;
    private final CommitCleaner commitCleaner;

    private boolean ignoreEmptyCommit;
    private CommitMetrics commitMetrics;
    private boolean appendCommitCheckConflict = false;

    public FileStoreCommitImpl(
            SnapshotCommit snapshotCommit,
            FileIO fileIO,
            SchemaManager schemaManager,
            String tableName,
            String commitUser,
            RowType partitionType,
            CoreOptions options,
            String partitionDefaultName,
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
            String branchName,
            StatsFileHandler statsFileHandler,
            BucketMode bucketMode,
            @Nullable Integer manifestReadParallelism,
            List<CommitCallback> commitCallbacks,
            int commitMaxRetries,
            long commitTimeout,
            long commitMinRetryWait,
            long commitMaxRetryWait,
            @Nullable Long strictModeLastSafeSnapshot,
            boolean rowTrackingEnabled,
            boolean discardDuplicateFiles,
            ConflictDetection conflictDetection) {
        this.snapshotCommit = snapshotCommit;
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.tableName = tableName;
        this.commitUser = commitUser;
        this.partitionType = partitionType;
        this.options = options;
        this.partitionDefaultName = partitionDefaultName;
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.manifestFile = manifestFileFactory.create();
        this.manifestList = manifestListFactory.create();
        this.indexManifestFile = indexManifestFileFactory.create();
        this.scanner = new CommitScanner(scan, indexManifestFile, options);
        this.numBucket = numBucket;
        this.manifestTargetSize = manifestTargetSize;
        this.manifestFullCompactionSize = manifestFullCompactionSize;
        this.manifestMergeMinCount = manifestMergeMinCount;
        this.dynamicPartitionOverwrite = dynamicPartitionOverwrite;
        this.branchName = branchName;
        this.manifestReadParallelism = manifestReadParallelism;
        this.commitCallbacks = commitCallbacks;
        this.commitMaxRetries = commitMaxRetries;
        this.commitTimeout = commitTimeout;
        this.commitMinRetryWait = commitMinRetryWait;
        this.commitMaxRetryWait = commitMaxRetryWait;
        this.strictModeLastSafeSnapshot = strictModeLastSafeSnapshot;
        this.partitionComputer =
                new InternalRowPartitionComputer(
                        options.partitionDefaultName(),
                        partitionType,
                        partitionType.getFieldNames().toArray(new String[0]),
                        options.legacyPartitionName());
        this.ignoreEmptyCommit = true;
        this.commitMetrics = null;
        this.statsFileHandler = statsFileHandler;
        this.bucketMode = bucketMode;
        this.rowTrackingEnabled = rowTrackingEnabled;
        this.discardDuplicateFiles = discardDuplicateFiles;
        this.conflictDetection = conflictDetection;
        this.commitCleaner = new CommitCleaner(manifestList, manifestFile, indexManifestFile);
    }

    @Override
    public FileStoreCommit ignoreEmptyCommit(boolean ignoreEmptyCommit) {
        this.ignoreEmptyCommit = ignoreEmptyCommit;
        return this;
    }

    @Override
    public FileStoreCommit withPartitionExpire(PartitionExpire partitionExpire) {
        this.conflictDetection.withPartitionExpire(partitionExpire);
        return this;
    }

    @Override
    public FileStoreCommit appendCommitCheckConflict(boolean appendCommitCheckConflict) {
        this.appendCommitCheckConflict = appendCommitCheckConflict;
        return this;
    }

    @Override
    public List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committables) {
        // nothing to filter, fast exit
        if (committables.isEmpty()) {
            return committables;
        }

        for (int i = 1; i < committables.size(); i++) {
            checkArgument(
                    committables.get(i).identifier() > committables.get(i - 1).identifier(),
                    "Committables must be sorted according to identifiers before filtering. This is unexpected.");
        }

        Optional<Snapshot> latestSnapshot = snapshotManager.latestSnapshotOfUser(commitUser);
        if (latestSnapshot.isPresent()) {
            List<ManifestCommittable> result = new ArrayList<>();
            for (ManifestCommittable committable : committables) {
                // if committable is newer than latest snapshot, then it hasn't been committed
                if (committable.identifier() > latestSnapshot.get().commitIdentifier()) {
                    result.add(committable);
                } else {
                    commitCallbacks.forEach(callback -> callback.retry(committable));
                }
            }
            return result;
        } else {
            // if there is no previous snapshots then nothing should be filtered
            return committables;
        }
    }

    @Override
    public int commit(ManifestCommittable committable, boolean checkAppendFiles) {
        LOG.info(
                "Ready to commit to table {}, number of commit messages: {}",
                tableName,
                committable.fileCommittables().size());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit\n{}", committable);
        }

        long started = System.nanoTime();
        int generatedSnapshot = 0;
        int attempts = 0;
        Snapshot latestSnapshot = null;
        Long safeLatestSnapshotId = null;
        List<SimpleFileEntry> baseEntries = new ArrayList<>();

        ManifestEntryChanges changes = collectChanges(committable.fileCommittables());
        try {
            List<SimpleFileEntry> appendSimpleEntries =
                    SimpleFileEntry.from(changes.appendTableFiles);
            if (!ignoreEmptyCommit
                    || !changes.appendTableFiles.isEmpty()
                    || !changes.appendChangelog.isEmpty()
                    || !changes.appendIndexFiles.isEmpty()) {
                // Optimization for common path.
                // Step 1:
                // Read manifest entries from changed partitions here and check for conflicts.
                // If there are no other jobs committing at the same time,
                // we can skip conflict checking in tryCommit method.
                // This optimization is mainly used to decrease the number of times we read from
                // files.
                latestSnapshot = snapshotManager.latestSnapshot();
                CommitKind commitKind = CommitKind.APPEND;
                ConflictCheck conflictCheck = noConflictCheck();
                if (containsFileDeletionOrDeletionVectors(
                        appendSimpleEntries, changes.appendIndexFiles)) {
                    commitKind = CommitKind.OVERWRITE;
                    conflictCheck = mustConflictCheck();
                } else if (latestSnapshot != null && appendCommitCheckConflict) {
                    conflictCheck = mustConflictCheck();
                }

                boolean discardDuplicate = discardDuplicateFiles && commitKind == CommitKind.APPEND;
                if (discardDuplicate) {
                    checkAppendFiles = true;
                }

                if (latestSnapshot != null && checkAppendFiles) {
                    // it is possible that some partitions only have compact changes,
                    // so we need to contain all changes
                    baseEntries.addAll(
                            scanner.readAllEntriesFromChangedPartitions(
                                    latestSnapshot,
                                    changedPartitions(
                                            changes.appendTableFiles,
                                            changes.compactTableFiles,
                                            changes.appendIndexFiles)));
                    if (discardDuplicate) {
                        Set<FileEntry.Identifier> baseIdentifiers =
                                baseEntries.stream()
                                        .map(FileEntry::identifier)
                                        .collect(Collectors.toSet());
                        changes.appendTableFiles =
                                changes.appendTableFiles.stream()
                                        .filter(
                                                entry ->
                                                        !baseIdentifiers.contains(
                                                                entry.identifier()))
                                        .collect(Collectors.toList());
                        appendSimpleEntries = SimpleFileEntry.from(changes.appendTableFiles);
                    }
                    conflictDetection.checkNoConflictsOrFail(
                            latestSnapshot,
                            baseEntries,
                            appendSimpleEntries,
                            changes.appendIndexFiles,
                            commitKind);
                    safeLatestSnapshotId = latestSnapshot.id();
                }

                attempts +=
                        tryCommit(
                                CommitChangesProvider.provider(
                                        changes.appendTableFiles,
                                        changes.appendChangelog,
                                        changes.appendIndexFiles),
                                committable.identifier(),
                                committable.watermark(),
                                committable.properties(),
                                CommitKindProvider.provider(commitKind),
                                conflictCheck,
                                null);
                generatedSnapshot += 1;
            }

            if (!changes.compactTableFiles.isEmpty()
                    || !changes.compactChangelog.isEmpty()
                    || !changes.compactIndexFiles.isEmpty()) {
                // Optimization for common path.
                // Step 2:
                // Add appendChanges to the manifest entries read above and check for conflicts.
                // If there are no other jobs committing at the same time,
                // we can skip conflict checking in tryCommit method.
                // This optimization is mainly used to decrease the number of times we read from
                // files.
                if (safeLatestSnapshotId != null) {
                    baseEntries.addAll(appendSimpleEntries);
                    conflictDetection.checkNoConflictsOrFail(
                            latestSnapshot,
                            baseEntries,
                            SimpleFileEntry.from(changes.compactTableFiles),
                            changes.compactIndexFiles,
                            CommitKind.COMPACT);
                    // assume this compact commit follows just after the append commit created above
                    safeLatestSnapshotId += 1;
                }

                attempts +=
                        tryCommit(
                                CommitChangesProvider.provider(
                                        changes.compactTableFiles,
                                        changes.compactChangelog,
                                        changes.compactIndexFiles),
                                committable.identifier(),
                                committable.watermark(),
                                committable.properties(),
                                CommitKindProvider.provider(CommitKind.COMPACT),
                                hasConflictChecked(safeLatestSnapshotId),
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            LOG.info(
                    "Finished (Uncertain of success) commit to table {}, duration {} ms",
                    tableName,
                    commitDuration);
            if (this.commitMetrics != null) {
                reportCommit(
                        changes.appendTableFiles,
                        changes.appendChangelog,
                        changes.compactTableFiles,
                        changes.compactChangelog,
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
        return generatedSnapshot;
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

    private <T extends FileEntry> boolean containsFileDeletionOrDeletionVectors(
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
        return false;
    }

    @Override
    public int overwritePartition(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties) {
        LOG.info(
                "Ready to overwrite to table {}, number of commit messages: {}",
                tableName,
                committable.fileCommittables().size());
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

        ManifestEntryChanges changes = collectChanges(committable.fileCommittables());
        if (!changes.appendChangelog.isEmpty() || !changes.compactChangelog.isEmpty()) {
            StringBuilder warnMessage =
                    new StringBuilder(
                            "Overwrite mode currently does not commit any changelog.\n"
                                    + "Please make sure that the partition you're overwriting "
                                    + "is not being consumed by a streaming reader.\n"
                                    + "Ignored changelog files are:\n");
            for (ManifestEntry entry : changes.appendChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            for (ManifestEntry entry : changes.compactChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            LOG.warn(warnMessage.toString());
        }

        try {
            boolean skipOverwrite = false;
            // partition filter is built from static or dynamic partition according to properties
            PartitionPredicate partitionFilter = null;
            if (dynamicPartitionOverwrite) {
                if (changes.appendTableFiles.isEmpty()) {
                    // in dynamic mode, if there is no changes to commit, no data will be deleted
                    skipOverwrite = true;
                } else {
                    Set<BinaryRow> partitions =
                            changes.appendTableFiles.stream()
                                    .map(ManifestEntry::partition)
                                    .collect(Collectors.toSet());
                    partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
                }
            } else {
                // partition may be partial partition fields, so here must use predicate way.
                Predicate partitionPredicate =
                        createPartitionPredicate(partition, partitionType, partitionDefaultName);
                partitionFilter =
                        PartitionPredicate.fromPredicate(partitionType, partitionPredicate);
                // sanity check, all changes must be done within the given partition
                if (partitionFilter != null) {
                    for (ManifestEntry entry : changes.appendTableFiles) {
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

            boolean withCompact =
                    !changes.compactTableFiles.isEmpty() || !changes.compactIndexFiles.isEmpty();

            if (!withCompact) {
                // try upgrade
                changes.appendTableFiles = tryUpgrade(changes.appendTableFiles);
            }

            // overwrite new files
            if (!skipOverwrite) {
                attempts +=
                        tryOverwritePartition(
                                partitionFilter,
                                changes.appendTableFiles,
                                changes.appendIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.properties());
                generatedSnapshot += 1;
            }

            if (withCompact) {
                attempts +=
                        tryCommit(
                                CommitChangesProvider.provider(
                                        changes.compactTableFiles,
                                        emptyList(),
                                        changes.compactIndexFiles),
                                committable.identifier(),
                                committable.watermark(),
                                committable.properties(),
                                CommitKindProvider.provider(CommitKind.COMPACT),
                                mustConflictCheck(),
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            LOG.info("Finished overwrite to table {}, duration {} ms", tableName, commitDuration);
            if (this.commitMetrics != null) {
                reportCommit(
                        changes.appendTableFiles,
                        emptyList(),
                        changes.compactTableFiles,
                        emptyList(),
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
        return generatedSnapshot;
    }

    private List<ManifestEntry> tryUpgrade(List<ManifestEntry> appendFiles) {
        if (!options.overwriteUpgrade()) {
            return appendFiles;
        }
        Comparator<InternalRow> keyComparator = conflictDetection.keyComparator();
        if (keyComparator == null) {
            return appendFiles;
        }
        for (ManifestEntry entry : appendFiles) {
            if (entry.level() > 0 || entry.bucket() < 0) {
                return appendFiles;
            }
        }

        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> buckets = new HashMap<>();
        for (ManifestEntry entry : appendFiles) {
            buckets.computeIfAbsent(
                            Pair.of(entry.partition(), entry.bucket()), k -> new ArrayList<>())
                    .add(entry);
        }

        List<ManifestEntry> results = new ArrayList<>();
        int maxLevel = options.numLevels() - 1;
        outer:
        for (List<ManifestEntry> entries : buckets.values()) {
            List<ManifestEntry> newEntries = new ArrayList<>(entries);
            newEntries.sort((a, b) -> keyComparator.compare(a.minKey(), b.minKey()));
            for (int i = 0; i + 1 < newEntries.size(); i++) {
                ManifestEntry a = newEntries.get(i);
                ManifestEntry b = newEntries.get(i + 1);
                if (keyComparator.compare(a.maxKey(), b.minKey()) >= 0) {
                    results.addAll(entries);
                    continue outer;
                }
            }
            LOG.info("Upgraded for overwrite commit.");
            for (ManifestEntry entry : newEntries) {
                results.add(entry.upgrade(maxLevel));
            }
        }

        return results;
    }

    @Override
    public void dropPartitions(List<Map<String, String>> partitions, long commitIdentifier) {
        checkArgument(!partitions.isEmpty(), "Partitions list cannot be empty.");

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ready to drop partitions {}",
                    partitions.stream().map(Objects::toString).collect(Collectors.joining(",")));
        }

        boolean fullMode =
                partitions.stream().allMatch(part -> part.size() == partitionType.getFieldCount());
        PartitionPredicate partitionFilter;
        if (fullMode) {
            List<BinaryRow> binaryPartitions =
                    createBinaryPartitions(partitions, partitionType, partitionDefaultName);
            partitionFilter = PartitionPredicate.fromMultiple(partitionType, binaryPartitions);
        } else {
            // partitions may be partial partition fields, so here must to use predicate way.
            Predicate predicate =
                    partitions.stream()
                            .map(
                                    partition ->
                                            createPartitionPredicate(
                                                    partition, partitionType, partitionDefaultName))
                            .reduce(PredicateBuilder::or)
                            .orElseThrow(
                                    () -> new RuntimeException("Failed to get partition filter."));
            partitionFilter = PartitionPredicate.fromPredicate(partitionType, predicate);
        }

        tryOverwritePartition(
                partitionFilter, emptyList(), emptyList(), commitIdentifier, null, new HashMap<>());
    }

    @Override
    public void truncateTable(long commitIdentifier) {
        tryOverwritePartition(
                null, emptyList(), emptyList(), commitIdentifier, null, new HashMap<>());
    }

    @Override
    public void abort(List<CommitMessage> commitMessages) {
        DataFilePathFactories factories = new DataFilePathFactories(pathFactory);
        for (CommitMessage message : commitMessages) {
            DataFilePathFactory pathFactory = factories.get(message.partition(), message.bucket());
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            List<DataFileMeta> toDelete = new ArrayList<>();
            toDelete.addAll(commitMessage.newFilesIncrement().newFiles());
            toDelete.addAll(commitMessage.newFilesIncrement().changelogFiles());
            toDelete.addAll(commitMessage.compactIncrement().compactAfter());
            toDelete.addAll(commitMessage.compactIncrement().changelogFiles());

            for (DataFileMeta file : toDelete) {
                fileIO.deleteQuietly(pathFactory.toPath(file));
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
                CommitChangesProvider.provider(emptyList(), emptyList(), emptyList()),
                commitIdentifier,
                null,
                Collections.emptyMap(),
                CommitKindProvider.provider(CommitKind.ANALYZE),
                noConflictCheck(),
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

    private ManifestEntryChanges collectChanges(List<CommitMessage> commitMessages) {
        ManifestEntryChanges changes = new ManifestEntryChanges(numBucket);
        commitMessages.forEach(changes::collect);
        LOG.info("Finished collecting changes, including: {}", changes);
        return changes;
    }

    private int tryCommit(
            CommitChangesProvider changesProvider,
            long identifier,
            @Nullable Long watermark,
            Map<String, String> properties,
            CommitKindProvider commitKindProvider,
            ConflictCheck conflictCheck,
            @Nullable String statsFileName) {
        int retryCount = 0;
        RetryCommitResult retryResult = null;
        long startMillis = System.currentTimeMillis();
        while (true) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();
            CommitChanges changes = changesProvider.provide(latestSnapshot);
            CommitKind commitKind = commitKindProvider.provide(changes);
            CommitResult result =
                    tryCommitOnce(
                            retryResult,
                            changes.tableFiles,
                            changes.changelogFiles,
                            changes.indexFiles,
                            identifier,
                            watermark,
                            properties,
                            commitKind,
                            latestSnapshot,
                            conflictCheck,
                            statsFileName);

            if (result.isSuccess()) {
                break;
            }

            retryResult = (RetryCommitResult) result;

            if (System.currentTimeMillis() - startMillis > commitTimeout
                    || retryCount >= commitMaxRetries) {
                String message =
                        String.format(
                                "Commit failed after %s millis with %s retries, there maybe exist commit conflicts between multiple jobs.",
                                commitTimeout, retryCount);
                throw new RuntimeException(message, retryResult.exception);
            }

            commitRetryWait(retryCount);
            retryCount++;
        }
        return retryCount + 1;
    }

    /**
     * Try to overwrite partition.
     *
     * @param partitionFilter Partition filter indicating which partitions to overwrite, if {@code
     *     null}, overwrites the entire table.
     */
    private int tryOverwritePartition(
            @Nullable PartitionPredicate partitionFilter,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<String, String> properties) {
        CommitKindProvider commitKindProvider =
                commitChanges ->
                        containsFileDeletionOrDeletionVectors(
                                        commitChanges.tableFiles, commitChanges.indexFiles)
                                ? CommitKind.OVERWRITE
                                : CommitKind.APPEND;
        return tryCommit(
                latestSnapshot ->
                        scanner.readOverwriteChanges(
                                numBucket, changes, indexFiles, latestSnapshot, partitionFilter),
                identifier,
                watermark,
                properties,
                commitKindProvider,
                mustConflictCheck(),
                null);
    }

    @VisibleForTesting
    CommitResult tryCommitOnce(
            @Nullable RetryCommitResult retryResult,
            List<ManifestEntry> deltaFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<String, String> properties,
            CommitKind commitKind,
            @Nullable Snapshot latestSnapshot,
            ConflictCheck conflictCheck,
            @Nullable String newStatsFileName) {
        long startMillis = System.currentTimeMillis();

        // Check if the commit has been completed. At this point, there will be no more repeated
        // commits and just return success
        if (retryResult != null && latestSnapshot != null) {
            Map<Long, Snapshot> snapshotCache = new HashMap<>();
            snapshotCache.put(latestSnapshot.id(), latestSnapshot);
            long startCheckSnapshot = Snapshot.FIRST_SNAPSHOT_ID;
            if (retryResult.latestSnapshot != null) {
                snapshotCache.put(retryResult.latestSnapshot.id(), retryResult.latestSnapshot);
                startCheckSnapshot = retryResult.latestSnapshot.id() + 1;
            }
            for (long i = startCheckSnapshot; i <= latestSnapshot.id(); i++) {
                Snapshot snapshot = snapshotCache.computeIfAbsent(i, snapshotManager::snapshot);
                if (snapshot.commitUser().equals(commitUser)
                        && snapshot.commitIdentifier() == identifier
                        && snapshot.commitKind() == commitKind) {
                    return new SuccessCommitResult();
                }
            }
        }

        long newSnapshotId = Snapshot.FIRST_SNAPSHOT_ID;
        long firstRowIdStart = 0;
        if (latestSnapshot != null) {
            newSnapshotId = latestSnapshot.id() + 1;
            Long nextRowId = latestSnapshot.nextRowId();
            if (nextRowId != null) {
                firstRowIdStart = nextRowId;
            }
        }

        if (strictModeLastSafeSnapshot != null && strictModeLastSafeSnapshot >= 0) {
            conflictDetection.commitStrictModeCheck(
                    strictModeLastSafeSnapshot, newSnapshotId, commitKind, snapshotManager);
            strictModeLastSafeSnapshot = newSnapshotId - 1;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit table files to snapshot {}", newSnapshotId);
            for (ManifestEntry entry : deltaFiles) {
                LOG.debug("  * {}", entry);
            }
            LOG.debug("Ready to commit changelog to snapshot {}", newSnapshotId);
            for (ManifestEntry entry : changelogFiles) {
                LOG.debug("  * {}", entry);
            }
        }

        List<SimpleFileEntry> baseDataFiles = new ArrayList<>();
        boolean discardDuplicate = discardDuplicateFiles && commitKind == CommitKind.APPEND;
        if (latestSnapshot != null
                && (discardDuplicate || conflictCheck.shouldCheck(latestSnapshot.id()))) {
            // latestSnapshotId is different from the snapshot id we've checked for conflicts,
            // so we have to check again
            List<BinaryRow> changedPartitions =
                    changedPartitions(deltaFiles, Collections.emptyList(), indexFiles);
            if (retryResult != null && retryResult.latestSnapshot != null) {
                baseDataFiles = new ArrayList<>(retryResult.baseDataFiles);
                List<SimpleFileEntry> incremental =
                        scanner.readIncrementalChanges(
                                retryResult.latestSnapshot, latestSnapshot, changedPartitions);
                if (!incremental.isEmpty()) {
                    baseDataFiles.addAll(incremental);
                    baseDataFiles = new ArrayList<>(FileEntry.mergeEntries(baseDataFiles));
                }
            } else {
                baseDataFiles =
                        scanner.readAllEntriesFromChangedPartitions(
                                latestSnapshot, changedPartitions);
            }
            if (discardDuplicate) {
                Set<FileEntry.Identifier> baseIdentifiers =
                        baseDataFiles.stream()
                                .map(FileEntry::identifier)
                                .collect(Collectors.toSet());
                deltaFiles =
                        deltaFiles.stream()
                                .filter(entry -> !baseIdentifiers.contains(entry.identifier()))
                                .collect(Collectors.toList());
            }
            conflictDetection.checkNoConflictsOrFail(
                    latestSnapshot,
                    baseDataFiles,
                    SimpleFileEntry.from(deltaFiles),
                    indexFiles,
                    commitKind);
        }

        Snapshot newSnapshot;
        Pair<String, Long> baseManifestList = null;
        Pair<String, Long> deltaManifestList = null;
        List<PartitionEntry> deltaStatistics;
        Pair<String, Long> changelogManifestList = null;
        String oldIndexManifest = null;
        String indexManifest = null;
        List<ManifestFileMeta> mergeBeforeManifests = new ArrayList<>();
        List<ManifestFileMeta> mergeAfterManifests = new ArrayList<>();
        long nextRowIdStart = firstRowIdStart;
        try {
            long previousTotalRecordCount = 0L;
            Long currentWatermark = watermark;
            if (latestSnapshot != null) {
                previousTotalRecordCount = latestSnapshot.totalRecordCount();
                // read all previous manifest files
                mergeBeforeManifests = manifestList.readDataManifests(latestSnapshot);
                Long latestWatermark = latestSnapshot.watermark();
                if (latestWatermark != null) {
                    currentWatermark =
                            currentWatermark == null
                                    ? latestWatermark
                                    : Math.max(currentWatermark, latestWatermark);
                }
                oldIndexManifest = latestSnapshot.indexManifest();
            }

            // try to merge old manifest files to create base manifest list
            mergeAfterManifests =
                    ManifestFileMerger.merge(
                            mergeBeforeManifests,
                            manifestFile,
                            manifestTargetSize.getBytes(),
                            manifestMergeMinCount,
                            manifestFullCompactionSize.getBytes(),
                            partitionType,
                            manifestReadParallelism);
            baseManifestList = manifestList.write(mergeAfterManifests);

            if (rowTrackingEnabled) {
                RowTrackingAssigned assigned =
                        assignRowTracking(newSnapshotId, firstRowIdStart, deltaFiles);
                nextRowIdStart = assigned.nextRowIdStart;
                deltaFiles = assigned.assignedEntries;
            }

            // the added records subtract the deleted records from
            long deltaRecordCount = recordCountAdd(deltaFiles) - recordCountDelete(deltaFiles);
            long totalRecordCount = previousTotalRecordCount + deltaRecordCount;

            // write new delta files into manifest files
            deltaStatistics = new ArrayList<>(PartitionEntry.merge(deltaFiles));
            deltaManifestList = manifestList.write(manifestFile.write(deltaFiles));

            // write changelog into manifest files
            if (!changelogFiles.isEmpty()) {
                changelogManifestList = manifestList.write(manifestFile.write(changelogFiles));
            }

            indexManifest =
                    indexManifestFile.writeIndexFiles(oldIndexManifest, indexFiles, bucketMode);

            long latestSchemaId =
                    schemaManager
                            .latestOrThrow("Cannot get latest schema for table " + tableName)
                            .id();

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
                            baseManifestList.getLeft(),
                            baseManifestList.getRight(),
                            deltaManifestList.getKey(),
                            deltaManifestList.getRight(),
                            changelogManifestList == null ? null : changelogManifestList.getKey(),
                            changelogManifestList == null ? null : changelogManifestList.getRight(),
                            indexManifest,
                            commitUser,
                            identifier,
                            commitKind,
                            System.currentTimeMillis(),
                            totalRecordCount,
                            deltaRecordCount,
                            nullableRecordCount(changelogFiles),
                            currentWatermark,
                            statsFileName,
                            // if empty properties, just set to null
                            properties.isEmpty() ? null : properties,
                            nextRowIdStart);
        } catch (Throwable e) {
            // fails when preparing for commit, we should clean up
            commitCleaner.cleanUpReuseTmpManifests(
                    deltaManifestList, changelogManifestList, oldIndexManifest, indexManifest);
            commitCleaner.cleanUpNoReuseTmpManifests(
                    baseManifestList, mergeBeforeManifests, mergeAfterManifests);
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when preparing snapshot #%d by user %s "
                                    + "with hash %s and kind %s. Clean up.",
                            newSnapshotId, commitUser, identifier, commitKind.name()),
                    e);
        }

        boolean success;
        try {
            success = commitSnapshotImpl(newSnapshot, deltaStatistics);
        } catch (Exception e) {
            // commit exception, not sure about the situation and should not clean up the files
            LOG.warn("Retry commit for exception.", e);
            return new RetryCommitResult(latestSnapshot, baseDataFiles, e);
        }

        if (!success) {
            // commit fails, should clean up the files
            long commitTime = (System.currentTimeMillis() - startMillis) / 1000;
            LOG.warn(
                    "Atomic commit failed for snapshot #{} by user {} "
                            + "with identifier {} and kind {} after {} seconds. "
                            + "Clean up and try again.",
                    newSnapshotId,
                    commitUser,
                    identifier,
                    commitKind.name(),
                    commitTime);
            commitCleaner.cleanUpNoReuseTmpManifests(
                    baseManifestList, mergeBeforeManifests, mergeAfterManifests);
            return new RetryCommitResult(latestSnapshot, baseDataFiles, null);
        }

        LOG.info(
                "Successfully commit snapshot {} to table {} by user {} "
                        + "with identifier {} and kind {}.",
                newSnapshotId,
                tableName,
                commitUser,
                identifier,
                commitKind.name());
        if (strictModeLastSafeSnapshot != null) {
            strictModeLastSafeSnapshot = newSnapshot.id();
        }
        final List<SimpleFileEntry> finalBaseFiles = baseDataFiles;
        final List<ManifestEntry> finalDeltaFiles = deltaFiles;
        commitCallbacks.forEach(
                callback ->
                        callback.call(finalBaseFiles, finalDeltaFiles, indexFiles, newSnapshot));
        return new SuccessCommitResult();
    }

    public boolean replaceManifestList(
            Snapshot latest,
            long totalRecordCount,
            Pair<String, Long> baseManifestList,
            Pair<String, Long> deltaManifestList) {
        Snapshot newSnapshot =
                new Snapshot(
                        latest.id() + 1,
                        latest.schemaId(),
                        baseManifestList.getLeft(),
                        baseManifestList.getRight(),
                        deltaManifestList.getKey(),
                        deltaManifestList.getRight(),
                        null,
                        null,
                        latest.indexManifest(),
                        commitUser,
                        Long.MAX_VALUE,
                        CommitKind.OVERWRITE,
                        System.currentTimeMillis(),
                        totalRecordCount,
                        0L,
                        null,
                        latest.watermark(),
                        latest.statistics(),
                        // if empty properties, just set to null
                        latest.properties(),
                        latest.nextRowId());

        return commitSnapshotImpl(newSnapshot, emptyList());
    }

    public void compactManifest() {
        int retryCount = 0;
        long startMillis = System.currentTimeMillis();
        while (true) {
            boolean success = compactManifestOnce();
            if (success) {
                break;
            }

            if (System.currentTimeMillis() - startMillis > commitTimeout
                    || retryCount >= commitMaxRetries) {
                throw new RuntimeException(
                        String.format(
                                "Commit failed after %s millis with %s retries, there maybe exist commit conflicts between multiple jobs.",
                                commitTimeout, retryCount));
            }

            commitRetryWait(retryCount);
            retryCount++;
        }
    }

    private boolean compactManifestOnce() {
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();

        if (latestSnapshot == null) {
            return true;
        }

        List<ManifestFileMeta> mergeBeforeManifests =
                manifestList.readDataManifests(latestSnapshot);
        List<ManifestFileMeta> mergeAfterManifests;

        // the fist trial
        mergeAfterManifests =
                ManifestFileMerger.merge(
                        mergeBeforeManifests,
                        manifestFile,
                        manifestTargetSize.getBytes(),
                        1,
                        1,
                        partitionType,
                        manifestReadParallelism);

        if (new HashSet<>(mergeBeforeManifests).equals(new HashSet<>(mergeAfterManifests))) {
            // no need to commit this snapshot, because no compact were happened
            return true;
        }

        Pair<String, Long> baseManifestList = manifestList.write(mergeAfterManifests);
        Pair<String, Long> deltaManifestList = manifestList.write(emptyList());

        // prepare snapshot file
        Snapshot newSnapshot =
                new Snapshot(
                        latestSnapshot.id() + 1,
                        latestSnapshot.schemaId(),
                        baseManifestList.getLeft(),
                        baseManifestList.getRight(),
                        deltaManifestList.getLeft(),
                        deltaManifestList.getRight(),
                        null,
                        null,
                        latestSnapshot.indexManifest(),
                        commitUser,
                        Long.MAX_VALUE,
                        CommitKind.COMPACT,
                        System.currentTimeMillis(),
                        latestSnapshot.totalRecordCount(),
                        0L,
                        null,
                        latestSnapshot.watermark(),
                        latestSnapshot.statistics(),
                        latestSnapshot.properties(),
                        latestSnapshot.nextRowId());

        return commitSnapshotImpl(newSnapshot, emptyList());
    }

    private boolean commitSnapshotImpl(Snapshot newSnapshot, List<PartitionEntry> deltaStatistics) {
        try {
            List<PartitionStatistics> statistics = new ArrayList<>(deltaStatistics.size());
            for (PartitionEntry entry : deltaStatistics) {
                statistics.add(entry.toPartitionStatistics(partitionComputer));
            }
            return snapshotCommit.commit(newSnapshot, branchName, statistics);
        } catch (Throwable e) {
            // exception when performing the atomic rename,
            // we cannot clean up because we can't determine the success
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when committing snapshot #%d by user %s "
                                    + "with identifier %s and kind %s. "
                                    + "Cannot clean up because we can't determine the success.",
                            newSnapshot.id(),
                            newSnapshot.commitUser(),
                            newSnapshot.commitIdentifier(),
                            newSnapshot.commitKind().name()),
                    e);
        }
    }

    private void commitRetryWait(int retryCount) {
        int retryWait =
                (int) Math.min(commitMinRetryWait * Math.pow(2, retryCount), commitMaxRetryWait);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        retryWait += random.nextInt(Math.max(1, (int) (retryWait * 0.2)));
        try {
            TimeUnit.MILLISECONDS.sleep(retryWait);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
    }

    @Override
    public void close() {
        IOUtils.closeAllQuietly(commitCallbacks);
        IOUtils.closeQuietly(snapshotCommit);
    }
}
