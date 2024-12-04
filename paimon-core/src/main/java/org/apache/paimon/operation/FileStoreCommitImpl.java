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
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;
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
import static org.apache.paimon.manifest.ManifestEntry.recordCount;
import static org.apache.paimon.manifest.ManifestEntry.recordCountAdd;
import static org.apache.paimon.manifest.ManifestEntry.recordCountDelete;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.InternalRowPartitionComputer.partToSimpleString;

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
    private final String tableName;
    private final String commitUser;
    private final RowType partitionType;
    private final String partitionDefaultName;
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
    @Nullable private final Integer manifestReadParallelism;
    private final List<CommitCallback> commitCallbacks;
    private final StatsFileHandler statsFileHandler;
    private final BucketMode bucketMode;
    private final int commitMaxRetries;

    @Nullable private Lock lock;
    private boolean ignoreEmptyCommit;
    private CommitMetrics commitMetrics;
    @Nullable private PartitionExpire partitionExpire;

    public FileStoreCommitImpl(
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
            @Nullable Comparator<InternalRow> keyComparator,
            String branchName,
            StatsFileHandler statsFileHandler,
            BucketMode bucketMode,
            @Nullable Integer manifestReadParallelism,
            List<CommitCallback> commitCallbacks,
            int commitMaxRetries) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.tableName = tableName;
        this.commitUser = commitUser;
        this.partitionType = partitionType;
        this.partitionDefaultName = partitionDefaultName;
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.manifestFile = manifestFileFactory.create();
        this.manifestList = manifestListFactory.create();
        this.indexManifestFile = indexManifestFileFactory.create();
        this.scan = scan;
        // Stats in DELETE Manifest Entries is useless
        if (options.manifestDeleteFileDropStats()) {
            this.scan.dropStats();
        }
        this.numBucket = numBucket;
        this.manifestTargetSize = manifestTargetSize;
        this.manifestFullCompactionSize = manifestFullCompactionSize;
        this.manifestMergeMinCount = manifestMergeMinCount;
        this.dynamicPartitionOverwrite = dynamicPartitionOverwrite;
        this.keyComparator = keyComparator;
        this.branchName = branchName;
        this.manifestReadParallelism = manifestReadParallelism;
        this.commitCallbacks = commitCallbacks;
        this.commitMaxRetries = commitMaxRetries;

        this.lock = null;
        this.ignoreEmptyCommit = true;
        this.commitMetrics = null;
        this.statsFileHandler = statsFileHandler;
        this.bucketMode = bucketMode;
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
    public FileStoreCommit withPartitionExpire(PartitionExpire partitionExpire) {
        this.partitionExpire = partitionExpire;
        return this;
    }

    @Override
    public List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committables) {
        // nothing to filter, fast exit
        if (committables.isEmpty()) {
            return committables;
        }

        for (int i = 1; i < committables.size(); i++) {
            Preconditions.checkArgument(
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
    public void commit(ManifestCommittable committable, Map<String, String> properties) {
        commit(committable, properties, false);
    }

    @Override
    public void commit(
            ManifestCommittable committable,
            Map<String, String> properties,
            boolean checkAppendFiles) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit\n{}", committable.toString());
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
                latestSnapshot = snapshotManager.latestSnapshot();
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
            PartitionPredicate partitionFilter = null;
            if (dynamicPartitionOverwrite) {
                if (appendTableFiles.isEmpty()) {
                    // in dynamic mode, if there is no changes to commit, no data will be deleted
                    skipOverwrite = true;
                } else {
                    Set<BinaryRow> partitions =
                            appendTableFiles.stream()
                                    .map(ManifestEntry::partition)
                                    .collect(Collectors.toSet());
                    partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
                }
            } else {
                // partition may be partial partition fields, so here must to use predicate way.
                Predicate partitionPredicate =
                        createPartitionPredicate(partition, partitionType, partitionDefaultName);
                partitionFilter =
                        PartitionPredicate.fromPredicate(partitionType, partitionPredicate);
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

        // partitions may be partial partition fields, so here must to use predicate way.
        Predicate predicate =
                partitions.stream()
                        .map(
                                partition ->
                                        createPartitionPredicate(
                                                partition, partitionType, partitionDefaultName))
                        .reduce(PredicateBuilder::or)
                        .orElseThrow(() -> new RuntimeException("Failed to get partition filter."));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, predicate);

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
            commitMessage
                    .indexIncrement()
                    .deletedIndexFiles()
                    .forEach(
                            f -> {
                                if (f.indexType().equals(DELETION_VECTORS_INDEX)) {
                                    compactDvIndexFiles.add(
                                            new IndexManifestEntry(
                                                    FileKind.DELETE,
                                                    commitMessage.partition(),
                                                    commitMessage.bucket(),
                                                    f));
                                } else {
                                    throw new RuntimeException(
                                            "This index type is not supported to delete: "
                                                    + f.indexType());
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
        RetryResult retryResult = null;
        while (true) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();
            cnt++;
            if (cnt >= commitMaxRetries) {
                if (retryResult != null) {
                    retryResult.cleanAll();
                }
                throw new RuntimeException(
                        String.format(
                                "Commit failed after %s attempts, there maybe exist commit conflicts between multiple jobs.",
                                commitMaxRetries));
            }

            CommitResult result =
                    tryCommitOnce(
                            retryResult,
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
                            statsFileName);

            if (result.isSuccess()) {
                break;
            }

            retryResult = (RetryResult) result;
        }
        return cnt;
    }

    private int tryOverwrite(
            @Nullable PartitionPredicate partitionFilter,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets) {
        int cnt = 0;
        while (true) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();

            cnt++;
            if (cnt >= commitMaxRetries) {
                throw new RuntimeException(
                        String.format(
                                "Commit failed after %s attempts, there maybe exist commit conflicts between multiple jobs.",
                                commitMaxRetries));
            }
            List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
            List<IndexManifestEntry> indexChangesWithOverwrite = new ArrayList<>();
            if (latestSnapshot != null) {
                List<ManifestEntry> currentEntries =
                        scan.withSnapshot(latestSnapshot)
                                .withPartitionFilter(partitionFilter)
                                .withKind(ScanMode.ALL)
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

            CommitResult result =
                    tryCommitOnce(
                            null,
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
                            null);

            if (result.isSuccess()) {
                break;
            }

            // TODO optimize OVERWRITE too
            RetryResult retryResult = (RetryResult) result;
            retryResult.cleanAll();
        }
        return cnt;
    }

    @VisibleForTesting
    CommitResult tryCommitOnce(
            @Nullable RetryResult retryResult,
            List<ManifestEntry> deltaFiles,
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
        long startMillis = System.currentTimeMillis();
        long newSnapshotId =
                latestSnapshot == null ? Snapshot.FIRST_SNAPSHOT_ID : latestSnapshot.id() + 1;
        Path newSnapshotPath =
                branchName.equals(DEFAULT_MAIN_BRANCH)
                        ? snapshotManager.snapshotPath(newSnapshotId)
                        : snapshotManager.copyWithBranch(branchName).snapshotPath(newSnapshotId);

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
        if (latestSnapshot != null && conflictCheck.shouldCheck(latestSnapshot.id())) {
            // latestSnapshotId is different from the snapshot id we've checked for conflicts,
            // so we have to check again
            try {
                List<BinaryRow> changedPartitions =
                        deltaFiles.stream()
                                .map(ManifestEntry::partition)
                                .distinct()
                                .collect(Collectors.toList());
                if (retryResult != null && retryResult.latestSnapshot != null) {
                    baseDataFiles = new ArrayList<>(retryResult.baseDataFiles);
                    List<SimpleFileEntry> incremental =
                            readIncrementalChanges(
                                    retryResult.latestSnapshot, latestSnapshot, changedPartitions);
                    if (!incremental.isEmpty()) {
                        baseDataFiles.addAll(incremental);
                        baseDataFiles = new ArrayList<>(FileEntry.mergeEntries(baseDataFiles));
                    }
                } else {
                    baseDataFiles =
                            readAllEntriesFromChangedPartitions(latestSnapshot, changedPartitions);
                }
                noConflictsOrFail(
                        latestSnapshot.commitUser(),
                        baseDataFiles,
                        SimpleFileEntry.from(deltaFiles));
            } catch (Exception e) {
                if (retryResult != null) {
                    retryResult.cleanAll();
                }
                throw e;
            }
        }

        Snapshot newSnapshot;
        String baseManifestList = null;
        String deltaManifestList = null;
        String changelogManifestList = null;
        String oldIndexManifest = null;
        String indexManifest = null;
        List<ManifestFileMeta> mergeBeforeManifests = new ArrayList<>();
        List<ManifestFileMeta> mergeAfterManifests = new ArrayList<>();
        try {
            long previousTotalRecordCount = 0L;
            Long currentWatermark = watermark;
            if (latestSnapshot != null) {
                previousTotalRecordCount = scan.totalRecordCount(latestSnapshot);
                // read all previous manifest files
                mergeBeforeManifests = manifestList.readDataManifests(latestSnapshot);
                // read the last snapshot to complete the bucket's offsets when logOffsets does not
                // contain all buckets
                Map<Integer, Long> latestLogOffsets = latestSnapshot.logOffsets();
                if (latestLogOffsets != null) {
                    latestLogOffsets.forEach(logOffsets::putIfAbsent);
                }
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

            // the added records subtract the deleted records from
            long deltaRecordCount = recordCountAdd(deltaFiles) - recordCountDelete(deltaFiles);
            long totalRecordCount = previousTotalRecordCount + deltaRecordCount;

            boolean rewriteIndexManifest = true;
            if (retryResult != null) {
                deltaManifestList = retryResult.deltaManifestList;
                changelogManifestList = retryResult.changelogManifestList;
                if (Objects.equals(oldIndexManifest, retryResult.oldIndexManifest)) {
                    rewriteIndexManifest = false;
                    indexManifest = retryResult.newIndexManifest;
                    LOG.info("Reusing index manifest {} for retry.", indexManifest);
                } else {
                    cleanIndexManifest(retryResult.oldIndexManifest, retryResult.newIndexManifest);
                }
            } else {
                // write new delta files into manifest files
                deltaManifestList = manifestList.write(manifestFile.write(deltaFiles));

                // write changelog into manifest files
                if (!changelogFiles.isEmpty()) {
                    changelogManifestList = manifestList.write(manifestFile.write(changelogFiles));
                }
            }

            if (rewriteIndexManifest) {
                indexManifest =
                        indexManifestFile.writeIndexFiles(oldIndexManifest, indexFiles, bucketMode);
            }

            long latestSchemaId = schemaManager.latest().get().id();

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
                            baseManifestList,
                            deltaManifestList,
                            changelogManifestList,
                            indexManifest,
                            commitUser,
                            identifier,
                            commitKind,
                            System.currentTimeMillis(),
                            logOffsets,
                            totalRecordCount,
                            deltaRecordCount,
                            recordCount(changelogFiles),
                            currentWatermark,
                            statsFileName);
        } catch (Throwable e) {
            // fails when preparing for commit, we should clean up
            if (retryResult != null) {
                retryResult.cleanAll();
            }
            cleanUpReuseTmpManifests(
                    deltaManifestList, changelogManifestList, oldIndexManifest, indexManifest);
            cleanUpNoReuseTmpManifests(baseManifestList, mergeBeforeManifests, mergeAfterManifests);
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

        if (commitSnapshotImpl(newSnapshot, newSnapshotPath)) {
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
            commitCallbacks.forEach(callback -> callback.call(deltaFiles, newSnapshot));
            return new SuccessResult();
        }

        // atomic rename fails, clean up and try again
        long commitTime = (System.currentTimeMillis() - startMillis) / 1000;
        LOG.warn(
                String.format(
                        "Atomic commit failed for snapshot #%d (path %s) by user %s "
                                + "with identifier %s and kind %s after %s seconds. "
                                + "Clean up and try again.",
                        newSnapshotId,
                        newSnapshotPath,
                        commitUser,
                        identifier,
                        commitKind.name(),
                        commitTime));
        cleanUpNoReuseTmpManifests(baseManifestList, mergeBeforeManifests, mergeAfterManifests);
        return new RetryResult(
                deltaManifestList,
                changelogManifestList,
                oldIndexManifest,
                indexManifest,
                latestSnapshot,
                baseDataFiles);
    }

    public void compactManifest() {
        int cnt = 0;
        ManifestCompactResult retryResult = null;
        while (true) {
            cnt++;
            retryResult = compactManifest(retryResult);
            if (retryResult.isSuccess()) {
                break;
            }

            if (cnt >= commitMaxRetries) {
                retryResult.cleanAll();
                throw new RuntimeException(
                        String.format(
                                "Commit compact manifest failed after %s attempts, there maybe exist commit conflicts between multiple jobs.",
                                commitMaxRetries));
            }
        }
    }

    private ManifestCompactResult compactManifest(@Nullable ManifestCompactResult lastResult) {
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();

        if (latestSnapshot == null) {
            return new SuccessManifestCompactResult();
        }

        List<ManifestFileMeta> mergeBeforeManifests =
                manifestList.readDataManifests(latestSnapshot);
        List<ManifestFileMeta> mergeAfterManifests;

        if (lastResult != null) {
            List<ManifestFileMeta> oldMergeBeforeManifests = lastResult.mergeBeforeManifests;
            List<ManifestFileMeta> oldMergeAfterManifests = lastResult.mergeAfterManifests;

            Set<String> retryMergeBefore =
                    oldMergeBeforeManifests.stream()
                            .map(ManifestFileMeta::fileName)
                            .collect(Collectors.toSet());

            List<ManifestFileMeta> manifestsFromOther =
                    mergeBeforeManifests.stream()
                            .filter(m -> !retryMergeBefore.remove(m.fileName()))
                            .collect(Collectors.toList());

            if (retryMergeBefore.isEmpty()) {
                // no manifest compact from latest failed commit to latest commit
                mergeAfterManifests = new ArrayList<>(oldMergeAfterManifests);
                mergeAfterManifests.addAll(manifestsFromOther);
            } else {
                // manifest compact happens, quit
                lastResult.cleanAll();
                return new SuccessManifestCompactResult();
            }
        } else {
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
                return new SuccessManifestCompactResult();
            }
        }

        String baseManifestList = manifestList.write(mergeAfterManifests);
        String deltaManifestList = manifestList.write(Collections.emptyList());

        // prepare snapshot file
        Snapshot newSnapshot =
                new Snapshot(
                        latestSnapshot.id() + 1,
                        latestSnapshot.schemaId(),
                        baseManifestList,
                        deltaManifestList,
                        null,
                        latestSnapshot.indexManifest(),
                        commitUser,
                        Long.MAX_VALUE,
                        Snapshot.CommitKind.COMPACT,
                        System.currentTimeMillis(),
                        latestSnapshot.logOffsets(),
                        latestSnapshot.totalRecordCount(),
                        0L,
                        0L,
                        latestSnapshot.watermark(),
                        latestSnapshot.statistics());

        Path newSnapshotPath =
                branchName.equals(DEFAULT_MAIN_BRANCH)
                        ? snapshotManager.snapshotPath(newSnapshot.id())
                        : snapshotManager.copyWithBranch(branchName).snapshotPath(newSnapshot.id());

        if (!commitSnapshotImpl(newSnapshot, newSnapshotPath)) {
            return new ManifestCompactResult(
                    baseManifestList, deltaManifestList, mergeBeforeManifests, mergeAfterManifests);
        } else {
            return new SuccessManifestCompactResult();
        }
    }

    private boolean commitSnapshotImpl(Snapshot newSnapshot, Path newSnapshotPath) {
        try {
            Callable<Boolean> callable =
                    () -> {
                        boolean committed =
                                fileIO.tryToWriteAtomic(newSnapshotPath, newSnapshot.toJson());
                        if (committed) {
                            snapshotManager.commitLatestHint(newSnapshot.id());
                        }
                        return committed;
                    };
            if (lock != null) {
                return lock.runWithLock(
                        () ->
                                // fs.rename may not returns false if target file
                                // already exists, or even not atomic
                                // as we're relying on external locking, we can first
                                // check if file exist then rename to work around this
                                // case
                                !fileIO.exists(newSnapshotPath) && callable.call());
            } else {
                return callable.call();
            }
        } catch (Throwable e) {
            // exception when performing the atomic rename,
            // we cannot clean up because we can't determine the success
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when committing snapshot #%d (path %s) by user %s "
                                    + "with identifier %s and kind %s. "
                                    + "Cannot clean up because we can't determine the success.",
                            newSnapshot.id(),
                            newSnapshotPath,
                            commitUser,
                            newSnapshot.commitIdentifier(),
                            newSnapshot.commitKind().name()),
                    e);
        }
    }

    private List<SimpleFileEntry> readIncrementalChanges(
            Snapshot from, Snapshot to, List<BinaryRow> changedPartitions) {
        List<SimpleFileEntry> entries = new ArrayList<>();
        for (long i = from.id() + 1; i <= to.id(); i++) {
            List<SimpleFileEntry> delta =
                    scan.withSnapshot(i)
                            .withKind(ScanMode.DELTA)
                            .withPartitionFilter(changedPartitions)
                            .readSimpleEntries();
            entries.addAll(delta);
        }
        return entries;
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
        return readAllEntriesFromChangedPartitions(snapshot, changedPartitions);
    }

    private List<SimpleFileEntry> readAllEntriesFromChangedPartitions(
            Snapshot snapshot, List<BinaryRow> changedPartitions) {
        try {
            return scan.withSnapshot(snapshot)
                    .withKind(ScanMode.ALL)
                    .withPartitionFilter(changedPartitions)
                    .readSimpleEntries();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read manifest entries from changed partitions.", e);
        }
    }

    private void noConflictsOrFail(
            String baseCommitUser,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> changes) {
        List<SimpleFileEntry> allEntries = new ArrayList<>(baseEntries);
        allEntries.addAll(changes);

        java.util.function.Consumer<Throwable> conflictHandler =
                e -> {
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
                };

        Collection<SimpleFileEntry> mergedEntries = null;
        try {
            // merge manifest entries and also check if the files we want to delete are still there
            mergedEntries = FileEntry.mergeEntries(allEntries);
        } catch (Throwable e) {
            conflictHandler.accept(e);
        }

        assertNoDelete(mergedEntries, conflictHandler);

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

    private void assertNoDelete(
            Collection<SimpleFileEntry> mergedEntries,
            java.util.function.Consumer<Throwable> conflictHandler) {
        try {
            for (SimpleFileEntry entry : mergedEntries) {
                Preconditions.checkState(
                        entry.kind() != FileKind.DELETE,
                        "Trying to delete file %s for table %s which is not previously added.",
                        entry.fileName(),
                        tableName);
            }
        } catch (Throwable e) {
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
                    throw new RuntimeException(
                            "You are writing data to expired partitions, and you can filter this data to avoid job failover."
                                    + " Otherwise, continuous expired records will cause the job to failover restart continuously."
                                    + " Expired partitions are: "
                                    + expiredPartitions);
                }
            }
            conflictHandler.accept(e);
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

    private void cleanUpNoReuseTmpManifests(
            String baseManifestList,
            List<ManifestFileMeta> mergeBeforeManifests,
            List<ManifestFileMeta> mergeAfterManifests) {
        if (baseManifestList != null) {
            manifestList.delete(baseManifestList);
        }
        Set<String> oldMetaSet =
                mergeBeforeManifests.stream()
                        .map(ManifestFileMeta::fileName)
                        .collect(Collectors.toSet());
        for (ManifestFileMeta suspect : mergeAfterManifests) {
            if (!oldMetaSet.contains(suspect.fileName())) {
                manifestFile.delete(suspect.fileName());
            }
        }
    }

    private void cleanUpReuseTmpManifests(
            String deltaManifestList,
            String changelogManifestList,
            String oldIndexManifest,
            String newIndexManifest) {
        if (deltaManifestList != null) {
            for (ManifestFileMeta manifest : manifestList.read(deltaManifestList)) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(deltaManifestList);
        }

        if (changelogManifestList != null) {
            for (ManifestFileMeta manifest : manifestList.read(changelogManifestList)) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(changelogManifestList);
        }

        cleanIndexManifest(oldIndexManifest, newIndexManifest);
    }

    private void cleanIndexManifest(String oldIndexManifest, String newIndexManifest) {
        if (newIndexManifest != null && !Objects.equals(oldIndexManifest, newIndexManifest)) {
            indexManifestFile.delete(newIndexManifest);
        }
    }

    @Override
    public void close() {
        for (CommitCallback callback : commitCallbacks) {
            IOUtils.closeQuietly(callback);
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

    @VisibleForTesting
    static ConflictCheck mustConflictCheck() {
        return latestSnapshot -> true;
    }

    private interface CommitResult {
        boolean isSuccess();
    }

    private static class SuccessResult implements CommitResult {

        @Override
        public boolean isSuccess() {
            return true;
        }
    }

    private class RetryResult implements CommitResult {

        private final String deltaManifestList;
        private final String changelogManifestList;

        private final String oldIndexManifest;
        private final String newIndexManifest;

        private final Snapshot latestSnapshot;
        private final List<SimpleFileEntry> baseDataFiles;

        private RetryResult(
                String deltaManifestList,
                String changelogManifestList,
                String oldIndexManifest,
                String newIndexManifest,
                Snapshot latestSnapshot,
                List<SimpleFileEntry> baseDataFiles) {
            this.deltaManifestList = deltaManifestList;
            this.changelogManifestList = changelogManifestList;
            this.oldIndexManifest = oldIndexManifest;
            this.newIndexManifest = newIndexManifest;
            this.latestSnapshot = latestSnapshot;
            this.baseDataFiles = baseDataFiles;
        }

        private void cleanAll() {
            cleanUpReuseTmpManifests(
                    deltaManifestList, changelogManifestList, oldIndexManifest, newIndexManifest);
        }

        @Override
        public boolean isSuccess() {
            return false;
        }
    }

    private class ManifestCompactResult implements CommitResult {

        private final String baseManifestList;
        private final String deltaManifestList;
        private final List<ManifestFileMeta> mergeBeforeManifests;
        private final List<ManifestFileMeta> mergeAfterManifests;

        public ManifestCompactResult(
                String baseManifestList,
                String deltaManifestList,
                List<ManifestFileMeta> mergeBeforeManifests,
                List<ManifestFileMeta> mergeAfterManifests) {
            this.baseManifestList = baseManifestList;
            this.deltaManifestList = deltaManifestList;
            this.mergeBeforeManifests = mergeBeforeManifests;
            this.mergeAfterManifests = mergeAfterManifests;
        }

        public void cleanAll() {
            manifestList.delete(deltaManifestList);
            cleanUpNoReuseTmpManifests(baseManifestList, mergeBeforeManifests, mergeAfterManifests);
        }

        @Override
        public boolean isSuccess() {
            return false;
        }
    }

    private class SuccessManifestCompactResult extends ManifestCompactResult {

        public SuccessManifestCompactResult() {
            super(null, null, null, null);
        }

        @Override
        public void cleanAll() {}

        @Override
        public boolean isSuccess() {
            return true;
        }
    }
}
