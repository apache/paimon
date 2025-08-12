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
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
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
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.manifest.ManifestEntry.recordCount;
import static org.apache.paimon.manifest.ManifestEntry.recordCountAdd;
import static org.apache.paimon.manifest.ManifestEntry.recordCountDelete;
import static org.apache.paimon.partition.PartitionPredicate.createBinaryPartitions;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.InternalRowPartitionComputer.partToSimpleString;
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
    private final long commitTimeout;
    private final long commitMinRetryWait;
    private final long commitMaxRetryWait;
    private final int commitMaxRetries;
    @Nullable private Long strictModeLastSafeSnapshot;
    private final InternalRowPartitionComputer partitionComputer;
    private final boolean rowTrackingEnabled;

    private boolean ignoreEmptyCommit;
    private CommitMetrics commitMetrics;
    @Nullable private PartitionExpire partitionExpire;

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
            @Nullable Comparator<InternalRow> keyComparator,
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
            boolean rowTrackingEnabled) {
        this.snapshotCommit = snapshotCommit;
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
                            latestSnapshot.commitUser(),
                            baseEntries,
                            appendSimpleEntries,
                            Snapshot.CommitKind.APPEND);
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
                                committable.properties(),
                                Snapshot.CommitKind.APPEND,
                                noConflictCheck(),
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
                            SimpleFileEntry.from(compactTableFiles),
                            Snapshot.CommitKind.COMPACT);
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
                                committable.properties(),
                                Snapshot.CommitKind.COMPACT,
                                hasConflictChecked(safeLatestSnapshotId),
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            LOG.info("Finished commit to table {}, duration {} ms", tableName, commitDuration);
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

    @Override
    public int overwrite(
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
                                committable.logOffsets(),
                                committable.properties());
                generatedSnapshot += 1;
            }

            if (!compactTableFiles.isEmpty() || !compactDvIndexFiles.isEmpty()) {
                attempts +=
                        tryCommit(
                                compactTableFiles,
                                emptyList(),
                                compactDvIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                committable.properties(),
                                Snapshot.CommitKind.COMPACT,
                                mustConflictCheck(),
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            LOG.info("Finished overwrite to table {}, duration {} ms", tableName, commitDuration);
            if (this.commitMetrics != null) {
                reportCommit(
                        appendTableFiles,
                        emptyList(),
                        compactTableFiles,
                        emptyList(),
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
        return generatedSnapshot;
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

        tryOverwrite(
                partitionFilter,
                emptyList(),
                emptyList(),
                commitIdentifier,
                null,
                new HashMap<>(),
                new HashMap<>());
    }

    @Override
    public void truncateTable(long commitIdentifier) {
        tryOverwrite(
                null,
                emptyList(),
                emptyList(),
                commitIdentifier,
                null,
                new HashMap<>(),
                new HashMap<>());
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
                emptyList(),
                emptyList(),
                emptyList(),
                commitIdentifier,
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Snapshot.CommitKind.ANALYZE,
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
        if (!commitMessages.isEmpty()) {
            List<String> msg = new ArrayList<>();
            if (!appendTableFiles.isEmpty()) {
                msg.add(appendTableFiles.size() + " append table files");
            }
            if (!appendChangelog.isEmpty()) {
                msg.add(appendChangelog.size() + " append Changelogs");
            }
            if (!compactTableFiles.isEmpty()) {
                msg.add(compactTableFiles.size() + " compact table files");
            }
            if (!compactChangelog.isEmpty()) {
                msg.add(compactChangelog.size() + " compact Changelogs");
            }
            if (!appendHashIndexFiles.isEmpty()) {
                msg.add(appendHashIndexFiles.size() + " append hash index files");
            }
            if (!compactDvIndexFiles.isEmpty()) {
                msg.add(compactDvIndexFiles.size() + " compact dv index files");
            }
            LOG.info("Finished collecting changes, including: {}", String.join(", ", msg));
        }
    }

    private ManifestEntry makeEntry(FileKind kind, CommitMessage commitMessage, DataFileMeta file) {
        Integer totalBuckets = commitMessage.totalBuckets();
        if (totalBuckets == null) {
            totalBuckets = numBucket;
        }

        return new ManifestEntry(
                kind, commitMessage.partition(), commitMessage.bucket(), totalBuckets, file);
    }

    private int tryCommit(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Map<String, String> properties,
            Snapshot.CommitKind commitKind,
            ConflictCheck conflictCheck,
            @Nullable String statsFileName) {
        int retryCount = 0;
        RetryResult retryResult = null;
        long startMillis = System.currentTimeMillis();
        while (true) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();
            CommitResult result =
                    tryCommitOnce(
                            retryResult,
                            tableFiles,
                            changelogFiles,
                            indexFiles,
                            identifier,
                            watermark,
                            logOffsets,
                            properties,
                            commitKind,
                            latestSnapshot,
                            conflictCheck,
                            statsFileName);

            if (result.isSuccess()) {
                break;
            }

            retryResult = (RetryResult) result;

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

    private int tryOverwrite(
            @Nullable PartitionPredicate partitionFilter,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Map<String, String> properties) {
        // collect all files with overwrite
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();
        List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
        List<IndexManifestEntry> indexChangesWithOverwrite = new ArrayList<>();
        if (latestSnapshot != null) {
            scan.withSnapshot(latestSnapshot)
                    .withPartitionFilter(partitionFilter)
                    .withKind(ScanMode.ALL);
            if (numBucket != BucketMode.POSTPONE_BUCKET) {
                // bucket = -2 can only be overwritten in postpone bucket tables
                scan.withBucketFilter(bucket -> bucket >= 0);
            }
            List<ManifestEntry> currentEntries = scan.plan().files();
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

        return tryCommit(
                changesWithOverwrite,
                emptyList(),
                indexChangesWithOverwrite,
                identifier,
                watermark,
                logOffsets,
                properties,
                Snapshot.CommitKind.OVERWRITE,
                mustConflictCheck(),
                null);
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
            Map<String, String> properties,
            Snapshot.CommitKind commitKind,
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
                    return new SuccessResult();
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
            for (long id = strictModeLastSafeSnapshot + 1; id < newSnapshotId; id++) {
                Snapshot snapshot = snapshotManager.snapshot(id);
                if ((snapshot.commitKind() == Snapshot.CommitKind.COMPACT
                                || snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE)
                        && !snapshot.commitUser().equals(commitUser)) {
                    throw new RuntimeException(
                            String.format(
                                    "When trying to commit snapshot %d, "
                                            + "commit user %s has found a %s snapshot (id: %d) by another user %s. "
                                            + "Giving up committing as %s is set.",
                                    newSnapshotId,
                                    commitUser,
                                    snapshot.commitKind().name(),
                                    id,
                                    snapshot.commitUser(),
                                    CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key()));
                }
            }
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
        if (latestSnapshot != null && conflictCheck.shouldCheck(latestSnapshot.id())) {
            // latestSnapshotId is different from the snapshot id we've checked for conflicts,
            // so we have to check again
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
                    SimpleFileEntry.from(deltaFiles),
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

            if (rowTrackingEnabled) {
                // assigned snapshot id to delta files
                List<ManifestEntry> snapshotAssigned = new ArrayList<>();
                assignSnapshotId(newSnapshotId, deltaFiles, snapshotAssigned);
                // assign row id for new files
                List<ManifestEntry> rowIdAssigned = new ArrayList<>();
                nextRowIdStart =
                        assignRowLineageMeta(firstRowIdStart, snapshotAssigned, rowIdAssigned);
                deltaFiles = rowIdAssigned;
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
                            logOffsets,
                            totalRecordCount,
                            deltaRecordCount,
                            recordCount(changelogFiles),
                            currentWatermark,
                            statsFileName,
                            // if empty properties, just set to null
                            properties.isEmpty() ? null : properties,
                            nextRowIdStart);
        } catch (Throwable e) {
            // fails when preparing for commit, we should clean up
            cleanUpReuseTmpManifests(
                    deltaManifestList, changelogManifestList, oldIndexManifest, indexManifest);
            cleanUpNoReuseTmpManifests(baseManifestList, mergeBeforeManifests, mergeAfterManifests);
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
            return new RetryResult(latestSnapshot, baseDataFiles, e);
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
            cleanUpNoReuseTmpManifests(baseManifestList, mergeBeforeManifests, mergeAfterManifests);
            return new RetryResult(latestSnapshot, baseDataFiles, null);
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
        return new SuccessResult();
    }

    private long assignRowLineageMeta(
            long firstRowIdStart,
            List<ManifestEntry> deltaFiles,
            List<ManifestEntry> rowIdAssigned) {
        if (deltaFiles.isEmpty()) {
            return firstRowIdStart;
        }
        // assign row id for new files
        long start = firstRowIdStart;
        for (ManifestEntry entry : deltaFiles) {
            checkArgument(
                    entry.file().fileSource().isPresent(),
                    "This is a bug, file source field for row-tracking table must present.");
            if (entry.file().fileSource().get().equals(FileSource.APPEND)
                    && entry.file().firstRowId() == null) {
                long rowCount = entry.file().rowCount();
                rowIdAssigned.add(entry.assignFirstRowId(start));
                start += rowCount;
            } else {
                // for compact file, do not assign first row id.
                rowIdAssigned.add(entry);
            }
        }
        return start;
    }

    private void assignSnapshotId(
            long snapshotId, List<ManifestEntry> deltaFiles, List<ManifestEntry> snapshotAssigned) {
        for (ManifestEntry entry : deltaFiles) {
            snapshotAssigned.add(entry.assignSequenceNumber(snapshotId, snapshotId));
        }
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
                        Snapshot.CommitKind.COMPACT,
                        System.currentTimeMillis(),
                        latestSnapshot.logOffsets(),
                        latestSnapshot.totalRecordCount(),
                        0L,
                        0L,
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
            List<SimpleFileEntry> changes,
            Snapshot.CommitKind commitKind) {
        List<SimpleFileEntry> allEntries = new ArrayList<>(baseEntries);
        allEntries.addAll(changes);

        if (commitKind != Snapshot.CommitKind.OVERWRITE) {
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
                                changes,
                                null);
                LOG.warn("", conflictException.getLeft());
                throw conflictException.getRight();
            }
        }

        Function<Throwable, RuntimeException> exceptionFunction =
                e -> {
                    Pair<RuntimeException, RuntimeException> conflictException =
                            createConflictException(
                                    "File deletion conflicts detected! Give up committing.",
                                    baseCommitUser,
                                    baseEntries,
                                    changes,
                                    e);
                    LOG.warn("", conflictException.getLeft());
                    return conflictException.getRight();
                };

        Collection<SimpleFileEntry> mergedEntries;
        try {
            // merge manifest entries and also check if the files we want to delete are still there
            mergedEntries = FileEntry.mergeEntries(allEntries);
        } catch (Throwable e) {
            throw exceptionFunction.apply(e);
        }

        assertNoDelete(mergedEntries, exceptionFunction);

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
                                    null);

                    LOG.warn("", conflictException.getLeft());
                    throw conflictException.getRight();
                }
            }
        }
    }

    private void assertNoDelete(
            Collection<SimpleFileEntry> mergedEntries,
            Function<Throwable, RuntimeException> exceptionFunction) {
        try {
            for (SimpleFileEntry entry : mergedEntries) {
                Preconditions.checkState(
                        entry.kind() != FileKind.DELETE,
                        "Trying to delete file %s for table %s which is not previously added.",
                        entry.fileName(),
                        tableName);
            }
        } catch (Throwable e) {
            assertConflictForPartitionExpire(mergedEntries);
            throw exceptionFunction.apply(e);
        }
    }

    private void assertConflictForPartitionExpire(Collection<SimpleFileEntry> mergedEntries) {
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

    private void cleanUpNoReuseTmpManifests(
            Pair<String, Long> baseManifestList,
            List<ManifestFileMeta> mergeBeforeManifests,
            List<ManifestFileMeta> mergeAfterManifests) {
        if (baseManifestList != null) {
            manifestList.delete(baseManifestList.getKey());
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
            Pair<String, Long> deltaManifestList,
            Pair<String, Long> changelogManifestList,
            String oldIndexManifest,
            String newIndexManifest) {
        if (deltaManifestList != null) {
            for (ManifestFileMeta manifest : manifestList.read(deltaManifestList.getKey())) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(deltaManifestList.getKey());
        }

        if (changelogManifestList != null) {
            for (ManifestFileMeta manifest : manifestList.read(changelogManifestList.getKey())) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(changelogManifestList.getKey());
        }

        cleanIndexManifest(oldIndexManifest, newIndexManifest);
    }

    private void cleanIndexManifest(String oldIndexManifest, String newIndexManifest) {
        if (newIndexManifest != null && !Objects.equals(oldIndexManifest, newIndexManifest)) {
            indexManifestFile.delete(newIndexManifest);
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
        for (CommitCallback callback : commitCallbacks) {
            IOUtils.closeQuietly(callback);
        }
        IOUtils.closeQuietly(snapshotCommit);
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

    @VisibleForTesting
    static class RetryResult implements CommitResult {

        private final Snapshot latestSnapshot;
        private final List<SimpleFileEntry> baseDataFiles;
        private final Exception exception;

        public RetryResult(
                Snapshot latestSnapshot, List<SimpleFileEntry> baseDataFiles, Exception exception) {
            this.latestSnapshot = latestSnapshot;
            this.baseDataFiles = baseDataFiles;
            this.exception = exception;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }
    }
}
