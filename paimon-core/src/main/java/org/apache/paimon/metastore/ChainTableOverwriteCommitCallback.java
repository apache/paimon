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

package org.apache.paimon.metastore;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.Triple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.createCommitUser;

/**
 * A {@link CommitCallback} implementation to maintain chain table snapshot branch for overwrite
 * commits.
 *
 * <p>When the following conditions are met, this callback will truncate the corresponding
 * partitions on the snapshot branch:
 *
 * <ul>
 *   <li>The committed snapshot kind is {@link CommitKind#OVERWRITE};
 *   <li>The table is a chain table and current branch is delta branch.
 * </ul>
 *
 * <p>This callback is designed to be idempotent. It may be invoked multiple times for the same
 * logical commit, but truncating the same partitions on the snapshot branch repeatedly is safe.
 */
public class ChainTableOverwriteCommitCallback implements CommitCallback {

    private static final Logger LOG =
            LoggerFactory.getLogger(ChainTableOverwriteCommitCallback.class);

    /**
     * The latest snapshot of the snapshot branch when an overwrite of the delta branch was
     * published, 0 if it had none, kept in the properties of the overwrite's snapshot.
     */
    public static final String SNAPSHOT_BRANCH_POSITION = "chain-table.snapshot-branch.snapshot-id";

    private transient FileStoreTable table;
    private transient CoreOptions coreOptions;
    private final String commitUser;

    public ChainTableOverwriteCommitCallback(FileStoreTable table, String commitUser) {
        this.table = table;
        this.coreOptions = table.coreOptions();
        this.commitUser = commitUser;
    }

    /**
     * Records where the snapshot branch stands as the overwrite is published: what the overwrite
     * supersedes there are the files that snapshot holds, and a {@link #retry} of the overwrite has
     * no other exact way to tell them from what the snapshot branch receives afterwards, since the
     * two branches commit independently and their clocks cannot order one against the other.
     *
     * <p>A snapshot branch that cannot be read fails the overwrite before it is published.
     */
    @Override
    public void beforeOverwrite(ManifestCommittable committable) {
        if (!ChainTableUtils.isScanFallbackDeltaBranch(coreOptions)) {
            return;
        }
        Long latest = snapshotTable().snapshotManager().latestSnapshotId();
        committable.addProperty(
                SNAPSHOT_BRANCH_POSITION, String.valueOf(latest == null ? 0L : latest));
    }

    @Override
    public void call(Context context) {
        if (!ChainTableUtils.isScanFallbackDeltaBranch(coreOptions)) {
            return;
        }
        if (context.snapshot.commitKind() != CommitKind.OVERWRITE) {
            return;
        }
        truncateSnapshotPartitions(context.deltaFiles);
    }

    /**
     * The commit of this committable was published by an earlier attempt whose callback may not
     * have completed, for example because the snapshot branch was unreachable right after the delta
     * snapshot was written. Resolve that snapshot and redo the cleanup, which is idempotent. The
     * partitions are taken from the manifest changes of the snapshot rather than from the
     * committable, since an overwrite also clears partitions it wrote no new file to.
     */
    @Override
    public void retry(ManifestCommittable committable) {
        if (!ChainTableUtils.isScanFallbackDeltaBranch(coreOptions)) {
            return;
        }
        List<Snapshot> snapshots =
                table.snapshotManager()
                        .findSnapshotsForIdentifiers(
                                commitUser, Collections.singletonList(committable.identifier()));
        if (snapshots.isEmpty()) {
            LOG.warn(
                    "No snapshot of commit user {} with identifier {} in table {}, "
                            + "cannot redo the snapshot branch cleanup of its overwrite.",
                    commitUser,
                    committable.identifier(),
                    table.name());
            return;
        }
        for (Snapshot snapshot : snapshots) {
            if (snapshot.commitKind() != CommitKind.OVERWRITE) {
                continue;
            }
            List<BinaryRow> overwritePartitions =
                    overwritePartitions(
                            table.store()
                                    .newScan()
                                    .withKind(ScanMode.DELTA)
                                    .withSnapshot(snapshot.id())
                                    .plan()
                                    .files());
            clearSnapshotFilesAsOf(overwritePartitions, snapshot);
        }
    }

    /**
     * Clear what the overwrite superseded in the given partitions of the snapshot branch, and
     * nothing that landed there since. That is what {@link #call} cleared at the time; a replay
     * that repeats it after later data arrived must not take that data with it, whether the
     * original cleanup had completed or not.
     *
     * <p>What the overwrite superseded are the files the snapshot branch held in those partitions
     * when the overwrite was published, and whatever compactions of the snapshot branch have since
     * rewritten from them alone. If a compaction has merged them with data written after the
     * overwrite, another commit such as a rescale has rewritten them, or the snapshot branch no
     * longer retains the history to tell, the cleanup cannot be done exactly, and the retry fails
     * rather than report a cleanup it did not do.
     */
    private void clearSnapshotFilesAsOf(List<BinaryRow> partitions, Snapshot overwrite) {
        if (partitions.isEmpty()) {
            return;
        }
        String position =
                overwrite.properties() == null
                        ? null
                        : overwrite.properties().get(SNAPSHOT_BRANCH_POSITION);
        if (position == null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot redo the snapshot branch cleanup of the overwrite of partitions "
                                    + "%s in table %s (snapshot %s): the overwrite did not record "
                                    + "where the snapshot branch stood when it was published, so "
                                    + "what it superseded cannot be told from what was written "
                                    + "after it. Clear the superseded rows of these partitions "
                                    + "manually.",
                            partitions, table.name(), overwrite.id()));
        }
        long asOfId = Long.parseLong(position);
        if (asOfId == 0) {
            // The snapshot branch had no snapshot when the overwrite was published.
            return;
        }
        FileStoreTable snapshotTable = snapshotTable();
        SnapshotManager snapshotManager = snapshotTable.snapshotManager();
        if (!snapshotManager.snapshotExists(asOfId)) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot redo the snapshot branch cleanup of the overwrite of partitions "
                                    + "%s in table %s (snapshot %s): the snapshot branch no longer "
                                    + "retains snapshot %s, where it stood when that overwrite was "
                                    + "published, so what the overwrite superseded cannot be told "
                                    + "from what was written after it. Retain snapshots of the "
                                    + "snapshot branch for longer than a job may take to recover, "
                                    + "and clear the superseded rows of these partitions manually.",
                            partitions, table.name(), overwrite.id(), asOfId));
        }
        Snapshot asOf = snapshotManager.snapshot(asOfId);
        Snapshot latest = snapshotManager.latestSnapshot();

        // Follow the superseded files through the rewrites of the snapshot branch since then.
        Set<FileEntry.Identifier> superseded = new HashSet<>();
        for (ManifestEntry entry : filesOf(snapshotTable, asOf, partitions)) {
            superseded.add(entry.identifier());
        }
        Set<FileEntry.Identifier> mixed = new HashSet<>();
        for (long id = asOf.id() + 1; id <= latest.id(); id++) {
            boolean compaction = snapshotManager.snapshot(id).commitKind() == CommitKind.COMPACT;
            Map<BinaryRow, List<ManifestEntry>> changes =
                    snapshotTable.store().newScan().withKind(ScanMode.DELTA).withSnapshot(id)
                            .withPartitionFilter(partitions).plan().files().stream()
                            .collect(Collectors.groupingBy(ManifestEntry::partition));
            for (List<ManifestEntry> partitionChanges : changes.values()) {
                if (compaction) {
                    // A compaction rewrites the files of one bucket into that bucket: its
                    // outputs hold superseded rows only if all its inputs did.
                    partitionChanges.stream()
                            .collect(Collectors.groupingBy(ManifestEntry::bucket))
                            .values()
                            .forEach(bucketChanges -> follow(bucketChanges, superseded, mixed));
                } else if (removesTracked(partitionChanges, superseded, mixed)) {
                    // Any other commit that removes superseded rows and adds files to the
                    // partition may have written them again, the way a rescale rewrites a
                    // partition into other buckets; nothing tells them from new data.
                    for (ManifestEntry entry : partitionChanges) {
                        if (entry.kind() == FileKind.ADD) {
                            mixed.add(entry.identifier());
                        }
                    }
                }
                // Anything else a commit adds is data written after the overwrite.
            }
        }

        List<ManifestEntry> current = filesOf(snapshotTable, latest, partitions);
        List<String> inseparable =
                current.stream()
                        .filter(entry -> mixed.contains(entry.identifier()))
                        .map(entry -> entry.file().fileName())
                        .collect(Collectors.toList());
        if (!inseparable.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot redo the snapshot branch cleanup of the overwrite of partitions "
                                    + "%s in table %s (snapshot %s): the snapshot branch has since "
                                    + "rewritten the rows that overwrite superseded into files %s, "
                                    + "either merged with rows written after it by a compaction or "
                                    + "by an overwrite such as a rescale, and they can no longer "
                                    + "be told apart. Clear the superseded rows of these "
                                    + "partitions manually.",
                            partitions, table.name(), overwrite.id(), inseparable));
        }
        Map<Triple<BinaryRow, Integer, Integer>, List<DataFileMeta>> toClear = new HashMap<>();
        for (ManifestEntry entry : current) {
            if (superseded.contains(entry.identifier())) {
                toClear.computeIfAbsent(
                                Triple.of(
                                        entry.partition().copy(),
                                        entry.bucket(),
                                        entry.totalBuckets()),
                                k -> new ArrayList<>())
                        .add(entry.file());
            }
        }
        if (toClear.isEmpty()) {
            return;
        }
        ManifestCommittable committable =
                new ManifestCommittable(BatchWriteBuilder.COMMIT_IDENTIFIER);
        toClear.forEach(
                (key, files) ->
                        committable.addFileCommittable(
                                new CommitMessageImpl(
                                        key.f0,
                                        key.f1,
                                        key.f2,
                                        new DataIncrement(
                                                Collections.emptyList(),
                                                files,
                                                Collections.emptyList()),
                                        CompactIncrement.emptyIncrement())));
        try (FileStoreCommit commit =
                snapshotTable
                        .store()
                        .newCommit(
                                createCommitUser(new Options(snapshotTable.options())),
                                snapshotTable)) {
            // The cleared partitions are the ones the overwrite rewrote on the delta branch, as
            // in the original cleanup, so the pre-callback must not reject dropping their
            // baselines on a retry either.
            Set<BinaryRow> previous =
                    ChainTableOverwriteScope.setFreshlyWrittenDeltaPartitions(
                            new HashSet<>(partitions));
            try {
                // The files being removed must still be there; a concurrent change to them is a
                // conflict to report, not to skip over.
                commit.commit(committable, true);
            } finally {
                ChainTableOverwriteScope.restore(previous);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to clear the files of partitions %s in the snapshot table.",
                            partitions),
                    e);
        }
    }

    /** Follows the files one bucket of a compaction removes into the files it adds. */
    private static void follow(
            List<ManifestEntry> bucketChanges,
            Set<FileEntry.Identifier> superseded,
            Set<FileEntry.Identifier> mixed) {
        boolean fromSuperseded = false;
        boolean fromOther = false;
        for (ManifestEntry entry : bucketChanges) {
            if (entry.kind() == FileKind.DELETE) {
                FileEntry.Identifier identifier = entry.identifier();
                if (superseded.remove(identifier)) {
                    fromSuperseded = true;
                } else if (mixed.remove(identifier)) {
                    fromSuperseded = true;
                    fromOther = true;
                } else {
                    fromOther = true;
                }
            }
        }
        if (!fromSuperseded) {
            return;
        }
        for (ManifestEntry entry : bucketChanges) {
            if (entry.kind() == FileKind.ADD) {
                (fromOther ? mixed : superseded).add(entry.identifier());
            }
        }
    }

    /** Stops tracking the files a commit removes, and returns whether any of them was tracked. */
    private static boolean removesTracked(
            List<ManifestEntry> changes,
            Set<FileEntry.Identifier> superseded,
            Set<FileEntry.Identifier> mixed) {
        boolean removed = false;
        for (ManifestEntry entry : changes) {
            if (entry.kind() == FileKind.DELETE) {
                FileEntry.Identifier identifier = entry.identifier();
                removed |= superseded.remove(identifier) | mixed.remove(identifier);
            }
        }
        return removed;
    }

    private static List<ManifestEntry> filesOf(
            FileStoreTable table, Snapshot snapshot, List<BinaryRow> partitions) {
        return table.store()
                .newScan()
                .withSnapshot(snapshot)
                .withPartitionFilter(partitions)
                .plan()
                .files();
    }

    private FileStoreTable snapshotTable() {
        FileStoreTable candidateTable = ChainTableUtils.resolveChainPrimaryTable(table);
        return candidateTable.switchToBranch(coreOptions.scanFallbackSnapshotBranch());
    }

    private static List<BinaryRow> overwritePartitions(List<ManifestEntry> deltaFiles) {
        return deltaFiles.stream()
                .map(ManifestEntry::partition)
                .distinct()
                .collect(Collectors.toList());
    }

    private void truncateSnapshotPartitions(List<ManifestEntry> deltaFiles) {
        List<BinaryRow> overwritePartitions = overwritePartitions(deltaFiles);
        if (overwritePartitions.isEmpty()) {
            return;
        }
        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        coreOptions.partitionDefaultName(),
                        table.schema().logicalPartitionType(),
                        table.schema().partitionKeys().toArray(new String[0]),
                        coreOptions.legacyPartitionName());
        List<Map<String, String>> candidatePartitions =
                overwritePartitions.stream()
                        .map(partitionComputer::generatePartValues)
                        .collect(Collectors.toList());
        FileStoreTable snapshotTable = snapshotTable();
        try (BatchTableCommit commit = snapshotTable.newBatchWriteBuilder().newCommit()) {
            // The truncated snapshot partitions are exactly the partitions this overwrite just
            // rewrote on the delta branch, so their surviving delta followers hold fresh data
            // and do not depend on a snapshot baseline. Hand that set to the pre-callback that
            // the truncate triggers so it does not reject dropping their baselines.
            Set<BinaryRow> freshlyWritten = new HashSet<>(overwritePartitions);
            Set<BinaryRow> previous =
                    ChainTableOverwriteScope.setFreshlyWrittenDeltaPartitions(freshlyWritten);
            try {
                commit.truncatePartitions(candidatePartitions);
            } finally {
                ChainTableOverwriteScope.restore(previous);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to truncate partitions in snapshot table: %s.",
                            candidatePartitions),
                    e);
        }
    }

    @Override
    public void close() throws Exception {
        // no resources to close
    }
}
