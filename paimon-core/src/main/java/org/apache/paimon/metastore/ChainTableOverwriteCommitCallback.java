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

    private transient FileStoreTable table;
    private transient CoreOptions coreOptions;
    private final String commitUser;

    public ChainTableOverwriteCommitCallback(FileStoreTable table, String commitUser) {
        this.table = table;
        this.coreOptions = table.coreOptions();
        this.commitUser = commitUser;
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
            clearSnapshotFilesAsOf(overwritePartitions, snapshot.timeMillis());
        }
    }

    /**
     * Clear the files of the given partitions that the snapshot branch held when the overwrite was
     * published, and nothing that landed there since. That is what {@link #call} cleared at the
     * time; a replay that repeats it after later data arrived must not take that data with it,
     * whether the original cleanup had completed or not.
     */
    private void clearSnapshotFilesAsOf(List<BinaryRow> partitions, long overwriteCommitMillis) {
        if (partitions.isEmpty()) {
            return;
        }
        FileStoreTable snapshotTable = snapshotTable();
        SnapshotManager snapshotManager = snapshotTable.snapshotManager();
        Snapshot asOf = snapshotManager.earlierOrEqualTimeMills(overwriteCommitMillis);
        Snapshot latest = snapshotManager.latestSnapshot();
        if (asOf == null || latest == null) {
            return;
        }
        Set<FileEntry.Identifier> current =
                filesOf(snapshotTable, latest, partitions).stream()
                        .map(ManifestEntry::identifier)
                        .collect(Collectors.toSet());
        // Files cleared since, by the original callback or an earlier retry, are gone already.
        Map<Triple<BinaryRow, Integer, Integer>, List<DataFileMeta>> superseded = new HashMap<>();
        for (ManifestEntry entry : filesOf(snapshotTable, asOf, partitions)) {
            if (current.contains(entry.identifier())) {
                superseded
                        .computeIfAbsent(
                                Triple.of(
                                        entry.partition().copy(),
                                        entry.bucket(),
                                        entry.totalBuckets()),
                                k -> new ArrayList<>())
                        .add(entry.file());
            }
        }
        if (superseded.isEmpty()) {
            return;
        }
        ManifestCommittable committable =
                new ManifestCommittable(BatchWriteBuilder.COMMIT_IDENTIFIER);
        superseded.forEach(
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
            // The files being removed must still be there; a concurrent change to them is a
            // conflict to report, not to skip over.
            commit.commit(committable, true);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to clear the files of partitions %s in the snapshot table.",
                            partitions),
                    e);
        }
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
            commit.truncatePartitions(candidatePartitions);
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
