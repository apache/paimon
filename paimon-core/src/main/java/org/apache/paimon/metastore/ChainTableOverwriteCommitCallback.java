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
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            truncateSnapshotPartitions(
                    table.store()
                            .newScan()
                            .withKind(ScanMode.DELTA)
                            .withSnapshot(snapshot.id())
                            .plan()
                            .files());
        }
    }

    private void truncateSnapshotPartitions(List<ManifestEntry> deltaFiles) {
        FileStoreTable candidateTable = ChainTableUtils.resolveChainPrimaryTable(table);
        FileStoreTable snapshotTable =
                candidateTable.switchToBranch(coreOptions.scanFallbackSnapshotBranch());
        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        coreOptions.partitionDefaultName(),
                        table.schema().logicalPartitionType(),
                        table.schema().partitionKeys().toArray(new String[0]),
                        coreOptions.legacyPartitionName());
        List<BinaryRow> overwritePartitions =
                deltaFiles.stream()
                        .map(ManifestEntry::partition)
                        .distinct()
                        .collect(Collectors.toList());
        if (overwritePartitions.isEmpty()) {
            return;
        }
        List<Map<String, String>> candidatePartitions =
                overwritePartitions.stream()
                        .map(partitionComputer::generatePartValues)
                        .collect(Collectors.toList());
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
