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
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;

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

    private transient FileStoreTable table;
    private transient CoreOptions coreOptions;

    public ChainTableOverwriteCommitCallback(FileStoreTable table) {
        this.table = table;
        this.coreOptions = table.coreOptions();
    }

    @Override
    public void call(
            List<SimpleFileEntry> baseFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot) {

        if (!ChainTableUtils.isScanFallbackDeltaBranch(coreOptions)) {
            return;
        }

        if (snapshot.commitKind() != CommitKind.OVERWRITE) {
            return;
        }

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
    public void retry(ManifestCommittable committable) {
        // No-op. Truncating the same partitions again is safe, but we prefer to only rely on the
        // successful commit callback.
    }

    @Override
    public void close() throws Exception {
        // no resources to close
    }
}
