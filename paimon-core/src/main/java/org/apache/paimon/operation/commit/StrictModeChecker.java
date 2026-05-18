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

package org.apache.paimon.operation.commit;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/** A checker to check strict mode based on last safe snapshot. */
public class StrictModeChecker {

    private final SnapshotManager snapshotManager;
    private final String commitUser;
    private final Supplier<FileStoreScan> scanSupplier;
    private final IndexManifestFile indexManifestFile;

    private long strictModeLastSafeSnapshot;

    public StrictModeChecker(
            SnapshotManager snapshotManager,
            String commitUser,
            Supplier<FileStoreScan> scanSupplier,
            IndexManifestFile indexManifestFile,
            long strictModeLastSafeSnapshot) {
        this.snapshotManager = snapshotManager;
        this.commitUser = commitUser;
        this.scanSupplier = scanSupplier;
        this.indexManifestFile = indexManifestFile;
        this.strictModeLastSafeSnapshot = strictModeLastSafeSnapshot;
    }

    public void check(
            long newSnapshotId, CommitKind newCommitKind, List<BinaryRow> newChangedPartitions) {
        Set<BinaryRow> newPartitions = new HashSet<>(newChangedPartitions);
        for (long id = strictModeLastSafeSnapshot + 1; id < newSnapshotId; id++) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.commitUser().equals(commitUser)) {
                continue;
            }
            if (snapshot.commitKind() == CommitKind.COMPACT
                    || snapshot.commitKind() == CommitKind.OVERWRITE) {
                if (hasOverlappedPartition(snapshot, newPartitions)) {
                    throw new RuntimeException(
                            String.format(
                                    "When trying to commit snapshot %d, "
                                            + "commit user %s has found a %s snapshot (id: %d) by another user %s "
                                            + "which modified the same partition. Giving up committing as %s is set.",
                                    newSnapshotId,
                                    commitUser,
                                    snapshot.commitKind().name(),
                                    id,
                                    snapshot.commitUser(),
                                    CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key()));
                }
            }
            if (snapshot.commitKind() == CommitKind.APPEND
                    && newCommitKind == CommitKind.OVERWRITE) {
                Iterator<ManifestEntry> entries =
                        scanSupplier
                                .get()
                                .withSnapshot(snapshot)
                                .withKind(ScanMode.DELTA)
                                .onlyReadRealBuckets()
                                .dropStats()
                                .readFileIterator();
                if (hasOverlappedPartition(entries, newPartitions)) {
                    throw new RuntimeException(
                            String.format(
                                    "When trying to commit snapshot %d, "
                                            + "commit user %s has found a APPEND snapshot (id: %d) by another user %s "
                                            + "which committed files to fixed bucket on the same partition. "
                                            + "Giving up committing as %s is set.",
                                    newSnapshotId,
                                    commitUser,
                                    id,
                                    snapshot.commitUser(),
                                    CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key()));
                }
            }
        }
    }

    private boolean hasOverlappedPartition(Snapshot snapshot, Set<BinaryRow> newPartitions) {
        if (newPartitions.isEmpty()) {
            return false;
        }
        Iterator<ManifestEntry> entries =
                scanSupplier
                        .get()
                        .withSnapshot(snapshot)
                        .withKind(ScanMode.DELTA)
                        .dropStats()
                        .readFileIterator();
        if (hasOverlappedPartition(entries, newPartitions)) {
            return true;
        }

        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null) {
            return false;
        }
        // Fast exit: if this snapshot's indexManifest file name equals the
        // previous snapshot's, no index file was added/removed by this commit
        // (writeIndexFiles reuses the previous file when newIndexFiles is empty).
        long prevId = snapshot.id() - 1;
        if (snapshotManager.snapshotExists(prevId)
                && indexManifest.equals(snapshotManager.snapshot(prevId).indexManifest())) {
            return false;
        }
        for (IndexManifestEntry entry : indexManifestFile.read(indexManifest)) {
            if (newPartitions.contains(entry.partition())) {
                return true;
            }
        }
        return false;
    }

    private boolean hasOverlappedPartition(
            Iterator<ManifestEntry> entries, Set<BinaryRow> newPartitions) {
        if (newPartitions.isEmpty()) {
            return false;
        }
        while (entries.hasNext()) {
            ManifestEntry entry = entries.next();
            if (newPartitions.contains(entry.partition())) {
                return true;
            }
        }
        return false;
    }

    public void update(long newSafeSnapshot) {
        strictModeLastSafeSnapshot = newSafeSnapshot;
    }
}
