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
import org.apache.paimon.utils.SnapshotManager;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** A checker to check strict mode based on last safe snapshot. */
public class StrictModeChecker {

    private final SnapshotManager snapshotManager;
    private final String commitUser;
    private final CommitScanner scanner;
    private final boolean dataEvolutionEnabled;

    private long lastSafeSnapshot;

    public StrictModeChecker(
            SnapshotManager snapshotManager,
            String commitUser,
            CommitScanner scanner,
            boolean dataEvolutionEnabled,
            long lastSafeSnapshot) {
        this.snapshotManager = snapshotManager;
        this.commitUser = commitUser;
        this.scanner = scanner;
        this.dataEvolutionEnabled = dataEvolutionEnabled;
        this.lastSafeSnapshot = lastSafeSnapshot;
    }

    public void check(
            long newSnapshotId,
            CommitKind newCommitKind,
            List<BinaryRow> newChangedPartitions,
            Set<Long> rebasedReassignments) {
        Set<BinaryRow> newPartitions = new HashSet<>(newChangedPartitions);
        for (long id = lastSafeSnapshot + 1; id < newSnapshotId; id++) {
            if ((newCommitKind == CommitKind.COMPACT || newCommitKind == CommitKind.APPEND)
                    && rebasedReassignments.contains(id)) {
                continue;
            }
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.commitUser().equals(commitUser)) {
                continue;
            }
            if (snapshot.commitKind() == CommitKind.COMPACT
                    || snapshot.commitKind() == CommitKind.OVERWRITE) {
                boolean hasOverlap = scanner.hasDataChanges(snapshot, newPartitions, false);
                // OVERWRITE may contain logical changes represented only by deletion vectors,
                // while an index-only COMPACT on a data evolution table does not change data.
                if (!hasOverlap
                        && (snapshot.commitKind() == CommitKind.OVERWRITE
                                || !dataEvolutionEnabled)) {
                    hasOverlap = scanner.hasIndexChanges(snapshot, newPartitions, false);
                }
                if (hasOverlap) {
                    throw new RuntimeException(
                            String.format(
                                    "When trying to commit snapshot %d, "
                                            + "commit user %s has found a %s snapshot (id: %d) by another user %s "
                                            + "which modified the same partition. Giving up committing as %s is true.",
                                    newSnapshotId,
                                    commitUser,
                                    snapshot.commitKind().name(),
                                    id,
                                    snapshot.commitUser(),
                                    CoreOptions.COMMIT_STRICT_MODE_ENABLED.key()));
                }
            }
            if (snapshot.commitKind() == CommitKind.APPEND
                    && newCommitKind == CommitKind.OVERWRITE
                    && scanner.hasDataChanges(snapshot, newPartitions, true)) {
                throw new RuntimeException(
                        String.format(
                                "When trying to commit snapshot %d, "
                                        + "commit user %s has found a APPEND snapshot (id: %d) by another user %s "
                                        + "which committed files to fixed bucket on the same partition. "
                                        + "Giving up committing as %s is true.",
                                newSnapshotId,
                                commitUser,
                                id,
                                snapshot.commitUser(),
                                CoreOptions.COMMIT_STRICT_MODE_ENABLED.key()));
            }
        }
    }

    public void update(long newSafeSnapshot) {
        lastSafeSnapshot = newSafeSnapshot;
    }
}
