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
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import java.util.Iterator;

/** A checker to check strict mode based on last safe snapshot. */
public class StrictModeChecker {

    private final SnapshotManager snapshotManager;
    private final String commitUser;
    private final FileStoreScan scan;

    private long strictModeLastSafeSnapshot;

    public StrictModeChecker(
            SnapshotManager snapshotManager,
            String commitUser,
            FileStoreScan scan,
            long strictModeLastSafeSnapshot) {
        this.snapshotManager = snapshotManager;
        this.commitUser = commitUser;
        this.scan = scan;
        this.strictModeLastSafeSnapshot = strictModeLastSafeSnapshot;
    }

    public void check(long newSnapshotId, CommitKind newCommitKind) {
        for (long id = strictModeLastSafeSnapshot + 1; id < newSnapshotId; id++) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.commitUser().equals(commitUser)) {
                continue;
            }
            if (snapshot.commitKind() == CommitKind.COMPACT
                    || snapshot.commitKind() == CommitKind.OVERWRITE) {
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
            if (snapshot.commitKind() == CommitKind.APPEND
                    && newCommitKind == CommitKind.OVERWRITE) {
                Iterator<ManifestEntry> entries =
                        scan.withSnapshot(snapshot)
                                .withKind(ScanMode.DELTA)
                                .onlyReadRealBuckets()
                                .dropStats()
                                .readFileIterator();
                if (entries.hasNext()) {
                    throw new RuntimeException(
                            String.format(
                                    "When trying to commit snapshot %d, "
                                            + "commit user %s has found a APPEND snapshot (id: %d) by another user %s "
                                            + "which committed files to fixed bucket. Giving up committing as %s is set.",
                                    newSnapshotId,
                                    commitUser,
                                    id,
                                    snapshot.commitUser(),
                                    CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key()));
                }
            }
        }
    }

    public void update(long newSafeSnapshot) {
        strictModeLastSafeSnapshot = newSafeSnapshot;
    }
}
