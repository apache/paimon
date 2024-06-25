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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.utils.SnapshotManager;

/**
 * {@link StartingScanner} for the {@link CoreOptions.StartupMode#FROM_SNAPSHOT} startup mode of a
 * streaming read.
 */
public class ContinuousFromSnapshotStartingScanner extends AbstractStartingScanner {

    private final boolean changelogDecoupled;

    public ContinuousFromSnapshotStartingScanner(
            SnapshotManager snapshotManager, long snapshotId, boolean changelogDecoupled) {
        super(snapshotManager);
        this.startingSnapshotId = snapshotId;
        this.changelogDecoupled = changelogDecoupled;
    }

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        Long earliestId = getEarliestId();
        if (earliestId == null) {
            return new NoSnapshot();
        }
        // We should return the specified snapshot as next snapshot to indicate to scan delta data
        // from it. If the snapshotId < earliestSnapshotId, start from the earliest.
        return new NextSnapshot(Math.max(startingSnapshotId, earliestId));
    }

    private Long getEarliestId() {
        Long earliestId;
        if (changelogDecoupled) {
            Long earliestChangelogId = snapshotManager.earliestLongLivedChangelogId();
            earliestId =
                    earliestChangelogId == null
                            ? snapshotManager.earliestSnapshotId()
                            : earliestChangelogId;
        } else {
            earliestId = snapshotManager.earliestSnapshotId();
        }
        return earliestId;
    }
}
