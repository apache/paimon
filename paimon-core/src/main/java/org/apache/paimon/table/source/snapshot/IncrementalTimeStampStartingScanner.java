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

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

/** {@link StartingScanner} for incremental changes by timestamp. */
public class IncrementalTimeStampStartingScanner extends AbstractStartingScanner {

    private final long startTimestamp;
    private final long endTimestamp;
    private final ScanMode scanMode;

    public IncrementalTimeStampStartingScanner(
            SnapshotManager snapshotManager,
            long startTimestamp,
            long endTimestamp,
            ScanMode scanMode) {
        super(snapshotManager);
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.scanMode = scanMode;
        Snapshot startingSnapshot = snapshotManager.earlierOrEqualTimeMills(startTimestamp);
        if (startingSnapshot != null) {
            this.startingSnapshotId = startingSnapshot.id();
        }
    }

    @Override
    public Result scan(SnapshotReader reader) {
        Snapshot earliestSnapshot = snapshotManager.snapshot(snapshotManager.earliestSnapshotId());
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();
        if (startTimestamp > latestSnapshot.timeMillis()
                || endTimestamp < earliestSnapshot.timeMillis()) {
            return new NoSnapshot();
        }
        // in org.apache.paimon.utils.SnapshotManager.earlierOrEqualTimeMills
        // 1. if earliestSnapshotId or latestSnapshotId is null startingSnapshotId will be null
        // 2. if earliestSnapShot.timeMillis() > startTimestamp startingSnapshotId will be
        // earliestSnapShotId
        // if  earliestSnapShot.timeMillis() > startTimestamp we should include the earliestSnapShot
        // data
        Long startSnapshotId =
                (startingSnapshotId == null || earliestSnapshot.timeMillis() > startTimestamp)
                        ? earliestSnapshot.id() - 1
                        : startingSnapshotId;
        Snapshot endSnapshot = snapshotManager.earlierOrEqualTimeMills(endTimestamp);
        Long endSnapshotId = (endSnapshot == null) ? latestSnapshot.id() : endSnapshot.id();
        IncrementalStartingScanner incrementalStartingScanner =
                new IncrementalStartingScanner(
                        snapshotManager, startSnapshotId, endSnapshotId, scanMode);
        return incrementalStartingScanner.scan(reader);
    }
}
