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
import org.apache.paimon.utils.SnapshotManager;

/** {@link StartingScanner} for incremental changes by timestamp. */
public class IncrementalTimeStampStartingScanner implements StartingScanner {

    private final long startTimestamp;
    private final long endTimestamp;

    public IncrementalTimeStampStartingScanner(long startTimestamp, long endTimestamp) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    @Override
    public Result scan(SnapshotManager manager, SnapshotReader reader) {
        Snapshot earliestSnapshot = manager.snapshot(manager.earliestSnapshotId());
        Snapshot latestSnapshot = manager.latestSnapshot();
        if (startTimestamp > latestSnapshot.timeMillis()
                || endTimestamp < earliestSnapshot.timeMillis()) {
            return new NoSnapshot();
        }
        Snapshot startSnapshot = manager.earlierOrEqualTimeMills(startTimestamp);
        Long startSnapshotId =
                (startSnapshot == null) ? earliestSnapshot.id() - 1 : startSnapshot.id();
        Snapshot endSnapshot = manager.earlierOrEqualTimeMills(endTimestamp);
        Long endSnapshotId = (endSnapshot == null) ? latestSnapshot.id() : endSnapshot.id();
        IncrementalStartingScanner incrementalStartingScanner =
                new IncrementalStartingScanner(startSnapshotId, endSnapshotId);
        return incrementalStartingScanner.scan(manager, reader);
    }
}
