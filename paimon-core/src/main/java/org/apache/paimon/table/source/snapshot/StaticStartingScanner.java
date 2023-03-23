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
import org.apache.paimon.Snapshot;
import org.apache.paimon.operation.ScanKind;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** {@link StartingScanner} for batch read. */
public class StaticStartingScanner implements StartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(StaticStartingScanner.class);

    private final CoreOptions options;

    public StaticStartingScanner(CoreOptions options) {
        this.options = options;
    }

    @Override
    @Nullable
    public Result scan(SnapshotManager snapshotManager, SnapshotSplitReader snapshotSplitReader) {
        Long snapshotId = scanStartSnapshot(options, snapshotManager);
        if (snapshotId == null) {
            LOG.debug(
                    "There is currently no snapshot for startup mode '{}'. Waiting for snapshot generation.",
                    options.startupMode());
            return null;
        }
        return new Result(
                snapshotId,
                snapshotSplitReader.withKind(ScanKind.ALL).withSnapshot(snapshotId).splits());
    }

    @Nullable
    public static Long scanStartSnapshot(CoreOptions options, SnapshotManager snapshotManager) {
        CoreOptions.StartupMode startupMode = options.startupMode();
        switch (startupMode) {
            case LATEST:
            case LATEST_FULL:
                return snapshotManager.latestSnapshotId();
            case COMPACTED_FULL:
                return snapshotManager.latestCompactedSnapshotId();
            case FROM_TIMESTAMP:
                Snapshot snapshot =
                        timeTravelToTimestamp(snapshotManager, options.scanTimestampMills());
                return snapshot == null ? null : snapshot.id();
            case FROM_SNAPSHOT:
                long snapshotId = options.scanSnapshotId();
                if (snapshotManager.earliestSnapshotId() == null
                        || snapshotId < snapshotManager.earliestSnapshotId()) {
                    return null;
                }
                return snapshotId;
            default:
                throw new UnsupportedOperationException(
                        "Unknown startup mode " + startupMode.name());
        }
    }

    @Nullable
    public static Snapshot timeTravelToTimestamp(SnapshotManager snapshotManager, long timestamp) {
        return snapshotManager.earlierOrEqualTimeMills(timestamp);
    }
}
