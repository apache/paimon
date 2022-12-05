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

package org.apache.flink.table.store.table.source.snapshot;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.operation.ScanKind;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;

/** {@link DataFileSnapshotEnumerator} base implementation for continuous snapshot enumerator. */
public abstract class ContinuousSnapshotEnumerator extends DataFileSnapshotEnumerator {
    private final CoreOptions.LogStartupMode startupMode;
    private @Nullable final Long startupMillis;

    public ContinuousSnapshotEnumerator(
            Path tablePath,
            DataTableScan scan,
            CoreOptions.LogStartupMode startupMode,
            @Nullable Long startupMillis,
            @Nullable Long nextSnapshotId) {
        super(tablePath, scan, nextSnapshotId);
        this.startupMode = startupMode;
        this.startupMillis = startupMillis;
    }

    @Override
    protected DataTableScan.DataFilePlan tryFirstEnumerate() {
        Long startingSnapshotId = snapshotManager.latestSnapshotId();
        if (startingSnapshotId == null) {
            LOG.debug("There is currently no snapshot. Wait for the snapshot generation.");
            return null;
        }

        DataTableScan.DataFilePlan plan;
        switch (startupMode) {
            case FULL:
                startingSnapshotId =
                        scan.options().readCompacted()
                                ? snapshotManager.latestCompactedSnapshotId()
                                : startingSnapshotId;
                if (startingSnapshotId == null) {
                    LOG.warn(
                            "There is currently no compacted snapshot when {} is true. Wait for the snapshot generation.",
                            CoreOptions.READ_COMPACTED.key());
                    return null;
                }
                plan = scan.withKind(ScanKind.ALL).withSnapshot(startingSnapshotId).plan();
                break;
            case FROM_TIMESTAMP:
                Preconditions.checkNotNull(
                        startupMillis,
                        String.format(
                                "%s can not be null when you use %s for %s",
                                CoreOptions.LOG_SCAN_TIMESTAMP_MILLS.key(),
                                CoreOptions.LogStartupMode.FROM_TIMESTAMP,
                                CoreOptions.LOG_SCAN.key()));
                startingSnapshotId = snapshotManager.earlierThanTimeMills(startupMillis);
                plan = new DataTableScan.DataFilePlan(startingSnapshotId, Collections.emptyList());
                break;
            case LATEST:
                plan = new DataTableScan.DataFilePlan(startingSnapshotId, Collections.emptyList());
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown log startup mode " + startupMode.name());
        }

        nextSnapshotId = startingSnapshotId + 1;
        return plan;
    }

    @Override
    protected DataTableScan.DataFilePlan nextEnumerate() {
        while (true) {
            if (!snapshotManager.snapshotExists(nextSnapshotId)) {
                LOG.debug(
                        "Next snapshot id {} does not exist, wait for the snapshot generation.",
                        nextSnapshotId);
                return null;
            }

            Snapshot snapshot = snapshotManager.snapshot(nextSnapshotId);

            if (shouldReadSnapshot(snapshot)) {
                LOG.debug("Find snapshot id {}.", nextSnapshotId);
                DataTableScan.DataFilePlan plan = getPlan(scan.withSnapshot(nextSnapshotId));
                nextSnapshotId++;
                return plan;
            } else {
                nextSnapshotId++;
            }
        }
    }

    protected abstract boolean shouldReadSnapshot(Snapshot snapshot);

    protected abstract DataTableScan.DataFilePlan getPlan(DataTableScan scan);
}
