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
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** {@link SnapshotEnumerator} for streaming read. */
public class ContinuousDataFileSnapshotEnumerator implements SnapshotEnumerator {

    private static final Logger LOG =
            LoggerFactory.getLogger(ContinuousDataFileSnapshotEnumerator.class);

    private final SnapshotManager snapshotManager;
    private final DataTableScan scan;
    private final StartingScanner startingScanner;
    private final FollowUpScanner followUpScanner;

    private @Nullable Long nextSnapshotId;

    public ContinuousDataFileSnapshotEnumerator(
            Path tablePath,
            DataTableScan scan,
            StartingScanner startingScanner,
            FollowUpScanner followUpScanner,
            @Nullable Long nextSnapshotId) {
        this.snapshotManager = new SnapshotManager(tablePath);
        this.scan = scan;
        this.startingScanner = startingScanner;
        this.followUpScanner = followUpScanner;

        this.nextSnapshotId = nextSnapshotId;
    }

    @Nullable
    @Override
    public DataTableScan.DataFilePlan enumerate() {
        if (nextSnapshotId == null) {
            return tryFirstEnumerate();
        } else {
            return nextEnumerate();
        }
    }

    private DataTableScan.DataFilePlan tryFirstEnumerate() {
        DataTableScan.DataFilePlan plan = startingScanner.getPlan(snapshotManager, scan);
        if (plan != null) {
            nextSnapshotId = plan.snapshotId + 1;
        }
        return plan;
    }

    private DataTableScan.DataFilePlan nextEnumerate() {
        while (true) {
            if (!snapshotManager.snapshotExists(nextSnapshotId)) {
                LOG.debug(
                        "Next snapshot id {} does not exist, wait for the snapshot generation.",
                        nextSnapshotId);
                return null;
            }

            Snapshot snapshot = snapshotManager.snapshot(nextSnapshotId);

            if (followUpScanner.shouldScanSnapshot(snapshot)) {
                LOG.debug("Find snapshot id {}.", nextSnapshotId);
                DataTableScan.DataFilePlan plan = followUpScanner.getPlan(nextSnapshotId, scan);
                nextSnapshotId++;
                return plan;
            } else {
                nextSnapshotId++;
            }
        }
    }

    // ------------------------------------------------------------------------
    //  static create methods
    // ------------------------------------------------------------------------

    public static ContinuousDataFileSnapshotEnumerator create(
            FileStoreTable table, DataTableScan scan, Long nextSnapshotId) {
        CoreOptions.StartupMode startupMode = table.options().startupMode();
        Long startupMillis = table.options().logScanTimestampMills();
        StartingScanner startingScanner;
        if (startupMode == CoreOptions.StartupMode.FULL) {
            startingScanner = new FullStartingScanner();
        } else if (startupMode == CoreOptions.StartupMode.LATEST) {
            startingScanner = new ContinuousLatestStartingScanner();
        } else if (startupMode == CoreOptions.StartupMode.COMPACTED) {
            startingScanner = new CompactedStartingScanner();
        } else if (startupMode == CoreOptions.StartupMode.FROM_TIMESTAMP) {
            Preconditions.checkNotNull(
                    startupMillis,
                    String.format(
                            "%s can not be null when you use %s for %s",
                            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                            CoreOptions.StartupMode.FROM_TIMESTAMP,
                            CoreOptions.SCAN_MODE.key()));
            startingScanner = new ContinuousFromTimestampStartingScanner(startupMillis);
        } else {
            throw new UnsupportedOperationException("Unknown startup mode " + startupMode.name());
        }

        CoreOptions.ChangelogProducer changelogProducer = table.options().changelogProducer();
        FollowUpScanner followUpScanner;
        if (changelogProducer == CoreOptions.ChangelogProducer.NONE) {
            followUpScanner = new DeltaFollowUpScanner();
        } else if (changelogProducer == CoreOptions.ChangelogProducer.INPUT) {
            followUpScanner = new InputChangelogFollowUpScanner();
        } else if (changelogProducer == CoreOptions.ChangelogProducer.FULL_COMPACTION) {
            // this change in scan will affect both starting scanner and follow-up scanner
            scan.withLevel(table.options().numLevels() - 1);
            followUpScanner = new CompactionChangelogFollowUpScanner();
        } else {
            throw new UnsupportedOperationException(
                    "Unknown changelog producer " + changelogProducer.name());
        }

        return new ContinuousDataFileSnapshotEnumerator(
                table.location(), scan, startingScanner, followUpScanner, nextSnapshotId);
    }
}
