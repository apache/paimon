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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.CoreOptions.LogStartupMode;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.DataTableScan.DataFilePlan;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.table.store.connector.source.PendingSplitsCheckpoint.INVALID_SNAPSHOT;

/** {@link Source} of file store. */
public class FileStoreSource extends FlinkSource {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;

    private final boolean isContinuous;

    private final long discoveryInterval;

    private final LogStartupMode startupMode;

    @Nullable private final Long startupMills;

    public FileStoreSource(
            FileStoreTable table,
            boolean isContinuous,
            long discoveryInterval,
            LogStartupMode startupMode,
            @Nullable Long startupMills,
            @Nullable int[][] projectedFields,
            @Nullable Predicate predicate,
            @Nullable Long limit) {
        super(table, projectedFields, predicate, limit);
        this.table = table;
        this.isContinuous = isContinuous;
        this.discoveryInterval = discoveryInterval;
        this.startupMode = startupMode;
        this.startupMills = startupMills;
    }

    @Override
    public Boundedness getBoundedness() {
        return isContinuous ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        SnapshotManager snapshotManager = table.snapshotManager();
        DataTableScan scan = table.newScan();
        if (predicate != null) {
            scan.withFilter(predicate);
        }

        Long snapshotId;
        Collection<FileStoreSourceSplit> splits;
        if (checkpoint == null) {
            DataFilePlan plan =
                    isContinuous
                            ? continuousFirstScanSplits(scan, snapshotManager)
                            : batchScanSplits(scan);
            snapshotId = plan.snapshotId;
            splits = new FileStoreSourceSplitGenerator().createSplits(plan);
        } else {
            // restore from checkpoint
            snapshotId = checkpoint.currentSnapshotId();
            if (snapshotId == INVALID_SNAPSHOT) {
                snapshotId = null;
            }
            splits = checkpoint.splits();
        }

        // create enumerator from snapshotId and splits
        if (isContinuous) {
            long currentSnapshot = snapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : snapshotId;
            return new ContinuousFileSplitEnumerator(
                    context,
                    table.location(),
                    scan.withIncremental(true), // the subsequent planning is all incremental
                    table.options().changelogProducer(),
                    splits,
                    currentSnapshot,
                    discoveryInterval);
        } else {
            Snapshot snapshot = snapshotId == null ? null : snapshotManager.snapshot(snapshotId);
            return new StaticFileStoreSplitEnumerator(context, snapshot, splits);
        }
    }

    private DataFilePlan batchScanSplits(DataTableScan scan) {
        return scan.plan();
    }

    private DataFilePlan continuousFirstScanSplits(
            DataTableScan scan, SnapshotManager snapshotManager) {
        switch (startupMode) {
            case FULL:
                DataFilePlan plan;
                if (table.options().changelogProducer()
                        == CoreOptions.ChangelogProducer.FULL_COMPACTION) {
                    // Read the results of the last full compaction.
                    // Only full compaction results will appear on the max level.
                    plan = scan.withLevel(table.options().numLevels() - 1).plan();
                } else {
                    plan = scan.plan();
                }
                return plan;
            case LATEST:
                return new DataFilePlan(
                        snapshotManager.latestSnapshotId(), Collections.emptyList());
            case FROM_TIMESTAMP:
                if (startupMills == null) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "%s can not be null when you use %s for %s",
                                    CoreOptions.LOG_SCAN_TIMESTAMP_MILLS.key(),
                                    LogStartupMode.FROM_TIMESTAMP,
                                    CoreOptions.LOG_SCAN.key()));
                }
                return new DataFilePlan(
                        snapshotManager.earlierThanTimeMills(startupMills),
                        Collections.emptyList());
            default:
                throw new UnsupportedOperationException("Unsupported startup mode: " + startupMode);
        }
    }
}
