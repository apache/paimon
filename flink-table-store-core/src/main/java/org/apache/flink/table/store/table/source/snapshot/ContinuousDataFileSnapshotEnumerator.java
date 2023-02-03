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

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.table.DataTable;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;

import static org.apache.flink.table.store.CoreOptions.ChangelogProducer.FULL_COMPACTION;

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
            FileIO fileIO,
            Path tablePath,
            DataTableScan scan,
            StartingScanner startingScanner,
            FollowUpScanner followUpScanner,
            @Nullable Long nextSnapshotId) {
        this.snapshotManager = new SnapshotManager(fileIO, tablePath);
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

    public static ContinuousDataFileSnapshotEnumerator createWithSnapshotStarting(
            DataTable table, DataTableScan scan) {
        StartingScanner startingScanner =
                table.options().startupMode() == CoreOptions.StartupMode.COMPACTED_FULL
                        ? new CompactedStartingScanner()
                        : new FullStartingScanner();
        return new ContinuousDataFileSnapshotEnumerator(
                table.fileIO(),
                table.location(),
                scan,
                startingScanner,
                createFollowUpScanner(table, scan),
                null);
    }

    public static ContinuousDataFileSnapshotEnumerator create(
            DataTable table, DataTableScan scan, @Nullable Long nextSnapshotId) {
        return new ContinuousDataFileSnapshotEnumerator(
                table.fileIO(),
                table.location(),
                scan,
                createStartingScanner(table),
                createFollowUpScanner(table, scan),
                nextSnapshotId);
    }

    private static StartingScanner createStartingScanner(DataTable table) {
        CoreOptions.StartupMode startupMode = table.options().startupMode();
        Long startupMillis = table.options().scanTimestampMills();
        if (startupMode == CoreOptions.StartupMode.LATEST_FULL) {
            return new FullStartingScanner();
        } else if (startupMode == CoreOptions.StartupMode.LATEST) {
            return new ContinuousLatestStartingScanner();
        } else if (startupMode == CoreOptions.StartupMode.COMPACTED_FULL) {
            return new CompactedStartingScanner();
        } else if (startupMode == CoreOptions.StartupMode.FROM_TIMESTAMP) {
            Preconditions.checkNotNull(
                    startupMillis,
                    String.format(
                            "%s can not be null when you use %s for %s",
                            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                            CoreOptions.StartupMode.FROM_TIMESTAMP,
                            CoreOptions.SCAN_MODE.key()));
            return new ContinuousFromTimestampStartingScanner(startupMillis);
        } else if (startupMode == CoreOptions.StartupMode.FROM_SNAPSHOT) {
            Long snapshotId = table.options().scanSnapshotId();
            Preconditions.checkNotNull(
                    snapshotId,
                    String.format(
                            "%s can not be null when you use %s for %s",
                            CoreOptions.SCAN_SNAPSHOT_ID.key(),
                            CoreOptions.StartupMode.FROM_SNAPSHOT,
                            CoreOptions.SCAN_MODE.key()));
            return new ContinuousFromSnapshotStartingScanner(snapshotId);
        } else {
            throw new UnsupportedOperationException("Unknown startup mode " + startupMode.name());
        }
    }

    private static FollowUpScanner createFollowUpScanner(DataTable table, DataTableScan scan) {
        CoreOptions.ChangelogProducer changelogProducer = table.options().changelogProducer();
        if (changelogProducer == CoreOptions.ChangelogProducer.NONE) {
            return new DeltaFollowUpScanner();
        } else if (changelogProducer == CoreOptions.ChangelogProducer.INPUT) {
            return new InputChangelogFollowUpScanner();
        } else if (changelogProducer == CoreOptions.ChangelogProducer.FULL_COMPACTION) {
            // this change in scan will affect both starting scanner and follow-up scanner
            scan.withLevel(table.options().numLevels() - 1);
            return new CompactionChangelogFollowUpScanner();
        } else {
            throw new UnsupportedOperationException(
                    "Unknown changelog producer " + changelogProducer.name());
        }
    }

    public static void validate(TableSchema schema) {
        CoreOptions options = new CoreOptions(schema.options());
        CoreOptions.MergeEngine mergeEngine = options.mergeEngine();
        HashMap<CoreOptions.MergeEngine, String> mergeEngineDesc =
                new HashMap<CoreOptions.MergeEngine, String>() {
                    {
                        put(CoreOptions.MergeEngine.PARTIAL_UPDATE, "Partial update");
                        put(CoreOptions.MergeEngine.AGGREGATE, "Pre-aggregate");
                    }
                };
        if (schema.primaryKeys().size() > 0
                && mergeEngineDesc.containsKey(mergeEngine)
                && options.changelogProducer() != FULL_COMPACTION) {
            throw new RuntimeException(
                    mergeEngineDesc.get(mergeEngine)
                            + " continuous reading is not supported. "
                            + "You can use full compaction changelog producer to support streaming reading.");
        }
    }

    // ------------------------------------------------------------------------
    //  factory interface
    // ------------------------------------------------------------------------

    /** Factory to create {@link ContinuousDataFileSnapshotEnumerator}. */
    public interface Factory extends Serializable {

        ContinuousDataFileSnapshotEnumerator create(
                DataTable table, DataTableScan scan, @Nullable Long nextSnapshotId);
    }
}
