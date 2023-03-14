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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.DataTable;
import org.apache.flink.table.store.table.source.snapshot.BoundedWatermarkFollowUpScanner;
import org.apache.flink.table.store.table.source.snapshot.CompactedStartingScanner;
import org.apache.flink.table.store.table.source.snapshot.CompactionChangelogFollowUpScanner;
import org.apache.flink.table.store.table.source.snapshot.DeltaFollowUpScanner;
import org.apache.flink.table.store.table.source.snapshot.FollowUpScanner;
import org.apache.flink.table.store.table.source.snapshot.FullStartingScanner;
import org.apache.flink.table.store.table.source.snapshot.InputChangelogFollowUpScanner;
import org.apache.flink.table.store.table.source.snapshot.SnapshotSplitReader;
import org.apache.flink.table.store.table.source.snapshot.StartingScanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;

import static org.apache.flink.table.store.CoreOptions.ChangelogProducer.FULL_COMPACTION;

/** {@link DataTableScan} for streaming planning. */
public abstract class StreamDataTableScan extends AbstractDataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(StreamDataTableScan.class);

    private final SnapshotManager snapshotManager;

    private StartingScanner startingScanner;
    private FollowUpScanner followUpScanner;
    @Nullable private Long nextSnapshotId;

    public StreamDataTableScan(
            CoreOptions options,
            SnapshotSplitReader snapshotSplitReader,
            SnapshotManager snapshotManager) {
        super(options, snapshotSplitReader);
        this.snapshotManager = snapshotManager;
    }

    public abstract boolean supportStreamingReadOverwrite();

    public StreamDataTableScan withStartingScanner(StartingScanner startingScanner) {
        this.startingScanner = startingScanner;
        return this;
    }

    public StreamDataTableScan withFollowUpScanner(FollowUpScanner followUpScanner) {
        this.followUpScanner = followUpScanner;
        return this;
    }

    public StreamDataTableScan withNextSnapshotId(@Nullable Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
        return this;
    }

    public StreamDataTableScan withSnapshotStarting() {
        startingScanner =
                options().startupMode() == CoreOptions.StartupMode.COMPACTED_FULL
                        ? new CompactedStartingScanner()
                        : new FullStartingScanner();
        return this;
    }

    @Nullable
    @Override
    public DataFilePlan plan() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(true);
        }
        if (followUpScanner == null) {
            followUpScanner = createFollowUpScanner();
        }

        if (nextSnapshotId == null) {
            return tryFirstPlan();
        } else {
            return nextPlan();
        }
    }

    private DataFilePlan tryFirstPlan() {
        DataTableScan.DataFilePlan plan =
                startingScanner.getPlan(snapshotManager, snapshotSplitReader);
        if (plan != null) {
            nextSnapshotId = plan.snapshotId + 1;
        }
        return plan;
    }

    private DataFilePlan nextPlan() {
        while (true) {
            if (!snapshotManager.snapshotExists(nextSnapshotId)) {
                LOG.debug(
                        "Next snapshot id {} does not exist, wait for the snapshot generation.",
                        nextSnapshotId);
                return null;
            }

            Snapshot snapshot = snapshotManager.snapshot(nextSnapshotId);

            if (followUpScanner.shouldEndInput(snapshot)) {
                throw new EndOfStreamException("This stream has ended.");
            }

            // first check changes of overwrite
            if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE
                    && supportStreamingReadOverwrite()) {
                LOG.debug("Find overwrite snapshot id {}.", nextSnapshotId);
                DataTableScan.DataFilePlan overwritePlan =
                        followUpScanner.getOverwriteChangesPlan(
                                nextSnapshotId, snapshotSplitReader);
                nextSnapshotId++;
                return overwritePlan;
            } else if (followUpScanner.shouldScanSnapshot(snapshot)) {
                LOG.debug("Find snapshot id {}.", nextSnapshotId);
                DataTableScan.DataFilePlan plan =
                        followUpScanner.getPlan(nextSnapshotId, snapshotSplitReader);
                nextSnapshotId++;
                return plan;
            } else {
                nextSnapshotId++;
            }
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

    public static StreamDataTableScan create(DataTable dataTable, @Nullable Long nextSnapshotId) {
        StreamDataTableScan scan = (StreamDataTableScan) dataTable.newStreamScan();
        return scan.withNextSnapshotId(nextSnapshotId);
    }

    private FollowUpScanner createFollowUpScanner() {
        CoreOptions.ChangelogProducer changelogProducer = options().changelogProducer();
        FollowUpScanner followUpScanner;
        if (changelogProducer == CoreOptions.ChangelogProducer.NONE) {
            followUpScanner = new DeltaFollowUpScanner();
        } else if (changelogProducer == CoreOptions.ChangelogProducer.INPUT) {
            followUpScanner = new InputChangelogFollowUpScanner();
        } else if (changelogProducer == CoreOptions.ChangelogProducer.FULL_COMPACTION) {
            // this change in data split reader will affect both starting scanner and follow-up
            // scanner
            snapshotSplitReader.withLevelFilter(level -> level == options().numLevels() - 1);
            followUpScanner = new CompactionChangelogFollowUpScanner();
        } else {
            throw new UnsupportedOperationException(
                    "Unknown changelog producer " + changelogProducer.name());
        }

        Long boundedWatermark = options().scanBoundedWatermark();
        if (boundedWatermark != null) {
            followUpScanner =
                    new BoundedWatermarkFollowUpScanner(followUpScanner, boundedWatermark);
        }
        return followUpScanner;
    }

    // ------------------------------------------------------------------------
    //  factory interface
    // ------------------------------------------------------------------------

    /** Factory to create {@link StreamDataTableScan}. */
    public interface Factory extends Serializable {
        StreamDataTableScan create(DataTable dataTable, @Nullable Long nextSnapshotId);
    }
}
