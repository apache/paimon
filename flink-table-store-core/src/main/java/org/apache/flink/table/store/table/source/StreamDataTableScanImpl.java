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
import org.apache.flink.table.store.file.utils.SnapshotManager;
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

/** {@link DataTableScan} for streaming planning. */
public class StreamDataTableScanImpl extends AbstractDataTableScan implements StreamDataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(StreamDataTableScan.class);

    private final CoreOptions options;
    private final SnapshotManager snapshotManager;
    private final boolean supportStreamingReadOverwrite;

    private StartingScanner startingScanner;
    private FollowUpScanner followUpScanner;
    @Nullable private Long nextSnapshotId;

    public StreamDataTableScanImpl(
            CoreOptions options,
            SnapshotSplitReader snapshotSplitReader,
            SnapshotManager snapshotManager,
            boolean supportStreamingReadOverwrite) {
        super(options, snapshotSplitReader);
        this.options = options;
        this.snapshotManager = snapshotManager;
        this.supportStreamingReadOverwrite = supportStreamingReadOverwrite;
    }

    @Override
    public boolean supportStreamingReadOverwrite() {
        return supportStreamingReadOverwrite;
    }

    public StreamDataTableScan withStartingScanner(StartingScanner startingScanner) {
        this.startingScanner = startingScanner;
        return this;
    }

    public StreamDataTableScan withFollowUpScanner(FollowUpScanner followUpScanner) {
        this.followUpScanner = followUpScanner;
        return this;
    }

    @Override
    public StreamDataTableScan withSnapshotStarting() {
        startingScanner =
                options.startupMode() == CoreOptions.StartupMode.COMPACTED_FULL
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
                throw new EndOfScanException();
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

    private FollowUpScanner createFollowUpScanner() {
        CoreOptions.ChangelogProducer changelogProducer = options.changelogProducer();
        FollowUpScanner followUpScanner;
        switch (changelogProducer) {
            case NONE:
                followUpScanner = new DeltaFollowUpScanner();
                break;
            case INPUT:
                followUpScanner = new InputChangelogFollowUpScanner();
                break;
            case FULL_COMPACTION:
                // this change in data split reader will affect both starting scanner and follow-up
                snapshotSplitReader.withLevelFilter(level -> level == options.numLevels() - 1);
                followUpScanner = new CompactionChangelogFollowUpScanner();
                break;
            case LOOKUP:
                // this change in data split reader will affect both starting scanner and follow-up
                snapshotSplitReader.withLevelFilter(level -> level > 0);
                followUpScanner = new CompactionChangelogFollowUpScanner();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown changelog producer " + changelogProducer.name());
        }

        Long boundedWatermark = options.scanBoundedWatermark();
        if (boundedWatermark != null) {
            followUpScanner =
                    new BoundedWatermarkFollowUpScanner(followUpScanner, boundedWatermark);
        }
        return followUpScanner;
    }

    @Nullable
    @Override
    public Long checkpoint() {
        return nextSnapshotId;
    }

    @Override
    public void restore(@Nullable Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
    }
}
