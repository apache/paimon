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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.CompactionChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.ContinuousAppendAndCompactFollowUpScanner;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorFollowUpScanner;
import org.apache.paimon.table.source.snapshot.DeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.InputChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner.ScannedResult;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** {@link StreamTableScan} implementation for streaming planning. */
public class InnerStreamTableScanImpl extends AbstractInnerTableScan
        implements InnerStreamTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(InnerStreamTableScanImpl.class);

    private final CoreOptions options;
    private final SnapshotManager snapshotManager;
    private final boolean supportStreamingReadOverwrite;
    private final DefaultValueAssigner defaultValueAssigner;

    private StartingScanner startingScanner;
    private FollowUpScanner followUpScanner;
    private BoundedChecker boundedChecker;
    private boolean isFullPhaseEnd = false;
    @Nullable private Long currentWatermark;
    @Nullable private Long nextSnapshotId;

    public InnerStreamTableScanImpl(
            CoreOptions options,
            SnapshotReader snapshotReader,
            SnapshotManager snapshotManager,
            boolean supportStreamingReadOverwrite,
            DefaultValueAssigner defaultValueAssigner) {
        super(options, snapshotReader);
        this.options = options;
        this.snapshotManager = snapshotManager;
        this.supportStreamingReadOverwrite = supportStreamingReadOverwrite;
        this.defaultValueAssigner = defaultValueAssigner;
    }

    @Override
    public InnerStreamTableScanImpl withFilter(Predicate predicate) {
        snapshotReader.withFilter(defaultValueAssigner.handlePredicate(predicate));
        return this;
    }

    @Override
    public Plan plan() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(true);
        }
        if (followUpScanner == null) {
            followUpScanner = createFollowUpScanner();
        }
        if (boundedChecker == null) {
            boundedChecker = createBoundedChecker();
        }

        if (nextSnapshotId == null) {
            return tryFirstPlan();
        } else {
            return nextPlan();
        }
    }

    private Plan tryFirstPlan() {
        StartingScanner.Result result = startingScanner.scan(snapshotManager, snapshotReader);
        if (result instanceof ScannedResult) {
            ScannedResult scannedResult = (ScannedResult) result;
            currentWatermark = scannedResult.currentWatermark();
            long currentSnapshotId = scannedResult.currentSnapshotId();
            nextSnapshotId = currentSnapshotId + 1;
            isFullPhaseEnd =
                    boundedChecker.shouldEndInput(snapshotManager.snapshot(currentSnapshotId));
            return DataFilePlan.fromResult(result);
        } else if (result instanceof StartingScanner.NextSnapshot) {
            nextSnapshotId = ((StartingScanner.NextSnapshot) result).nextSnapshotId();
            isFullPhaseEnd =
                    snapshotManager.snapshotExists(nextSnapshotId - 1)
                            && boundedChecker.shouldEndInput(
                                    snapshotManager.snapshot(nextSnapshotId - 1));
        }
        return SnapshotNotExistPlan.INSTANCE;
    }

    private Plan nextPlan() {
        while (true) {
            if (isFullPhaseEnd) {
                throw new EndOfScanException();
            }

            if (!snapshotManager.snapshotExists(nextSnapshotId)) {
                Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
                if (earliestSnapshotId != null && earliestSnapshotId > nextSnapshotId) {
                    throw new OutOfRangeException(
                            String.format(
                                    "The snapshot with id %d has expired. You can: "
                                            + "1. increase the snapshot expiration time. "
                                            + "2. use consumer-id to ensure that unconsumed snapshots will not be expired.",
                                    nextSnapshotId));
                }
                LOG.debug(
                        "Next snapshot id {} does not exist, wait for the snapshot generation.",
                        nextSnapshotId);
                return SnapshotNotExistPlan.INSTANCE;
            }

            Snapshot snapshot = snapshotManager.snapshot(nextSnapshotId);

            if (boundedChecker.shouldEndInput(snapshot)) {
                throw new EndOfScanException();
            }

            // first check changes of overwrite
            if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE
                    && supportStreamingReadOverwrite) {
                LOG.debug("Find overwrite snapshot id {}.", nextSnapshotId);
                SnapshotReader.Plan overwritePlan =
                        followUpScanner.getOverwriteChangesPlan(nextSnapshotId, snapshotReader);
                currentWatermark = overwritePlan.watermark();
                nextSnapshotId++;
                return overwritePlan;
            } else if (followUpScanner.shouldScanSnapshot(snapshot)) {
                LOG.debug("Find snapshot id {}.", nextSnapshotId);
                SnapshotReader.Plan plan = followUpScanner.scan(nextSnapshotId, snapshotReader);
                currentWatermark = plan.watermark();
                nextSnapshotId++;
                return plan;
            } else {
                nextSnapshotId++;
            }
        }
    }

    private FollowUpScanner createFollowUpScanner() {
        CoreOptions.StreamingCompactionType type =
                options.toConfiguration().get(CoreOptions.STREAMING_COMPACT);
        switch (type) {
            case NORMAL:
                {
                    return new ContinuousCompactorFollowUpScanner();
                }
            case BUCKET_UNAWARE:
                {
                    return new ContinuousAppendAndCompactFollowUpScanner();
                }
        }

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
                snapshotReader.withLevelFilter(level -> level == options.numLevels() - 1);
                followUpScanner = new CompactionChangelogFollowUpScanner();
                break;
            case LOOKUP:
                // this change in data split reader will affect both starting scanner and follow-up
                snapshotReader.withLevelFilter(level -> level > 0);
                followUpScanner = new CompactionChangelogFollowUpScanner();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown changelog producer " + changelogProducer.name());
        }
        return followUpScanner;
    }

    private BoundedChecker createBoundedChecker() {
        Long boundedWatermark = options.scanBoundedWatermark();
        return boundedWatermark != null
                ? BoundedChecker.watermark(boundedWatermark)
                : BoundedChecker.neverEnd();
    }

    @Nullable
    @Override
    public Long checkpoint() {
        return nextSnapshotId;
    }

    @Nullable
    @Override
    public Long watermark() {
        return currentWatermark;
    }

    @Override
    public void restore(@Nullable Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
    }

    @Override
    public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {
        if (nextSnapshot == null) {
            return;
        }

        String consumerId = options.consumerId();
        if (consumerId != null) {
            snapshotReader.consumerManager().resetConsumer(consumerId, new Consumer(nextSnapshot));
        }
    }
}
