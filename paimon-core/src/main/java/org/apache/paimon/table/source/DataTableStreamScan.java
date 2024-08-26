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
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.snapshot.AllDeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.CompactionChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.ContinuousAppendAndCompactFollowUpScanner;
import org.apache.paimon.table.source.snapshot.DeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.InputChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingContext;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner.ScannedResult;
import org.apache.paimon.table.source.snapshot.StaticFromSnapshotStartingScanner;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.NextSnapshotFetcher;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.apache.paimon.CoreOptions.ChangelogProducer.FULL_COMPACTION;

/** {@link StreamTableScan} implementation for streaming planning. */
public class DataTableStreamScan extends AbstractDataTableScan implements StreamDataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(DataTableStreamScan.class);

    private final CoreOptions options;
    private final SnapshotManager snapshotManager;
    private final boolean supportStreamingReadOverwrite;
    private final DefaultValueAssigner defaultValueAssigner;
    private final NextSnapshotFetcher nextSnapshotProvider;

    private boolean initialized = false;
    private StartingScanner startingScanner;
    private FollowUpScanner followUpScanner;
    private BoundedChecker boundedChecker;

    private boolean isFullPhaseEnd = false;
    @Nullable private Long currentWatermark;
    @Nullable private Long nextSnapshotId;

    public DataTableStreamScan(
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
        this.nextSnapshotProvider =
                new NextSnapshotFetcher(snapshotManager, options.changelogLifecycleDecoupled());
    }

    @Override
    public DataTableStreamScan withFilter(Predicate predicate) {
        snapshotReader.withFilter(defaultValueAssigner.handlePredicate(predicate));
        return this;
    }

    @Override
    public StartingContext startingContext() {
        if (!initialized) {
            initScanner();
        }
        return startingScanner.startingContext();
    }

    @Override
    public Plan plan() {
        if (!initialized) {
            initScanner();
        }

        if (nextSnapshotId == null) {
            return tryFirstPlan();
        } else {
            return nextPlan();
        }
    }

    private void initScanner() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(true);
        }
        if (followUpScanner == null) {
            followUpScanner = createFollowUpScanner();
        }
        if (boundedChecker == null) {
            boundedChecker = createBoundedChecker();
        }
        initialized = true;
    }

    private Plan tryFirstPlan() {
        StartingScanner.Result result;
        if (options.needLookup()) {
            result = startingScanner.scan(snapshotReader.withLevelFilter(level -> level > 0));
            snapshotReader.withLevelFilter(Filter.alwaysTrue());
        } else if (options.changelogProducer().equals(FULL_COMPACTION)) {
            result =
                    startingScanner.scan(
                            snapshotReader.withLevelFilter(
                                    level -> level == options.numLevels() - 1));
            snapshotReader.withLevelFilter(Filter.alwaysTrue());
        } else {
            result = startingScanner.scan(snapshotReader);
        }

        if (result instanceof ScannedResult) {
            ScannedResult scannedResult = (ScannedResult) result;
            currentWatermark = scannedResult.currentWatermark();
            long currentSnapshotId = scannedResult.currentSnapshotId();
            LookupStrategy lookupStrategy = options.lookupStrategy();
            if (!lookupStrategy.produceChangelog && lookupStrategy.deletionVector) {
                // For DELETION_VECTOR_ONLY mode, we need to return the remaining data from level 0
                // in the subsequent plan.
                nextSnapshotId = currentSnapshotId;
            } else {
                nextSnapshotId = currentSnapshotId + 1;
            }
            isFullPhaseEnd =
                    boundedChecker.shouldEndInput(snapshotManager.snapshot(currentSnapshotId));
            return scannedResult.plan();
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

            Snapshot snapshot = nextSnapshotProvider.getNextSnapshot(nextSnapshotId);
            if (snapshot == null) {
                return SnapshotNotExistPlan.INSTANCE;
            }

            if (boundedChecker.shouldEndInput(snapshot)) {
                throw new EndOfScanException();
            }

            if (checkDelaySnapshot(nextSnapshotId)) {
                continue;
            }

            // first check changes of overwrite
            if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE
                    && supportStreamingReadOverwrite) {
                LOG.debug("Find overwrite snapshot id {}.", nextSnapshotId);
                SnapshotReader.Plan overwritePlan =
                        followUpScanner.getOverwriteChangesPlan(snapshot, snapshotReader);
                currentWatermark = overwritePlan.watermark();
                nextSnapshotId++;
                return overwritePlan;
            } else if (followUpScanner.shouldScanSnapshot(snapshot)) {
                LOG.debug("Find snapshot id {}.", nextSnapshotId);
                SnapshotReader.Plan plan = followUpScanner.scan(snapshot, snapshotReader);
                currentWatermark = plan.watermark();
                nextSnapshotId++;
                return plan;
            } else {
                nextSnapshotId++;
            }
        }
    }

    private boolean checkDelaySnapshot(long snapshotId) {
        if (options.scanDelayDuration() == null) {
            return false;
        }
        long delayMillis = options.scanDelayDuration().toMillis();
        long snapshotMills = System.currentTimeMillis() - delayMillis;
        if (snapshotManager.snapshotExists(snapshotId)
                && snapshotManager.snapshot(snapshotId).timeMillis() > snapshotMills) {
            return true;
        }
        return false;
    }

    private FollowUpScanner createFollowUpScanner() {
        CoreOptions.StreamScanMode type =
                options.toConfiguration().get(CoreOptions.STREAM_SCAN_MODE);
        switch (type) {
            case COMPACT_BUCKET_TABLE:
                return new DeltaFollowUpScanner();
            case COMPACT_APPEND_NO_BUCKET:
                return new ContinuousAppendAndCompactFollowUpScanner();
            case FILE_MONITOR:
                return new AllDeltaFollowUpScanner();
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
            case LOOKUP:
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
    public void restore(@Nullable Long nextSnapshotId, boolean scanAllSnapshot) {
        if (nextSnapshotId != null && scanAllSnapshot) {
            startingScanner =
                    new StaticFromSnapshotStartingScanner(snapshotManager, nextSnapshotId);
            restore(null);
        } else {
            restore(nextSnapshotId);
        }
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

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        snapshotReader.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }
}
