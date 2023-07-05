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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.SnapshotNotExistPlan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.AvailabilityProvider.AVAILABLE;
import static org.apache.paimon.flink.FlinkConnectorOptions.CheckpointAlignMode;

/**
 * A non-parallel {@link org.apache.flink.api.connector.source.SourceReader} that uses Flip-27 to
 * achieve the same function as {@link org.apache.paimon.flink.source.operator.MonitorFunction}.
 *
 * <p>Send {@link Split} to the downstream task at paimon snapshot granularity, and provide two
 * alignment modes: {@link CheckpointAlignMode#STRICTLY} and {@link CheckpointAlignMode#LOOSELY}.
 *
 * <ol>
 *   <li>STRICTLY: only one paimon snapshot is processed within a checkpoint interval.
 *   <li>LOOSELY: several paimon snapshots are processed within a checkpoint interval.
 * </ol>
 */
@SuppressWarnings("unchecked")
public abstract class AlignedSourceReader
        implements ExternallyInducedSourceReader<Split, AlignedSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(AlignedSourceReader.class);
    private static final int MAX_PENDING_PLANS = 10;

    private final ReadBuilder readBuilder;

    private final FutureCompletingBlockingDeque<AlignedSourceSplit> pendingSplits;

    private final TreeSet<Long> pendingCheckpoints;

    private final TreeMap<Long, Long> nextSnapshotPerCheckpoint;

    private final long scanInterval;

    private final boolean emitSnapshotWatermark;

    private final ScheduledExecutorService executors;

    private Long snapshotIdByNextCheckpoint;

    private boolean blocking;

    private boolean endOfScan;

    private CompletableFuture<Void> availabilityFuture;

    private transient StreamTableScan scan;

    public AlignedSourceReader(
            ReadBuilder readBuilder,
            long scanInterval,
            boolean emitSnapshotWatermark,
            ScheduledExecutorService executors) {
        this.readBuilder = readBuilder;
        this.pendingSplits = new FutureCompletingBlockingDeque<>();
        this.pendingCheckpoints = new TreeSet<>();
        this.availabilityFuture = (CompletableFuture<Void>) AVAILABLE;
        this.nextSnapshotPerCheckpoint = new TreeMap<>();
        this.scanInterval = scanInterval;
        this.emitSnapshotWatermark = emitSnapshotWatermark;
        this.executors = executors;
        this.snapshotIdByNextCheckpoint = null;
        this.blocking = false;
        this.endOfScan = false;
    }

    @Override
    public void start() {
        this.scan = readBuilder.newStreamScan();
        if (!pendingSplits.isEmpty()) {
            AlignedSourceSplit split = pendingSplits.peekLast();
            LOG.info(
                    "Restore from the latest checkpoint with next snapshotId {}",
                    split.getNextSnapshotId());
            if (split.isPlaceHolder()) {
                Preconditions.checkArgument(
                        pendingSplits.size() == 1,
                        "This is a bug, pendingSplits should contain only one split.");
                snapshotIdByNextCheckpoint = split.getNextSnapshotId();
                pendingSplits.poll();
            }
            scan.restore(split.getNextSnapshotId());
        }
        executors.scheduleWithFixedDelay(
                this::scanNextSnapshot, 0, scanInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Split> readerOutput) throws Exception {
        AlignedSourceSplit alignedSourceSplit;
        if (!blocking && (alignedSourceSplit = pendingSplits.poll()) != null) {
            snapshotIdByNextCheckpoint = alignedSourceSplit.getNextSnapshotId();
            for (Split split : alignedSourceSplit.getSplits()) {
                readerOutput.collect(split);
            }

            if (emitSnapshotWatermark) {
                Long watermark = alignedSourceSplit.getWatermark();
                if (watermark != null) {
                    readerOutput.emitWatermark(new Watermark(watermark));
                }
            }
            blocking = blockingAfterSendSnapshot(!pendingCheckpoints.isEmpty());
        }

        if (blocking) {
            switchToUnavailable();
        }
        // checkpoint might has not been triggered
        if (!blocking && endOfScan && pendingSplits.isEmpty()) {
            return InputStatus.END_OF_INPUT;
        } else if (!blocking && !pendingSplits.isEmpty() && pendingCheckpoints.isEmpty()) {
            return InputStatus.MORE_AVAILABLE;
        } else {
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    @Override
    public List<AlignedSourceSplit> snapshotState(long checkpointId) {
        blocking = false;
        nextSnapshotPerCheckpoint.put(checkpointId, snapshotIdByNextCheckpoint);
        switchToAvailable();
        if (pendingSplits.isEmpty() && snapshotIdByNextCheckpoint != null) {
            return Collections.singletonList(
                    new AlignedSourceSplit(
                            Collections.emptyList(), snapshotIdByNextCheckpoint, null, true));
        } else {
            return new ArrayList<>(pendingSplits.remainingElements());
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return (CompletableFuture<Void>)
                AvailabilityProvider.and(availabilityFuture, pendingSplits.getAvailabilityFuture());
    }

    @Override
    public void addSplits(List<AlignedSourceSplit> splits) {
        pendingSplits.putAll(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        endOfScan = true;
    }

    @Override
    public Optional<Long> shouldTriggerCheckpoint() {
        LOG.debug(
                "Ask if checkpoint can be triggered. waiting for checkpoint {}, pending checkpoints {}",
                blocking,
                pendingCheckpoints);
        if (shouldTriggerCheckpoint(blocking)) {
            Long checkpoint = pendingCheckpoints.pollFirst();
            if (checkpoint != null) {
                return Optional.of(checkpoint);
            }
        }
        return Optional.empty();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof CheckpointEvent) {
            LOG.info("Received checkpoint event {}", sourceEvent);
            long checkpointId = ((CheckpointEvent) sourceEvent).getCheckpointId();
            pendingCheckpoints.add(checkpointId);
            switchToAvailable();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (nextSnapshotPerCheckpoint.firstKey() != checkpointId) {
            handleCheckpointCompletedOutOfOrder(checkpointId);
        }
        Long nextSnapshotId = nextSnapshotPerCheckpoint.get(checkpointId);
        scan.notifyCheckpointComplete(nextSnapshotId);
        nextSnapshotPerCheckpoint.headMap(checkpointId, true).clear();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (!pendingCheckpoints.contains(checkpointId)) {
            // checkpoint has been triggered from source
            handleCheckpointAborted(checkpointId);
        } else {
            // checkpoint may not been triggered from source
            pendingCheckpoints.remove(checkpointId);
        }
    }

    @Override
    public void close() throws Exception {
        if (executors != null) {
            executors.shutdownNow();
        }
    }

    /** After sending a snapshot, decide whether to block or not. */
    protected abstract boolean blockingAfterSendSnapshot(boolean hasPendingCheckpoint);

    /** Decide whether a checkpoint should be triggered. */
    protected abstract boolean shouldTriggerCheckpoint(boolean blocking);

    /** Handle the case where checkpoints are completed out of order. */
    protected abstract void handleCheckpointCompletedOutOfOrder(long checkpointId);

    /** Handle the case where the checkpoint is aborted. */
    protected abstract void handleCheckpointAborted(long checkpointId);

    private void scanNextSnapshot() {
        LOG.debug(
                "SnapshotId by next checkpoint is {}, pending splits {}",
                snapshotIdByNextCheckpoint,
                pendingSplits);
        if (pendingSplits.size() < MAX_PENDING_PLANS && !endOfScan) {
            try {
                TableScan.Plan plan = scan.plan();
                if (!(plan instanceof SnapshotNotExistPlan)) {
                    pendingSplits.put(
                            new AlignedSourceSplit(
                                    plan.splits(), scan.checkpoint(), scan.watermark(), false));
                }
            } catch (EndOfScanException e) {
                LOG.info("Catching EndOfScanException, the stream is finished.");
                endOfScan = true;
            } catch (Throwable error) {
                LOG.error("Failed to get plans", error);
            }
        }
    }

    private void switchToUnavailable() {
        final CompletableFuture<Void> current = availabilityFuture;
        if (current == AvailabilityProvider.AVAILABLE) {
            LOG.debug("source temporarily switched to unavailable.");
            availabilityFuture = new CompletableFuture<>();
        }
    }

    private void switchToAvailable() {
        final CompletableFuture<Void> current = availabilityFuture;
        if (current != AvailabilityProvider.AVAILABLE) {
            LOG.debug("source switched to available.");
            availabilityFuture = (CompletableFuture<Void>) AVAILABLE;
            current.complete(null);
        }
        pendingSplits.notifyAvailable();
    }
}
