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
import org.apache.flink.util.FlinkRuntimeException;
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
import java.util.concurrent.Executors;
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
public class AlignedSourceReader
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

    private final CheckpointAlignMode alignMode;

    private Long snapshotIdByNextCheckpoint;

    private boolean blocking;

    private boolean endOfScan;

    private CompletableFuture<Void> availabilityFuture;

    private transient StreamTableScan scan;

    public AlignedSourceReader(
            ReadBuilder readBuilder,
            long scanInterval,
            boolean emitSnapshotWatermark,
            CheckpointAlignMode alignMode) {
        this(
                readBuilder,
                scanInterval,
                emitSnapshotWatermark,
                alignMode,
                Executors.newScheduledThreadPool(
                        1,
                        r ->
                                new Thread(
                                        r,
                                        "Aligned source scan for "
                                                + Thread.currentThread().getName())));
    }

    public AlignedSourceReader(
            ReadBuilder readBuilder,
            long scanInterval,
            boolean emitSnapshotWatermark,
            CheckpointAlignMode alignMode,
            ScheduledExecutorService executors) {
        this.readBuilder = readBuilder;
        this.pendingSplits = new FutureCompletingBlockingDeque<>();
        this.pendingCheckpoints = new TreeSet<>();
        this.availabilityFuture = (CompletableFuture<Void>) AVAILABLE;
        this.nextSnapshotPerCheckpoint = new TreeMap<>();
        this.scanInterval = scanInterval;
        this.emitSnapshotWatermark = emitSnapshotWatermark;
        this.alignMode = alignMode;
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
        if (!blocking && !pendingSplits.isEmpty()) {
            AlignedSourceSplit alignedSourceSplit = pendingSplits.poll();
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

            if (alignMode == CheckpointAlignMode.STRICTLY) {
                blocking = true;
                switchToUnavailable();
            }
        }

        // checkpoint might has not been triggered
        if (!blocking && endOfScan && pendingSplits.isEmpty()) {
            return InputStatus.END_OF_INPUT;
        } else if (alignMode == CheckpointAlignMode.LOOSELY
                && !pendingSplits.isEmpty()
                && pendingCheckpoints.isEmpty()) {
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
        return blocking ? availabilityFuture : pendingSplits.getAvailabilityFuture();
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
                "Ask if checkpoint can be triggered. blocking {}, pending checkpoints {}",
                blocking,
                pendingCheckpoints);
        if (blocking || alignMode == CheckpointAlignMode.LOOSELY) {
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
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (alignMode == CheckpointAlignMode.STRICTLY) {
            // There is no guarantee that checkpoints will be completed in order.
            // Some checkpoints triggered later may be completed first, which will
            // cause the previous checkpoints to be aborted.
            Preconditions.checkArgument(
                    nextSnapshotPerCheckpoint.firstKey() == checkpointId,
                    "Checkpoint should be completed in order.");
        }
        Long nextSnapshotId = nextSnapshotPerCheckpoint.get(checkpointId);
        scan.notifyCheckpointComplete(nextSnapshotId);
        nextSnapshotPerCheckpoint.headMap(checkpointId, true).clear();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (alignMode == CheckpointAlignMode.STRICTLY
                && !pendingCheckpoints.contains(checkpointId)) {
            // checkpoint has been triggered from source
            throw new FlinkRuntimeException(
                    String.format(
                            "The alignment mode of strictly requires that the checkpoint %s must be successful.",
                            checkpointId));
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

    private void scanNextSnapshot() {
        LOG.debug(
                "SnapshotId by next checkpoint is {}, pending splits {}",
                snapshotIdByNextCheckpoint,
                pendingSplits);
        if (pendingSplits.size() < MAX_PENDING_PLANS) {
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
    }
}
