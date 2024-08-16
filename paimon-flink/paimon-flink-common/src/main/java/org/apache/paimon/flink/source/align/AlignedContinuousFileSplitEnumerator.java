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

import org.apache.paimon.flink.source.ContinuousFileSplitEnumerator;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.PendingSplitsCheckpoint;
import org.apache.paimon.flink.source.assigners.AlignedSplitAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.SnapshotNotExistPlan;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * An aligned continuously monitoring enumerator. It requires checkpoint to be aligned with
 * snapshot.
 *
 * <p>There are two alignment cases here:
 *
 * <ul>
 *   <li>The snapshot has been consumed, but the checkpoint has not yet been triggered: {@link
 *       AlignedSourceReader} will not request splits until checkpoint is triggered.
 *   <li>The checkpoint has been triggered, but the snapshot has not yet been produced: The
 *       checkpoint will block until the snapshot of the upstream table is generated or times out.
 * </ul>
 */
public class AlignedContinuousFileSplitEnumerator extends ContinuousFileSplitEnumerator {

    private static final Logger LOG =
            LoggerFactory.getLogger(AlignedContinuousFileSplitEnumerator.class);

    private static final String PLACEHOLDER_SPLIT = "placeholder";

    private static final int MAX_PENDING_PLAN = 10;

    private final ArrayBlockingQueue<PlanWithNextSnapshotId> pendingPlans;

    private final AlignedSplitAssigner alignedAssigner;

    private final long alignTimeout;

    private final Object lock;

    private long currentCheckpointId;

    private Long lastConsumedSnapshotId;

    private boolean closed;

    public AlignedContinuousFileSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> remainSplits,
            @Nullable Long nextSnapshotId,
            long discoveryInterval,
            StreamTableScan scan,
            BucketMode bucketMode,
            long alignTimeout,
            int splitPerTaskMax,
            boolean shuffleBucketWithPartition) {
        super(
                context,
                remainSplits,
                nextSnapshotId,
                discoveryInterval,
                scan,
                bucketMode,
                splitPerTaskMax,
                shuffleBucketWithPartition);
        this.pendingPlans = new ArrayBlockingQueue<>(MAX_PENDING_PLAN);
        this.alignedAssigner = (AlignedSplitAssigner) super.splitAssigner;
        this.nextSnapshotId = nextSnapshotId;
        this.alignTimeout = alignTimeout;
        this.lock = new Object();
        this.currentCheckpointId = Long.MIN_VALUE;
        this.lastConsumedSnapshotId = null;
        this.closed = false;
    }

    @Override
    protected void addSplits(Collection<FileStoreSourceSplit> splits) {
        Map<Long, List<FileStoreSourceSplit>> splitsBySnapshot = new TreeMap<>();

        for (FileStoreSourceSplit split : splits) {
            long snapshotId = ((DataSplit) split.split()).snapshotId();
            splitsBySnapshot.computeIfAbsent(snapshotId, snapshot -> new ArrayList<>()).add(split);
        }

        for (List<FileStoreSourceSplit> previousSplits : splitsBySnapshot.values()) {
            Map<Integer, List<FileStoreSourceSplit>> subtaskSplits =
                    computeForBucket(previousSplits);
            subtaskSplits.forEach(
                    (subtask, taskSplits) ->
                            taskSplits.forEach(split -> splitAssigner.addSplit(subtask, split)));
        }
    }

    private Map<Integer, List<FileStoreSourceSplit>> computeForBucket(
            Collection<FileStoreSourceSplit> splits) {
        Map<Integer, List<FileStoreSourceSplit>> subtaskSplits = new HashMap<>();
        for (FileStoreSourceSplit split : splits) {
            subtaskSplits
                    .computeIfAbsent(assignSuggestedTask(split), subtask -> new ArrayList<>())
                    .add(split);
        }
        return subtaskSplits;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> splits, int subtaskId) {
        super.addSplitsBack(splits, subtaskId);
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) throws Exception {
        if (!alignedAssigner.isAligned() && !closed) {
            synchronized (lock) {
                if (pendingPlans.isEmpty()) {
                    lock.wait(alignTimeout);
                    Preconditions.checkArgument(!closed, "Enumerator has been closed.");
                    Preconditions.checkArgument(
                            !pendingPlans.isEmpty(),
                            "Timeout while waiting for snapshot from paimon source.");
                }
            }
            PlanWithNextSnapshotId pendingPlan = pendingPlans.poll();
            addSplits(splitGenerator.createSplits(Objects.requireNonNull(pendingPlan).plan()));
            nextSnapshotId = pendingPlan.nextSnapshotId();
            assignSplits();
        }
        Preconditions.checkArgument(alignedAssigner.isAligned());
        lastConsumedSnapshotId = alignedAssigner.getNextSnapshotId(0).orElse(null);
        alignedAssigner.removeFirst();
        currentCheckpointId = checkpointId;

        // send checkpoint event to the source reader
        CheckpointEvent event = new CheckpointEvent(checkpointId);
        for (int i = 0; i < context.currentParallelism(); i++) {
            context.sendEventToSourceReader(i, event);
        }
        return new PendingSplitsCheckpoint(alignedAssigner.remainingSplits(), nextSnapshotId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        if (currentCheckpointId == checkpointId) {
            throw new FlinkRuntimeException("Checkpoint failure is not allowed in aligned mode.");
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        currentCheckpointId = Long.MIN_VALUE;
        Long nextSnapshot = lastConsumedSnapshotId == null ? null : lastConsumedSnapshotId + 1;
        scan.notifyCheckpointComplete(nextSnapshot);
    }

    // ------------------------------------------------------------------------

    @Override
    protected Optional<PlanWithNextSnapshotId> scanNextSnapshot() {
        if (pendingPlans.remainingCapacity() > 0) {
            Optional<PlanWithNextSnapshotId> scannedPlanOptional = super.scanNextSnapshot();
            if (scannedPlanOptional.isPresent()) {
                PlanWithNextSnapshotId scannedPlan = scannedPlanOptional.get();
                if (!(scannedPlan.plan() instanceof SnapshotNotExistPlan)) {
                    synchronized (lock) {
                        pendingPlans.add(scannedPlan);
                        lock.notifyAll();
                    }
                }
            }
        }
        return Optional.empty();
    }

    @Override
    protected void processDiscoveredSplits(
            Optional<PlanWithNextSnapshotId> ignore, Throwable error) {
        if (error != null) {
            if (error instanceof EndOfScanException) {
                // finished
                LOG.debug("Catching EndOfStreamException, the stream is finished.");
                finished = true;
            } else {
                LOG.error("Failed to enumerate files", error);
                throw new RuntimeException(error);
            }
        }

        if (alignedAssigner.remainingSnapshots() >= MAX_PENDING_PLAN) {
            assignSplits();
            return;
        }

        PlanWithNextSnapshotId nextPlan = pendingPlans.poll();
        if (nextPlan != null) {
            nextSnapshotId = nextPlan.nextSnapshotId();
            Objects.requireNonNull(nextSnapshotId);
            TableScan.Plan plan = nextPlan.plan();
            if (plan.splits().isEmpty()) {
                addSplits(
                        Collections.singletonList(
                                new FileStoreSourceSplit(
                                        PLACEHOLDER_SPLIT,
                                        new PlaceholderSplit(nextSnapshotId - 1))));
            } else {
                addSplits(splitGenerator.createSplits(plan));
            }
        }
        assignSplits();
    }

    @Override
    protected boolean noMoreSplits() {
        return super.noMoreSplits()
                && alignedAssigner.remainingSnapshots() == 0
                && pendingPlans.isEmpty();
    }

    @Override
    protected SplitAssigner createSplitAssigner(BucketMode bucketMode) {
        return new AlignedSplitAssigner();
    }
}
