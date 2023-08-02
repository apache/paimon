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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.source.assigners.FIFOSplitAssigner;
import org.apache.paimon.flink.source.assigners.PreAssignSplitAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.SnapshotNotExistPlan;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A continuously monitoring enumerator. */
public class ContinuousFileSplitEnumerator
        implements SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileSplitEnumerator.class);
    private static final int SPLIT_MAX_NUM = 1_000;

    protected final SplitEnumeratorContext<FileStoreSourceSplit> context;

    protected final long discoveryInterval;

    protected final Set<Integer> readersAwaitingSplit;

    protected final FileStoreSourceSplitGenerator splitGenerator;

    protected final StreamTableScan scan;

    protected final SplitAssigner splitAssigner;

    protected final BucketMode bucketMode;

    @Nullable protected Long nextSnapshotId;

    protected volatile boolean hasReadLatest = false;
    protected boolean finished = false;

    public ContinuousFileSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> remainSplits,
            @Nullable Long nextSnapshotId,
            long discoveryInterval,
            StreamTableScan scan,
            BucketMode bucketMode) {
        checkArgument(discoveryInterval > 0L);
        this.context = checkNotNull(context);
        this.nextSnapshotId = nextSnapshotId;
        this.discoveryInterval = discoveryInterval;
        this.readersAwaitingSplit = new LinkedHashSet<>();
        this.splitGenerator = new FileStoreSourceSplitGenerator();
        this.scan = scan;
        this.bucketMode = bucketMode;
        this.splitAssigner = createSplitAssigner();
        addSplits(remainSplits);
    }

    protected void addSplits(Collection<FileStoreSourceSplit> splits) {
        splits.forEach(this::addSplit);
    }

    private void addSplit(FileStoreSourceSplit split) {
        splitAssigner.addSplit(assignTask(((DataSplit) split.split()).bucket()), split);
    }

    @Override
    public void start() {
        context.callAsync(
                this::scanNextSnapshot, this::processDiscoveredSplits, 0, discoveryInterval);
    }

    @Override
    public void close() throws IOException {
        // no resources to close
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        readersAwaitingSplit.add(subtaskId);
        doHandleSplitRequest(subtaskId);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> splits, int subtaskId) {
        LOG.debug("File Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplitsBack(subtaskId, splits);
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) throws Exception {
        List<FileStoreSourceSplit> splits = new ArrayList<>(splitAssigner.remainingSplits());
        final PendingSplitsCheckpoint checkpoint =
                new PendingSplitsCheckpoint(splits, nextSnapshotId);

        LOG.debug("Source Checkpoint is {}", checkpoint);
        return checkpoint;
    }

    // ------------------------------------------------------------------------

    // this need to be synchronized because scan object is not thread safe. handleSplitRequest and
    // context.callAsync will invoke this.
    protected synchronized PlanWithNextSnapshotId scanNextSnapshot() {
        TableScan.Plan plan = scan.plan();
        Long nextSnapshotId = scan.checkpoint();
        return new PlanWithNextSnapshotId(plan, nextSnapshotId);
    }

    // this mothod could not be synchronized, because it runs in coordinatorThread, which will make
    // it serialize.
    protected void processDiscoveredSplits(
            PlanWithNextSnapshotId planWithNextSnapshotId, Throwable error) {
        if (error != null) {
            if (error instanceof EndOfScanException) {
                // finished
                LOG.debug("Catching EndOfStreamException, the stream is finished.");
                finished = true;
                assignSplits();
            } else {
                LOG.error("Failed to enumerate files", error);
            }
            return;
        }

        nextSnapshotId = planWithNextSnapshotId.nextSnapshotId;
        TableScan.Plan plan = planWithNextSnapshotId.plan;
        if (plan.equals(SnapshotNotExistPlan.INSTANCE)) {
            hasReadLatest = true;
            return;
        } else {
            hasReadLatest = false;
        }

        if (plan.splits().isEmpty()) {
            return;
        }

        addSplits(splitGenerator.createSplits(plan));
        assignSplits();
    }

    private void doHandleSplitRequest(int taskId) {
        assignSplits();
        // if current task assigned no split, we check conditions to scan one more time
        if (readersAwaitingSplit.contains(taskId)) {
            if (!shouldInvokeScan()) {
                return;
            }
            context.callAsync(this::scanNextSnapshot, this::processDiscoveredSplits);
        }
    }

    /**
     * Method should be synchronized because {@link #handleSplitRequest} and {@link
     * #processDiscoveredSplits} have thread conflicts.
     */
    protected synchronized void assignSplits() {
        // create assignment
        Map<Integer, List<FileStoreSourceSplit>> assignment = new HashMap<>();
        Iterator<Integer> readersAwait = readersAwaitingSplit.iterator();
        while (readersAwait.hasNext()) {
            Integer task = readersAwait.next();
            if (!context.registeredReaders().containsKey(task)) {
                readersAwait.remove();
                continue;
            }
            List<FileStoreSourceSplit> splits = splitAssigner.getNext(task, null);
            if (splits.size() > 0) {
                assignment.put(task, splits);
            }
        }

        // remove readers who fetched splits
        if (noMoreSplits()) {
            Iterator<Integer> iterator = readersAwaitingSplit.iterator();
            while (iterator.hasNext()) {
                Integer reader = iterator.next();
                if (!assignment.containsKey(reader)) {
                    context.signalNoMoreSplits(reader);
                    iterator.remove();
                }
            }
        }
        assignment.keySet().forEach(readersAwaitingSplit::remove);
        context.assignSplits(new SplitsAssignment<>(assignment));
    }

    private boolean shouldInvokeScan() {
        return !hasReadLatest && splitAssigner.remainingSplits().size() <= SPLIT_MAX_NUM;
    }

    protected int assignTask(int bucket) {
        if (bucketMode == BucketMode.UNAWARE) {
            // we just assign task 0 when bucket unaware, because we don't need this.
            return 0;
        } else {
            // if not bucket unaware, we assign the bucket % parallelism, the same bucket data go
            // into the same task
            return bucket % context.currentParallelism();
        }
    }

    protected SplitAssigner createSplitAssigner() {
        return bucketMode == BucketMode.UNAWARE
                ? new FIFOSplitAssigner(Collections.emptyList())
                : new PreAssignSplitAssigner(1, context, Collections.emptyList());
    }

    protected boolean noMoreSplits() {
        return finished;
    }

    /** The result of scan. */
    protected static class PlanWithNextSnapshotId {
        private final TableScan.Plan plan;
        private final Long nextSnapshotId;

        public PlanWithNextSnapshotId(TableScan.Plan plan, Long nextSnapshotId) {
            this.plan = plan;
            this.nextSnapshotId = nextSnapshotId;
        }

        public TableScan.Plan plan() {
            return plan;
        }

        public Long nextSnapshotId() {
            return nextSnapshotId;
        }
    }
}
