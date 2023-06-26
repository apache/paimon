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

import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A continuously monitoring enumerator. */
public class ContinuousFileSplitEnumerator
        implements SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileSplitEnumerator.class);

    private final SplitEnumeratorContext<FileStoreSourceSplit> context;

    private final Map<Integer, LinkedList<FileStoreSourceSplit>> bucketSplits;

    private final long discoveryInterval;

    private final Set<Integer> readersAwaitingSplit;

    private final FileStoreSourceSplitGenerator splitGenerator;

    private final StreamTableScan scan;

    @Nullable private Long nextSnapshotId;

    private boolean finished = false;

    private final BucketMode bucketMode;

    public ContinuousFileSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> remainSplits,
            @Nullable Long nextSnapshotId,
            long discoveryInterval,
            StreamTableScan scan,
            BucketMode bucketMode) {
        checkArgument(discoveryInterval > 0L);
        this.context = checkNotNull(context);
        this.bucketSplits = new HashMap<>();
        addSplits(remainSplits);
        this.nextSnapshotId = nextSnapshotId;
        this.discoveryInterval = discoveryInterval;
        this.readersAwaitingSplit = new HashSet<>();
        this.splitGenerator = new FileStoreSourceSplitGenerator();
        this.scan = scan;
        this.bucketMode = bucketMode;
    }

    private void addSplits(Collection<FileStoreSourceSplit> splits) {
        splits.forEach(this::addSplit);
    }

    private void addSplit(FileStoreSourceSplit split) {
        bucketSplits
                .computeIfAbsent(((DataSplit) split.split()).bucket(), i -> new LinkedList<>())
                .add(split);
    }

    private void addSplitsBack(Collection<FileStoreSourceSplit> splits) {
        new LinkedList<>(splits).descendingIterator().forEachRemaining(this::addSplitToHead);
    }

    private void addSplitToHead(FileStoreSourceSplit split) {
        bucketSplits
                .computeIfAbsent(((DataSplit) split.split()).bucket(), i -> new LinkedList<>())
                .addFirst(split);
    }

    @Override
    public void start() {
        context.callAsync(scan::plan, this::processDiscoveredSplits, 0, discoveryInterval);
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
        assignSplits();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> splits, int subtaskId) {
        LOG.debug("File Source Enumerator adds splits back: {}", splits);
        addSplitsBack(splits);
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) {
        List<FileStoreSourceSplit> splits = new ArrayList<>();
        bucketSplits.values().forEach(splits::addAll);
        final PendingSplitsCheckpoint checkpoint =
                new PendingSplitsCheckpoint(splits, nextSnapshotId);

        LOG.debug("Source Checkpoint is {}", checkpoint);
        return checkpoint;
    }

    // ------------------------------------------------------------------------

    private void processDiscoveredSplits(TableScan.Plan plan, Throwable error) {
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

        nextSnapshotId = scan.checkpoint();

        if (plan.splits().isEmpty()) {
            return;
        }

        addSplits(splitGenerator.createSplits(plan));
        assignSplits();
    }

    private void assignSplits() {
        Map<Integer, List<FileStoreSourceSplit>> assignment = createAssignment();
        if (finished) {
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

    private Map<Integer, List<FileStoreSourceSplit>> createAssignment() {
        Map<Integer, List<FileStoreSourceSplit>> assignment = new HashMap<>();
        Deque<Integer> readersId = new ArrayDeque<>(readersAwaitingSplit);
        for (Map.Entry<Integer, LinkedList<FileStoreSourceSplit>> entry : bucketSplits.entrySet()) {
            Integer bucket = entry.getKey();
            LinkedList<FileStoreSourceSplit> splits = entry.getValue();

            if (splits.size() > 0 && readersId.size() > 0) {
                int polledReader = readersId.poll();
                int subTask = assignTask(bucket, polledReader);

                if (readersAwaitingSplit.contains(subTask)) {
                    // if the reader that requested another split has failed in the
                    // meantime, remove
                    // it from the list of waiting readers
                    if (!context.registeredReaders().containsKey(subTask)) {
                        readersAwaitingSplit.remove(subTask);
                        continue;
                    }
                    assignment.computeIfAbsent(subTask, i -> new ArrayList<>()).add(splits.poll());
                }
                readersId.addLast(polledReader);
            }
        }
        return assignment;
    }

    private int assignTask(int bucket, int firstReader) {
        if (bucketMode == BucketMode.UNAWARE) {
            // if bucket unaware, we just simply assign the first reader in the loop
            return firstReader;
        } else {
            // if not bucket unaware, we assign the bucket % parallelism, the same bucket data go
            // into the same task
            return bucket % context.currentParallelism();
        }
    }
}
