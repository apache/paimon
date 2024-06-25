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

package org.apache.paimon.flink.source.assigners;

import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.align.PlaceholderSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Splits are allocated at the granularity of snapshots. When the splits of the current snapshot are
 * not fully allocated and checkpoint are not triggered, the next snapshot will not be allocated.
 */
public class AlignedSplitAssigner implements SplitAssigner {

    private final Deque<PendingSnapshot> pendingSplitAssignment;

    private final AtomicInteger numberOfPendingSplits;

    public AlignedSplitAssigner() {
        this.pendingSplitAssignment = new LinkedList<>();
        this.numberOfPendingSplits = new AtomicInteger(0);
    }

    @Override
    public List<FileStoreSourceSplit> getNext(int subtask, @Nullable String hostname) {
        PendingSnapshot head = pendingSplitAssignment.peek();
        if (head != null && !head.isPlaceHolder) {
            List<FileStoreSourceSplit> subtaskSplits = head.remove(subtask);
            if (subtaskSplits != null) {
                numberOfPendingSplits.getAndAdd(-subtaskSplits.size());
                return subtaskSplits;
            }
        }
        return Collections.emptyList();
    }

    @Override
    public void addSplit(int subtask, FileStoreSourceSplit splits) {
        long snapshotId = ((DataSplit) splits.split()).snapshotId();
        PendingSnapshot last = pendingSplitAssignment.peekLast();
        boolean isPlaceholder = splits.split() instanceof PlaceholderSplit;
        if (last == null || last.snapshotId != snapshotId) {
            last = new PendingSnapshot(snapshotId, isPlaceholder, new HashMap<>());
            last.add(subtask, splits);
            pendingSplitAssignment.addLast(last);
        } else {
            last.add(subtask, splits);
        }
        numberOfPendingSplits.incrementAndGet();
    }

    @Override
    public void addSplitsBack(int suggestedTask, List<FileStoreSourceSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }

        long snapshotId = ((DataSplit) splits.get(0).split()).snapshotId();
        boolean isPlaceholder = splits.get(0).split() instanceof PlaceholderSplit;
        PendingSnapshot head = pendingSplitAssignment.peek();
        if (head == null || snapshotId != head.snapshotId) {
            head = new PendingSnapshot(snapshotId, isPlaceholder, new HashMap<>());
            head.addAll(suggestedTask, splits);
            pendingSplitAssignment.addFirst(head);
        } else {
            head.addAll(suggestedTask, splits);
        }
        numberOfPendingSplits.getAndAdd(splits.size());
    }

    @Override
    public Collection<FileStoreSourceSplit> remainingSplits() {
        List<FileStoreSourceSplit> remainingSplits = new ArrayList<>();
        for (PendingSnapshot pendingSnapshot : pendingSplitAssignment) {
            pendingSnapshot.subtaskSplits.values().forEach(remainingSplits::addAll);
        }
        return remainingSplits;
    }

    @Override
    public Optional<Long> getNextSnapshotId(int subtask) {
        PendingSnapshot head = pendingSplitAssignment.peek();
        return Optional.ofNullable(head != null ? head.snapshotId : null);
    }

    @Override
    public int numberOfRemainingSplits() {
        return numberOfPendingSplits.get();
    }

    public boolean isAligned() {
        PendingSnapshot head = pendingSplitAssignment.peek();
        return head != null && head.empty();
    }

    public int remainingSnapshots() {
        return pendingSplitAssignment.size();
    }

    public void removeFirst() {
        PendingSnapshot head = pendingSplitAssignment.poll();
        Preconditions.checkArgument(
                head != null && head.empty(),
                "The head pending splits is not empty. This is a bug, please file an issue.");
    }

    private static class PendingSnapshot {
        private final long snapshotId;
        private final boolean isPlaceHolder;
        private final Map<Integer, List<FileStoreSourceSplit>> subtaskSplits;

        public PendingSnapshot(
                long snapshotId,
                boolean isPlaceHolder,
                Map<Integer, List<FileStoreSourceSplit>> subtaskSplits) {
            this.snapshotId = snapshotId;
            this.isPlaceHolder = isPlaceHolder;
            this.subtaskSplits = subtaskSplits;
        }

        public List<FileStoreSourceSplit> remove(int subtask) {
            return subtaskSplits.remove(subtask);
        }

        public void add(int subtask, FileStoreSourceSplit split) {
            Preconditions.checkArgument(
                    ((DataSplit) split.split()).snapshotId() == snapshotId,
                    "SnapshotId not equal. This is a bug, please file an issue.");
            subtaskSplits.computeIfAbsent(subtask, id -> new ArrayList<>()).add(split);
        }

        public void addAll(int subtask, List<FileStoreSourceSplit> splits) {
            Preconditions.checkArgument(
                    !subtaskSplits.containsKey(subtask),
                    "Encountered a non-empty list of subtask pending splits. This is a bug, please file an issue.");
            splits.forEach(
                    split ->
                            Preconditions.checkArgument(
                                    ((DataSplit) split.split()).snapshotId() == snapshotId,
                                    "SnapshotId not equal"));
            subtaskSplits.put(subtask, splits);
        }

        public boolean empty() {
            return subtaskSplits.isEmpty() || isPlaceHolder;
        }
    }
}
