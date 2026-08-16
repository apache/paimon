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

package org.apache.paimon.flink.pipeline.cdc.source.enumerator;

import org.apache.paimon.flink.pipeline.cdc.source.TableAwareFileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.utils.BinPacking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pre-calculate which splits each task should process according to the weight, and then distribute
 * the splits fairly.
 *
 * <p>This class is similar to {@link
 * org.apache.paimon.flink.source.assigners.PreAssignSplitAssigner} but is integrated with {@link
 * TableAwareFileStoreSourceSplit} instead of {@link FileStoreSourceSplit}.
 */
public class CDCSplitAssigner {

    /** Default batch splits size to avoid exceed `akka.framesize`. */
    private final int splitBatchSize;

    private final Map<Integer, LinkedList<TableAwareFileStoreSourceSplit>> pendingSplitAssignment;

    private final AtomicInteger numberOfPendingSplits;

    public CDCSplitAssigner(int splitBatchSize, int parallelism) {
        this.splitBatchSize = splitBatchSize;
        this.pendingSplitAssignment =
                createBatchFairSplitAssignment(Collections.emptyList(), parallelism);
        this.numberOfPendingSplits = new AtomicInteger(0);
    }

    public List<TableAwareFileStoreSourceSplit> getNext(int subtask) {
        // The following batch assignment operation is for two purposes:
        // To distribute splits evenly when batch reading to prevent a few tasks from reading all
        // the data (for example, the current resource can only schedule part of the tasks).
        Queue<TableAwareFileStoreSourceSplit> taskSplits = pendingSplitAssignment.get(subtask);
        List<TableAwareFileStoreSourceSplit> assignment = new ArrayList<>();
        while (taskSplits != null && !taskSplits.isEmpty() && assignment.size() < splitBatchSize) {
            assignment.add(taskSplits.poll());
        }
        numberOfPendingSplits.getAndAdd(-assignment.size());
        return assignment;
    }

    public void addSplit(int suggestedTask, TableAwareFileStoreSourceSplit split) {
        pendingSplitAssignment.computeIfAbsent(suggestedTask, k -> new LinkedList<>()).add(split);
        numberOfPendingSplits.incrementAndGet();
    }

    public void addSplitsBack(int subtask, List<TableAwareFileStoreSourceSplit> splits) {
        LinkedList<TableAwareFileStoreSourceSplit> remainingSplits =
                pendingSplitAssignment.computeIfAbsent(subtask, k -> new LinkedList<>());
        ListIterator<TableAwareFileStoreSourceSplit> iterator = splits.listIterator(splits.size());
        while (iterator.hasPrevious()) {
            remainingSplits.addFirst(iterator.previous());
        }
        numberOfPendingSplits.getAndAdd(splits.size());
    }

    public Collection<TableAwareFileStoreSourceSplit> remainingSplits() {
        List<TableAwareFileStoreSourceSplit> splits = new ArrayList<>();
        pendingSplitAssignment.values().forEach(splits::addAll);
        return splits;
    }

    /**
     * this method only reload restore for batch execute, because in streaming mode, we need to
     * assign certain bucket to certain task.
     */
    private static Map<Integer, LinkedList<TableAwareFileStoreSourceSplit>>
            createBatchFairSplitAssignment(
                    Collection<TableAwareFileStoreSourceSplit> splits, int numReaders) {
        List<List<TableAwareFileStoreSourceSplit>> assignmentList =
                BinPacking.packForFixedBinNumber(
                        splits, split -> split.split().rowCount(), numReaders);
        Map<Integer, LinkedList<TableAwareFileStoreSourceSplit>> assignment = new HashMap<>();
        for (int i = 0; i < assignmentList.size(); i++) {
            assignment.put(i, new LinkedList<>(assignmentList.get(i)));
        }
        return assignment;
    }

    public int numberOfRemainingSplits() {
        return numberOfPendingSplits.get();
    }
}
