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

import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.BinPacking;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.connector.source.DynamicFilteringData;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.utils.TableScanUtils.getSnapshotId;

/**
 * Pre-calculate which splits each task should process according to the weight or given
 * DynamicFilteringData, and then distribute the splits fairly.
 */
public class PreAssignSplitAssigner implements SplitAssigner {

    /** Default batch splits size to avoid exceed `akka.framesize`. */
    private final int splitBatchSize;

    private final int parallelism;

    private final Map<Integer, LinkedList<FileStoreSourceSplit>> pendingSplitAssignment;

    private final AtomicInteger numberOfPendingSplits;
    private final Collection<FileStoreSourceSplit> splits;

    public PreAssignSplitAssigner(
            int splitBatchSize,
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits) {
        this(splitBatchSize, context.currentParallelism(), splits);
    }

    public PreAssignSplitAssigner(
            int splitBatchSize,
            int parallelism,
            Collection<FileStoreSourceSplit> splits,
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData) {
        this(
                splitBatchSize,
                parallelism,
                splits.stream()
                        .filter(s -> filter(partitionRowProjection, dynamicFilteringData, s))
                        .collect(Collectors.toList()));
    }

    public PreAssignSplitAssigner(
            int splitBatchSize, int parallelism, Collection<FileStoreSourceSplit> splits) {
        this.splitBatchSize = splitBatchSize;
        this.parallelism = parallelism;
        this.splits = splits;
        this.pendingSplitAssignment = createBatchFairSplitAssignment(splits, parallelism);
        this.numberOfPendingSplits = new AtomicInteger(splits.size());
    }

    @Override
    public List<FileStoreSourceSplit> getNext(int subtask, @Nullable String hostname) {
        // The following batch assignment operation is for two purposes:
        // To distribute splits evenly when batch reading to prevent a few tasks from reading all
        // the data (for example, the current resource can only schedule part of the tasks).
        Queue<FileStoreSourceSplit> taskSplits = pendingSplitAssignment.get(subtask);
        List<FileStoreSourceSplit> assignment = new ArrayList<>();
        while (taskSplits != null && !taskSplits.isEmpty() && assignment.size() < splitBatchSize) {
            assignment.add(taskSplits.poll());
        }
        numberOfPendingSplits.getAndAdd(-assignment.size());
        return assignment;
    }

    @Override
    public void addSplit(int suggestedTask, FileStoreSourceSplit split) {
        pendingSplitAssignment.computeIfAbsent(suggestedTask, k -> new LinkedList<>()).add(split);
        numberOfPendingSplits.incrementAndGet();
    }

    @Override
    public void addSplitsBack(int subtask, List<FileStoreSourceSplit> splits) {
        LinkedList<FileStoreSourceSplit> remainingSplits =
                pendingSplitAssignment.computeIfAbsent(subtask, k -> new LinkedList<>());
        ListIterator<FileStoreSourceSplit> iterator = splits.listIterator(splits.size());
        while (iterator.hasPrevious()) {
            remainingSplits.addFirst(iterator.previous());
        }
        numberOfPendingSplits.getAndAdd(splits.size());
    }

    @Override
    public Collection<FileStoreSourceSplit> remainingSplits() {
        List<FileStoreSourceSplit> splits = new ArrayList<>();
        pendingSplitAssignment.values().forEach(splits::addAll);
        return splits;
    }

    /**
     * this method only reload restore for batch execute, because in streaming mode, we need to
     * assign certain bucket to certain task.
     */
    private static Map<Integer, LinkedList<FileStoreSourceSplit>> createBatchFairSplitAssignment(
            Collection<FileStoreSourceSplit> splits, int numReaders) {
        List<List<FileStoreSourceSplit>> assignmentList =
                BinPacking.packForFixedBinNumber(
                        splits, split -> split.split().rowCount(), numReaders);
        Map<Integer, LinkedList<FileStoreSourceSplit>> assignment = new HashMap<>();
        for (int i = 0; i < assignmentList.size(); i++) {
            assignment.put(i, new LinkedList<>(assignmentList.get(i)));
        }
        return assignment;
    }

    @Override
    public Optional<Long> getNextSnapshotId(int subtask) {
        LinkedList<FileStoreSourceSplit> pendingSplits = pendingSplitAssignment.get(subtask);
        return (pendingSplits == null || pendingSplits.isEmpty())
                ? Optional.empty()
                : getSnapshotId(pendingSplits.peekFirst());
    }

    @Override
    public int numberOfRemainingSplits() {
        return numberOfPendingSplits.get();
    }

    public SplitAssigner ofDynamicPartitionPruning(
            Projection partitionRowProjection, DynamicFilteringData dynamicFilteringData) {
        return new PreAssignSplitAssigner(
                splitBatchSize, parallelism, splits, partitionRowProjection, dynamicFilteringData);
    }

    private static boolean filter(
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData,
            FileStoreSourceSplit sourceSplit) {
        DataSplit dataSplit = (DataSplit) sourceSplit.split();
        BinaryRow partition = dataSplit.partition();
        FlinkRowData projected = new FlinkRowData(partitionRowProjection.apply(partition));
        return dynamicFilteringData.contains(projected);
    }
}
