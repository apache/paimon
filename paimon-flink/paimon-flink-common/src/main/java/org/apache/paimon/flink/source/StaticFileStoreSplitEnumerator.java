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

import org.apache.paimon.Snapshot;
import org.apache.paimon.utils.FixBinPacking;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/** A {@link SplitEnumerator} implementation for {@link StaticFileStoreSource} input. */
public class StaticFileStoreSplitEnumerator
        implements SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private final SplitEnumeratorContext<FileStoreSourceSplit> context;

    @Nullable private final Snapshot snapshot;

    /** Default batch splits size to avoid exceed `akka.framesize`. */
    private final int splitBatchSize;

    private final Map<Integer, Queue<FileStoreSourceSplit>> pendingSplitAssignment;

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            Collection<FileStoreSourceSplit> splits,
            int splitBatchSize) {
        this.context = context;
        this.snapshot = snapshot;
        this.pendingSplitAssignment = createSplitAssignment(splits, context.currentParallelism());
        this.splitBatchSize = splitBatchSize;
    }

    private static Map<Integer, Queue<FileStoreSourceSplit>> createSplitAssignment(
            Collection<FileStoreSourceSplit> splits, int numReaders) {
        List<List<FileStoreSourceSplit>> assignmentList =
                FixBinPacking.pack(splits, split -> split.split().rowCount(), numReaders);
        Map<Integer, Queue<FileStoreSourceSplit>> assignment = new HashMap<>();
        for (int i = 0; i < assignmentList.size(); i++) {
            assignment.put(i, new LinkedList<>(assignmentList.get(i)));
        }
        return assignment;
    }

    @Override
    public void start() {
        // no resources to start
    }

    @Override
    public void handleSplitRequest(int subtask, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtask)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        // The following batch assignment operation is for two purposes:
        // To distribute splits evenly when batch reading to prevent a few tasks from reading all
        // the data (for example, the current resource can only schedule part of the tasks).
        Queue<FileStoreSourceSplit> taskSplits = pendingSplitAssignment.get(subtask);
        List<FileStoreSourceSplit> assignment = new ArrayList<>();
        while (taskSplits != null && !taskSplits.isEmpty() && assignment.size() < splitBatchSize) {
            assignment.add(taskSplits.poll());
        }
        if (assignment.size() > 0) {
            context.assignSplits(
                    new SplitsAssignment<>(Collections.singletonMap(subtask, assignment)));
        } else {
            context.signalNoMoreSplits(subtask);
        }
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> backSplits, int subtaskId) {
        pendingSplitAssignment
                .computeIfAbsent(subtaskId, k -> new LinkedList<>())
                .addAll(backSplits);
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) {
        List<FileStoreSourceSplit> splits = new ArrayList<>();
        pendingSplitAssignment.values().forEach(splits::addAll);
        return new PendingSplitsCheckpoint(splits, snapshot == null ? null : snapshot.id());
    }

    @Override
    public void close() {
        // no resources to close
    }

    @Nullable
    public Snapshot snapshot() {
        return snapshot;
    }
}
