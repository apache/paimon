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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A {@link SplitEnumerator} implementation for {@link StaticFileStoreSource} input. */
public class StaticFileStoreSplitEnumerator
        implements SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private final SplitEnumeratorContext<FileStoreSourceSplit> context;

    @Nullable private final Snapshot snapshot;

    private final Map<Integer, List<FileStoreSourceSplit>> pendingSplitAssignment;

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            Collection<FileStoreSourceSplit> splits) {
        this.context = context;
        this.snapshot = snapshot;
        this.pendingSplitAssignment = createSplitAssignment(splits, context.currentParallelism());
    }

    private static Map<Integer, List<FileStoreSourceSplit>> createSplitAssignment(
            Collection<FileStoreSourceSplit> splits, int numReaders) {
        Map<Integer, List<FileStoreSourceSplit>> assignment = new HashMap<>();
        int i = 0;
        for (FileStoreSourceSplit split : splits) {
            int task = i % numReaders;
            assignment.computeIfAbsent(task, k -> new ArrayList<>()).add(split);
            i++;
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
        // TODO: assignment is already created in constructor, here can just assign per batch
        List<FileStoreSourceSplit> splits = pendingSplitAssignment.remove(subtask);
        if (splits != null && splits.size() > 0) {
            context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subtask, splits)));
        } else {
            context.signalNoMoreSplits(subtask);
        }
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> backSplits, int subtaskId) {
        pendingSplitAssignment
                .computeIfAbsent(subtaskId, k -> new ArrayList<>())
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
