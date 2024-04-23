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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

import static org.apache.paimon.flink.utils.TableScanUtils.getSnapshotId;

/**
 * Splits are assigned preemptively in the order requested by the task. Only one split is assigned
 * to the task at a time. This is unaware of subtask, it does not care about the relationship
 * between splits and subtask, so only one queue is created and splits are fetched in order.
 */
public class FIFOSplitAssigner implements SplitAssigner {

    private final LinkedList<FileStoreSourceSplit> pendingSplitAssignment;

    public FIFOSplitAssigner(Collection<FileStoreSourceSplit> splits) {
        this.pendingSplitAssignment = new LinkedList<>(splits);
    }

    @Override
    public List<FileStoreSourceSplit> getNext(int subtask, @Nullable String hostname) {
        FileStoreSourceSplit split = pendingSplitAssignment.poll();
        return split == null ? Collections.emptyList() : Collections.singletonList(split);
    }

    @Override
    public void addSplit(int suggestedTask, FileStoreSourceSplit split) {
        pendingSplitAssignment.add(split);
    }

    @Override
    public void addSplitsBack(int subtask, List<FileStoreSourceSplit> splits) {
        ListIterator<FileStoreSourceSplit> iterator = splits.listIterator(splits.size());
        while (iterator.hasPrevious()) {
            pendingSplitAssignment.addFirst(iterator.previous());
        }
    }

    @Override
    public Collection<FileStoreSourceSplit> remainingSplits() {
        return new ArrayList<>(pendingSplitAssignment);
    }

    @Override
    public Optional<Long> getNextSnapshotId(int subtask) {
        return pendingSplitAssignment.isEmpty()
                ? Optional.empty()
                : getSnapshotId(pendingSplitAssignment.peekFirst());
    }

    @Override
    public int numberOfRemainingSplits() {
        return pendingSplitAssignment.size();
    }
}
