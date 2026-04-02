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

import org.apache.paimon.flink.utils.TableScanUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

/** Calculator for calculating consumer consumption progress. */
public class ConsumerProgressCalculator {
    private final TreeMap<Long, Long> minNextSnapshotPerCheckpoint;

    private final Map<Integer, Long> assignedSnapshotPerReader;

    private final Map<Integer, Long> consumingSnapshotPerReader;

    public ConsumerProgressCalculator(int parallelism) {
        this.minNextSnapshotPerCheckpoint = new TreeMap<>();
        this.assignedSnapshotPerReader = new HashMap<>(parallelism);
        this.consumingSnapshotPerReader = new HashMap<>(parallelism);
    }

    public void updateConsumeProgress(int subtaskId, ReaderConsumeProgressEvent event) {
        consumingSnapshotPerReader.put(subtaskId, event.lastConsumeSnapshotId());
    }

    public void updateAssignInformation(int subtaskId, FileStoreSourceSplit split) {
        TableScanUtils.getSnapshotId(split)
                .ifPresent(snapshotId -> assignedSnapshotPerReader.put(subtaskId, snapshotId));
    }

    public void notifySnapshotState(
            long checkpointId,
            Set<Integer> readersAwaitingSplit,
            Function<Integer, Long> unassignedCalculationFunction,
            int parallelism) {
        computeMinNextSnapshotId(readersAwaitingSplit, unassignedCalculationFunction, parallelism)
                .ifPresent(
                        minNextSnapshotId ->
                                minNextSnapshotPerCheckpoint.put(checkpointId, minNextSnapshotId));
    }

    public OptionalLong notifyCheckpointComplete(long checkpointId) {
        NavigableMap<Long, Long> nextSnapshots =
                minNextSnapshotPerCheckpoint.headMap(checkpointId, true);
        OptionalLong max = nextSnapshots.values().stream().mapToLong(Long::longValue).max();
        nextSnapshots.clear();
        return max;
    }

    /** Calculate the minimum snapshot currently being consumed by all readers. */
    private Optional<Long> computeMinNextSnapshotId(
            Set<Integer> readersAwaitingSplit,
            Function<Integer, Long> unassignedCalculationFunction,
            int parallelism) {
        long globalMinSnapshotId = Long.MAX_VALUE;
        for (int subtask = 0; subtask < parallelism; subtask++) {
            // 1. if the reader is in the waiting list, it means that all allocated splits have
            // been
            // consumed, and the next snapshotId is calculated from splitAssigner.
            //
            // 2. if the reader is not in the waiting list, the larger value between the
            // consumption
            // progress reported by the reader and the most recently assigned snapshot id is
            // used.
            Long snapshotIdForSubtask;
            if (readersAwaitingSplit.contains(subtask)) {
                snapshotIdForSubtask = unassignedCalculationFunction.apply(subtask);
            } else {
                Long consumingSnapshotId = consumingSnapshotPerReader.get(subtask);
                Long assignedSnapshotId = assignedSnapshotPerReader.get(subtask);
                if (consumingSnapshotId != null && assignedSnapshotId != null) {
                    snapshotIdForSubtask = Math.max(consumingSnapshotId, assignedSnapshotId);
                } else {
                    snapshotIdForSubtask =
                            consumingSnapshotId != null ? consumingSnapshotId : assignedSnapshotId;
                }
            }

            if (snapshotIdForSubtask != null) {
                globalMinSnapshotId = Math.min(globalMinSnapshotId, snapshotIdForSubtask);
            } else {
                return Optional.empty();
            }
        }
        return Optional.of(globalMinSnapshotId);
    }
}
