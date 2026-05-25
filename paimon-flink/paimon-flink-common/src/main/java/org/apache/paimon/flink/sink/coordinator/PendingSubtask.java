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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.flink.sink.Committable;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** Subtask manager for commit coordinator. */
public class PendingSubtask {
    private final HashMap<Integer, HashMap<Integer, OperatorCoordinator.SubtaskGateway>>
            registeredSubtask;
    private final CommitterCoordinator coordinator;
    Map<Long, Set<Integer>> notYetCheckpoint;

    // taskId-chk-fileInfo
    Map<Integer, Map<Long, List<Committable>>> temporaryCommittables = new HashMap<>();

    private volatile ConcurrentHashMap<Long, CompletableFuture<Void>> futures =
            new ConcurrentHashMap<>();

    public PendingSubtask(CommitterCoordinator coordinator) {
        this.registeredSubtask = new HashMap<>();
        this.coordinator = coordinator;
        this.notYetCheckpoint = new HashMap<>();
    }

    public void registerSubtask(
            int subtask, int attemptNumber, OperatorCoordinator.SubtaskGateway gateway) {
        registeredSubtask
                .computeIfAbsent(subtask, a -> new HashMap<>())
                .put(attemptNumber, gateway);
    }

    public void unregisterSubtask(int subtask, int attemptNumber, Throwable throwable) {
        registeredSubtask.computeIfAbsent(subtask, a -> new HashMap<>()).remove(attemptNumber);
    }

    public boolean isValid(int subtask, int attemptNumber) {
        HashMap<Integer, OperatorCoordinator.SubtaskGateway> subtaskMap =
                registeredSubtask.get(subtask);
        return subtaskMap != null && subtaskMap.containsKey(attemptNumber);
    }

    private boolean allReceived(long checkpoint) {
        return notYetCheckpoint.get(checkpoint).isEmpty();
    }

    public void receive(int subtask, FileInfoEvent fileInfoEvent) throws Exception {
        long checkpointId = fileInfoEvent.checkpoint();
        temporaryCommittables
                .computeIfAbsent(subtask, k -> new HashMap<>())
                .put(
                        checkpointId,
                        CoordinatedFileInfoSender.deserializeCommittables(
                                fileInfoEvent.getSerializedData()));
        notYetCheckpoint
                .computeIfAbsent(checkpointId, k -> new HashSet<>(registeredSubtask.keySet()))
                .remove(subtask);
        if (allReceived(checkpointId)
                && coordinator.save(getAllTemporaryCommittables(checkpointId), checkpointId)) {
            signal(checkpointId);
            cleanupCheckpointState(checkpointId);
        }
    }

    public void notifyEndInput(int subtask) throws Exception {
        notYetCheckpoint
                .computeIfAbsent(
                        CommitterCoordinator.END_INPUT_CHECKPOINT_ID,
                        k -> new HashSet<>(registeredSubtask.keySet()))
                .remove(subtask);
        if (allReceived(CommitterCoordinator.END_INPUT_CHECKPOINT_ID)) {
            coordinator.endInput();
        }
    }

    private List<Committable> getAllTemporaryCommittables(long checkpoint) {

        List<Committable> committables = new ArrayList<>();
        for (Map<Long, List<Committable>> map : temporaryCommittables.values()) {
            committables.addAll(map.get(checkpoint));
        }
        return committables;
    }

    public void abortCheckpoint(long checkpointId) {
        cleanupCheckpointState(checkpointId);
        remove(checkpointId);
    }

    private void cleanupCheckpointState(long checkpointId) {
        notYetCheckpoint.remove(checkpointId);

        for (Map<Long, List<Committable>> subtaskState : temporaryCommittables.values()) {
            subtaskState.remove(checkpointId);
        }

        temporaryCommittables.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    public void signal(long checkpointId) {
        futures.compute(
                checkpointId,
                (k, existingFuture) -> {
                    if (existingFuture == null) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        existingFuture.complete(null);
                        return existingFuture;
                    }
                });
    }

    public CompletableFuture<Void> waitFor(long checkpointId) {
        return futures.computeIfAbsent(checkpointId, k -> new CompletableFuture<>());
    }

    public void remove(long value) {
        futures.remove(value);
    }

    public void close() {
        notYetCheckpoint.clear();
        temporaryCommittables.clear();
        futures.clear();
    }
}
