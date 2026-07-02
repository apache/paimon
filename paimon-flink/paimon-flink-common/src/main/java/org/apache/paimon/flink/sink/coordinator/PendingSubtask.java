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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/** Tracks writer subtasks and pending checkpoint file information for PWC. */
public class PendingSubtask {

    private final Map<Integer, Map<Integer, OperatorCoordinator.SubtaskGateway>> registeredSubtasks;
    private final NavigableMap<Long, PendingCheckpoint> checkpoints;
    private final Map<Long, Map<Integer, FileInfoRequest>> pendingEnvelopes;
    private final Set<Long> abortedCheckpoints;
    private final CommitterCoordinator<Committable, ?> coordinator;

    private int parallelism;
    private long maxCommittedCheckpointId;
    private long restoredCheckpointId;

    public PendingSubtask(CommitterCoordinator<Committable, ?> coordinator) {
        this.coordinator = coordinator;
        this.registeredSubtasks = new HashMap<>();
        this.checkpoints = new TreeMap<>();
        this.pendingEnvelopes = new HashMap<>();
        this.abortedCheckpoints = new HashSet<>();
        this.maxCommittedCheckpointId = Long.MIN_VALUE;
        this.restoredCheckpointId = Long.MIN_VALUE;
    }

    public void init(int parallelism) {
        this.parallelism = parallelism;
    }

    public void registerSubtask(
            int subtask, int attemptNumber, OperatorCoordinator.SubtaskGateway gateway) {
        Map<Integer, OperatorCoordinator.SubtaskGateway> attempts =
                registeredSubtasks.computeIfAbsent(subtask, ignored -> new HashMap<>());
        if (!attempts.isEmpty() && !attempts.containsKey(attemptNumber)) {
            attempts.clear();
            removePendingSubtask(subtask);
        }
        attempts.put(attemptNumber, gateway);
    }

    public void unregisterSubtask(int subtask, int attemptNumber, Throwable throwable) {
        Map<Integer, OperatorCoordinator.SubtaskGateway> attempts = registeredSubtasks.get(subtask);
        if (attempts != null) {
            attempts.remove(attemptNumber);
        }
        removePendingSubtask(subtask);
    }

    public boolean isValid(int subtask, int attemptNumber) {
        Map<Integer, OperatorCoordinator.SubtaskGateway> attempts = registeredSubtasks.get(subtask);
        return attempts != null && attempts.containsKey(attemptNumber);
    }

    public Collection<OperatorCoordinator.SubtaskGateway> activeGateways() {
        Collection<OperatorCoordinator.SubtaskGateway> gateways = new ArrayList<>();
        for (Map<Integer, OperatorCoordinator.SubtaskGateway> attempts :
                registeredSubtasks.values()) {
            gateways.addAll(attempts.values());
        }
        return gateways;
    }

    public CommitResult receive(int subtask, FileInfoRequest request) throws Exception {
        long envelopeCheckpointId = request.checkpointId();
        if (envelopeCheckpointId <= maxCommittedCheckpointId) {
            return new CommitResult(true, 0, maxCommittedCheckpointId, false);
        }

        recordEnvelope(envelopeCheckpointId, subtask, request);
        recordFileInfos(subtask, request);
        if (!envelopeAllReceived(envelopeCheckpointId)) {
            return CommitResult.NONE;
        }

        if (envelopeCheckpointId != restoredCheckpointId) {
            return CommitResult.NONE;
        }

        saveCheckpointsUpTo(envelopeCheckpointId);

        if (!envelopeAllRecovered(envelopeCheckpointId)) {
            throw new IllegalStateException(
                    String.format(
                            "Restored checkpoint %d contains non-recovered file info.",
                            restoredCheckpointId));
        }

        int committedCount = coordinator.filterAndCommitUpToCheckpoint(restoredCheckpointId);
        maxCommittedCheckpointId = Math.max(maxCommittedCheckpointId, restoredCheckpointId);
        cleanupCommittedCheckpoints(restoredCheckpointId);
        return new CommitResult(true, committedCount, restoredCheckpointId, true);
    }

    public CommitResult notifyCheckpointComplete(long checkpointId) throws Exception {
        if (checkpointId <= maxCommittedCheckpointId) {
            return new CommitResult(true, 0, maxCommittedCheckpointId, false);
        }
        if (!envelopeAllReceived(checkpointId)) {
            throw new IllegalStateException(
                    String.format(
                            "Checkpoint %d completed before PWC received file info from all subtasks.",
                            checkpointId));
        }

        saveCheckpointsUpTo(checkpointId);
        coordinator.notifyCheckpointComplete(checkpointId);
        maxCommittedCheckpointId = Math.max(maxCommittedCheckpointId, checkpointId);
        cleanupCommittedCheckpoints(checkpointId);
        return new CommitResult(true, 0, checkpointId, false);
    }

    public void notifyCheckpointAborted(long checkpointId) {
        abortedCheckpoints.add(checkpointId);
        coordinator.notifyCheckpointAborted(checkpointId);
    }

    public void restoreCheckpoint(long checkpointId) {
        restoredCheckpointId = checkpointId;
    }

    private void recordEnvelope(long checkpointId, int subtask, FileInfoRequest request) {
        Map<Integer, FileInfoRequest> envelope =
                pendingEnvelopes.computeIfAbsent(checkpointId, ignored -> new HashMap<>());
        if (envelope.containsKey(subtask)) {
            throw new IllegalStateException(
                    String.format(
                            "Repeated file info envelope received for checkpoint %d subtask %d.",
                            checkpointId, subtask));
        }
        envelope.put(subtask, request);
    }

    private void recordFileInfos(int subtask, FileInfoRequest request) throws Exception {
        Map<Long, List<Committable>> committablesByCheckpoint = new TreeMap<>();
        for (Committable committable :
                CoordinatedFileInfoSender.deserializeCommittables(request.serializedData())) {
            committablesByCheckpoint
                    .computeIfAbsent(committable.checkpointId(), ignored -> new ArrayList<>())
                    .add(committable);
        }
        for (Map.Entry<Long, List<Committable>> entry : committablesByCheckpoint.entrySet()) {
            if (entry.getKey() > maxCommittedCheckpointId) {
                checkpoint(entry.getKey()).receive(subtask, request, entry.getValue());
            }
        }
    }

    private PendingCheckpoint checkpoint(long checkpointId) {
        return checkpoints.computeIfAbsent(
                checkpointId, ignored -> new PendingCheckpoint(checkpointId));
    }

    private boolean envelopeAllReceived(long checkpointId) {
        Map<Integer, FileInfoRequest> receivedSubtasks = pendingEnvelopes.get(checkpointId);
        return receivedSubtasks != null
                && receivedSubtasks.keySet().containsAll(expectedSubtasks());
    }

    private boolean envelopeAllRecovered(long checkpointId) {
        if (!envelopeAllReceived(checkpointId)) {
            return false;
        }
        for (FileInfoRequest request : pendingEnvelopes.get(checkpointId).values()) {
            if (!request.recovered()) {
                return false;
            }
        }
        for (PendingCheckpoint checkpoint : checkpoints.headMap(checkpointId, true).values()) {
            if (!checkpoint.staged()) {
                continue;
            }
            for (SubtaskFileInfo fileInfo : checkpoint.fileInfos()) {
                if (!fileInfo.request().recovered()) {
                    return false;
                }
            }
        }
        return true;
    }

    private Set<Integer> expectedSubtasks() {
        Set<Integer> subtasks = new HashSet<>();
        for (int i = 0; i < parallelism; i++) {
            subtasks.add(i);
        }
        return subtasks;
    }

    private void saveCheckpointsUpTo(long checkpointId) throws Exception {
        long watermark = envelopeWatermark(checkpointId);
        boolean savedEnvelopeCheckpoint = false;
        for (PendingCheckpoint checkpoint : checkpoints.headMap(checkpointId, true).values()) {
            if (!checkpoint.staged()) {
                saveCheckpoint(checkpoint, watermark);
                checkpoint.markStaged();
            }
            if (checkpoint.checkpointId() == checkpointId) {
                savedEnvelopeCheckpoint = true;
            }
        }
        if (!savedEnvelopeCheckpoint) {
            coordinator.save(Collections.emptyList(), checkpointId, watermark);
        }
    }

    private long envelopeWatermark(long checkpointId) {
        long watermark = Long.MAX_VALUE;
        for (FileInfoRequest request : pendingEnvelopes.get(checkpointId).values()) {
            watermark = Math.min(watermark, request.watermark());
        }
        return watermark;
    }

    private void saveCheckpoint(PendingCheckpoint checkpoint, long watermark) throws Exception {
        coordinator.save(
                checkpoint.committablesAfter(maxCommittedCheckpointId),
                checkpoint.checkpointId(),
                watermark);
    }

    private void cleanupCommittedCheckpoints(long checkpointId) {
        checkpoints.keySet().removeIf(id -> id <= checkpointId);
        pendingEnvelopes.keySet().removeIf(id -> id <= checkpointId);
        abortedCheckpoints.removeIf(id -> id <= checkpointId);
    }

    private void removePendingSubtask(int subtask) {
        for (PendingCheckpoint checkpoint : checkpoints.values()) {
            if (!checkpoint.staged()) {
                checkpoint.removeSubtask(subtask);
            }
        }
        checkpoints
                .entrySet()
                .removeIf(entry -> !entry.getValue().staged() && entry.getValue().isEmpty());
        for (Map<Integer, FileInfoRequest> subtasks : pendingEnvelopes.values()) {
            subtasks.remove(subtask);
        }
        pendingEnvelopes.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    public void close() {
        registeredSubtasks.clear();
        checkpoints.clear();
        pendingEnvelopes.clear();
        abortedCheckpoints.clear();
    }
}
