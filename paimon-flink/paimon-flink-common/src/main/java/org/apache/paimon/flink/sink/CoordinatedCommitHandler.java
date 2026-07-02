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

package org.apache.paimon.flink.sink;

import org.apache.paimon.flink.sink.coordinator.CommitCompleteEvent;
import org.apache.paimon.flink.sink.coordinator.CoordinatedCommittableState;
import org.apache.paimon.flink.sink.coordinator.CoordinatedFileInfoSender;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;

import java.util.List;

import static org.apache.paimon.flink.sink.coordinator.CommitterCoordinator.END_INPUT_CHECKPOINT_ID;

/** PIP-30 writer-side handler for the Paimon writer coordinator. */
public class CoordinatedCommitHandler extends CommitHandler {

    private final CoordinatedFileInfoSender sender;

    private transient CoordinatedCommittableState state;

    public CoordinatedCommitHandler(TaskOperatorEventGateway gateway, OperatorID operatorId) {
        this.sender = new CoordinatedFileInfoSender(gateway, operatorId);
    }

    @Override
    public void initialize(
            StateInitializationContext context, int subtaskId, int attemptNumber, String commitUser)
            throws Exception {
        sender.setSubtaskId(subtaskId);
        sender.setAttemptNumber(attemptNumber);
        state = new CoordinatedCommittableState();
        state.initialize(context);
        if (context.isRestored()) {
            long restoredCheckpointId =
                    context.getRestoredCheckpointId()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Restored checkpoint id is missing."));
            List<Committable> committables = state.committables();
            sender.sendRecoveredFileInfoToCoordinator(
                    restoredCheckpointId, commitUser, committables);
            state.markAcknowledged(committables);
        }
    }

    @Override
    public void processWatermark(long watermark) {
        sender.processWatermark(watermark);
    }

    @Override
    public void snapshot(StateSnapshotContext context) throws Exception {
        if (state != null) {
            state.snapshot(context.getCheckpointId());
        }
        if (!sender.isEndInput()) {
            sendUnacknowledgedCommittables(context.getCheckpointId());
        }
    }

    @Override
    public void handleCommittables(long checkpointId) {
        if (checkpointId == END_INPUT_CHECKPOINT_ID) {
            sendUnacknowledgedCommittables(checkpointId);
        }
    }

    @Override
    public boolean requiresStableCommitUser() {
        return true;
    }

    @Override
    public boolean collect(Committable committable) {
        if (state != null) {
            state.add(committable);
        }
        return true;
    }

    @Override
    public boolean handleOperatorEvent(OperatorEvent event) {
        if (event instanceof CommitCompleteEvent) {
            if (state != null) {
                state.markCommittedUpTo(((CommitCompleteEvent) event).checkpointId());
            }
            return true;
        }
        return false;
    }

    private void sendUnacknowledgedCommittables(long checkpointId) {
        if (state == null) {
            return;
        }

        List<Committable> committables = state.unacknowledgedCommittables();
        sender.sendToCoordinator(checkpointId, committables);
        state.markAcknowledged(committables);
    }
}
