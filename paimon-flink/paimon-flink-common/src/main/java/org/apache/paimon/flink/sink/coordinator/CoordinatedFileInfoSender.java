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
import org.apache.paimon.flink.sink.CommittableSerializer;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Sends writer committables to the Paimon writer coordinator. */
public class CoordinatedFileInfoSender {

    private static final int LENGTH_FIELD_SIZE = 4;
    private static final int COUNT_FIELD_SIZE = 4;

    private final TaskOperatorEventGateway gateway;
    private final OperatorID operatorId;
    private final CommittableSerializer serializer;

    private int subtaskId;
    private int attemptNumber;
    private long watermark;
    private boolean endInput;

    public CoordinatedFileInfoSender(TaskOperatorEventGateway gateway, OperatorID operatorId) {
        this.gateway = gateway;
        this.operatorId = operatorId;
        this.serializer = new CommittableSerializer(new CommitMessageSerializer());
        this.subtaskId = -1;
        this.attemptNumber = -1;
        this.watermark = Long.MIN_VALUE;
    }

    public void setSubtaskId(int subtaskId) {
        this.subtaskId = subtaskId;
    }

    public void setAttemptNumber(int attemptNumber) {
        this.attemptNumber = attemptNumber;
    }

    public void processWatermark(long watermark) {
        if (watermark != Long.MAX_VALUE) {
            this.watermark = Math.max(this.watermark, watermark);
        }
    }

    public boolean isEndInput() {
        return endInput;
    }

    public void sendToCoordinator(long checkpointId, List<Committable> committables) {
        if (checkpointId == CommitterCoordinator.END_INPUT_CHECKPOINT_ID) {
            endInput = true;
        }
        byte[] data = serializeCommittables(committables);
        FileInfoRequest request =
                FileInfoRequest.fileInfo(
                        checkpointId,
                        subtaskId,
                        attemptNumber,
                        watermark,
                        data,
                        committables.size());
        sendRequest(request);
    }

    public void sendRecoveredFileInfoToCoordinator(
            long checkpointId, String commitUser, List<Committable> committables) {
        byte[] data = serializeCommittables(committables);
        sendRequest(
                FileInfoRequest.recoveredFileInfo(
                        checkpointId,
                        subtaskId,
                        attemptNumber,
                        watermark,
                        data,
                        committables.size(),
                        commitUser));
    }

    private void sendRequest(FileInfoRequest request) {
        try {
            SerializedValue<CoordinationRequest> serializedRequest =
                    new SerializedValue<CoordinationRequest>(request);
            FileInfoReceivedResponse response =
                    CoordinationResponseUtils.unwrap(
                            gateway.sendRequestToCoordinator(operatorId, serializedRequest).get());
            Preconditions.checkState(
                    response.checkpointId() == request.checkpointId()
                            && response.subtaskId() == request.subtaskId(),
                    "Unexpected file info ACK response for checkpoint %s subtask %s: checkpoint %s subtask %s.",
                    request.checkpointId(),
                    request.subtaskId(),
                    response.checkpointId(),
                    response.subtaskId());
        } catch (IOException | ExecutionException e) {
            throw new RuntimeException("Failed to send file info to coordinator.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while sending file info to coordinator.", e);
        }
    }

    private byte[] serializeCommittables(List<Committable> committables) {
        try {
            int totalBytes = COUNT_FIELD_SIZE;
            List<byte[]> serializedCommittables = new ArrayList<>(committables.size());
            for (Committable committable : committables) {
                Preconditions.checkNotNull(committable, "Committable cannot be null");
                byte[] serialized = serializer.serialize(committable);
                serializedCommittables.add(serialized);
                totalBytes += LENGTH_FIELD_SIZE + serialized.length;
            }

            byte[] result = new byte[totalBytes];
            ByteBuffer resultBuffer = ByteBuffer.wrap(result);
            resultBuffer.putInt(committables.size());
            for (byte[] serialized : serializedCommittables) {
                resultBuffer.putInt(serialized.length);
                resultBuffer.put(serialized);
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize committable.", e);
        }
    }

    public static List<Committable> deserializeCommittables(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data == null ? new byte[0] : data);
        if (buffer.remaining() < COUNT_FIELD_SIZE) {
            throw new IOException("Invalid committable data: missing count field.");
        }

        int count = buffer.getInt();
        List<Committable> result = new ArrayList<>(count);
        CommittableSerializer serializer = new CommittableSerializer(new CommitMessageSerializer());
        int version = serializer.getVersion();
        for (int i = 0; i < count; i++) {
            if (buffer.remaining() < LENGTH_FIELD_SIZE) {
                throw new IOException("Invalid committable data: missing length field.");
            }
            int length = buffer.getInt();
            if (length < 0 || length > buffer.remaining()) {
                throw new IOException("Invalid committable data: corrupted length field.");
            }
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            result.add(serializer.deserialize(version, bytes));
        }

        return result;
    }
}
