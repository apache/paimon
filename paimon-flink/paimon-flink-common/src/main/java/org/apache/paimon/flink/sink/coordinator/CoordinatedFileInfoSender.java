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
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.paimon.flink.sink.coordinator.CommitterCoordinator.END_INPUT_CHECKPOINT_ID;

/** Use direct byte buffer save serialized committables. */
public class CoordinatedFileInfoSender {
    private static final int DEFAULT_BUFFER_SIZE = 32 * 1024;
    private static final int LENGTH_FIELD_SIZE = 4;
    private static final int COUNT_FIELD_SIZE = 4;

    protected final boolean streamingCheckpointEnabled;
    private final List<ByteBuffer> buffers;
    private ByteBuffer currentBuffer;
    private transient boolean endInput;

    private final CommittableSerializer serializer;
    private final TaskOperatorEventGateway gateway;
    private final OperatorID operatorID;
    private int totalCommittables = 0;

    public CoordinatedFileInfoSender(
            TaskOperatorEventGateway gateway,
            OperatorID operatorID,
            int bufferSize,
            boolean streamingCheckpointEnabled) {
        this.gateway = gateway;
        this.operatorID = operatorID;
        this.streamingCheckpointEnabled = streamingCheckpointEnabled;
        this.serializer = new CommittableSerializer(new CommitMessageSerializer());
        this.buffers = new ArrayList<>();

        allocateNewBuffer(bufferSize > 0 ? bufferSize : DEFAULT_BUFFER_SIZE);
    }

    public CoordinatedFileInfoSender(
            TaskOperatorEventGateway gateway,
            OperatorID operatorID,
            boolean streamingCheckpointEnabled) {
        this(gateway, operatorID, DEFAULT_BUFFER_SIZE, streamingCheckpointEnabled);
    }

    private void allocateNewBuffer(int capacity) {
        this.currentBuffer = ByteBuffer.allocateDirect(capacity);
        this.buffers.add(this.currentBuffer);
    }

    /*
     * serialize committable to Direct ByteBuffer
     */
    public void collect(Committable committable) {
        Preconditions.checkNotNull(committable, "Committable cannot be null");

        try {
            byte[] serialized = serializer.serialize(committable);
            int dataLength = serialized.length;

            int requiredSpace = LENGTH_FIELD_SIZE + dataLength;

            if (currentBuffer.remaining() < requiredSpace) {
                currentBuffer.flip();
                allocateNewBuffer(Math.max(DEFAULT_BUFFER_SIZE, requiredSpace));
            }

            currentBuffer.putInt(dataLength);
            currentBuffer.put(serialized);

            totalCommittables++;

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize committable to off-heap buffer", e);
        }
    }

    public void sendToCoordinator(long checkpointId) {
        if (checkpointId == END_INPUT_CHECKPOINT_ID) {
            this.endInput = true;
        }
        sendFileInfo(checkpointId);
    }

    private void sendFileInfo(long checkpointId) {
        Preconditions.checkState(!buffers.isEmpty(), "No buffers to send");
        try {
            if (currentBuffer.position() > 0) {
                currentBuffer.flip();
            }

            byte[] finalData = null;
            if (totalCommittables != 0) {
                finalData = assembleFinalData();
            }
            FileInfoEvent event = new FileInfoEvent(finalData, checkpointId);

            SerializedValue<OperatorEvent> serializedRequest = new SerializedValue<>(event);
            gateway.sendOperatorEventToCoordinator(operatorID, serializedRequest);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to send FileInfo for checkpoint "
                            + checkpointId
                            + ": "
                            + e.getMessage(),
                    e);
        } finally {
            clear();
        }
    }

    public void snapshot(long checkpointId) {
        try {
            FileInfoConfirmRequest request =
                    new FileInfoConfirmRequest(endInput ? END_INPUT_CHECKPOINT_ID : checkpointId);
            SerializedValue<CoordinationRequest> serializedRequest = new SerializedValue<>(request);
            gateway.sendRequestToCoordinator(operatorID, serializedRequest).get();
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(
                    "Failed to send snapshot for checkpoint "
                            + checkpointId
                            + ": "
                            + e.getMessage(),
                    e);
        } finally {
            clear();
        }
    }

    private byte[] assembleFinalData() {
        int actualCount = 0;
        int actualTotalBytes = 0;

        List<ByteBuffer> readBuffers = new ArrayList<>();

        for (ByteBuffer buffer : buffers) {
            if (buffer.position() > 0 && !buffer.isReadOnly()) {
                buffer.flip();
            }

            ByteBuffer tempBuffer = buffer.duplicate();

            while (tempBuffer.hasRemaining()) {
                if (tempBuffer.remaining() < 4) {
                    break;
                }
                int length = tempBuffer.getInt();
                if (length < 0 || length > tempBuffer.remaining()) {
                    break;
                }

                tempBuffer.position(tempBuffer.position() + length);

                actualCount++;
                actualTotalBytes += length;
            }

            readBuffers.add(buffer);
        }

        int totalSize = COUNT_FIELD_SIZE + actualTotalBytes + (actualCount * LENGTH_FIELD_SIZE);
        byte[] result = new byte[totalSize];
        ByteBuffer resultBuffer = ByteBuffer.wrap(result);

        resultBuffer.putInt(actualCount);

        for (ByteBuffer buffer : readBuffers) {
            buffer.rewind();

            while (buffer.hasRemaining()) {
                if (buffer.remaining() < 4) {
                    break;
                }
                int length = buffer.getInt();
                if (length < 0 || length > buffer.remaining()) {
                    break;
                }

                byte[] data = new byte[length];
                buffer.get(data);

                resultBuffer.putInt(length);
                resultBuffer.put(data);
            }
        }
        totalCommittables = 0;

        return result;
    }

    private void clear() {
        buffers.clear();
        totalCommittables = 0;
        allocateNewBuffer(DEFAULT_BUFFER_SIZE);
    }

    public static List<Committable> deserializeCommittables(byte[] data) throws IOException {
        if (data == null) {
            return new ArrayList<>();
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);

        if (buffer.remaining() < 4) {
            throw new IOException("Invalid data: too short");
        }

        int count = buffer.getInt();
        List<Committable> result = new ArrayList<>(count);

        CommittableSerializer serializer = new CommittableSerializer(new CommitMessageSerializer());
        int version = serializer.getVersion();

        for (int i = 0; i < count; i++) {
            if (buffer.remaining() < 4) {
                throw new IOException("Invalid data: missing length field");
            }
            int length = buffer.getInt();
            if (length < 0 || length > buffer.remaining()) {
                throw new IOException("Invalid data: length field corrupted");
            }
            byte[] committableBytes = new byte[length];
            buffer.get(committableBytes);

            Committable committable = serializer.deserialize(version, committableBytes);
            result.add(committable);
        }

        return result;
    }

    public void sendWatermark(long watermark) {
        try {
            gateway.sendOperatorEventToCoordinator(
                    operatorID, new SerializedValue<>(new WatermarkEvent(watermark)));
        } catch (IOException e) {
            throw new RuntimeException("Fail to send watermark " + watermark, e);
        }
    }
}
