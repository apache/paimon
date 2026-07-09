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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.io.IOException;

/**
 * Operator event sent from a writer subtask to {@link CommittingWriteOperatorCoordinator} at each
 * barrier, carrying the {@link CheckpointCommittables} produced for that single checkpoint.
 *
 * <p>Payload is pre-serialized to bytes on the writer side. The wrapping {@link
 * org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy} prepends the payload
 * version to the byte stream, so the version does not need to be a field of this event.
 */
public class CommittableEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final long checkpointId;
    private final byte[] serialized;

    private CommittableEvent(long checkpointId, byte[] serialized) {
        this.checkpointId = checkpointId;
        this.serialized = serialized;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public byte[] getSerialized() {
        return serialized;
    }

    public CheckpointCommittables deserialize(TypeSerializer<CheckpointCommittables> serializer)
            throws IOException {
        return serializer.deserialize(new DataInputDeserializer(serialized));
    }

    @Override
    public String toString() {
        return String.format(
                "CommittableEvent{checkpointId=%d, serializedSize=%d}",
                checkpointId, serialized.length);
    }

    public static CommittableEvent create(
            long checkpointId,
            CheckpointCommittables entry,
            TypeSerializer<CheckpointCommittables> serializer)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        serializer.serialize(entry, out);
        return new CommittableEvent(checkpointId, out.getCopyOfBuffer());
    }
}
