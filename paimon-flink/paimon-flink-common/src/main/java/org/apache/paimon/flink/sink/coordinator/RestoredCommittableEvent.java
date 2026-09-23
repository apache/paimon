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
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.io.IOException;
import java.util.List;

/**
 * Operator event sent exactly once per writer subtask from {@code initializeState} after a restore,
 * carrying the full set of {@link CheckpointCommittables} the subtask had persisted but not yet
 * acknowledged by the coordinator. The {@link #restoredCheckpointId} is the checkpoint the writer
 * restored from and drives coordinator-side restore alignment.
 *
 * <p>Kept as a distinct type from {@link CommittableEvent} so that the "one event per subtask"
 * restore contract is enforced at the type level and cannot be conflated with steady-state
 * per-checkpoint traffic.
 */
public class RestoredCommittableEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final long restoredCheckpointId;
    private final byte[] serialized;

    private RestoredCommittableEvent(long restoredCheckpointId, byte[] serialized) {
        this.restoredCheckpointId = restoredCheckpointId;
        this.serialized = serialized;
    }

    public long getRestoredCheckpointId() {
        return restoredCheckpointId;
    }

    public byte[] getSerialized() {
        return serialized;
    }

    public List<CheckpointCommittables> deserialize(
            TypeSerializer<CheckpointCommittables> serializer) throws IOException {
        return new ListSerializer<>(serializer).deserialize(new DataInputDeserializer(serialized));
    }

    @Override
    public String toString() {
        return String.format(
                "RestoredCommittableEvent{restoredCheckpointId=%d, serializedSize=%d}",
                restoredCheckpointId, serialized.length);
    }

    public static RestoredCommittableEvent create(
            long restoredCheckpointId,
            List<CheckpointCommittables> entries,
            TypeSerializer<CheckpointCommittables> serializer)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        new ListSerializer<>(serializer).serialize(entries, out);
        return new RestoredCommittableEvent(restoredCheckpointId, out.getCopyOfBuffer());
    }
}
