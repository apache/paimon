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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.io.IOException;
import java.util.List;

/**
 * Operator event sent from a writer subtask to {@link WriteOperatorCoordinator}, carrying the
 * committables produced for one checkpoint.
 *
 * <p>{@link Committable} is not directly serializable, so the payload is pre-serialized to {@code
 * byte[]} and decoded on the coordinator side.
 *
 * <p>{@code isRestoring=true} marks events emitted from {@code initializeState} after a global
 * failover, so the coordinator can distinguish replay traffic from normal in-flight checkpoints.
 */
public class CommittableEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final long checkpointId;
    private final boolean isRestoring;
    private final byte[] serialized;

    private CommittableEvent(long checkpointId, boolean isRestoring, byte[] serialized) {
        this.checkpointId = checkpointId;
        this.isRestoring = isRestoring;
        this.serialized = serialized;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public boolean isRestoring() {
        return isRestoring;
    }

    public byte[] getSerialized() {
        return serialized;
    }

    @Override
    public String toString() {
        return String.format(
                "CommittableEvent{checkpointId=%d, isRestoring=%b, serializedSize=%d}",
                checkpointId, isRestoring, serialized.length);
    }

    public static CommittableEvent create(
            long checkpointId,
            boolean isRestoring,
            List<Committable> committables,
            ListSerializer<Committable> listSerializer)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        listSerializer.serialize(committables, out);
        return new CommittableEvent(checkpointId, isRestoring, out.getCopyOfBuffer());
    }

    public static ListSerializer<Committable> createCommittableListSerializer() {
        return new ListSerializer<>(createCommittableSerializer());
    }

    private static TypeSerializer<Committable> createCommittableSerializer() {
        return new SimpleVersionedSerializerTypeSerializerProxy<>(
                () -> new CommittableSerializer(new CommitMessageSerializer()));
    }
}
