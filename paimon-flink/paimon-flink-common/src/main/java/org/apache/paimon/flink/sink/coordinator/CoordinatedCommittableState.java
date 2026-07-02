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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StateInitializationContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Writer-side state for committables which have not been committed by PWC. */
public class CoordinatedCommittableState {

    private static final String STATE_NAME = "pwc_pending_committables";

    private ListState<byte[]> state;
    private CheckpointCommittablesSerializer serializer;
    private final NavigableMap<Long, List<Committable>> pendingCommittables = new TreeMap<>();
    private final Set<Long> acknowledgedCheckpoints = new TreeSet<>();

    public void initialize(StateInitializationContext context) throws Exception {
        CommittableSerializer committableSerializer =
                new CommittableSerializer(new CommitMessageSerializer());
        serializer = new CheckpointCommittablesSerializer(committableSerializer);
        state =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        STATE_NAME, BytePrimitiveArraySerializer.INSTANCE));
        for (byte[] bytes : state.get()) {
            CheckpointCommittables checkpoint = serializer.deserialize(bytes);
            pendingCommittables
                    .computeIfAbsent(checkpoint.checkpointId(), ignored -> new ArrayList<>())
                    .addAll(checkpoint.committables());
        }
    }

    public void add(Committable committable) {
        pendingCommittables
                .computeIfAbsent(committable.checkpointId(), ignored -> new ArrayList<>())
                .add(committable);
    }

    public void snapshot(long checkpointId) throws Exception {
        pendingCommittables.computeIfAbsent(checkpointId, ignored -> new ArrayList<>());
        List<byte[]> checkpoints = new ArrayList<>();
        for (Map.Entry<Long, List<Committable>> entry : pendingCommittables.entrySet()) {
            checkpoints.add(
                    serializer.serialize(
                            new CheckpointCommittables(
                                    entry.getKey(), new ArrayList<>(entry.getValue()))));
        }
        state.update(checkpoints);
    }

    public Map<Long, List<Committable>> pendingCommittables() {
        Map<Long, List<Committable>> result = new TreeMap<>();
        for (Map.Entry<Long, List<Committable>> entry : pendingCommittables.entrySet()) {
            result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        return result;
    }

    public List<Committable> committables() {
        List<Committable> result = new ArrayList<>();
        for (List<Committable> committables : pendingCommittables.values()) {
            result.addAll(committables);
        }
        return result;
    }

    public List<Committable> unacknowledgedCommittables() {
        List<Committable> result = new ArrayList<>();
        for (Map.Entry<Long, List<Committable>> entry : pendingCommittables.entrySet()) {
            if (!acknowledgedCheckpoints.contains(entry.getKey())) {
                result.addAll(entry.getValue());
            }
        }
        return result;
    }

    public void markAcknowledged(List<Committable> committables) {
        for (Committable committable : committables) {
            acknowledgedCheckpoints.add(committable.checkpointId());
        }
    }

    public void markCommittedUpTo(long checkpointId) {
        pendingCommittables.headMap(checkpointId, true).clear();
        acknowledgedCheckpoints.removeIf(id -> id <= checkpointId);
    }

    public void clear() throws Exception {
        pendingCommittables.clear();
        acknowledgedCheckpoints.clear();
        if (state != null) {
            state.clear();
        }
    }

    private static class CheckpointCommittables {

        private final long checkpointId;
        private final List<Committable> committables;

        private CheckpointCommittables(long checkpointId, List<Committable> committables) {
            this.checkpointId = checkpointId;
            this.committables = committables;
        }

        private long checkpointId() {
            return checkpointId;
        }

        private List<Committable> committables() {
            return committables;
        }
    }

    /** Serializer for checkpoint committables. */
    private static class CheckpointCommittablesSerializer {

        private final CommittableSerializer committableSerializer;

        private CheckpointCommittablesSerializer(CommittableSerializer committableSerializer) {
            this.committableSerializer = committableSerializer;
        }

        private byte[] serialize(CheckpointCommittables checkpoint) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
            view.writeLong(checkpoint.checkpointId());
            view.writeInt(checkpoint.committables().size());
            for (Committable committable : checkpoint.committables()) {
                byte[] bytes = committableSerializer.serialize(committable);
                view.writeInt(bytes.length);
                view.write(bytes);
            }
            return out.toByteArray();
        }

        private CheckpointCommittables deserialize(byte[] serialized) throws IOException {
            DataInputDeserializer view = new DataInputDeserializer(serialized);
            long checkpointId = view.readLong();
            int count = view.readInt();
            if (count < 0) {
                throw new IOException("Negative committable count: " + count);
            }

            List<Committable> committables = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int length = view.readInt();
                if (length < 0) {
                    throw new IOException("Negative committable length: " + length);
                }
                byte[] bytes = new byte[length];
                view.readFully(bytes);
                Committable committable =
                        committableSerializer.deserialize(
                                committableSerializer.getVersion(), bytes);
                if (committable.checkpointId() != checkpointId) {
                    throw new IOException(
                            String.format(
                                    "Committable checkpoint %s does not match state checkpoint %s.",
                                    committable.checkpointId(), checkpointId));
                }
                committables.add(committable);
            }
            return new CheckpointCommittables(checkpointId, committables);
        }
    }
}
