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

package org.apache.paimon.flink.sink.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link StateStore} that lives entirely in memory. Designed to be used inside an {@link
 * org.apache.flink.runtime.operators.coordination.OperatorCoordinator}, where state is serialized
 * to bytes during {@code checkpointCoordinator} and restored from bytes during {@code
 * resetToCheckpoint}.
 */
public class MemoryBackendStateStore implements StateStore {

    private final Map<String, MemoryListState<?>> registeredStates = new HashMap<>();
    private final Map<String, byte[]> serializedStates;

    public MemoryBackendStateStore() {
        this(Collections.emptyMap());
    }

    public MemoryBackendStateStore(Map<String, byte[]> restoredStates) {
        this.serializedStates = new HashMap<>(restoredStates);
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> descriptor) throws Exception {
        String stateName = descriptor.getName();
        MemoryListState<?> existing = registeredStates.get(stateName);
        if (existing != null) {
            //noinspection unchecked
            return (ListState<T>) existing;
        }
        byte[] restored = serializedStates.get(stateName);
        MemoryListState<T> state =
                new MemoryListState<>(descriptor.getElementSerializer(), restored);
        registeredStates.put(stateName, state);
        return state;
    }

    /**
     * Serializes all currently-registered states to bytes. Restored-but-unread state entries are
     * kept verbatim so that they survive coordinator failover even if no consumer registered a
     * matching descriptor in this lifecycle.
     */
    public Map<String, byte[]> getSerializedStates() throws IOException {
        for (Map.Entry<String, MemoryListState<?>> entry : registeredStates.entrySet()) {
            serializedStates.put(entry.getKey(), entry.getValue().serialize());
        }
        return serializedStates;
    }

    private static class MemoryListState<T> implements ListState<T> {

        private final ListSerializer<T> serializer;
        private final List<T> values;

        MemoryListState(TypeSerializer<T> elementSerializer, @Nullable byte[] restored)
                throws IOException {
            this.serializer = new ListSerializer<>(elementSerializer);
            if (restored != null) {
                DataInputDeserializer in = new DataInputDeserializer(restored);
                this.values = serializer.deserialize(in);
            } else {
                this.values = new ArrayList<>();
            }
        }

        @Override
        public Iterable<T> get() {
            return values;
        }

        @Override
        public void add(T value) {
            values.add(value);
        }

        @Override
        public void update(List<T> newValues) {
            values.clear();
            values.addAll(newValues);
        }

        @Override
        public void addAll(List<T> toAdd) {
            values.addAll(toAdd);
        }

        @Override
        public void clear() {
            values.clear();
        }

        byte[] serialize() throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            serializer.serialize(values, out);
            return out.getCopyOfBuffer();
        }
    }
}
