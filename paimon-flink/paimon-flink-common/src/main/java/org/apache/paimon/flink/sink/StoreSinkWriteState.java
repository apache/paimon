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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * States for {@link StoreSinkWrite}s.
 *
 * <p>States are positioned first by table name and then by key name. This class should be initiated
 * in a sink operator and then given to {@link StoreSinkWrite}.
 */
public class StoreSinkWriteState {

    private final StateValueFilter stateValueFilter;

    private final ListState<Tuple5<String, String, byte[], Integer, byte[]>> listState;
    private final Map<String, Map<String, List<StateValue>>> map;

    @SuppressWarnings("unchecked")
    public StoreSinkWriteState(
            StateInitializationContext context, StateValueFilter stateValueFilter)
            throws Exception {
        this.stateValueFilter = stateValueFilter;
        TupleSerializer<Tuple5<String, String, byte[], Integer, byte[]>> listStateSerializer =
                new TupleSerializer<>(
                        (Class<Tuple5<String, String, byte[], Integer, byte[]>>)
                                (Class<?>) Tuple5.class,
                        new TypeSerializer[] {
                            StringSerializer.INSTANCE,
                            StringSerializer.INSTANCE,
                            BytePrimitiveArraySerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            BytePrimitiveArraySerializer.INSTANCE
                        });
        listState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        "paimon_store_sink_write_state", listStateSerializer));

        map = new HashMap<>();
        for (Tuple5<String, String, byte[], Integer, byte[]> tuple : listState.get()) {
            BinaryRow partition = SerializationUtils.deserializeBinaryRow(tuple.f2);
            if (stateValueFilter.filter(tuple.f0, partition, tuple.f3)) {
                map.computeIfAbsent(tuple.f0, k -> new HashMap<>())
                        .computeIfAbsent(tuple.f1, k -> new ArrayList<>())
                        .add(new StateValue(partition, tuple.f3, tuple.f4));
            }
        }
    }

    public StateValueFilter stateValueFilter() {
        return stateValueFilter;
    }

    public @Nullable List<StateValue> get(String tableName, String key) {
        Map<String, List<StateValue>> innerMap = map.get(tableName);
        return innerMap == null ? null : innerMap.get(key);
    }

    public void put(String tableName, String key, List<StateValue> stateValues) {
        map.computeIfAbsent(tableName, k -> new HashMap<>()).put(key, stateValues);
    }

    public void snapshotState() throws Exception {
        List<Tuple5<String, String, byte[], Integer, byte[]>> list = new ArrayList<>();
        for (Map.Entry<String, Map<String, List<StateValue>>> tables : map.entrySet()) {
            for (Map.Entry<String, List<StateValue>> entry : tables.getValue().entrySet()) {
                for (StateValue stateValue : entry.getValue()) {
                    list.add(
                            Tuple5.of(
                                    tables.getKey(),
                                    entry.getKey(),
                                    SerializationUtils.serializeBinaryRow(stateValue.partition()),
                                    stateValue.bucket(),
                                    stateValue.value()));
                }
            }
        }
        listState.update(list);
    }

    /**
     * A state value for {@link StoreSinkWrite}. All state values should be given a partition and a
     * bucket so that they can be redistributed once the sink parallelism is changed.
     */
    public static class StateValue {

        private final BinaryRow partition;
        private final int bucket;
        private final byte[] value;

        public StateValue(BinaryRow partition, int bucket, byte[] value) {
            this.partition = partition;
            this.bucket = bucket;
            this.value = value;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public byte[] value() {
            return value;
        }
    }

    /**
     * Given the table name, partition and bucket of a {@link StateValue} in a union list state,
     * decide whether to keep this {@link StateValue} in this subtask.
     */
    public interface StateValueFilter {

        boolean filter(String tableName, BinaryRow partition, int bucket);
    }
}
