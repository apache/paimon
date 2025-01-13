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
 * Default implementation for {@link StoreSinkWriteState}.
 *
 * <p>States are positioned first by table name and then by key name. This class should be initiated
 * in a sink operator and then given to {@link StoreSinkWrite}.
 */
public class StoreSinkWriteStateImpl implements StoreSinkWriteState {

    private final StoreSinkWriteState.StateValueFilter stateValueFilter;

    private final ListState<Tuple5<String, String, byte[], Integer, byte[]>> listState;
    private final Map<String, Map<String, List<StoreSinkWriteState.StateValue>>> map;

    @SuppressWarnings("unchecked")
    public StoreSinkWriteStateImpl(
            StateInitializationContext context,
            StoreSinkWriteState.StateValueFilter stateValueFilter)
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
                        .add(new StoreSinkWriteState.StateValue(partition, tuple.f3, tuple.f4));
            }
        }
    }

    @Override
    public StoreSinkWriteState.StateValueFilter stateValueFilter() {
        return stateValueFilter;
    }

    @Override
    public @Nullable List<StoreSinkWriteState.StateValue> get(String tableName, String key) {
        Map<String, List<StoreSinkWriteState.StateValue>> innerMap = map.get(tableName);
        return innerMap == null ? null : innerMap.get(key);
    }

    public void put(
            String tableName, String key, List<StoreSinkWriteState.StateValue> stateValues) {
        map.computeIfAbsent(tableName, k -> new HashMap<>()).put(key, stateValues);
    }

    public void snapshotState() throws Exception {
        List<Tuple5<String, String, byte[], Integer, byte[]>> list = new ArrayList<>();
        for (Map.Entry<String, Map<String, List<StoreSinkWriteState.StateValue>>> tables :
                map.entrySet()) {
            for (Map.Entry<String, List<StoreSinkWriteState.StateValue>> entry :
                    tables.getValue().entrySet()) {
                for (StoreSinkWriteState.StateValue stateValue : entry.getValue()) {
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
}
