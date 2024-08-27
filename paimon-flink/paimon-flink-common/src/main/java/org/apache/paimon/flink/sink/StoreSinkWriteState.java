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

import java.util.Collections;
import java.util.List;

/**
 * States for {@link StoreSinkWrite}s.
 *
 * <p>States are positioned first by table name and then by key name. This class should be initiated
 * in a sink operator and then given to {@link StoreSinkWrite}.
 */
public class StoreSinkWriteState {

    protected final StateValueFilter stateValueFilter;

    @SuppressWarnings("unchecked")
    public StoreSinkWriteState(StateValueFilter stateValueFilter) throws Exception {
        this.stateValueFilter = stateValueFilter;
    }

    public StateValueFilter stateValueFilter() {
        return stateValueFilter;
    }

    List<StateValue> get(String tableName, String key) {
        return Collections.emptyList();
    }

    void put(String tableName, String key, List<StoreSinkWriteState.StateValue> stateValues) {}

    void snapshotState() throws Exception {}

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
}
