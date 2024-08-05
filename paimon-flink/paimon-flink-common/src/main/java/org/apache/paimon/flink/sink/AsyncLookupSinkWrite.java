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
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link StoreSinkWrite} for tables with lookup changelog producer and {@link
 * org.apache.paimon.CoreOptions#LOOKUP_WAIT} set to false.
 */
public class AsyncLookupSinkWrite extends StoreSinkWriteImpl {

    private static final String ACTIVE_BUCKETS_STATE_NAME = "paimon_async_lookup_active_buckets";

    private final String tableName;

    public AsyncLookupSinkWrite(
            FileStoreTable table,
            String commitUser,
            StoreSinkWriteState state,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreaming,
            @Nullable MemorySegmentPool memoryPool,
            MetricGroup metricGroup) {
        super(
                table,
                commitUser,
                state,
                ioManager,
                ignorePreviousFiles,
                waitCompaction,
                isStreaming,
                memoryPool,
                metricGroup);

        this.tableName = table.name();

        List<StoreSinkWriteState.StateValue> activeBucketsStateValues =
                state.get(tableName, ACTIVE_BUCKETS_STATE_NAME);
        if (activeBucketsStateValues != null) {
            for (StoreSinkWriteState.StateValue stateValue : activeBucketsStateValues) {
                try {
                    write.compact(stateValue.partition(), stateValue.bucket(), false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void snapshotState() throws Exception {
        super.snapshotState();

        List<StoreSinkWriteState.StateValue> activeBucketsList = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<Integer>> partitions :
                ((AbstractFileStoreWrite<?>) write.getWrite()).getActiveBuckets().entrySet()) {
            for (int bucket : partitions.getValue()) {
                activeBucketsList.add(
                        new StoreSinkWriteState.StateValue(
                                partitions.getKey(), bucket, new byte[0]));
            }
        }
        state.put(tableName, ACTIVE_BUCKETS_STATE_NAME, activeBucketsList);
    }
}
