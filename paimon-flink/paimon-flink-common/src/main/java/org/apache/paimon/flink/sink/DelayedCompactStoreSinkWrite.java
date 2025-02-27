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
import org.apache.paimon.utils.RecordWriter;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** {@link StoreSinkWrite} for tables that need delayed compaction trigger. */
public class DelayedCompactStoreSinkWrite extends StoreSinkWriteImpl {

    private final String tableName;

    private static final String HAS_DELAYED_COMPACT_BUCKETS_STATE_NAME =
            "paimon_has_delayed_compact_buckets";

    public DelayedCompactStoreSinkWrite(
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
        List<StoreSinkWriteState.StateValue> lateCompactBucketsStateValues =
                state.get(tableName, HAS_DELAYED_COMPACT_BUCKETS_STATE_NAME);
        if (lateCompactBucketsStateValues != null) {
            for (StoreSinkWriteState.StateValue stateValue : lateCompactBucketsStateValues) {
                ((AbstractFileStoreWrite<?>) write.getWrite())
                        .getWriterWrapper(stateValue.partition(), stateValue.bucket());
                stateValue.bucket();
            }
        }
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void snapshotState() throws Exception {
        super.snapshotState();

        Map<BinaryRow, Map<Integer, AbstractFileStoreWrite.WriterContainer>> writerContainerMap =
                ((AbstractFileStoreWrite) write.getWrite()).writers();
        List<StoreSinkWriteState.StateValue> delayedCompactBucketList = new ArrayList<>();

        for (Map.Entry<BinaryRow, Map<Integer, AbstractFileStoreWrite.WriterContainer>>
                partitionEntry : writerContainerMap.entrySet()) {
            for (Map.Entry<Integer, AbstractFileStoreWrite.WriterContainer> bucketEntry :
                    partitionEntry.getValue().entrySet()) {
                RecordWriter writer = bucketEntry.getValue().writer;
                if (writer.hasDelayedCompact()) {
                    delayedCompactBucketList.add(
                            new StoreSinkWriteState.StateValue(
                                    partitionEntry.getKey(), bucketEntry.getKey(), new byte[0]));
                }
            }
        }

        state.put(tableName, HAS_DELAYED_COMPACT_BUCKETS_STATE_NAME, delayedCompactBucketList);
    }
}
