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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.utils.OffsetRowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * {@link StreamPartitioner} to partition {@link RowData} according to its hash value from an {@link
 * OffsetRowData}.
 */
public class OffsetRowDataHashStreamPartitioner extends StreamPartitioner<RowData> {

    private final RowType offsetRowType;
    private final int offset;

    private transient OffsetRowData offsetRowData;
    private transient RowDataSerializer serializer;

    public OffsetRowDataHashStreamPartitioner(RowType offsetRowType, int offset) {
        this.offsetRowType = offsetRowType;
        this.offset = offset;
    }

    @Override
    public void setup(int numberOfChannels) {
        super.setup(numberOfChannels);
        this.offsetRowData = new OffsetRowData(offsetRowType.getFieldCount(), offset);
        serializer = new RowDataSerializer(offsetRowType);
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<RowData>> record) {
        RowData rowData = record.getInstance().getValue();
        int hash = serializer.toBinaryRow(offsetRowData.replace(rowData)).hashCode();
        return Math.abs(hash) % numberOfChannels;
    }

    @Override
    public StreamPartitioner<RowData> copy() {
        return this;
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.FULL;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "compactor-stream-partitioner";
    }
}
