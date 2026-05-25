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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.RowType;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/** {@link SimpleVersionedSerializer} for {@link UpsertRecord}. */
public class UpsertRecordSerializer implements SimpleVersionedSerializer<UpsertRecord> {

    private final InternalRowSerializer rowSerializer;
    private final int partitionArity;

    public UpsertRecordSerializer(RowType rowType, int partitionArity) {
        this.rowSerializer = new InternalRowSerializer(rowType);
        this.partitionArity = partitionArity;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(UpsertRecord record) throws IOException {
        BinaryRow partition = record.partition();
        byte[] partitionBytes = partition.getFieldCount() == 0 ? new byte[0] : partition.toBytes();

        BinaryRow binaryRow = rowSerializer.toBinaryRow(record.row());
        byte[] rowBytes = binaryRow.toBytes();

        ByteBuffer buffer =
                ByteBuffer.allocate(8 + 8 + 4 + partitionBytes.length + 4 + rowBytes.length);
        buffer.putLong(record.firstRowId());
        buffer.putLong(record.offset());
        buffer.putInt(partitionBytes.length);
        buffer.put(partitionBytes);
        buffer.putInt(rowBytes.length);
        buffer.put(rowBytes);
        return buffer.array();
    }

    @Override
    public UpsertRecord deserialize(int version, byte[] bytes) throws IOException {
        if (version != getVersion()) {
            throw new RuntimeException("Cannot deserialize version: " + version);
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long firstRowId = buffer.getLong();
        long offset = buffer.getLong();

        int partitionLen = buffer.getInt();
        BinaryRow partition;
        if (partitionLen == 0) {
            partition = BinaryRow.EMPTY_ROW;
        } else {
            byte[] partitionBytes = new byte[partitionLen];
            buffer.get(partitionBytes);
            partition = new BinaryRow(partitionArity);
            partition.pointTo(MemorySegment.wrap(partitionBytes), 0, partitionLen);
        }

        int rowLen = buffer.getInt();
        byte[] rowBytes = new byte[rowLen];
        buffer.get(rowBytes);
        BinaryRow row = new BinaryRow(rowSerializer.getArity());
        row.pointTo(MemorySegment.wrap(rowBytes), 0, rowLen);

        return new UpsertRecord(partition, firstRowId, offset, row);
    }
}
