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
import org.apache.flink.table.store.connector.FlinkRowWrapper;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.table.sink.BucketComputer;
import org.apache.flink.table.store.table.sink.PartitionComputer;

import java.util.Objects;
import java.util.function.Function;

/** A {@link StreamPartitioner} to partition records by bucket. */
public class BucketStreamPartitioner extends StreamPartitioner<RowData> {

    private final TableSchema schema;
    private final boolean shuffleByPartitionEnable;

    private transient Function<InternalRow, Integer> partitioner;

    public BucketStreamPartitioner(TableSchema schema, boolean shuffleByPartitionEnable) {
        this.schema = schema;
        this.shuffleByPartitionEnable = shuffleByPartitionEnable;
    }

    @Override
    public void setup(int numberOfChannels) {
        super.setup(numberOfChannels);
        BucketComputer bucketComputer = new BucketComputer(schema);
        if (shuffleByPartitionEnable) {
            PartitionComputer partitionComputer = new PartitionComputer(schema);
            partitioner =
                    row ->
                            Math.abs(
                                            Objects.hash(
                                                    bucketComputer.bucket(row),
                                                    partitionComputer.partition(row)))
                                    % numberOfChannels;
        } else {
            partitioner = row -> bucketComputer.bucket(row) % numberOfChannels;
        }
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<RowData>> record) {
        return partitioner.apply(new FlinkRowWrapper(record.getInstance().getValue()));
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
        return shuffleByPartitionEnable ? "bucket-partition-assigner" : "bucket-assigner";
    }
}
