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
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.system.ModifiedPartitionBucketTable;

import org.apache.flink.table.data.RowData;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * {@link ChannelComputer} to partition {@link RowData} from {@link ModifiedPartitionBucketTable}.
 */
public class BucketsRowChannelComputer implements ChannelComputer<RowData> {

    private transient int numberOfChannels;

    @Override
    public void setup(int numberOfChannels) {
        this.numberOfChannels = numberOfChannels;
    }

    @Override
    public int channel(RowData rowData) {
        BinaryRow partition = deserializeBinaryRow(rowData.getBinary(1));
        int bucket = rowData.getInt(2);
        return ChannelComputer.select(partition, bucket, numberOfChannels);
    }

    @Override
    public String toString() {
        return "compactor-partitioner";
    }
}
