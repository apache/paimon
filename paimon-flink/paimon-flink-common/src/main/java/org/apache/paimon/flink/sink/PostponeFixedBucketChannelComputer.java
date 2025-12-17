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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;

import java.util.Map;

/**
 * {@link ChannelComputer} for writing {@link InternalRow}s into the fixed bucket of postpone-bucket
 * tables.
 */
public class PostponeFixedBucketChannelComputer implements ChannelComputer<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final TableSchema schema;
    private final Map<BinaryRow, Integer> knownNumBuckets;

    private transient int numChannels;
    private transient RowPartitionKeyExtractor partitionKeyExtractor;

    public PostponeFixedBucketChannelComputer(
            TableSchema schema, Map<BinaryRow, Integer> knownNumBuckets) {
        this.schema = schema;
        this.knownNumBuckets = knownNumBuckets;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.partitionKeyExtractor = new RowPartitionKeyExtractor(schema);
    }

    @Override
    public int channel(InternalRow record) {
        BinaryRow partition = partitionKeyExtractor.partition(record);
        int bucket = knownNumBuckets.computeIfAbsent(partition, p -> numChannels);
        return ChannelComputer.select(partition, bucket, numChannels);
    }

    @Override
    public String toString() {
        return "shuffle by bucket";
    }
}
