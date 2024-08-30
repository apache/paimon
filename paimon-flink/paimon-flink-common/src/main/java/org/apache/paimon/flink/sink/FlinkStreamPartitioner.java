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

import org.apache.paimon.table.sink.ChannelComputer;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** A {@link StreamPartitioner} which wraps a {@link ChannelComputer}. */
public class FlinkStreamPartitioner<T> extends StreamPartitioner<T> {

    private final ChannelComputer<T> channelComputer;

    public FlinkStreamPartitioner(ChannelComputer<T> channelComputer) {
        this.channelComputer = channelComputer;
    }

    @Override
    public void setup(int numberOfChannels) {
        super.setup(numberOfChannels);
        channelComputer.setup(numberOfChannels);
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        return channelComputer.channel(record.getInstance().getValue());
    }

    @Override
    public StreamPartitioner<T> copy() {
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
        return channelComputer.toString();
    }

    public static <T> DataStream<T> partition(
            DataStream<T> input, ChannelComputer<T> channelComputer, Integer parallelism) {
        FlinkStreamPartitioner<T> partitioner = new FlinkStreamPartitioner<>(channelComputer);
        PartitionTransformation<T> partitioned =
                new PartitionTransformation<>(input.getTransformation(), partitioner);
        if (parallelism != null) {
            partitioned.setParallelism(parallelism, true);
        }
        return new DataStream<>(input.getExecutionEnvironment(), partitioned);
    }
}
