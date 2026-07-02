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

import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkConnectorOptions.CompactionBucketDistributionStrategy;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;

import java.util.Optional;

import static org.apache.paimon.flink.sink.FlinkSink.createCommitUser;
import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/** Builder for {@link CompactorSink}. */
public class CompactorSinkBuilder {

    private final FileStoreTable table;

    private DataStream<RowData> input;

    private final boolean fullCompaction;

    private CompactionBucketDistributionStrategy bucketDistributionStrategy =
            CompactionBucketDistributionStrategy.LINEAR;

    public CompactorSinkBuilder(FileStoreTable table, boolean fullCompaction) {
        this.table = table;
        this.fullCompaction = fullCompaction;
    }

    public CompactorSinkBuilder withInput(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    public CompactorSinkBuilder withBucketDistributionStrategy(
            CompactionBucketDistributionStrategy bucketDistributionStrategy) {
        this.bucketDistributionStrategy = bucketDistributionStrategy;
        return this;
    }

    public DataStreamSink<?> build() {
        BucketMode bucketMode = table.bucketMode();
        switch (bucketMode) {
            case HASH_FIXED:
            case HASH_DYNAMIC:
                return buildForBucketAware();
            default:
                throw new UnsupportedOperationException("Unsupported bucket mode: " + bucketMode);
        }
    }

    private DataStreamSink<?> buildForBucketAware() {
        Integer parallelism =
                Optional.ofNullable(
                                table.options().get(FlinkConnectorOptions.SINK_PARALLELISM.key()))
                        .map(Integer::valueOf)
                        .orElse(null);
        switch (bucketDistributionStrategy) {
            case SIZE_AWARE_BATCH:
                CompactorSink sink = new CompactorSink(table, fullCompaction);
                String commitUser = createCommitUser(table.coreOptions().toConfiguration());
                DataStream<Committable> written = sink.doWrite(input, commitUser, null);
                return sink.doCommit(
                        ((SingleOutputStreamOperator<Committable>) written).startNewChain(),
                        commitUser);
            case LINEAR:
            default:
                DataStream<RowData> partitioned =
                        partition(input, new BucketsRowChannelComputer(), parallelism);
                return new CompactorSink(table, fullCompaction).sinkFrom(partitioned);
        }
    }
}
