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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;

/** Builder for {@link FileStoreSink}. */
public class FlinkSinkBuilder {

    private final FileStoreTable table;

    private DataStream<RowData> input;
    private Lock.Factory lockFactory = Lock.emptyFactory();
    @Nullable private Map<String, String> overwritePartition;
    @Nullable private LogSinkFunction logSinkFunction;
    @Nullable private Integer parallelism;
    @Nullable private String commitUser;
    @Nullable private StoreSinkWrite.Provider sinkProvider;

    public FlinkSinkBuilder(FileStoreTable table) {
        this.table = table;
    }

    public FlinkSinkBuilder withInput(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    public FlinkSinkBuilder withLockFactory(Lock.Factory lockFactory) {
        this.lockFactory = lockFactory;
        return this;
    }

    public FlinkSinkBuilder withOverwritePartition(Map<String, String> overwritePartition) {
        this.overwritePartition = overwritePartition;
        return this;
    }

    public FlinkSinkBuilder withLogSinkFunction(@Nullable LogSinkFunction logSinkFunction) {
        this.logSinkFunction = logSinkFunction;
        return this;
    }

    public FlinkSinkBuilder withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    @VisibleForTesting
    public FlinkSinkBuilder withSinkProvider(
            String commitUser, StoreSinkWrite.Provider sinkProvider) {
        this.commitUser = commitUser;
        this.sinkProvider = sinkProvider;
        return this;
    }

    public DataStreamSink<?> build() {
        BucketStreamPartitioner partitioner =
                new BucketStreamPartitioner(
                        table.schema(),
                        table.coreOptions()
                                .toConfiguration()
                                .get(FlinkConnectorOptions.SINK_SHUFFLE_BY_PARTITION));
        PartitionTransformation<RowData> partitioned =
                new PartitionTransformation<>(input.getTransformation(), partitioner);
        if (parallelism != null) {
            partitioned.setParallelism(parallelism);
        }

        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        FileStoreSink sink =
                new FileStoreSink(table, lockFactory, overwritePartition, logSinkFunction);
        return commitUser != null && sinkProvider != null
                ? sink.sinkFrom(new DataStream<>(env, partitioned), commitUser, sinkProvider)
                : sink.sinkFrom(new DataStream<>(env, partitioned));
    }
}
