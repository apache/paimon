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

import org.apache.paimon.flink.compact.UnawareBucketCompactionTopoBuilder;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.AppendOnlyFileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.UUID;

/**
 * Build topology for unaware bucket table sink.
 *
 * <p>Note: in unaware-bucket mode, we don't shuffle by bucket in inserting. We can assign
 * compaction to the inserting jobs aside.
 */
public class UnawareBucketWriteSink {

    private final AppendOnlyFileStoreTable table;
    private final Lock.Factory lockFactory;
    private final boolean enableCompaction;
    @Nullable private final Map<String, String> overwritePartitions;
    private final LogSinkFunction logSinkFunction;

    public UnawareBucketWriteSink(
            AppendOnlyFileStoreTable table,
            Lock.Factory lock,
            Map<String, String> overwritePartitions,
            LogSinkFunction logSinkFunction) {
        this.table = table;
        this.lockFactory = lock;
        this.enableCompaction = !table.coreOptions().writeOnly();
        this.overwritePartitions = overwritePartitions;
        this.logSinkFunction = logSinkFunction;
    }

    public DataStreamSink<?> build(DataStream<RowData> input, Integer parallelism) {
        String commitUser = UUID.randomUUID().toString();
        // we set partitioner equals null, if the parallelism we set equals to input, we could chain
        // sink operator and input in one stage
        PartitionTransformation<RowData> partitioned =
                new PartitionTransformation<>(input.getTransformation(), null);
        if (parallelism != null) {
            partitioned.setParallelism(parallelism);
        }
        FileStoreSink sink =
                new FileStoreSink(table, lockFactory, overwritePartitions, logSinkFunction, true);
        DataStream<Committable> written =
                sink.doWrite(
                        new DataStream<>(input.getExecutionEnvironment(), partitioned), commitUser);

        boolean isStreamingMode =
                input.getExecutionEnvironment()
                                .getConfiguration()
                                .get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        // if enable compaction, we need to add compaction topology to this job
        if (enableCompaction) {
            UnawareBucketCompactionTopoBuilder builder =
                    new UnawareBucketCompactionTopoBuilder(
                            input.getExecutionEnvironment(), table.name(), table);
            builder.withContinuousMode(isStreamingMode);
            written = written.union(builder.fetchUncommitted(commitUser));
        }

        return sink.doCommit(written, commitUser);
    }
}
