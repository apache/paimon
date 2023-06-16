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

import java.util.Map;

/**
 * Build topology for unaware bucket table sink.
 *
 * <p>Note: in unaware-bucket mode, we don't shuffle by bucket in inserting. We can assign
 * compaction to the inserting jobs aside.
 */
public class UnawareBucketWriteSink extends FileStoreSink {

    private final boolean enableCompaction;
    private final AppendOnlyFileStoreTable table;
    private final Integer parallelism;

    public UnawareBucketWriteSink(
            AppendOnlyFileStoreTable table,
            Lock.Factory lock,
            Map<String, String> overwritePartitions,
            LogSinkFunction logSinkFunction,
            Integer parallelism) {
        super(table, lock, overwritePartitions, logSinkFunction);
        this.table = table;
        this.enableCompaction = !table.coreOptions().writeOnly();
        this.parallelism = parallelism;
    }

    @Override
    public DataStreamSink<?> sinkFrom(DataStream<RowData> input, String initialCommitUser) {
        // partition input to meet parallelism of 'sink.parallelism'
        DataStream<RowData> partitioned = partition(input, parallelism);

        // do the actually writing action, no snapshot generated in this stage
        DataStream<Committable> written = doWrite(partitioned, initialCommitUser);

        // if enable compaction, we need to add compaction topology to this job
        if (enableCompaction) {
            boolean isStreamingMode =
                    input.getExecutionEnvironment()
                                    .getConfiguration()
                                    .get(ExecutionOptions.RUNTIME_MODE)
                            == RuntimeExecutionMode.STREAMING;

            UnawareBucketCompactionTopoBuilder builder =
                    new UnawareBucketCompactionTopoBuilder(
                            input.getExecutionEnvironment(), table.name(), table);
            builder.withContinuousMode(isStreamingMode);
            written = written.union(builder.fetchUncommitted(initialCommitUser));
        }

        // commit the committable to generate a new snapshot
        return doCommit(written, initialCommitUser);
    }

    private DataStream<RowData> partition(DataStream<RowData> input, Integer parallelism) {
        // we set partitioner equals null, if the parallelism we set equals to input, we could chain
        // sink operator and input in one stage
        PartitionTransformation<RowData> partitionTransformation =
                new PartitionTransformation<>(input.getTransformation(), null);
        if (parallelism != null) {
            partitionTransformation.setParallelism(parallelism);
        }
        return new DataStream<>(input.getExecutionEnvironment(), partitionTransformation);
    }
}
