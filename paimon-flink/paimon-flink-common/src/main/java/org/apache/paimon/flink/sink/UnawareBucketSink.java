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
import org.apache.paimon.flink.compact.UnawareBucketNewFilesCompactionCoordinatorOperator;
import org.apache.paimon.flink.compact.UnawareBucketNewFilesCompactionWorkerOperator;
import org.apache.paimon.flink.source.AppendBypassCoordinateOperatorFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Sink for unaware-bucket table.
 *
 * <p>Note: in unaware-bucket mode, we don't shuffle by bucket in inserting. We can assign
 * compaction to the inserting jobs aside.
 */
public abstract class UnawareBucketSink<T> extends FlinkWriteSink<T> {

    protected final FileStoreTable table;
    protected final LogSinkFunction logSinkFunction;

    @Nullable protected final Integer parallelism;

    public UnawareBucketSink(
            FileStoreTable table,
            @Nullable Map<String, String> overwritePartitions,
            LogSinkFunction logSinkFunction,
            @Nullable Integer parallelism) {
        super(table, overwritePartitions);
        this.table = table;
        this.logSinkFunction = logSinkFunction;
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<Committable> doWrite(
            DataStream<T> input, String initialCommitUser, @Nullable Integer parallelism) {
        DataStream<Committable> written = super.doWrite(input, initialCommitUser, this.parallelism);

        Options options = new Options(table.options());
        if (options.get(FlinkConnectorOptions.PRECOMMIT_COMPACT)) {
            written =
                    written.transform(
                                    "New Files Compact Coordinator: " + table.name(),
                                    new EitherTypeInfo<>(
                                            new CommittableTypeInfo(),
                                            new TupleTypeInfo<>(
                                                    BasicTypeInfo.LONG_TYPE_INFO,
                                                    new CompactionTaskTypeInfo())),
                                    new UnawareBucketNewFilesCompactionCoordinatorOperator(
                                            table.coreOptions()))
                            .startNewChain()
                            .forceNonParallel()
                            .transform(
                                    "New Files Compact Worker: " + table.name(),
                                    new CommittableTypeInfo(),
                                    new UnawareBucketNewFilesCompactionWorkerOperator(table))
                            .startNewChain()
                            .setParallelism(written.getParallelism());
        }

        boolean enableCompaction = !table.coreOptions().writeOnly();
        boolean isStreamingMode =
                input.getExecutionEnvironment()
                                .getConfiguration()
                                .get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;
        // if enable compaction, we need to add compaction topology to this job
        if (enableCompaction && isStreamingMode) {
            written =
                    written.transform(
                                    "Compact Coordinator: " + table.name(),
                                    new EitherTypeInfo<>(
                                            new CommittableTypeInfo(),
                                            new CompactionTaskTypeInfo()),
                                    new AppendBypassCoordinateOperatorFactory<>(table))
                            .startNewChain()
                            .forceNonParallel()
                            .transform(
                                    "Compact Worker: " + table.name(),
                                    new CommittableTypeInfo(),
                                    new AppendBypassCompactWorkerOperator.Factory(
                                            table, initialCommitUser))
                            .startNewChain()
                            .setParallelism(written.getParallelism());
        }

        return written;
    }
}
