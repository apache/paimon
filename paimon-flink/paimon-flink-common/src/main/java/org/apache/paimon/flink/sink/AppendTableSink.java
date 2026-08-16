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
import org.apache.paimon.flink.compact.AppendPreCommitCompactCoordinatorOperator;
import org.apache.paimon.flink.compact.AppendPreCommitCompactWorkerOperator;
import org.apache.paimon.flink.source.AppendBypassCoordinateOperatorFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_MANAGED_WRITER_BUFFER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.declareManagedMemory;
import static org.apache.paimon.flink.utils.ParallelismUtils.forwardParallelism;
import static org.apache.paimon.flink.utils.ParallelismUtils.setParallelism;

/**
 * Sink for unaware-bucket table.
 *
 * <p>Note: in unaware-bucket mode, we don't shuffle by bucket in inserting. We can assign
 * compaction to the inserting jobs aside.
 */
public abstract class AppendTableSink<T> extends FlinkWriteSink<T> {

    private static final long serialVersionUID = 1L;

    protected final FileStoreTable table;

    @Nullable protected final Integer parallelism;

    public AppendTableSink(
            FileStoreTable table,
            @Nullable Map<String, String> overwritePartitions,
            @Nullable Integer parallelism) {
        super(table, overwritePartitions);
        this.table = table;
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<Committable> doWrite(
            DataStream<T> input, String initialCommitUser, @Nullable Integer parallelism) {
        DataStream<Committable> written = super.doWrite(input, initialCommitUser, this.parallelism);

        Options options = new Options(table.options());
        if (options.get(FlinkConnectorOptions.PRECOMMIT_COMPACT)) {
            SingleOutputStreamOperator<Committable> newWritten =
                    written.transform(
                                    "New Files Compact Coordinator: " + table.name(),
                                    new EitherTypeInfo<>(
                                            new CommittableTypeInfo(),
                                            new TupleTypeInfo<>(
                                                    BasicTypeInfo.LONG_TYPE_INFO,
                                                    new CompactionTaskTypeInfo())),
                                    new AppendPreCommitCompactCoordinatorOperator(
                                            table.coreOptions()))
                            .startNewChain()
                            .forceNonParallel()
                            .transform(
                                    "New Files Compact Worker: " + table.name(),
                                    new CommittableTypeInfo(),
                                    new AppendPreCommitCompactWorkerOperator(table))
                            .startNewChain();
            forwardParallelism(newWritten, written);
            written = newWritten;
        }

        boolean enableCompaction =
                !table.coreOptions().writeOnly()
                        && !table.coreOptions().dataEvolutionEnabled()
                        && !(table.bucketMode() == BucketMode.BUCKET_UNAWARE
                                && table.coreOptions().clusteringIncrementalEnabled());
        boolean isStreamingMode =
                input.getExecutionEnvironment()
                                .getConfiguration()
                                .get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;
        // if enable compaction, we need to add compaction topology to this job
        if (enableCompaction && isStreamingMode) {
            SingleOutputStreamOperator<Committable> newWritten =
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
                                            table, initialCommitUser, true))
                            .startNewChain();
            setParallelism(newWritten, written.getParallelism(), false);
            written = newWritten;

            if (options.get(SINK_USE_MANAGED_MEMORY)) {
                declareManagedMemory(written, options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY));
            }
        }

        return written;
    }
}
