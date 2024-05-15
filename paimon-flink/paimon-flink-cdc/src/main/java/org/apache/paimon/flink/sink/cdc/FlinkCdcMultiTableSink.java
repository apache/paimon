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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.flink.sink.CommittableStateManager;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.CommitterOperator;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.MultiTableCommittableTypeInfo;
import org.apache.paimon.flink.sink.RestoreAndFailCommittableStateManager;
import org.apache.paimon.flink.sink.StoreMultiCommitter;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.sink.WrappedManifestCommittableSerializer;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.UUID;

import static org.apache.paimon.flink.sink.FlinkSink.assertStreamingConfiguration;
import static org.apache.paimon.flink.sink.FlinkSink.configureGlobalCommitter;

/**
 * A {@link FlinkSink} which accepts {@link CdcRecord} and waits for a schema change if necessary.
 */
public class FlinkCdcMultiTableSink implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String WRITER_NAME = "CDC MultiplexWriter";
    private static final String GLOBAL_COMMITTER_NAME = "Multiplex Global Committer";

    private final boolean isOverwrite = false;
    private final Catalog.Loader catalogLoader;
    private final double commitCpuCores;
    @Nullable private final MemorySize commitHeapMemory;
    private final boolean commitChaining;

    public FlinkCdcMultiTableSink(
            Catalog.Loader catalogLoader,
            double commitCpuCores,
            @Nullable MemorySize commitHeapMemory,
            boolean commitChaining) {
        this.catalogLoader = catalogLoader;
        this.commitCpuCores = commitCpuCores;
        this.commitHeapMemory = commitHeapMemory;
        this.commitChaining = commitChaining;
    }

    private StoreSinkWrite.WithWriteBufferProvider createWriteProvider() {
        // for now, no compaction for multiplexed sink
        return (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) ->
                new StoreSinkWriteImpl(
                        table,
                        commitUser,
                        state,
                        ioManager,
                        isOverwrite,
                        FlinkConnectorOptions.prepareCommitWaitCompaction(
                                table.coreOptions().toConfiguration()),
                        true,
                        memoryPoolFactory,
                        metricGroup);
    }

    public DataStreamSink<?> sinkFrom(DataStream<CdcMultiplexRecord> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        String initialCommitUser = UUID.randomUUID().toString();
        return sinkFrom(input, initialCommitUser, createWriteProvider());
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<CdcMultiplexRecord> input,
            String commitUser,
            StoreSinkWrite.WithWriteBufferProvider sinkProvider) {
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        assertStreamingConfiguration(env);
        MultiTableCommittableTypeInfo typeInfo = new MultiTableCommittableTypeInfo();
        SingleOutputStreamOperator<MultiTableCommittable> written =
                input.transform(
                                WRITER_NAME,
                                typeInfo,
                                createWriteOperator(sinkProvider, commitUser))
                        .setParallelism(input.getParallelism());

        // shuffle committables by table
        DataStream<MultiTableCommittable> partitioned =
                FlinkStreamPartitioner.partition(
                        written,
                        new MultiTableCommittableChannelComputer(),
                        input.getParallelism());

        SingleOutputStreamOperator<?> committed =
                partitioned
                        .transform(
                                GLOBAL_COMMITTER_NAME,
                                typeInfo,
                                new CommitterOperator<>(
                                        true,
                                        false,
                                        commitChaining,
                                        commitUser,
                                        createCommitterFactory(),
                                        createCommittableStateManager()))
                        .setParallelism(input.getParallelism());
        configureGlobalCommitter(committed, commitCpuCores, commitHeapMemory);
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    protected OneInputStreamOperator<CdcMultiplexRecord, MultiTableCommittable> createWriteOperator(
            StoreSinkWrite.WithWriteBufferProvider writeProvider, String commitUser) {
        return new CdcRecordStoreMultiWriteOperator(
                catalogLoader, writeProvider, commitUser, new Options());
    }

    // Table committers are dynamically created at runtime
    protected Committer.Factory<MultiTableCommittable, WrappedManifestCommittable>
            createCommitterFactory() {
        // If checkpoint is enabled for streaming job, we have to
        // commit new files list even if they're empty.
        // Otherwise we can't tell if the commit is successful after
        // a restart.
        return context -> new StoreMultiCommitter(catalogLoader, context);
    }

    protected CommittableStateManager<WrappedManifestCommittable> createCommittableStateManager() {
        return new RestoreAndFailCommittableStateManager<>(
                () -> new VersionedSerializerWrapper<>(new WrappedManifestCommittableSerializer()));
    }
}
