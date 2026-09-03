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

import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.flink.sink.CommittableStateManager;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.CommitterOperatorFactory;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.MultiTableCommittableChannelComputer;
import org.apache.paimon.flink.sink.MultiTableCommittableTypeInfo;
import org.apache.paimon.flink.sink.RestoreAndFailCommittableStateManager;
import org.apache.paimon.flink.sink.StoreMultiCommitter;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.sink.TableFilter;
import org.apache.paimon.flink.sink.WrappedManifestCommittableSerializer;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;

import static org.apache.paimon.flink.sink.FlinkSink.assertStreamingConfiguration;
import static org.apache.paimon.flink.sink.FlinkSink.configureSlotSharingGroup;
import static org.apache.paimon.flink.utils.ParallelismUtils.forwardParallelism;

/**
 * A {@link FlinkSink} which accepts {@link CdcRecord} and waits for a schema change if necessary.
 *
 * <p>When created with a database name, this sink partitions its input with {@link
 * CdcMultiplexRecordChannelComputer} itself, so that records and the writer states of their buckets
 * are routed to the same subtask. The compatibility constructor preserves the legacy behavior and
 * expects its input to have already been partitioned.
 */
public class FlinkCdcMultiTableSink implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String WRITER_NAME = "CDC MultiplexWriter";
    private static final String GLOBAL_COMMITTER_NAME = "Multiplex Global Committer";

    private final boolean isOverwrite = false;
    private final CatalogLoader catalogLoader;
    private final double writeCpuCores;
    private final MemorySize writeHeapMemory;
    private final double commitCpuCores;
    @Nullable private final MemorySize commitHeapMemory;
    private final String commitUser;
    @Nullable private final String databaseName;
    private boolean eagerInit = false;
    private TableFilter tableFilter;

    /**
     * @deprecated Use {@link #FlinkCdcMultiTableSink(CatalogLoader, String, double, MemorySize,
     *     double, MemorySize, String, boolean, TableFilter)} instead. Without a database name,
     *     every subtask restores all union writer state, unique bucket-state ownership is not
     *     guaranteed after a restore, and the input must be partitioned by the caller.
     */
    @Deprecated
    public FlinkCdcMultiTableSink(
            CatalogLoader catalogLoader,
            double writeCpuCores,
            @Nullable MemorySize writeHeapMemory,
            double commitCpuCores,
            @Nullable MemorySize commitHeapMemory,
            String commitUser,
            boolean eagerInit,
            TableFilter tableFilter) {
        this.catalogLoader = catalogLoader;
        this.databaseName = null;
        this.writeCpuCores = writeCpuCores;
        this.writeHeapMemory = writeHeapMemory;
        this.commitCpuCores = commitCpuCores;
        this.commitHeapMemory = commitHeapMemory;
        this.commitUser = commitUser;
        this.eagerInit = eagerInit;
        this.tableFilter = tableFilter;
    }

    public FlinkCdcMultiTableSink(
            CatalogLoader catalogLoader,
            String databaseName,
            double writeCpuCores,
            @Nullable MemorySize writeHeapMemory,
            double commitCpuCores,
            @Nullable MemorySize commitHeapMemory,
            String commitUser,
            boolean eagerInit,
            TableFilter tableFilter) {
        this.catalogLoader = catalogLoader;
        this.databaseName = Preconditions.checkNotNull(databaseName);
        this.writeCpuCores = writeCpuCores;
        this.writeHeapMemory = writeHeapMemory;
        this.commitCpuCores = commitCpuCores;
        this.commitHeapMemory = commitHeapMemory;
        this.commitUser = commitUser;
        this.eagerInit = eagerInit;
        this.tableFilter = tableFilter;
    }

    private StoreSinkWrite.Provider createWriteProvider() {
        // for now, no compaction for multiplexed sink
        return (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) ->
                new StoreSinkWriteImpl(
                        table,
                        commitUser,
                        state,
                        ioManager,
                        isOverwrite,
                        table.coreOptions().prepareCommitWaitCompaction(),
                        true,
                        memoryPoolFactory,
                        metricGroup);
    }

    public DataStreamSink<?> sinkFrom(DataStream<CdcMultiplexRecord> input) {
        return sinkFrom(input, null);
    }

    /**
     * @param parallelism parallelism of the writer and committer operators, or null to forward the
     *     parallelism of {@code input}.
     */
    public DataStreamSink<?> sinkFrom(
            DataStream<CdcMultiplexRecord> input, @Nullable Integer parallelism) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        return sinkFrom(input, parallelism, commitUser, createWriteProvider());
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<CdcMultiplexRecord> input,
            String commitUser,
            StoreSinkWrite.Provider sinkProvider) {
        return sinkFrom(input, null, commitUser, sinkProvider);
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<CdcMultiplexRecord> input,
            @Nullable Integer parallelism,
            String commitUser,
            StoreSinkWrite.Provider sinkProvider) {
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        assertStreamingConfiguration(env);
        Preconditions.checkArgument(
                databaseName != null || parallelism == null,
                "Explicit parallelism is only supported by the constructor which takes a "
                        + "database name.");

        // Keep the old constructor's topology unchanged for compatibility. The database-aware
        // constructor can shuffle by bucket itself and use the same formula to redistribute writer
        // state on restore.
        DataStream<CdcMultiplexRecord> shuffled =
                databaseName == null
                        ? input
                        : FlinkStreamPartitioner.partition(
                                input,
                                new CdcMultiplexRecordChannelComputer(catalogLoader),
                                parallelism);

        MultiTableCommittableTypeInfo typeInfo = new MultiTableCommittableTypeInfo();
        SingleOutputStreamOperator<MultiTableCommittable> written =
                shuffled.transform(
                        WRITER_NAME, typeInfo, createWriteOperator(sinkProvider, commitUser));
        forwardParallelism(written, shuffled);
        configureSlotSharingGroup(written, writeCpuCores, writeHeapMemory);

        // shuffle committables by table
        DataStream<MultiTableCommittable> partitioned =
                FlinkStreamPartitioner.partition(
                        written,
                        new MultiTableCommittableChannelComputer(),
                        shuffled.getParallelism());

        SingleOutputStreamOperator<?> committed =
                partitioned.transform(
                        GLOBAL_COMMITTER_NAME,
                        typeInfo,
                        new CommitterOperatorFactory<>(
                                true,
                                false,
                                commitUser,
                                createCommitterFactory(tableFilter),
                                createCommittableStateManager()));
        forwardParallelism(committed, shuffled);
        configureSlotSharingGroup(committed, commitCpuCores, commitHeapMemory);
        return committed.sinkTo(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    protected OneInputStreamOperatorFactory<CdcMultiplexRecord, MultiTableCommittable>
            createWriteOperator(StoreSinkWrite.Provider writeProvider, String commitUser) {
        return databaseName == null
                ? createCompatibilityWriteOperator(writeProvider, commitUser)
                : new CdcRecordStoreMultiWriteOperator.Factory(
                        catalogLoader, writeProvider, commitUser, databaseName, new Options());
    }

    @SuppressWarnings("deprecation")
    private OneInputStreamOperatorFactory<CdcMultiplexRecord, MultiTableCommittable>
            createCompatibilityWriteOperator(
                    StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new CdcRecordStoreMultiWriteOperator.Factory(
                catalogLoader, writeProvider, commitUser, new Options());
    }

    // Table committers are dynamically created at runtime
    protected Committer.Factory<MultiTableCommittable, WrappedManifestCommittable>
            createCommitterFactory(TableFilter tableFilter) {

        // If checkpoint is enabled for streaming job, we have to
        // commit new files list even if they're empty.
        // Otherwise we can't tell if the commit is successful after
        // a restart.
        return context ->
                new StoreMultiCommitter(
                        catalogLoader,
                        context,
                        false,
                        Collections.emptyMap(),
                        null,
                        eagerInit,
                        tableFilter);
    }

    protected CommittableStateManager<WrappedManifestCommittable> createCommittableStateManager() {
        return new RestoreAndFailCommittableStateManager<>(
                WrappedManifestCommittableSerializer::new,
                true,
                StoreMultiCommitter.END_INPUT_HANDLER);
    }
}
