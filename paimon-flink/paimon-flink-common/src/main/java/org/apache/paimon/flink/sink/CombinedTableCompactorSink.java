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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.MultiTableUnawareAppendCompactionTask;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.Options;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Map;

import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.flink.FlinkConnectorOptions.END_INPUT_WATERMARK;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_OPERATOR_CHAINING;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_MANAGED_WRITER_BUFFER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.sink.FlinkSink.assertBatchAdaptiveParallelism;
import static org.apache.paimon.flink.sink.FlinkSink.assertStreamingConfiguration;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.declareManagedMemory;

/** A sink for processing multi-tables in dedicated compaction job. */
public class CombinedTableCompactorSink implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "Writer";
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    private final CatalogLoader catalogLoader;
    private final boolean ignorePreviousFiles;
    private final boolean fullCompaction;

    private final Options options;

    public CombinedTableCompactorSink(
            CatalogLoader catalogLoader, Options options, boolean fullCompaction) {
        this.catalogLoader = catalogLoader;
        this.ignorePreviousFiles = false;
        this.fullCompaction = fullCompaction;
        this.options = options;
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<RowData> awareBucketTableSource,
            DataStream<MultiTableUnawareAppendCompactionTask> unawareBucketTableSource) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        return sinkFrom(
                awareBucketTableSource, unawareBucketTableSource, createCommitUser(options));
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<RowData> awareBucketTableSource,
            DataStream<MultiTableUnawareAppendCompactionTask> unawareBucketTableSource,
            String initialCommitUser) {
        // do the actually writing action, no snapshot generated in this stage
        DataStream<MultiTableCommittable> written =
                doWrite(awareBucketTableSource, unawareBucketTableSource, initialCommitUser);

        // commit the committable to generate a new snapshot
        return doCommit(written, initialCommitUser);
    }

    public DataStream<MultiTableCommittable> doWrite(
            DataStream<RowData> awareBucketTableSource,
            DataStream<MultiTableUnawareAppendCompactionTask> unawareBucketTableSource,
            String commitUser) {
        StreamExecutionEnvironment env = awareBucketTableSource.getExecutionEnvironment();
        boolean isStreaming =
                env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        SingleOutputStreamOperator<MultiTableCommittable> multiBucketTableRewriter =
                awareBucketTableSource
                        .transform(
                                String.format("%s-%s", "Multi-Bucket-Table", WRITER_NAME),
                                new MultiTableCommittableTypeInfo(),
                                combinedMultiComacptionWriteOperator(
                                        env.getCheckpointConfig(),
                                        isStreaming,
                                        fullCompaction,
                                        commitUser))
                        .setParallelism(awareBucketTableSource.getParallelism());

        SingleOutputStreamOperator<MultiTableCommittable> unawareBucketTableRewriter =
                unawareBucketTableSource
                        .transform(
                                String.format("%s-%s", "Unaware-Bucket-Table", WRITER_NAME),
                                new MultiTableCommittableTypeInfo(),
                                new AppendOnlyMultiTableCompactionWorkerOperator.Factory(
                                        catalogLoader, commitUser, options))
                        .setParallelism(unawareBucketTableSource.getParallelism());

        if (!isStreaming) {
            assertBatchAdaptiveParallelism(env, multiBucketTableRewriter.getParallelism());
            assertBatchAdaptiveParallelism(env, unawareBucketTableRewriter.getParallelism());
        }

        if (options.get(SINK_USE_MANAGED_MEMORY)) {
            declareManagedMemory(
                    multiBucketTableRewriter, options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY));
            declareManagedMemory(
                    unawareBucketTableRewriter, options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY));
        }
        return multiBucketTableRewriter.union(unawareBucketTableRewriter);
    }

    protected DataStreamSink<?> doCommit(
            DataStream<MultiTableCommittable> written, String commitUser) {
        StreamExecutionEnvironment env = written.getExecutionEnvironment();
        ReadableConfig conf = env.getConfiguration();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        boolean streamingCheckpointEnabled =
                isStreaming && checkpointConfig.isCheckpointingEnabled();
        if (streamingCheckpointEnabled) {
            assertStreamingConfiguration(env);
        }

        DataStream<MultiTableCommittable> partitioned =
                FlinkStreamPartitioner.partition(
                        written,
                        new MultiTableCommittableChannelComputer(),
                        written.getParallelism());
        SingleOutputStreamOperator<?> committed =
                partitioned
                        .transform(
                                GLOBAL_COMMITTER_NAME,
                                new MultiTableCommittableTypeInfo(),
                                new CommitterOperatorFactory<>(
                                        streamingCheckpointEnabled,
                                        false,
                                        commitUser,
                                        createCommitterFactory(isStreaming),
                                        createCommittableStateManager(),
                                        options.get(END_INPUT_WATERMARK)))
                        .setParallelism(written.getParallelism());
        if (!options.get(SINK_COMMITTER_OPERATOR_CHAINING)) {
            committed = committed.startNewChain();
        }
        return committed.sinkTo(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    // TODO:refactor FlinkSink to adopt this sink
    protected OneInputStreamOperatorFactory<RowData, MultiTableCommittable>
            combinedMultiComacptionWriteOperator(
                    CheckpointConfig checkpointConfig,
                    boolean isStreaming,
                    boolean fullCompaction,
                    String commitUser) {
        return new MultiTablesStoreCompactOperator.Factory(
                catalogLoader,
                commitUser,
                checkpointConfig,
                isStreaming,
                ignorePreviousFiles,
                fullCompaction,
                options);
    }

    protected Committer.Factory<MultiTableCommittable, WrappedManifestCommittable>
            createCommitterFactory(boolean isStreaming) {
        Map<String, String> dynamicOptions = options.toMap();
        dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
        if (isStreaming) {
            dynamicOptions.put(CoreOptions.NUM_SORTED_RUNS_STOP_TRIGGER.key(), "2147483647");
            dynamicOptions.put(CoreOptions.SORT_SPILL_THRESHOLD.key(), "10");
            dynamicOptions.put(CoreOptions.LOOKUP_WAIT.key(), "false");
        }
        return context -> new StoreMultiCommitter(catalogLoader, context, true, dynamicOptions);
    }

    protected CommittableStateManager<WrappedManifestCommittable> createCommittableStateManager() {
        return new RestoreAndFailCommittableStateManager<>(
                WrappedManifestCommittableSerializer::new);
    }
}
