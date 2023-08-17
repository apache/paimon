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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.UUID;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_LOOKUP_WAIT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_MANAGED_WRITER_BUFFER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** this is a doc. */
public class CompactorSinkForDb implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "Writer";
    private static final String WRITER_WRITE_ONLY_NAME = "Writer(write-only)";
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    //    protected final FileStoreTable table;
    //    protected final List<FileStoreTable> tables;
    private final Catalog.Loader catalogLoader;
    private final boolean ignorePreviousFiles;

    private Options options;
    private CoreOptions coreOptions;

    public CompactorSinkForDb(
            Catalog.Loader catalogLoader, Options options, CoreOptions coreOptions) {
        this.catalogLoader = catalogLoader;
        this.ignorePreviousFiles = false;
        this.options = options;
        this.coreOptions = coreOptions;
    }

    private StoreSinkWrite.Provider createWriteProvider(
            CheckpointConfig checkpointConfig, boolean isStreaming) {
        boolean waitCompaction;
        if (coreOptions.writeOnly()) {
            waitCompaction = false;
        } else {
            Options options = coreOptions.toConfiguration();
            CoreOptions.ChangelogProducer changelogProducer = coreOptions.changelogProducer();
            waitCompaction =
                    changelogProducer == CoreOptions.ChangelogProducer.LOOKUP
                            && options.get(CHANGELOG_PRODUCER_LOOKUP_WAIT);

            int deltaCommits = -1;
            if (options.contains(FULL_COMPACTION_DELTA_COMMITS)) {
                deltaCommits = options.get(FULL_COMPACTION_DELTA_COMMITS);
            } else if (options.contains(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL)) {
                long fullCompactionThresholdMs =
                        options.get(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL).toMillis();
                deltaCommits =
                        (int)
                                (fullCompactionThresholdMs
                                        / checkpointConfig.getCheckpointInterval());
            }

            if (changelogProducer == CoreOptions.ChangelogProducer.FULL_COMPACTION
                    || deltaCommits >= 0) {
                int finalDeltaCommits = Math.max(deltaCommits, 1);
                // 这个 lamda 表达式代表接口 Provider 的具体实现
                return (table, commitUser, state, ioManager, memoryPool) ->
                        new GlobalFullCompactionSinkWrite(
                                table,
                                commitUser,
                                state,
                                ioManager,
                                ignorePreviousFiles,
                                waitCompaction,
                                finalDeltaCommits,
                                isStreaming,
                                memoryPool);
            }
        }

        return (table, commitUser, state, ioManager, memoryPool) ->
                new StoreSinkWriteImpl(
                        table,
                        commitUser,
                        state,
                        ioManager,
                        ignorePreviousFiles,
                        waitCompaction,
                        isStreaming,
                        memoryPool);
    }

    public DataStreamSink<?> sinkFrom(DataStream<RowData> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        String initialCommitUser = UUID.randomUUID().toString();
        return sinkFrom(input, initialCommitUser);
    }

    public DataStreamSink<?> sinkFrom(DataStream<RowData> input, String initialCommitUser) {
        // do the actually writing action, no snapshot generated in this stage
        SingleOutputStreamOperator<MultiTableCommittable> written =
                doWrite(input, initialCommitUser, input.getParallelism());

        // commit the committable to generate a new snapshot
        return doCommit(written, initialCommitUser);
    }

    public SingleOutputStreamOperator<MultiTableCommittable> doWrite(
            DataStream<RowData> input, String commitUser, Integer parallelism) {
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        boolean isStreaming =
                StreamExecutionEnvironmentUtils.getConfiguration(env)
                                .get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        Boolean writeOnly = coreOptions.writeOnly();
        SingleOutputStreamOperator<MultiTableCommittable> written =
                input.transform(
                                (writeOnly ? WRITER_WRITE_ONLY_NAME : WRITER_NAME),
                                new MultiTableCommittableTypeInfo(),
                                createWriteOperator(
                                        createWriteProvider(env.getCheckpointConfig(), isStreaming),
                                        commitUser))
                        .setParallelism(parallelism == null ? input.getParallelism() : parallelism);

        if (!isStreaming) {
            assertBatchConfiguration(env, written.getParallelism());
        }

        //        Options options = Options.fromMap(tables.get(0).options());
        if (options.get(SINK_USE_MANAGED_MEMORY)) {
            MemorySize memorySize = options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY);
            written.getTransformation()
                    .declareManagedMemoryUseCaseAtOperatorScope(
                            ManagedMemoryUseCase.OPERATOR, memorySize.getMebiBytes());
        }
        return written;
    }

    protected DataStreamSink<?> doCommit(
            DataStream<MultiTableCommittable> written, String commitUser) {
        StreamExecutionEnvironment env = written.getExecutionEnvironment();
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        boolean streamingCheckpointEnabled =
                isStreaming && checkpointConfig.isCheckpointingEnabled();
        if (streamingCheckpointEnabled) {
            assertStreamingConfiguration(env);
        }

        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME,
                                new MultiTableCommittableTypeInfo(),
                                new CommitterOperator<>(
                                        streamingCheckpointEnabled,
                                        commitUser,
                                        createCommitterFactory(streamingCheckpointEnabled),
                                        createCommittableStateManager()))
                        .setParallelism(1)
                        .setMaxParallelism(1);
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    public static void assertStreamingConfiguration(StreamExecutionEnvironment env) {
        checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "Paimon sink currently does not support unaligned checkpoints. Please set "
                        + ExecutionCheckpointingOptions.ENABLE_UNALIGNED.key()
                        + " to false.");
        checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "Paimon sink currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key()
                        + " to exactly-once");
    }

    private void assertBatchConfiguration(StreamExecutionEnvironment env, int sinkParallelism) {
        try {
            checkArgument(
                    sinkParallelism != -1 || !AdaptiveParallelism.isEnabled(env),
                    "Paimon Sink does not support Flink's Adaptive Parallelism mode. "
                            + "Please manually turn it off or set Paimon `sink.parallelism` manually.");
        } catch (NoClassDefFoundError ignored) {
            // before 1.17, there is no adaptive parallelism
        }
    }

    protected OneInputStreamOperator<RowData, MultiTableCommittable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new StoreCompactOperatorForDb(catalogLoader, writeProvider, commitUser, options);
    }

    protected SerializableFunction<
                    String, Committer<MultiTableCommittable, WrappedManifestCommittable>>
            createCommitterFactory(boolean streamingCheckpointEnabled) {
        return user -> new StoreMultiCommitter(user, catalogLoader);
    }

    protected CommittableStateManager<WrappedManifestCommittable> createCommittableStateManager() {
        return new RestoreAndFailCommittableStateManager<>(
                () -> new VersionedSerializerWrapper<>(new WrappedManifestCommittableSerializer()));
    }
}
