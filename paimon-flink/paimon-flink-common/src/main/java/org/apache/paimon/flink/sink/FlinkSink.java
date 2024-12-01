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
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.TagCreationMode;
import org.apache.paimon.flink.compact.changelog.ChangelogCompactCoordinateOperator;
import org.apache.paimon.flink.compact.changelog.ChangelogCompactWorkerOperator;
import org.apache.paimon.flink.compact.changelog.ChangelogTaskTypeInfo;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SerializableRunnable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRECOMMIT_COMPACT;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.END_INPUT_WATERMARK;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_AUTO_TAG_FOR_SAVEPOINT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_CPU;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_OPERATOR_CHAINING;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_MANAGED_WRITER_BUFFER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.generateCustomUid;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.declareManagedMemory;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Abstract sink of paimon. */
public abstract class FlinkSink<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "Writer";
    private static final String WRITER_WRITE_ONLY_NAME = "Writer(write-only)";
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    protected final FileStoreTable table;
    private final boolean ignorePreviousFiles;

    public FlinkSink(FileStoreTable table, boolean ignorePreviousFiles) {
        this.table = table;
        this.ignorePreviousFiles = ignorePreviousFiles;
    }

    private StoreSinkWrite.Provider createWriteProvider(
            CheckpointConfig checkpointConfig, boolean isStreaming, boolean hasSinkMaterializer) {
        SerializableRunnable assertNoSinkMaterializer =
                () ->
                        Preconditions.checkArgument(
                                !hasSinkMaterializer,
                                String.format(
                                        "Sink materializer must not be used with Paimon sink. "
                                                + "Please set '%s' to '%s' in Flink's config.",
                                        ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE
                                                .key(),
                                        ExecutionConfigOptions.UpsertMaterialize.NONE.name()));

        Options options = table.coreOptions().toConfiguration();
        ChangelogProducer changelogProducer = table.coreOptions().changelogProducer();
        boolean waitCompaction;
        CoreOptions coreOptions = table.coreOptions();
        if (coreOptions.writeOnly()) {
            waitCompaction = false;
        } else {
            waitCompaction = coreOptions.prepareCommitWaitCompaction();
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

            if (changelogProducer == ChangelogProducer.FULL_COMPACTION || deltaCommits >= 0) {
                int finalDeltaCommits = Math.max(deltaCommits, 1);
                return (table, commitUser, state, ioManager, memoryPool, metricGroup) -> {
                    assertNoSinkMaterializer.run();
                    return new GlobalFullCompactionSinkWrite(
                            table,
                            commitUser,
                            state,
                            ioManager,
                            ignorePreviousFiles,
                            waitCompaction,
                            finalDeltaCommits,
                            isStreaming,
                            memoryPool,
                            metricGroup);
                };
            }
        }

        if (coreOptions.needLookup() && !coreOptions.prepareCommitWaitCompaction()) {
            return (table, commitUser, state, ioManager, memoryPool, metricGroup) -> {
                assertNoSinkMaterializer.run();
                return new AsyncLookupSinkWrite(
                        table,
                        commitUser,
                        state,
                        ioManager,
                        ignorePreviousFiles,
                        waitCompaction,
                        isStreaming,
                        memoryPool,
                        metricGroup);
            };
        }

        return (table, commitUser, state, ioManager, memoryPool, metricGroup) -> {
            assertNoSinkMaterializer.run();
            return new StoreSinkWriteImpl(
                    table,
                    commitUser,
                    state,
                    ioManager,
                    ignorePreviousFiles,
                    waitCompaction,
                    isStreaming,
                    memoryPool,
                    metricGroup);
        };
    }

    public DataStreamSink<?> sinkFrom(DataStream<T> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        return sinkFrom(input, createCommitUser(table.coreOptions().toConfiguration()));
    }

    public DataStreamSink<?> sinkFrom(DataStream<T> input, String initialCommitUser) {
        // do the actually writing action, no snapshot generated in this stage
        DataStream<Committable> written = doWrite(input, initialCommitUser, input.getParallelism());
        // commit the committable to generate a new snapshot
        return doCommit(written, initialCommitUser);
    }

    private boolean hasSinkMaterializer(DataStream<T> input) {
        // traverse the transformation graph with breadth first search
        Set<Integer> visited = new HashSet<>();
        Queue<Transformation<?>> queue = new LinkedList<>();
        queue.add(input.getTransformation());
        visited.add(input.getTransformation().getId());
        while (!queue.isEmpty()) {
            Transformation<?> transformation = queue.poll();
            if (transformation.getName().startsWith("SinkMaterializer")) {
                return true;
            }
            for (Transformation<?> prev : transformation.getInputs()) {
                if (!visited.contains(prev.getId())) {
                    queue.add(prev);
                    visited.add(prev.getId());
                }
            }
        }
        return false;
    }

    public DataStream<Committable> doWrite(
            DataStream<T> input, String commitUser, @Nullable Integer parallelism) {
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        boolean isStreaming = isStreaming(input);

        boolean writeOnly = table.coreOptions().writeOnly();
        SingleOutputStreamOperator<Committable> written =
                input.transform(
                                (writeOnly ? WRITER_WRITE_ONLY_NAME : WRITER_NAME)
                                        + " : "
                                        + table.name(),
                                new CommittableTypeInfo(),
                                createWriteOperatorFactory(
                                        createWriteProvider(
                                                env.getCheckpointConfig(),
                                                isStreaming,
                                                hasSinkMaterializer(input)),
                                        commitUser))
                        .setParallelism(parallelism == null ? input.getParallelism() : parallelism);

        Options options = Options.fromMap(table.options());

        String uidSuffix = options.get(SINK_OPERATOR_UID_SUFFIX);
        if (options.get(SINK_OPERATOR_UID_SUFFIX) != null) {
            written = written.uid(generateCustomUid(WRITER_NAME, table.name(), uidSuffix));
        }

        if (options.get(SINK_USE_MANAGED_MEMORY)) {
            declareManagedMemory(written, options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY));
        }

        if (options.get(CHANGELOG_PRECOMMIT_COMPACT)) {
            written =
                    written.transform(
                                    "Changelog Compact Coordinator",
                                    new EitherTypeInfo<>(
                                            new CommittableTypeInfo(), new ChangelogTaskTypeInfo()),
                                    new ChangelogCompactCoordinateOperator(table))
                            .forceNonParallel()
                            .transform(
                                    "Changelog Compact Worker",
                                    new CommittableTypeInfo(),
                                    new ChangelogCompactWorkerOperator(table))
                            .setParallelism(written.getParallelism());
        }

        return written;
    }

    protected DataStreamSink<?> doCommit(DataStream<Committable> written, String commitUser) {
        StreamExecutionEnvironment env = written.getExecutionEnvironment();
        ReadableConfig conf = env.getConfiguration();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        boolean streamingCheckpointEnabled =
                isStreaming(written) && checkpointConfig.isCheckpointingEnabled();
        if (streamingCheckpointEnabled) {
            assertStreamingConfiguration(env);
        }

        Options options = Options.fromMap(table.options());
        OneInputStreamOperatorFactory<Committable, Committable> committerOperator =
                new CommitterOperatorFactory<>(
                        streamingCheckpointEnabled,
                        true,
                        commitUser,
                        createCommitterFactory(),
                        createCommittableStateManager(),
                        options.get(END_INPUT_WATERMARK));

        if (options.get(SINK_AUTO_TAG_FOR_SAVEPOINT)) {
            committerOperator =
                    new AutoTagForSavepointCommitterOperatorFactory<>(
                            (CommitterOperatorFactory<Committable, ManifestCommittable>)
                                    committerOperator,
                            table::snapshotManager,
                            table::tagManager,
                            () -> table.store().newTagDeletion(),
                            () -> table.store().createTagCallbacks(),
                            table.coreOptions().tagDefaultTimeRetained());
        }
        if (conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.BATCH
                && table.coreOptions().tagCreationMode() == TagCreationMode.BATCH) {
            committerOperator =
                    new BatchWriteGeneratorTagOperatorFactory<>(
                            (CommitterOperatorFactory<Committable, ManifestCommittable>)
                                    committerOperator,
                            table);
        }
        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME + " : " + table.name(),
                                new CommittableTypeInfo(),
                                committerOperator)
                        .setParallelism(1)
                        .setMaxParallelism(1);
        if (options.get(SINK_OPERATOR_UID_SUFFIX) != null) {
            committed =
                    committed.uid(
                            generateCustomUid(
                                    GLOBAL_COMMITTER_NAME,
                                    table.name(),
                                    options.get(SINK_OPERATOR_UID_SUFFIX)));
        }
        if (!options.get(SINK_COMMITTER_OPERATOR_CHAINING)) {
            committed = committed.startNewChain();
        }
        configureGlobalCommitter(
                committed, options.get(SINK_COMMITTER_CPU), options.get(SINK_COMMITTER_MEMORY));
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    public static void configureGlobalCommitter(
            SingleOutputStreamOperator<?> committed,
            double cpuCores,
            @Nullable MemorySize heapMemory) {
        if (heapMemory == null) {
            return;
        }

        SlotSharingGroup slotSharingGroup =
                SlotSharingGroup.newBuilder(committed.getName())
                        .setCpuCores(cpuCores)
                        .setTaskHeapMemory(
                                new org.apache.flink.configuration.MemorySize(
                                        heapMemory.getBytes()))
                        .build();
        committed.slotSharingGroup(slotSharingGroup);
    }

    public static void assertStreamingConfiguration(StreamExecutionEnvironment env) {
        checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "Paimon sink currently does not support unaligned checkpoints. Please set "
                        + "execution.checkpointing.unaligned.enabled to false.");
        checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "Paimon sink currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + "execution.checkpointing.mode to exactly-once");
    }

    public static void assertBatchAdaptiveParallelism(
            StreamExecutionEnvironment env, int sinkParallelism) {
        String msg =
                "Paimon Sink does not support Flink's Adaptive Parallelism mode. "
                        + "Please manually turn it off or set Paimon `sink.parallelism` manually.";
        assertBatchAdaptiveParallelism(env, sinkParallelism, msg);
    }

    public static void assertBatchAdaptiveParallelism(
            StreamExecutionEnvironment env, int sinkParallelism, String exceptionMsg) {
        try {
            checkArgument(
                    sinkParallelism != -1 || !AdaptiveParallelism.isEnabled(env), exceptionMsg);
        } catch (NoClassDefFoundError ignored) {
            // before 1.17, there is no adaptive parallelism
        }
    }

    protected abstract OneInputStreamOperatorFactory<T, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser);

    protected abstract Committer.Factory<Committable, ManifestCommittable> createCommitterFactory();

    protected abstract CommittableStateManager<ManifestCommittable> createCommittableStateManager();

    public static boolean isStreaming(DataStream<?> input) {
        return isStreaming(input.getExecutionEnvironment());
    }

    public static boolean isStreaming(StreamExecutionEnvironment env) {
        return env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                == RuntimeExecutionMode.STREAMING;
    }
}
