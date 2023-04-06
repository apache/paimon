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

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.function.SerializableFunction;

import java.io.Serializable;
import java.util.UUID;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_LOOKUP_WAIT;

/** Abstract sink of paimon. */
public abstract class FlinkSink<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "Writer";
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    protected final FileStoreTable table;
    private final boolean isOverwrite;

    public FlinkSink(FileStoreTable table, boolean isOverwrite) {
        this.table = table;
        this.isOverwrite = isOverwrite;
    }

    protected StoreSinkWrite.Provider createWriteProvider(String initialCommitUser) {
        boolean waitCompaction;
        if (table.coreOptions().writeOnly()) {
            waitCompaction = false;
        } else {
            Options options = table.coreOptions().toConfiguration();
            ChangelogProducer changelogProducer = table.coreOptions().changelogProducer();
            if (changelogProducer == ChangelogProducer.FULL_COMPACTION
                    && options.contains(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL)) {
                long fullCompactionThresholdMs =
                        options.get(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL).toMillis();
                return (table, context, ioManager) ->
                        new FullChangelogStoreSinkWrite(
                                table,
                                context,
                                initialCommitUser,
                                ioManager,
                                isOverwrite,
                                fullCompactionThresholdMs);
            }

            waitCompaction =
                    changelogProducer == ChangelogProducer.LOOKUP
                            && options.get(CHANGELOG_PRODUCER_LOOKUP_WAIT);
            if (changelogProducer == ChangelogProducer.FULL_COMPACTION
                    || options.contains(FULL_COMPACTION_DELTA_COMMITS)) {
                int deltaCommits = options.getOptional(FULL_COMPACTION_DELTA_COMMITS).orElse(1);
                return (table, context, ioManager) ->
                        new GlobalFullCompactionSinkWrite(
                                table,
                                context,
                                initialCommitUser,
                                ioManager,
                                isOverwrite,
                                waitCompaction,
                                deltaCommits);
            }
        }

        return (table, context, ioManager) ->
                new StoreSinkWriteImpl(
                        table, context, initialCommitUser, ioManager, isOverwrite, waitCompaction);
    }

    public DataStreamSink<?> sinkFrom(DataStream<T> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        String initialCommitUser = UUID.randomUUID().toString();
        return sinkFrom(input, initialCommitUser, createWriteProvider(initialCommitUser));
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<T> input, String commitUser, StoreSinkWrite.Provider sinkProvider) {
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        boolean streamingCheckpointEnabled =
                isStreaming && checkpointConfig.isCheckpointingEnabled();
        if (streamingCheckpointEnabled) {
            assertCheckpointConfiguration(env);
        }

        CommittableTypeInfo typeInfo = new CommittableTypeInfo();
        SingleOutputStreamOperator<Committable> written =
                input.transform(
                                WRITER_NAME + " -> " + table.name(),
                                typeInfo,
                                createWriteOperator(sinkProvider, isStreaming))
                        .setParallelism(input.getParallelism());

        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME + " -> " + table.name(),
                                typeInfo,
                                new CommitterOperator(
                                        streamingCheckpointEnabled,
                                        commitUser,
                                        createCommitterFactory(streamingCheckpointEnabled),
                                        createCommittableStateManager()))
                        .setParallelism(1)
                        .setMaxParallelism(1);
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    private void assertCheckpointConfiguration(StreamExecutionEnvironment env) {
        Preconditions.checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "Paimon sink currently does not support unaligned checkpoints. Please set "
                        + ExecutionCheckpointingOptions.ENABLE_UNALIGNED.key()
                        + " to false.");
        Preconditions.checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "Paimon sink currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key()
                        + " to exactly-once");
    }

    protected abstract OneInputStreamOperator<T, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, boolean isStreaming);

    protected abstract SerializableFunction<String, Committer> createCommitterFactory(
            boolean streamingCheckpointEnabled);

    protected abstract CommittableStateManager createCommittableStateManager();
}
