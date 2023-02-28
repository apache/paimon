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

package org.apache.flink.table.store.connector.sink;

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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.connector.utils.StreamExecutionEnvironmentUtils;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.utils.Preconditions;
import org.apache.flink.util.function.SerializableFunction;

import java.io.Serializable;
import java.util.UUID;

import static org.apache.flink.table.store.connector.FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL;

/** Abstract sink of table store. */
public abstract class FlinkSink implements Serializable {

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
        if (table.options().changelogProducer() == CoreOptions.ChangelogProducer.FULL_COMPACTION
                && !table.options().writeOnly()) {
            long fullCompactionThresholdMs =
                    table.options()
                            .toConfiguration()
                            .get(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL)
                            .toMillis();
            return (table, context, ioManager) ->
                    new FullChangelogStoreSinkWrite(
                            table,
                            context,
                            initialCommitUser,
                            ioManager,
                            isOverwrite,
                            fullCompactionThresholdMs);
        } else {
            return (table, context, ioManager) ->
                    new StoreSinkWriteImpl(
                            table, context, initialCommitUser, ioManager, isOverwrite);
        }
    }

    public DataStreamSink<?> sinkFrom(DataStream<RowData> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        String initialCommitUser = UUID.randomUUID().toString();
        return sinkFrom(input, initialCommitUser, createWriteProvider(initialCommitUser));
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<RowData> input, String commitUser, StoreSinkWrite.Provider sinkProvider) {
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
                "Table Store sink currently does not support unaligned checkpoints. Please set "
                        + ExecutionCheckpointingOptions.ENABLE_UNALIGNED.key()
                        + " to false.");
        Preconditions.checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "Table Store sink currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key()
                        + " to exactly-once");
    }

    protected abstract OneInputStreamOperator<RowData, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, boolean isStreaming);

    protected abstract SerializableFunction<String, Committer> createCommitterFactory(
            boolean streamingCheckpointEnabled);

    protected abstract CommittableStateManager createCommittableStateManager();
}
