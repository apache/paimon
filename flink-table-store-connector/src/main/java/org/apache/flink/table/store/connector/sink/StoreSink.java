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
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializer;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.LogSinkFunction;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/** Sink of dynamic store. */
public class StoreSink implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "Writer";

    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    private final FileStoreTable table;
    private final Lock.Factory lockFactory;
    @Nullable private final Map<String, String> overwritePartition;
    @Nullable private final LogSinkFunction logSinkFunction;

    public StoreSink(
            FileStoreTable table,
            Lock.Factory lockFactory,
            @Nullable Map<String, String> overwritePartition,
            @Nullable LogSinkFunction logSinkFunction) {
        this.table = table;
        this.lockFactory = lockFactory;
        this.overwritePartition = overwritePartition;
        this.logSinkFunction = logSinkFunction;
    }

    private OneInputStreamOperator<RowData, Committable> createWriteOperator(
            String initialCommitUser) {
        boolean isOverwrite = overwritePartition != null;
        StoreSinkWrite.Provider writeProvider;
        if (table.options().changelogProducer() == CoreOptions.ChangelogProducer.FULL_COMPACTION) {
            long fullCompactionThresholdMs =
                    table.options().changelogProducerFullCompactionTriggerInterval().toMillis();
            writeProvider =
                    (table, context, ioManager) ->
                            new FullChangelogStoreSinkWrite(
                                    table,
                                    context,
                                    initialCommitUser,
                                    ioManager,
                                    isOverwrite,
                                    fullCompactionThresholdMs);
        } else {
            writeProvider =
                    (table, context, ioManager) ->
                            new StoreSinkWriteImpl(
                                    table, context, initialCommitUser, ioManager, isOverwrite);
        }

        return new StoreWriteOperator(table, logSinkFunction, writeProvider);
    }

    private StoreCommitter createCommitter(String user, boolean createEmptyCommit) {
        return new StoreCommitter(
                table.newCommit(user)
                        .withOverwritePartition(overwritePartition)
                        .withCreateEmptyCommit(createEmptyCommit)
                        .withLock(lockFactory.create()));
    }

    public DataStreamSink<?> sinkFrom(DataStream<RowData> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        String initialCommitUser = UUID.randomUUID().toString();

        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        CommittableTypeInfo typeInfo = new CommittableTypeInfo();
        SingleOutputStreamOperator<Committable> written =
                input.transform(WRITER_NAME, typeInfo, createWriteOperator(initialCommitUser))
                        .setParallelism(input.getParallelism());

        boolean streamingCheckpointEnabled =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING
                        && checkpointConfig.isCheckpointingEnabled();
        if (streamingCheckpointEnabled) {
            assertCheckpointConfiguration(env);
        }

        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME,
                                typeInfo,
                                new CommitterOperator(
                                        streamingCheckpointEnabled,
                                        initialCommitUser,
                                        // If checkpoint is enabled for streaming job, we have to
                                        // commit new files list even if they're empty.
                                        // Otherwise we can't tell if the commit is successful after
                                        // a restart.
                                        user -> createCommitter(user, streamingCheckpointEnabled),
                                        ManifestCommittableSerializer::new))
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
}
