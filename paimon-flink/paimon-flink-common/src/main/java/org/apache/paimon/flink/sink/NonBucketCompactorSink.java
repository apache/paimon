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

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.function.SerializableFunction;

import java.io.Serializable;
import java.util.UUID;

/** Dedicated non-bucket compact jobs. */
public class NonBucketCompactorSink implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String COMPACTION_COORDINATOR_NAME = "Compaction Coordinator";
    private static final String COMPACTION_WORKER_NAME = "Compaction Worker";
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    private final Lock.Factory lockFactory;
    protected final AppendOnlyFileStoreTable table;
    private Integer compactionWorkerParallelism;

    public NonBucketCompactorSink(AppendOnlyFileStoreTable table, Lock.Factory lockFactory) {
        this.table = table;
        this.lockFactory = lockFactory;
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

    public DataStreamSink<?> sinkFrom(DataStream<RowData> input, String commitUser) {
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

        CompactionTaskTypeInfo compactionTaskTypeInfo = new CompactionTaskTypeInfo();
        SingleOutputStreamOperator<AppendOnlyCompactionTask> compactionTaskAssigned =
                input.transform(
                                COMPACTION_COORDINATOR_NAME + " -> " + table.name(),
                                compactionTaskTypeInfo,
                                createCompactionOperator(table))
                        .setParallelism(1);

        CommittableTypeInfo committableTypeInfo = new CommittableTypeInfo();
        SingleOutputStreamOperator<Committable> compacted =
                compactionTaskAssigned.transform(
                        COMPACTION_WORKER_NAME + " -> " + table.name(),
                        committableTypeInfo,
                        createCompactionWorkerOperator(table, commitUser));

        if (compactionWorkerParallelism != null) {
            compacted.setParallelism(compactionWorkerParallelism);
        }

        SingleOutputStreamOperator<?> committed =
                compacted
                        .transform(
                                GLOBAL_COMMITTER_NAME + " -> " + table.name(),
                                committableTypeInfo,
                                new CommitterOperator(
                                        streamingCheckpointEnabled,
                                        commitUser,
                                        createCommitterFactory(),
                                        new NoopCommittableStateManager()))
                        .setParallelism(1)
                        .setMaxParallelism(1);

        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    public void withCompactionWorkerParallelism(Integer compactionWorkerParallelism) {
        this.compactionWorkerParallelism = compactionWorkerParallelism;
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

    private OneInputStreamOperator<RowData, AppendOnlyCompactionTask> createCompactionOperator(
            AppendOnlyFileStoreTable table) {
        return new AppendOnlyTableCompactionCoordinatorOperator(table);
    }

    private OneInputStreamOperator<AppendOnlyCompactionTask, Committable>
            createCompactionWorkerOperator(AppendOnlyFileStoreTable table, String commitUser) {
        return new AppendOnlyTableCompactionWorkerOperator(table, commitUser);
    }

    private SerializableFunction<String, Committer> createCommitterFactory() {
        return user -> new StoreCommitter(table.newCommit(user).withLock(lockFactory.create()));
    }
}
