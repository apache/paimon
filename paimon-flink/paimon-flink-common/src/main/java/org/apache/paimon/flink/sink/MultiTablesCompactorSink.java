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
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_OPERATOR_CHAINING;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_MANAGED_WRITER_BUFFER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.sink.FlinkSink.assertBatchConfiguration;
import static org.apache.paimon.flink.sink.FlinkSink.assertStreamingConfiguration;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.declareManagedMemory;

/** A sink for processing multi-tables in dedicated compaction job. */
public class MultiTablesCompactorSink implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "Writer";
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    private final Catalog.Loader catalogLoader;
    private final boolean ignorePreviousFiles;

    private final Options options;

    public MultiTablesCompactorSink(Catalog.Loader catalogLoader, Options options) {
        this.catalogLoader = catalogLoader;
        this.ignorePreviousFiles = false;
        this.options = options;
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
                env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        SingleOutputStreamOperator<MultiTableCommittable> written =
                input.transform(
                                WRITER_NAME,
                                new MultiTableCommittableTypeInfo(),
                                createWriteOperator(
                                        env.getCheckpointConfig(), isStreaming, commitUser))
                        .setParallelism(parallelism == null ? input.getParallelism() : parallelism);

        if (!isStreaming) {
            assertBatchConfiguration(env, written.getParallelism());
        }

        if (options.get(SINK_USE_MANAGED_MEMORY)) {
            declareManagedMemory(written, options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY));
        }
        return written;
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
        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME,
                                new MultiTableCommittableTypeInfo(),
                                new CommitterOperator<>(
                                        streamingCheckpointEnabled,
                                        false,
                                        options.get(SINK_COMMITTER_OPERATOR_CHAINING),
                                        commitUser,
                                        createCommitterFactory(),
                                        createCommittableStateManager()))
                        .setParallelism(1)
                        .setMaxParallelism(1);
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    // TODO:refactor FlinkSink to adopt this sink
    protected OneInputStreamOperator<RowData, MultiTableCommittable> createWriteOperator(
            CheckpointConfig checkpointConfig, boolean isStreaming, String commitUser) {
        return new MultiTablesStoreCompactOperator(
                catalogLoader,
                commitUser,
                checkpointConfig,
                isStreaming,
                ignorePreviousFiles,
                options);
    }

    protected Committer.Factory<MultiTableCommittable, WrappedManifestCommittable>
            createCommitterFactory() {
        Map<String, String> dynamicOptions = options.toMap();
        dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
        return (user, metricGroup) ->
                new StoreMultiCommitter(catalogLoader, user, metricGroup, true, dynamicOptions);
    }

    protected CommittableStateManager<WrappedManifestCommittable> createCommittableStateManager() {
        return new RestoreAndFailCommittableStateManager<>(
                () -> new VersionedSerializerWrapper<>(new WrappedManifestCommittableSerializer()));
    }
}
