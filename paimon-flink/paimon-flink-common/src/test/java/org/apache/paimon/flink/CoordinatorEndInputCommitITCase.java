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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableSerializer;
import org.apache.paimon.flink.sink.RowAppendTableSink;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.coordinator.CheckpointCommittables;
import org.apache.paimon.flink.sink.coordinator.CheckpointCommittablesSerializer;
import org.apache.paimon.flink.sink.coordinator.CommittableEvent;
import org.apache.paimon.flink.sink.coordinator.RestoredCommittableEvent;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.source.SplitListState;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for end-input watermark on unaware-bucket append tables. */
public class CoordinatorEndInputCommitITCase {

    private static final int DEFAULT_PARALLELISM = 2;
    private static final AtomicBoolean COORDINATOR_CHECKPOINT_COMPLETED = new AtomicBoolean();
    private static final AtomicBoolean END_INPUT_EVENT_DELAYED = new AtomicBoolean();
    private static final AtomicBoolean CHECKPOINT_COMPLETION_OVERTAKES_END_INPUT =
            new AtomicBoolean();
    private static final AtomicBoolean END_INPUT_COMMITTABLE_RESTORED = new AtomicBoolean();
    private static final long WAIT_TIMEOUT_MILLIS = 60_000L;
    private static final InMemoryReporter reporter = InMemoryReporter.create();

    @RegisterExtension
    protected static final org.apache.paimon.flink.util.MiniClusterWithClientExtension
            MINI_CLUSTER_EXTENSION =
                    new org.apache.paimon.flink.util.MiniClusterWithClientExtension(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setConfiguration(
                                            reporter.addToConfiguration(new Configuration()))
                                    .build());

    @TempDir Path tempPath;

    private final CommonEndInputTestUtils commonEndInputTestUtils = new CommonEndInputTestUtils();

    @AfterEach
    public final void cleanupRunningJobs() throws Exception {
        ClusterClient<?> clusterClient = MINI_CLUSTER_EXTENSION.createRestClusterClient();
        for (JobStatusMessage job : clusterClient.listJobs().get()) {
            if (!job.getJobState().isTerminalState()) {
                try {
                    clusterClient.cancel(job.getJobId()).get(30, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    // Common end-input commit tests
    // ------------------------------------------------------------------------

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testCoordinatorCommitEndInputInStreamingMode() throws Exception {
        commonEndInputTestUtils.assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                true,
                true,
                12345L);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testOperatorCommitEndInputInStreamingMode() throws Exception {
        commonEndInputTestUtils.assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                false,
                true,
                12345L);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testCoordinatorEndInputIsCommittedWithoutCheckpointsAfterTasksFinish()
            throws Exception {
        commonEndInputTestUtils.assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                true,
                true,
                12345L,
                false,
                false);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testOperatorEndInputIsNotCommittedWithoutCheckpointsAfterTasksFinish()
            throws Exception {
        commonEndInputTestUtils.assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                false,
                true,
                12345L,
                false,
                false);
    }

    // ------------------------------------------------------------------------
    // Special final-commit construction tests
    // ------------------------------------------------------------------------

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testLateEndInputAfterCheckpointCompletionIsCommitted() throws Exception {
        COORDINATOR_CHECKPOINT_COMPLETED.set(false);
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        streamEnv.setParallelism(1);
        streamEnv.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));
        Configuration configuration = new Configuration();
        configuration.setString("execution.checkpointing.checkpoints-after-tasks-finish", "false");
        configuration.setString("restart-strategy.type", "none");
        streamEnv.configure(configuration);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamEnv);
        tEnv.executeSql(
                "CREATE CATALOG racecat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG racecat");
        tEnv.executeSql(
                "CREATE TABLE T_END_INPUT_RACE (id INT, data STRING) WITH ("
                        + "'bucket' = '-1', "
                        + "'write-only' = 'true', "
                        + "'sink.coordinator-commit.enabled' = 'true')");

        FileStoreTable table =
                (FileStoreTable)
                        ((FlinkCatalog) tEnv.getCatalog("racecat").get())
                                .catalog()
                                .getTable(Identifier.create("default", "T_END_INPUT_RACE"));

        DataStream<InternalRow> source =
                streamEnv
                        .fromSource(
                                new EndInputValidationTestUtils
                                        .EmitAfterCoordinatorCheckpointCompleteSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Emit After Completed Checkpoint")
                        .setParallelism(1)
                        .map(
                                (MapFunction<Row, InternalRow>)
                                        row ->
                                                GenericRow.of(
                                                        row.<Integer>getFieldAs(0),
                                                        BinaryString.fromString(
                                                                row.<String>getFieldAs(1))))
                        .returns(InternalTypeInfo.fromRowType(table.rowType()))
                        .setParallelism(1);

        new EndInputValidationTestUtils.CheckpointTrackingRowAppendTableSink(table)
                .sinkFrom(source);
        streamEnv.execute("late-end-input-after-checkpoint-completion");

        waitUntilRowCount(table, 1L);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testFinalCheckpointCompletionBeforeEndInputEventCommitsAfterFailover()
            throws Exception {
        END_INPUT_EVENT_DELAYED.set(false);
        CHECKPOINT_COMPLETION_OVERTAKES_END_INPUT.set(false);
        END_INPUT_COMMITTABLE_RESTORED.set(false);

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        streamEnv.setParallelism(1);
        // Keep periodic checkpoints out of the gap between emitting the final record and
        // END_OF_INPUT. This test triggers both checkpoints explicitly below.
        streamEnv.enableCheckpointing(TimeUnit.HOURS.toMillis(1));
        Configuration configuration = new Configuration();
        configuration.setString("execution.checkpointing.checkpoints-after-tasks-finish", "true");
        configuration.setString("restart-strategy.type", "fixed-delay");
        // The first restart recovers and commits END_INPUT. Paimon intentionally fails once more
        // after committing restored data so that all writers restart from the new snapshot.
        configuration.setString("restart-strategy.fixed-delay.attempts", "2");
        configuration.setString("restart-strategy.fixed-delay.delay", "0 ms");
        streamEnv.configure(configuration);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamEnv);
        tEnv.executeSql(
                "CREATE CATALOG racecat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG racecat");
        tEnv.executeSql(
                "CREATE TABLE T_END_INPUT_ALIGNMENT_RACE (id INT, data STRING) WITH ("
                        + "'bucket' = '-1', "
                        + "'write-only' = 'true', "
                        + "'sink.coordinator-commit.enabled' = 'true')");

        FileStoreTable table =
                (FileStoreTable)
                        ((FlinkCatalog) tEnv.getCatalog("racecat").get())
                                .catalog()
                                .getTable(
                                        Identifier.create("default", "T_END_INPUT_ALIGNMENT_RACE"));

        DataStream<InternalRow> source =
                streamEnv
                        .fromSource(
                                new FinalCommitTestUtils.EmitAfterFirstCheckpointSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Emit And Finish After First Checkpoint")
                        .setParallelism(1)
                        .map(
                                (MapFunction<Row, InternalRow>)
                                        row ->
                                                GenericRow.of(
                                                        row.<Integer>getFieldAs(0),
                                                        BinaryString.fromString(
                                                                row.<String>getFieldAs(1))))
                        .returns(InternalTypeInfo.fromRowType(table.rowType()))
                        .setParallelism(1);

        new EndInputValidationTestUtils.CheckpointTrackingRowAppendTableSink(table, true)
                .sinkFrom(source);

        // Keep one task running after the Paimon writer has finished. With checkpoints after tasks
        // finish enabled, this guarantees another real checkpoint completion which can overtake
        // the delayed END_INPUT event.
        streamEnv
                .fromSource(
                        new FinalCommitTestUtils.KeepJobRunningSource(),
                        WatermarkStrategy.noWatermarks(),
                        "Keep Job Running")
                .setParallelism(1)
                .sinkTo(new DiscardingSink<>())
                .setParallelism(1);

        JobClient jobClient =
                streamEnv.executeAsync(
                        "final-checkpoint-completion-before-end-input-event-failover");
        try {
            try (ClusterClient<?> clusterClient =
                    MINI_CLUSTER_EXTENSION.createRestClusterClient()) {
                // Checkpoint 1 lets the controlled source emit its only record and finish.
                triggerCheckpointWhenTasksRunning(clusterClient, jobClient);
                waitUntilTrue(END_INPUT_EVENT_DELAYED, "END_INPUT event was not delayed");

                // The writer has already reached END_INPUT, so checkpoint 2 cannot produce an
                // ordinary checkpoint committable. Its completion now deterministically overtakes
                // the delayed END_INPUT event and triggers recovery from checkpointed writer state.
                triggerCheckpointWhenTasksRunning(clusterClient, jobClient);

                // The final record becomes visible only after the END_INPUT committable has been
                // recovered and committed.
                waitUntilRowCount(table, 1L);
                // Verify that checkpoint completion reached the coordinator while the original
                // END_INPUT event was still delayed.
                assertThat(CHECKPOINT_COMPLETION_OVERTAKES_END_INPUT.get()).isTrue();
                // Verify that failover restored a non-empty END_INPUT committable instead of
                // relying on the old attempt's delayed event.
                assertThat(END_INPUT_COMMITTABLE_RESTORED.get()).isTrue();
            }
        } finally {
            try {
                jobClient.cancel().get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // Best-effort cleanup; @AfterEach also cancels any remaining running job.
            }
        }
    }

    // ------------------------------------------------------------------------
    // End-input validity tests
    // ------------------------------------------------------------------------

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testEmptyFinalCommitDoesNotUpdateEndInputWatermarkInStreamingMode()
            throws Exception {
        commonEndInputTestUtils.assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                true,
                false,
                12345L);
        commonEndInputTestUtils.assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                false,
                false,
                12345L);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testForceCreateSnapshotUpdatesEndInputWatermarkInStreamingMode() throws Exception {
        commonEndInputTestUtils.assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                true,
                false,
                12345L,
                true);
        commonEndInputTestUtils.assertEndInputWatermark(
                EnvironmentSettings.newInstance().inStreamingMode().build(),
                true,
                false,
                false,
                12345L,
                true);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testCoordinatorCommitEndInputInBatchMode() throws Exception {
        commonEndInputTestUtils.assertEndInputWatermark(
                EnvironmentSettings.newInstance().inBatchMode().build(),
                false,
                true,
                false,
                12345L);
    }

    /** Shared setup and assertions for the common end-input commit tests. */
    private final class CommonEndInputTestUtils {

        private CommonEndInputTestUtils() {}

        private void assertEndInputWatermark(
                EnvironmentSettings settings,
                boolean streaming,
                boolean coordinatorCommit,
                boolean emitAfterFirstCheckpoint,
                long expectedWatermark)
                throws Exception {
            assertEndInputWatermark(
                    settings,
                    streaming,
                    coordinatorCommit,
                    emitAfterFirstCheckpoint,
                    expectedWatermark,
                    false);
        }

        private void assertEndInputWatermark(
                EnvironmentSettings settings,
                boolean streaming,
                boolean coordinatorCommit,
                boolean emitAfterFirstCheckpoint,
                long expectedWatermark,
                boolean forceCreateSnapshot)
                throws Exception {
            assertEndInputWatermark(
                    settings,
                    streaming,
                    coordinatorCommit,
                    emitAfterFirstCheckpoint,
                    expectedWatermark,
                    forceCreateSnapshot,
                    true);
        }

        private void assertEndInputWatermark(
                EnvironmentSettings settings,
                boolean streaming,
                boolean coordinatorCommit,
                boolean emitAfterFirstCheckpoint,
                long expectedWatermark,
                boolean forceCreateSnapshot,
                boolean checkpointsAfterTasksFinish)
                throws Exception {
            final long endInputWatermark = 12345L;
            String tableName =
                    coordinatorCommit ? "T_END_INPUT_COORDINATOR" : "T_END_INPUT_OPERATOR";
            StreamExecutionEnvironment streamEnv = null;
            TableEnvironment tEnv;
            if (streaming) {
                streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
                streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
                streamEnv.setParallelism(DEFAULT_PARALLELISM);
                streamEnv.enableCheckpointing(
                        checkpointsAfterTasksFinish ? 200L : TimeUnit.HOURS.toMillis(1));
                Configuration configuration = new Configuration();
                configuration.setString(
                        "execution.checkpointing.checkpoints-after-tasks-finish",
                        String.valueOf(checkpointsAfterTasksFinish));
                if (!checkpointsAfterTasksFinish) {
                    configuration.setString("restart-strategy.type", "none");
                }
                streamEnv.configure(configuration);
                tEnv = StreamTableEnvironment.create(streamEnv);
            } else {
                tEnv = TableEnvironment.create(settings);
            }
            tEnv.getConfig()
                    .getConfiguration()
                    .setString("table.exec.resource.default-parallelism", "2");

            tEnv.executeSql(
                    "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '"
                            + tempPath
                            + "' )");
            tEnv.executeSql("USE CATALOG mycat");
            tEnv.executeSql(
                    "CREATE TABLE "
                            + tableName
                            + " (id INT, data STRING) WITH ("
                            + "'bucket' = '-1', "
                            + "'write-only' = 'true', "
                            + (coordinatorCommit
                                    ? "'sink.coordinator-commit.enabled' = 'true', "
                                    : "")
                            + (forceCreateSnapshot
                                    ? "'"
                                            + CoreOptions.COMMIT_FORCE_CREATE_SNAPSHOT.key()
                                            + "' = 'true', "
                                    : "")
                            + "'end-input.watermark' = '"
                            + endInputWatermark
                            + "')");
            int expectedRowCount;
            if (streaming) {
                // Control the final-commit contents instead of relying on the timing between
                // datagen
                // completion and periodic checkpoints. Emitting after a completed checkpoint leaves
                // a
                // non-empty END_INPUT committable; emitting before it and ending only after it has
                // completed leaves an empty one.
                DataStream<Row> source =
                        checkpointsAfterTasksFinish
                                ? streamEnv
                                        .fromSource(
                                                emitAfterFirstCheckpoint
                                                        ? new FinalCommitTestUtils
                                                                .EmitAfterFirstCheckpointSource()
                                                        : new FinalCommitTestUtils
                                                                .EmitBeforeFirstCheckpointThenFinishSource(),
                                                WatermarkStrategy.noWatermarks(),
                                                "Controlled End Input Source")
                                        .map(row -> row)
                                        .returns(Types.ROW(Types.INT, Types.STRING))
                                        .setParallelism(1)
                                : StreamExecutionEnvironmentUtils.fromData(
                                                streamEnv,
                                                Types.ROW(Types.INT, Types.STRING),
                                                Row.of(1, "end-input"))
                                        .setParallelism(1);
                StreamTableEnvironment streamTableEnv = (StreamTableEnvironment) tEnv;
                streamTableEnv.createTemporaryView("src", streamTableEnv.fromDataStream(source));
                // The checkpoint-controlled source keeps the environment parallelism, so both
                // source
                // readers emit one record. The no-checkpoint fromData source is explicitly
                // parallelism
                // one and contains one record.
                expectedRowCount = checkpointsAfterTasksFinish ? DEFAULT_PARALLELISM : 1;
            } else {
                tEnv.executeSql(
                        "CREATE TEMPORARY TABLE src (id INT, data STRING) WITH ("
                                + "'connector' = 'datagen', "
                                + "'number-of-rows' = '20', "
                                + "'rows-per-second' = '10', "
                                + "'fields.id.kind' = 'sequence', "
                                + "'fields.id.start' = '1', "
                                + "'fields.id.end' = '20', "
                                + "'fields.data.length' = '8')");
                expectedRowCount = 20;
            }

            tEnv.executeSql("INSERT INTO " + tableName + " SELECT * FROM src").await();

            FileStoreTable table =
                    (FileStoreTable)
                            ((FlinkCatalog) tEnv.getCatalog("mycat").get())
                                    .catalog()
                                    .getTable(Identifier.create("default", tableName));
            if (!checkpointsAfterTasksFinish && !coordinatorCommit) {
                assertThat(table.snapshotManager().latestSnapshot()).isNull();
                return;
            }
            waitUntilRowCount(table, expectedRowCount);
            Snapshot snapshot = table.snapshotManager().latestSnapshot();
            assertThat(snapshot).isNotNull();
            assertThat(snapshot.watermark()).isEqualTo(expectedWatermark);
        }
    }

    /** Sources used to deterministically construct the final commit. */
    private static final class FinalCommitTestUtils {

        private FinalCommitTestUtils() {}

        /** Emits the only record after checkpoint 1 has completed. */
        private static class EmitAfterFirstCheckpointSource
                extends AbstractNonCoordinatedSource<Row> {

            private static final long serialVersionUID = 1L;

            public Boundedness getBoundedness() {
                return Boundedness.BOUNDED;
            }

            @Override
            public SourceReader<Row, SimpleSourceSplit> createReader(SourceReaderContext context) {
                return new Reader();
            }

            private static class Reader extends AbstractNonCoordinatedSourceReader<Row> {

                private final SplitListState<Boolean> emittedState =
                        new SplitListState<>(
                                "emitted", value -> String.valueOf(value), Boolean::parseBoolean);
                private boolean emitted;
                private boolean firstCheckpointCompleted;

                @Override
                public InputStatus pollNext(ReaderOutput<Row> output) {
                    if (!firstCheckpointCompleted) {
                        return InputStatus.MORE_AVAILABLE;
                    }
                    if (!emitted) {
                        output.collect(Row.of(1, "end-input"));
                        emitted = true;
                        return InputStatus.MORE_AVAILABLE;
                    }
                    return InputStatus.END_OF_INPUT;
                }

                @Override
                public void addSplits(List<SimpleSourceSplit> splits) {
                    emittedState.restoreState(splits);
                    for (Boolean state : emittedState.get()) {
                        emitted = state;
                    }
                }

                @Override
                public List<SimpleSourceSplit> snapshotState(long checkpointId) {
                    emittedState.clear();
                    emittedState.add(emitted);
                    return emittedState.snapshotState();
                }

                @Override
                public void notifyCheckpointComplete(long checkpointId) {
                    firstCheckpointCompleted = true;
                }
            }
        }

        /** Emits the only record before checkpoint 1, then ends only after it has completed. */
        private static class EmitBeforeFirstCheckpointThenFinishSource
                extends AbstractNonCoordinatedSource<Row> {

            private static final long serialVersionUID = 1L;

            @Override
            public Boundedness getBoundedness() {
                return Boundedness.BOUNDED;
            }

            @Override
            public SourceReader<Row, SimpleSourceSplit> createReader(SourceReaderContext context) {
                return new Reader();
            }

            private static class Reader extends AbstractNonCoordinatedSourceReader<Row> {

                private final SplitListState<Boolean> emittedState =
                        new SplitListState<>(
                                "emitted", value -> String.valueOf(value), Boolean::parseBoolean);
                private boolean emitted;
                private boolean firstCheckpointCompleted;

                @Override
                public InputStatus pollNext(ReaderOutput<Row> output) {
                    if (!emitted) {
                        output.collect(Row.of(1, "before-checkpoint"));
                        emitted = true;
                        return InputStatus.MORE_AVAILABLE;
                    }
                    if (!firstCheckpointCompleted) {
                        return InputStatus.MORE_AVAILABLE;
                    }
                    return InputStatus.END_OF_INPUT;
                }

                @Override
                public void addSplits(List<SimpleSourceSplit> splits) {
                    emittedState.restoreState(splits);
                    for (Boolean state : emittedState.get()) {
                        emitted = state;
                    }
                }

                @Override
                public List<SimpleSourceSplit> snapshotState(long checkpointId) {
                    emittedState.clear();
                    emittedState.add(emitted);
                    return emittedState.snapshotState();
                }

                @Override
                public void notifyCheckpointComplete(long checkpointId) {
                    firstCheckpointCompleted = true;
                }
            }
        }

        /**
         * Keeps the job alive so checkpoints continue after the bounded Paimon writer has finished.
         */
        private static class KeepJobRunningSource extends AbstractNonCoordinatedSource<Row> {

            private static final long serialVersionUID = 1L;

            @Override
            public Boundedness getBoundedness() {
                return Boundedness.CONTINUOUS_UNBOUNDED;
            }

            @Override
            public SourceReader<Row, SimpleSourceSplit> createReader(SourceReaderContext context) {
                return new Reader();
            }

            private static class Reader extends AbstractNonCoordinatedSourceReader<Row> {

                @Override
                public InputStatus pollNext(ReaderOutput<Row> output) {
                    return InputStatus.NOTHING_AVAILABLE;
                }
            }
        }
    }

    /** Test doubles used to validate end-input event and checkpoint ordering. */
    private static final class EndInputValidationTestUtils {

        private EndInputValidationTestUtils() {}

        /** Emits the only record after the coordinator has observed checkpoint completion. */
        private static class EmitAfterCoordinatorCheckpointCompleteSource
                extends AbstractNonCoordinatedSource<Row> {

            private static final long serialVersionUID = 1L;

            @Override
            public Boundedness getBoundedness() {
                return Boundedness.BOUNDED;
            }

            @Override
            public SourceReader<Row, SimpleSourceSplit> createReader(SourceReaderContext context) {
                return new Reader();
            }

            private static class Reader extends AbstractNonCoordinatedSourceReader<Row> {

                private boolean emitted;

                @Override
                public InputStatus pollNext(ReaderOutput<Row> output) {
                    if (!COORDINATOR_CHECKPOINT_COMPLETED.get()) {
                        return InputStatus.MORE_AVAILABLE;
                    }
                    if (!emitted) {
                        output.collect(Row.of(1, "end-input"));
                        emitted = true;
                        return InputStatus.MORE_AVAILABLE;
                    }
                    return InputStatus.END_OF_INPUT;
                }

                @Override
                public void addSplits(List<SimpleSourceSplit> splits) {}
            }
        }

        /**
         * Test-only sink which exposes coordinator checkpoint completion to the controlled source.
         */
        private static class CheckpointTrackingRowAppendTableSink extends RowAppendTableSink {

            private static final long serialVersionUID = 1L;

            private final boolean delayEndInputUntilCheckpointComplete;

            private CheckpointTrackingRowAppendTableSink(FileStoreTable table) {
                this(table, false);
            }

            private CheckpointTrackingRowAppendTableSink(
                    FileStoreTable table, boolean delayEndInputUntilCheckpointComplete) {
                super(table, null, 1);
                this.delayEndInputUntilCheckpointComplete = delayEndInputUntilCheckpointComplete;
            }

            @Override
            protected OneInputStreamOperatorFactory<InternalRow, Committable>
                    createWriteOperatorFactory(
                            StoreSinkWrite.Provider writeProvider,
                            String commitUser,
                            boolean streamingCheckpointEnabled,
                            Long endInputWatermark) {
                OneInputStreamOperatorFactory<InternalRow, Committable> delegate =
                        super.createWriteOperatorFactory(
                                writeProvider,
                                commitUser,
                                streamingCheckpointEnabled,
                                endInputWatermark);
                return new CheckpointTrackingOperatorFactory(
                        delegate, delayEndInputUntilCheckpointComplete);
            }
        }

        private static class CheckpointTrackingOperatorFactory
                implements OneInputStreamOperatorFactory<InternalRow, Committable>,
                        CoordinatedOperatorFactory<Committable> {

            private static final long serialVersionUID = 1L;

            private final OneInputStreamOperatorFactory<InternalRow, Committable> delegate;
            private final CoordinatedOperatorFactory<Committable> coordinatedDelegate;
            private final boolean delayEndInputUntilCheckpointComplete;

            @SuppressWarnings("unchecked")
            private CheckpointTrackingOperatorFactory(
                    OneInputStreamOperatorFactory<InternalRow, Committable> delegate,
                    boolean delayEndInputUntilCheckpointComplete) {
                this.delegate = delegate;
                this.coordinatedDelegate = (CoordinatedOperatorFactory<Committable>) delegate;
                this.delayEndInputUntilCheckpointComplete = delayEndInputUntilCheckpointComplete;
            }

            @Override
            public OperatorCoordinator.Provider getCoordinatorProvider(
                    String operatorName, OperatorID operatorID) {
                return new CheckpointTrackingCoordinatorProvider(
                        coordinatedDelegate.getCoordinatorProvider(operatorName, operatorID),
                        delayEndInputUntilCheckpointComplete);
            }

            @Override
            public <T extends StreamOperator<Committable>> T createStreamOperator(
                    StreamOperatorParameters<Committable> parameters) {
                return delegate.createStreamOperator(parameters);
            }

            @Override
            public void setChainingStrategy(ChainingStrategy strategy) {
                delegate.setChainingStrategy(strategy);
            }

            @Override
            public ChainingStrategy getChainingStrategy() {
                return delegate.getChainingStrategy();
            }

            @Override
            public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
                return delegate.getStreamOperatorClass(classLoader);
            }
        }

        private static class CheckpointTrackingCoordinatorProvider
                implements OperatorCoordinator.Provider {

            private static final long serialVersionUID = 1L;

            private final OperatorCoordinator.Provider delegate;
            private final boolean delayEndInputUntilCheckpointComplete;

            private CheckpointTrackingCoordinatorProvider(
                    OperatorCoordinator.Provider delegate,
                    boolean delayEndInputUntilCheckpointComplete) {
                this.delegate = delegate;
                this.delayEndInputUntilCheckpointComplete = delayEndInputUntilCheckpointComplete;
            }

            @Override
            public OperatorID getOperatorId() {
                return delegate.getOperatorId();
            }

            @Override
            public OperatorCoordinator create(OperatorCoordinator.Context context)
                    throws Exception {
                return new CheckpointTrackingCoordinator(
                        delegate.create(context), delayEndInputUntilCheckpointComplete);
            }
        }

        private static class CheckpointTrackingCoordinator implements OperatorCoordinator {

            private final OperatorCoordinator delegate;
            private final boolean delayEndInputUntilCheckpointComplete;
            private boolean endInputEventDelayed;
            private boolean delayedEndInputReleased;

            private CheckpointTrackingCoordinator(
                    OperatorCoordinator delegate, boolean delayEndInputUntilCheckpointComplete) {
                this.delegate = delegate;
                this.delayEndInputUntilCheckpointComplete = delayEndInputUntilCheckpointComplete;
            }

            @Override
            public void start() throws Exception {
                delegate.start();
            }

            @Override
            public void close() throws Exception {
                delegate.close();
            }

            @Override
            public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
                    throws Exception {
                if (event instanceof RestoredCommittableEvent
                        && containsNonEmptyEndInput((RestoredCommittableEvent) event)) {
                    END_INPUT_COMMITTABLE_RESTORED.set(true);
                    delayedEndInputReleased = true;
                }
                if (delayEndInputUntilCheckpointComplete
                        && !delayedEndInputReleased
                        && event instanceof CommittableEvent
                        && ((CommittableEvent) event).getCheckpointId() == Long.MAX_VALUE) {
                    endInputEventDelayed = true;
                    END_INPUT_EVENT_DELAYED.set(true);
                    return;
                }
                delegate.handleEventFromOperator(subtask, attemptNumber, event);
            }

            private boolean containsNonEmptyEndInput(RestoredCommittableEvent event)
                    throws Exception {
                return event.deserialize(createCheckpointCommittablesSerializer()).stream()
                        .anyMatch(
                                committables ->
                                        committables.checkpointId() == Long.MAX_VALUE
                                                && !committables.isEmpty());
            }

            private TypeSerializer<CheckpointCommittables>
                    createCheckpointCommittablesSerializer() {
                return new SimpleVersionedSerializerTypeSerializerProxy<>(
                        () ->
                                new CheckpointCommittablesSerializer(
                                        new CommittableSerializer(new CommitMessageSerializer())));
            }

            @Override
            public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
                    throws Exception {
                delegate.checkpointCoordinator(checkpointId, result);
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) {
                delegate.notifyCheckpointComplete(checkpointId);
                COORDINATOR_CHECKPOINT_COMPLETED.set(true);
                if (endInputEventDelayed) {
                    delayedEndInputReleased = true;
                    CHECKPOINT_COMPLETION_OVERTAKES_END_INPUT.set(true);
                    // The old attempt's event is intentionally dropped. The completion callback
                    // above
                    // triggers failover, and the test must prove that the checkpointed writer state
                    // replays the END_INPUT committable to the new coordinator.
                    endInputEventDelayed = false;
                }
            }

            @Override
            public void notifyCheckpointAborted(long checkpointId) {
                delegate.notifyCheckpointAborted(checkpointId);
            }

            @Override
            public void resetToCheckpoint(long checkpointId, byte[] checkpointData)
                    throws Exception {
                delegate.resetToCheckpoint(checkpointId, checkpointData);
            }

            @Override
            public void subtaskReset(int subtask, long checkpointId) {
                delegate.subtaskReset(subtask, checkpointId);
            }

            @Override
            public void executionAttemptFailed(int subtask, int attemptNumber, Throwable reason) {
                delegate.executionAttemptFailed(subtask, attemptNumber, reason);
            }

            @Override
            public void executionAttemptReady(
                    int subtask, int attemptNumber, SubtaskGateway gateway) {
                delegate.executionAttemptReady(subtask, attemptNumber, gateway);
            }
        }
    }

    private long readRowCount(FileStoreTable table) throws Exception {
        RecordReader<InternalRow> reader =
                table.newRead().createReader(table.newSnapshotReader().read());
        long rowCount = 0L;
        try (CloseableIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            while (iterator.hasNext()) {
                iterator.next();
                rowCount++;
            }
        }
        return rowCount;
    }

    private void waitUntilRowCount(FileStoreTable table, long expectedRowCount) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        long rowCount;
        do {
            rowCount = readRowCount(table);
            if (rowCount == expectedRowCount) {
                return;
            }
            Thread.sleep(100);
        } while (System.currentTimeMillis() < deadline);

        assertThat(rowCount).isEqualTo(expectedRowCount);
    }

    private long triggerCheckpointWhenTasksRunning(
            ClusterClient<?> clusterClient, JobClient jobClient) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        Exception lastFailure = null;
        do {
            try {
                return clusterClient
                        .triggerCheckpoint(jobClient.getJobID(), CheckpointType.CONFIGURED)
                        .get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                boolean jobMasterInitializing = containsMessage(e, "JobManager is initializing");
                boolean tasksNotRunning =
                        containsMessage(e, "Not all required tasks are currently running");
                if (!jobMasterInitializing && !tasksNotRunning) {
                    throw e;
                }
                lastFailure = e;
            }
            Thread.sleep(100);
        } while (System.currentTimeMillis() < deadline);

        throw new AssertionError("Tasks did not become checkpointable in time.", lastFailure);
    }

    private boolean containsMessage(Throwable throwable, String expected) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current.getMessage() != null && current.getMessage().contains(expected)) {
                return true;
            }
        }
        return false;
    }

    private void waitUntilTrue(AtomicBoolean condition, String description) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (!condition.get() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertThat(condition.get()).as(description).isTrue();
    }
}
