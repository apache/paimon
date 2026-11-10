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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.sink.state.CoordinatorState;
import org.apache.paimon.flink.sink.state.CoordinatorStateSerializer;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for JM-side commit on unaware-bucket append tables. */
public class CoordinatorCommitITCase {

    private static final int DEFAULT_PARALLELISM = 2;
    private static final int SCRIPTED_PARALLELISM = 2;
    private static final long WAIT_TIMEOUT_MILLIS = 60_000L;
    private static final InMemoryReporter reporter = InMemoryReporter.create();

    @RegisterExtension
    protected static final org.apache.paimon.flink.util.MiniClusterWithClientExtension
            MINI_CLUSTER_EXTENSION =
                    new org.apache.paimon.flink.util.MiniClusterWithClientExtension(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setConfiguration(miniClusterConfiguration())
                                    .build());

    @TempDir Path tempPath;

    private static Configuration miniClusterConfiguration() {
        Configuration configuration = reporter.addToConfiguration(new Configuration());
        configuration.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        return configuration;
    }

    @AfterEach
    public final void cleanupRunningJobs() throws Exception {
        RestClusterClient<?> clusterClient = MINI_CLUSTER_EXTENSION.createRestClusterClient();
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

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testCoordinatorCommitRemovesGlobalCommitterOperator() throws Exception {
        RunningJob coordinatorCommitJob = startStreamingInsert(true);
        waitUntilWriterMetricGroupsRegistered(coordinatorCommitJob.jobId);
        assertThat(findGlobalCommitterMetricGroups(coordinatorCommitJob.jobId)).isEmpty();
        coordinatorCommitJob.cancel();

        RunningJob defaultCommitJob = startStreamingInsert(false);
        waitUntilGlobalCommitterMetricGroupsRegistered(defaultCommitJob.jobId);
        defaultCommitJob.cancel();
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testCoordinatorCommitMetricsAndCommittedRows() throws Exception {
        RunningJob runningJob = startStreamingInsert(true);
        waitUntilWriterInputRecords(runningJob.jobId);
        waitUntilCoordinatorCommitMetricsRegistered(runningJob.jobId);
        assertThat(findGlobalCommitterMetricGroups(runningJob.jobId)).isEmpty();
        waitUntilRowsCommitted(runningJob);
        runningJob.cancel();

        assertThat(readRowCount(runningJob.table)).isGreaterThan(0L);
    }

    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @Test
    public void testCoordinatorCommitWritesDataEvolutionTableWithUnalignedCheckpoints()
            throws Exception {
        RunningJob runningJob = startStreamingInsert(true, true, true);
        assertThat(runningJob.table.coreOptions().dataEvolutionEnabled()).isTrue();
        assertThat(runningJob.table.coreOptions().rowTrackingEnabled()).isTrue();
        waitUntilWriterInputRecords(runningJob.jobId);
        waitUntilCoordinatorCommitMetricsRegistered(runningJob.jobId);
        waitUntilRowsCommitted(runningJob);
        runningJob.cancel();

        assertThat(readRowCount(runningJob.table)).isGreaterThan(0L);
    }

    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @Test
    public void testCoordinatorCommitEndInputFinalizesAndFinishes() throws Exception {
        String tableName = "T_COORDINATOR_END_INPUT";
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.executeSql(
                "CREATE CATALOG endinputcat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG endinputcat");
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, data STRING) WITH ("
                        + "'bucket' = '-1', 'write-only' = 'true', "
                        + "'sink.coordinator-commit.enabled' = 'true')");
        FileStoreTable table =
                (FileStoreTable)
                        ((FlinkCatalog) tEnv.getCatalog("endinputcat").get())
                                .catalog()
                                .getTable(Identifier.create("default", tableName));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(SCRIPTED_PARALLELISM);
        env.enableCheckpointing(200L);
        DataStreamSource<RowData> source =
                env.fromSource(
                                new EndInputSource(),
                                org.apache.flink.api.common.eventtime.WatermarkStrategy
                                        .noWatermarks(),
                                "coordinator-end-input-source")
                        .setParallelism(SCRIPTED_PARALLELISM);
        new FlinkSinkBuilder(table).forRowData(source).build();

        JobClient client = env.executeAsync("coordinator-end-input");
        try {
            waitUntilRowsCommitted(new RunningJob(table, client));
            assertThat(readRowCount(table)).isEqualTo(2L);
            client.getJobExecutionResult().get(120, TimeUnit.SECONDS);
            assertThat(client.getJobStatus().get()).isEqualTo(JobStatus.FINISHED);
            assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                    .isEqualTo(Long.MAX_VALUE);
            assertThat(readRowCount(table)).isEqualTo(2L);
        } finally {
            if (!client.getJobStatus().get().isTerminalState()) {
                client.cancel().get(30, TimeUnit.SECONDS);
            }
        }
    }

    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @Test
    public void testEarlyTerminalWriterFinishesWhileOtherWriterRemainsRunning() throws Exception {
        String tableName = "T_COORDINATOR_EARLY_END_INPUT";
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.executeSql(
                "CREATE CATALOG earlyendcat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG earlyendcat");
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, data STRING) WITH ("
                        + "'bucket' = '-1', 'write-only' = 'true', "
                        + "'sink.coordinator-commit.enabled' = 'true')");
        FileStoreTable table =
                (FileStoreTable)
                        ((FlinkCatalog) tEnv.getCatalog("earlyendcat").get())
                                .catalog()
                                .getTable(Identifier.create("default", tableName));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(SCRIPTED_PARALLELISM);
        env.enableCheckpointing(200L);
        DataStreamSource<RowData> source =
                env.fromSource(
                                new AsymmetricEndInputSource(),
                                org.apache.flink.api.common.eventtime.WatermarkStrategy
                                        .noWatermarks(),
                                "coordinator-early-end-input-source")
                        .setParallelism(SCRIPTED_PARALLELISM);
        new FlinkSinkBuilder(table).forRowData(source).build();

        JobClient client = env.executeAsync("coordinator-early-end-input");
        try {
            // Subtask 0 reaches EndInput earlier and can finish with early permission.
            // Subtask 1 remains active, so the job stays RUNNING without global MAX
            // finalization. Ordinary data is committed independently of writer completion.
            waitUntilWriterTasks(client, 1, 1);
            assertThat(client.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);
            waitUntilRowsCommitted(new RunningJob(table, client));
            assertThat(readRowCount(table)).isEqualTo(1L);
            assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                    .isLessThan(Long.MAX_VALUE);
        } finally {
            if (!client.getJobStatus().get().isTerminalState()) {
                client.cancel().get(30, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Exercises an actual full-job restart after an early-finished writer has been covered by two
     * completed checkpoints. This deliberately uses the MiniCluster checkpoint API and a task
     * failure, rather than invoking an operator coordinator reset directly.
     */
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @Test
    public void testPartiallyFinishedWriterRestoresAfterGlobalFailover() throws Exception {
        testPartialWriterFailover("full");
    }

    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @Test
    public void testPartiallyFinishedWriterRestoresAfterRegionFailover() throws Exception {
        testPartialWriterFailover("region");
    }

    private void testPartialWriterFailover(String failoverStrategy) throws Exception {
        PartialFinishControl.reset();
        FileStoreTable table = createCoordinatorCommitTable("T_PARTIAL_FINISH_FAILOVER");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(SCRIPTED_PARALLELISM);
        env.enableCheckpointing(TimeUnit.HOURS.toMillis(1));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        Configuration failoverConfiguration = new Configuration();
        failoverConfiguration.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        failoverConfiguration.setString("restart-strategy.type", "fixed-delay");
        failoverConfiguration.setString("restart-strategy.fixed-delay.attempts", "1");
        failoverConfiguration.setString("restart-strategy.fixed-delay.delay", "0 ms");
        failoverConfiguration.setString("jobmanager.execution.failover-strategy", failoverStrategy);
        env.configure(failoverConfiguration, getClass().getClassLoader());

        DataStreamSource<RowData> source =
                env.fromSource(
                                new PartiallyFinishedFailoverSource(),
                                org.apache.flink.api.common.eventtime.WatermarkStrategy
                                        .noWatermarks(),
                                "partial-finish-failover-source")
                        .setParallelism(SCRIPTED_PARALLELISM);
        new FlinkSinkBuilder(table).forRowData(source).build();

        JobClient client = env.executeAsync("partial-finish-global-failover");
        try {
            MiniCluster miniCluster = miniCluster(client);
            waitUntilJobRunning(client);
            assertThat(
                            PartialFinishControl.readersStarted.await(
                                    WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                    .isTrue();
            waitUntilWriterTasks(client, 0, 2);
            WriterAttempts initial = writerAttempts(miniCluster, client.getJobID());
            assertThat(initial.running).isEqualTo(2);
            assertThat(initial.finished).isZero();

            long c0 = triggerCheckpoint(miniCluster, client.getJobID());
            assertThat(
                            PartialFinishControl.baselineCheckpointCompleted.await(
                                    WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                    .isTrue();
            PartialFinishControl.allowW0EndInput.countDown();
            PartialFinishControl.w0Availability.complete(null);
            assertThat(
                            PartialFinishControl.w0EndInputReached.await(
                                    WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                    .isTrue();

            // C1 captures and commits W0's terminal tail.
            long c1 = triggerCheckpoint(miniCluster, client.getJobID());
            waitUntilWriterTasks(client, 1, 1);
            WriterAttempts before = writerAttempts(miniCluster, client.getJobID());
            assertThat(before.finished).isEqualTo(1);
            assertThat(before.running).isEqualTo(1);

            // C2 persists the coordinator state after W0's C1 terminal tail was committed.
            long c2 = triggerCheckpoint(miniCluster, client.getJobID());
            PartialFinishControl.allowFailure.set(true);
            PartialFinishControl.failAfterC2.countDown();
            PartialFinishControl.failureAvailability.complete(null);
            waitUntilRestarted(miniCluster, client.getJobID());
            WriterAttempts after = writerAttempts(miniCluster, client.getJobID());
            assertThat(after.running).isEqualTo(1);
            assertThat(after.finished).isEqualTo(1);
            assertThat(after.runningAttempt).isGreaterThan(before.runningAttempt);
            assertThat(after.finishedAttempt).isEqualTo(before.finishedAttempt);

            // The restarted W1 produces a record, and C3 proves that checkpoint/commit remains
            // live after restoring the coordinator state containing terminalCoveredBy for W0.
            assertThat(
                            PartialFinishControl.postFailoverRecordEmitted.await(
                                    WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                    .isTrue();
            long c3 = triggerCheckpoint(miniCluster, client.getJobID());
            waitUntilRowCount(table, 3L);
            assertThat(client.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);
            assertThat(c0).isLessThan(c1);
            assertThat(c1).isLessThan(c2);
            assertThat(c2).isLessThan(c3);
        } finally {
            if (!client.getJobStatus().get().isTerminalState()) {
                client.cancel().get(30, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * A partial-finished checkpoint/savepoint does not use FINISHED_ON_RESTORE for the whole writer
     * vertex. The restored W0 is therefore a fresh running attempt. Its first ordinary empty
     * checkpoint must be accepted even though the coordinator has durable coverage for W0's
     * terminal tail from the original job.
     */
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @Test
    public void testPartiallyFinishedWriterRestoresFromSavepointWithEmptyCheckpoint()
            throws Exception {
        testPartialSavepointRestore(RestoreBehavior.EMPTY_CHECKPOINT);
    }

    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @ParameterizedTest
    @EnumSource(
            value = RestoreBehavior.class,
            names = {"NEW_RECORD", "IMMEDIATE_END_INPUT"})
    public void testPartiallyFinishedWriterRestoresFromSavepoint(RestoreBehavior behavior)
            throws Exception {
        testPartialSavepointRestore(behavior);
    }

    private void testPartialSavepointRestore(RestoreBehavior behavior) throws Exception {
        PartialSavepointControl.reset();
        FileStoreTable table =
                createCoordinatorCommitTable("T_PARTIAL_SAVEPOINT_RESTORE")
                        .copy(Collections.singletonMap("commit.force-create-snapshot", "true"));
        JobClient firstClient = startPartialSavepointJob(table, null);
        String savepointPath;
        try {
            MiniCluster cluster = miniCluster(firstClient);
            waitUntilJobRunning(firstClient);
            waitUntilWriterTasks(firstClient, 0, 2);
            triggerCheckpoint(cluster, firstClient.getJobID());

            PartialSavepointControl.allowW0EndInput.countDown();
            PartialSavepointControl.firstAvailability.complete(null);
            assertThat(
                            PartialSavepointControl.w0EndInputReached.await(
                                    WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                    .isTrue();
            triggerCheckpoint(cluster, firstClient.getJobID());
            waitUntilWriterTasks(firstClient, 1, 1);
            waitUntilRowCount(table, 1L);

            savepointPath =
                    firstClient
                            .triggerSavepoint(
                                    tempPath.resolve("partial-savepoint").toString(),
                                    SavepointFormatType.DEFAULT)
                            .get(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            OperatorState writerState = loadWriterState(savepointPath, false);
            assertThat(writerState.getState(0).isFinished()).isTrue();
            assertThat(writerState.getState(1).isFinished()).isFalse();
            assertThat(writerState.getCoordinatorState()).isNotNull();
            CoordinatorState coordinatorState =
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            new CoordinatorStateSerializer(),
                            writerState.getCoordinatorState().getData());
            assertThat(coordinatorState.getTerminalCoveredBy()).hasSize(2);
            assertThat(coordinatorState.getTerminalCoveredBy()[0]).isGreaterThanOrEqualTo(0);
            assertThat(coordinatorState.getTerminalCoveredBy()[1]).isEqualTo(-1);
        } finally {
            if (!firstClient.getJobStatus().get().isTerminalState()) {
                firstClient.cancel().get(30, TimeUnit.SECONDS);
            }
        }

        PartialSavepointControl.beginRestore(behavior);
        JobClient restoredClient = startPartialSavepointJob(table, savepointPath);
        try {
            MiniCluster cluster = miniCluster(restoredClient);
            if (behavior != RestoreBehavior.IMMEDIATE_END_INPUT) {
                waitUntilJobRunning(restoredClient);
                waitUntilWriterTasks(restoredClient, 0, 2);
            }

            if (behavior == RestoreBehavior.NEW_RECORD) {
                assertThat(
                                PartialSavepointControl.restoredRecordEmitted.await(
                                        WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                        .isTrue();
            }
            if (behavior == RestoreBehavior.IMMEDIATE_END_INPUT) {
                assertThat(
                                PartialSavepointControl.restoredEndInputReached.await(
                                        WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                        .isTrue();
            } else {
                // Wait for the external commit, not only Flink's checkpoint future: commit
                // failures are asynchronous and must not race with a RUNNING assertion.
                long checkpoint = triggerCheckpoint(cluster, restoredClient.getJobID());
                waitUntilCheckpointCommitted(restoredClient, table, checkpoint);
                triggerCheckpoint(cluster, restoredClient.getJobID());
                assertThat(readRowCount(table))
                        .isEqualTo(behavior == RestoreBehavior.NEW_RECORD ? 2L : 1L);
                PartialSavepointControl.finishRestored.set(true);
                PartialSavepointControl.restoredAvailability.complete(null);
                assertThat(
                                PartialSavepointControl.restoredEndInputReached.await(
                                        WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                        .isTrue();
            }
            restoredClient.getJobExecutionResult().get(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            assertThat(readRowCount(table))
                    .isEqualTo(behavior == RestoreBehavior.NEW_RECORD ? 2L : 1L);
            assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                    .isEqualTo(Long.MAX_VALUE);
        } finally {
            if (!restoredClient.getJobStatus().get().isTerminalState()) {
                restoredClient.cancel().get(30, TimeUnit.SECONDS);
            }
        }
    }

    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @Test
    public void testFullyFinishedWriterRestoresAfterGlobalFailover() throws Exception {
        FullyFinishedControl.reset();
        FileStoreTable table = createCoordinatorCommitTable("T_FULLY_FINISHED_FAILOVER");
        JobClient client = startFullyFinishedJob(table, null);
        try {
            MiniCluster cluster = miniCluster(client);
            waitUntilJobRunning(client);
            waitUntilWriterTasks(client, 0, 2);
            waitUntilVertexTasksRunning(cluster, client.getJobID(), "still-running-source", 1);
            long c0 = triggerCheckpoint(cluster, client.getJobID());
            FullyFinishedControl.releaseEndInput.countDown();
            FullyFinishedControl.availability.complete(null);
            assertThat(
                            FullyFinishedControl.endInputReached.await(
                                    WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                    .isTrue();
            long c1 = triggerCheckpoint(cluster, client.getJobID());
            waitUntilWriterTasks(client, 2, 0);
            assertThat(client.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);

            long c2 = triggerCheckpoint(cluster, client.getJobID());
            FullyFinishedControl.allowFailure.set(true);
            FullyFinishedControl.failureAvailability.complete(null);
            waitUntilBranchRestarted(cluster, client.getJobID());
            long c3 = triggerCheckpoint(cluster, client.getJobID());
            waitUntilRowCount(table, 2L);
            assertThat(client.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);
            assertThat(c0).isLessThan(c1);
            assertThat(c1).isLessThan(c2);
            assertThat(c2).isLessThan(c3);
        } finally {
            if (!client.getJobStatus().get().isTerminalState()) {
                client.cancel().get(30, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testFullyFinishedWriterRestoresFromSavepoint() throws Exception {
        FullyFinishedControl.reset();
        FileStoreTable table = createCoordinatorCommitTable("T_FULLY_FINISHED_SAVEPOINT");
        JobClient client = startFullyFinishedJob(table, null);
        String savepointPath;
        long snapshotId;
        OperatorID writerId;
        try {
            MiniCluster cluster = miniCluster(client);
            waitUntilWriterTasks(client, 0, 2);
            waitUntilVertexTasksRunning(cluster, client.getJobID(), "still-running-source", 1);
            triggerCheckpoint(cluster, client.getJobID());
            String baseline =
                    client.triggerSavepoint(
                                    tempPath.resolve("fully-running-savepoint").toString(),
                                    SavepointFormatType.DEFAULT)
                            .get(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            writerId = loadWriterState(baseline, false).getOperatorID();
            FullyFinishedControl.releaseEndInput.countDown();
            FullyFinishedControl.availability.complete(null);
            assertThat(
                            FullyFinishedControl.endInputReached.await(
                                    WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                    .isTrue();
            triggerCheckpoint(cluster, client.getJobID());
            waitUntilWriterTasks(client, 2, 0);
            savepointPath =
                    client.triggerSavepoint(
                                    tempPath.resolve("fully-finished-savepoint").toString(),
                                    SavepointFormatType.DEFAULT)
                            .get(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            OperatorState state =
                    loadMetadata(savepointPath).getOperatorStates().stream()
                            .filter(candidate -> candidate.getOperatorID().equals(writerId))
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("Missing writer state"));
            assertThat(state.isFullyFinished()).isTrue();
            assertThat(state.getCoordinatorState()).isNull();
            assertThat(readRowCount(table)).isEqualTo(2);
            assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                    .isEqualTo(Long.MAX_VALUE);
            snapshotId = table.snapshotManager().latestSnapshotId();
        } finally {
            client.cancel().get(30, TimeUnit.SECONDS);
        }
        JobClient restored = startFullyFinishedJob(table, savepointPath);
        try {
            MiniCluster cluster = miniCluster(restored);
            // FINISHED_ON_RESTORE tasks can remain RUNNING while the other branch is active.
            // Verify that they did not start new source readers instead of inferring it from
            // the scheduler's task status.
            waitUntilWriterTasks(restored, 0, 2);
            waitUntilVertexTasksRunning(cluster, restored.getJobID(), "still-running-source", 1);
            assertThat(FullyFinishedControl.readersStarted.get()).isEqualTo(2);
            triggerCheckpoint(cluster, restored.getJobID());
            // This first checkpoint releases Flink's FINISHED_ON_RESTORE tasks. Wait until
            // the scheduler observes their termination before requesting another checkpoint.
            waitUntilWriterTasks(restored, 2, 0);
            triggerCheckpoint(cluster, restored.getJobID());
            assertThat(restored.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshotId);
            assertThat(readRowCount(table)).isEqualTo(2);
        } finally {
            if (!restored.getJobStatus().get().isTerminalState()) {
                restored.cancel().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private JobClient startFullyFinishedJob(FileStoreTable table, String savepointPath)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(SCRIPTED_PARALLELISM);
        env.enableCheckpointing(TimeUnit.HOURS.toMillis(1));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        configuration.setString("restart-strategy.type", "fixed-delay");
        configuration.setString("restart-strategy.fixed-delay.attempts", "1");
        configuration.setString("restart-strategy.fixed-delay.delay", "0 ms");
        configuration.setString("jobmanager.execution.failover-strategy", "full");
        if (savepointPath != null) {
            SavepointRestoreSettings.toConfiguration(
                    SavepointRestoreSettings.forPath(savepointPath, false), configuration);
            configuration.setString("restart-strategy.type", "none");
        }
        env.configure(configuration, getClass().getClassLoader());

        DataStreamSource<RowData> finite =
                env.fromSource(
                                new FullyFinishedSource(),
                                org.apache.flink.api.common.eventtime.WatermarkStrategy
                                        .noWatermarks(),
                                "fully-finished-source")
                        .setParallelism(SCRIPTED_PARALLELISM);
        new FlinkSinkBuilder(table).forRowData(finite).build();
        env.fromSource(
                        new FailingBranchSource(),
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "still-running-source")
                .setParallelism(1)
                .sinkTo(new DiscardingSink<>());

        return env.executeAsync("fully-finished-global-failover");
    }

    /**
     * Idle watermark parity: the snapshot watermark observed with coordinator-commit enabled must
     * match the one produced by the classic {@code CommitterOperator} path under the same input
     * script. See {@link IdleWatermarkScriptedSource} for the three scenarios covered — steady
     * multi-active min, one subtask idle, and all subtasks idle.
     */
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    @Test
    public void testIdleWatermarkParityAcrossCommitPaths() throws Exception {
        for (IdleWatermarkScriptedSource.Scenario scenario :
                IdleWatermarkScriptedSource.Scenario.values()) {
            long coordinatorWatermark = runIdleWatermarkScenario(scenario, true);
            long committerWatermark = runIdleWatermarkScenario(scenario, false);
            assertThat(coordinatorWatermark)
                    .describedAs(
                            "coordinator vs committer snapshot watermark mismatch for scenario "
                                    + scenario)
                    .isEqualTo(committerWatermark);
            assertThat(coordinatorWatermark)
                    .describedAs("scenario " + scenario + " expected watermark")
                    .isEqualTo(scenario.expectedWatermark);
        }
    }

    private long runIdleWatermarkScenario(
            IdleWatermarkScriptedSource.Scenario scenario, boolean coordinatorCommitEnabled)
            throws Exception {
        String tableName =
                "T_IDLE_"
                        + scenario.name()
                        + "_"
                        + (coordinatorCommitEnabled ? "COORD" : "CLASSIC");
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "200 ms");
        tEnv.executeSql(
                "CREATE CATALOG idlecat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "/"
                        + scenario.name()
                        + '_'
                        + coordinatorCommitEnabled
                        + "' )");
        tEnv.executeSql("USE CATALOG idlecat");
        // write-only=true is kept identical across both paths so the only variable under test is
        // sink.coordinator-commit.enabled; otherwise compaction snapshots would perturb the
        // "latest snapshot watermark" observations differently between paths.
        String coordinatorOption =
                coordinatorCommitEnabled ? ", 'sink.coordinator-commit.enabled' = 'true'" : "";
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, data STRING) WITH ("
                        + "'bucket' = '-1', 'write-only' = 'true'"
                        + coordinatorOption
                        + ")");

        FileStoreTable table =
                (FileStoreTable)
                        ((FlinkCatalog) tEnv.getCatalog("idlecat").get())
                                .catalog()
                                .getTable(Identifier.create("default", tableName));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(SCRIPTED_PARALLELISM);
        env.enableCheckpointing(200);

        DataStreamSource<RowData> source =
                env.fromSource(
                                new IdleWatermarkScriptedSource(scenario),
                                org.apache.flink.api.common.eventtime.WatermarkStrategy
                                        .noWatermarks(),
                                "idle-watermark-source")
                        .setParallelism(SCRIPTED_PARALLELISM);

        new FlinkSinkBuilder(table).forRowData(source).build();

        JobClient client = env.executeAsync("idle-watermark-" + tableName);
        JobID jobId = client.getJobID();
        try {
            // The option only expresses intent; a failed precondition would silently fall back to
            // the classic path. Confirm the intended path is actually running by waiting for its
            // own positive metric signal before trusting any watermark observation.
            if (coordinatorCommitEnabled) {
                waitUntilCoordinatorCommitMetricsRegistered(jobId);
            } else {
                waitUntilGlobalCommitterMetricGroupsRegistered(jobId);
            }

            long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
            long observedWatermark = Long.MIN_VALUE;
            while (System.currentTimeMillis() < deadline) {
                Snapshot snapshot = table.snapshotManager().latestSnapshot();
                if (snapshot != null && snapshot.watermark() != null) {
                    observedWatermark = snapshot.watermark();
                    if (observedWatermark >= scenario.expectedWatermark) {
                        break;
                    }
                }
                Thread.sleep(200);
            }
            return observedWatermark;
        } finally {
            client.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    private RunningJob startStreamingInsert(boolean coordinatorCommitEnabled) throws Exception {
        return startStreamingInsert(coordinatorCommitEnabled, false, false);
    }

    private RunningJob startStreamingInsert(
            boolean coordinatorCommitEnabled,
            boolean dataEvolutionEnabled,
            boolean unalignedCheckpointsEnabled)
            throws Exception {
        String tableName = coordinatorCommitEnabled ? "T_COORDINATOR_COMMIT" : "T_DEFAULT_COMMIT";
        if (dataEvolutionEnabled) {
            tableName += "_DATA_EVOLUTION";
        }
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "200 ms");
        tEnv.getConfig()
                .getConfiguration()
                .setString(
                        "execution.checkpointing.unaligned.enabled",
                        Boolean.toString(unalignedCheckpointsEnabled));
        if (unalignedCheckpointsEnabled) {
            tEnv.getConfig().getConfiguration().setString("restart-strategy.type", "none");
        }

        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG mycat");
        String coordinatorCommitOption =
                coordinatorCommitEnabled
                        ? ", 'sink.coordinator-commit.enabled' = 'true', 'write-only' = 'true'"
                        : "";
        String dataEvolutionOptions =
                dataEvolutionEnabled
                        ? ", 'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true'"
                        : "";
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, data STRING) WITH ("
                        + "'bucket' = '-1'"
                        + coordinatorCommitOption
                        + dataEvolutionOptions
                        + ")");
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE src (id INT, data STRING) WITH ("
                        + "'connector' = 'datagen', "
                        + "'rows-per-second' = '20')");
        TableResult tableResult =
                tEnv.executeSql("INSERT INTO " + tableName + " SELECT * FROM src");
        JobClient client = tableResult.getJobClient().get();
        FileStoreTable table =
                (FileStoreTable)
                        ((FlinkCatalog) tEnv.getCatalog("mycat").get())
                                .catalog()
                                .getTable(Identifier.create("default", tableName));
        return new RunningJob(table, client);
    }

    private List<?> findGlobalCommitterMetricGroups(JobID jobId) {
        return reporter.findOperatorMetricGroups(jobId, "Global Committer.*");
    }

    private void waitUntilWriterMetricGroupsRegistered(JobID jobId) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (reporter.findOperatorMetricGroups(jobId, "Writer.*").isEmpty()
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
        }
        assertThat(reporter.findOperatorMetricGroups(jobId, "Writer.*")).isNotEmpty();
    }

    private void waitUntilGlobalCommitterMetricGroupsRegistered(JobID jobId) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (findGlobalCommitterMetricGroups(jobId).isEmpty()
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
        }
        assertThat(findGlobalCommitterMetricGroups(jobId)).isNotEmpty();
    }

    private void waitUntilWriterInputRecords(JobID jobId) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (writerInputRecords(jobId) > 0) {
                return;
            }
            Thread.sleep(500);
        }
        assertThat(writerInputRecords(jobId))
                .describedAs(describeWriterMetrics(jobId))
                .isPositive();
    }

    private long writerInputRecords(JobID jobId) {
        long records = 0L;
        for (OperatorMetricGroup group : reporter.findOperatorMetricGroups(jobId, "Writer.*")) {
            records += group.getIOMetricGroup().getNumRecordsInCounter().getCount();
        }
        return records;
    }

    private void waitUntilCoordinatorCommitMetricsRegistered(JobID jobId) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (hasCoordinatorCommitMetrics(jobId)) {
                return;
            }
            Thread.sleep(500);
        }
        assertThat(hasCoordinatorCommitMetrics(jobId))
                .describedAs(describeCommitMetrics(jobId))
                .isTrue();
    }

    private boolean hasCoordinatorCommitMetrics(JobID jobId) {
        for (java.util.Map.Entry<MetricGroup, java.util.Map<String, Metric>> group :
                reporter.getMetricsByGroup().entrySet()) {
            if (!isMetricGroupForJob(group.getKey(), jobId)) {
                continue;
            }
            if (!group.getKey().getClass().getSimpleName().equals("GenericMetricGroup")) {
                continue;
            }
            for (String metricName : group.getValue().keySet()) {
                if (metricName.equals("commitDuration")
                        || metricName.equals("lastCommittedSnapshotId")) {
                    return true;
                }
            }
        }
        return false;
    }

    private String describeCommitMetrics(JobID jobId) {
        StringBuilder builder = new StringBuilder("commit metrics:");
        for (java.util.Map.Entry<MetricGroup, java.util.Map<String, Metric>> group :
                reporter.getMetricsByGroup().entrySet()) {
            if (!isMetricGroupForJob(group.getKey(), jobId)) {
                continue;
            }
            for (java.util.Map.Entry<String, Metric> metric : group.getValue().entrySet()) {
                if (metric.getKey().contains("Commit")
                        || metric.getKey().contains("commit")
                        || metric.getKey().contains("numRecordsOut")) {
                    builder.append(' ')
                            .append(group.getKey().getClass().getSimpleName())
                            .append('#')
                            .append(metric.getKey())
                            .append('=')
                            .append(metric.getValue().getClass().getSimpleName());
                }
            }
        }
        return builder.toString();
    }

    private boolean isMetricGroupForJob(MetricGroup metricGroup, JobID jobId) {
        return metricGroup.getAllVariables().containsValue(jobId.toString());
    }

    private String describeWriterMetrics(JobID jobId) {
        StringBuilder builder = new StringBuilder("writer metrics:");
        for (OperatorMetricGroup group : reporter.findOperatorMetricGroups(jobId, "Writer.*")) {
            builder.append(' ')
                    .append(group.getIOMetricGroup().getNumRecordsInCounter().getCount())
                    .append('/')
                    .append(group.getIOMetricGroup().getNumRecordsOutCounter().getCount());
        }
        return builder.toString();
    }

    private void waitUntilWriterTasks(JobClient client, int finished, int running)
            throws Exception {
        RestClusterClient<?> clusterClient = MINI_CLUSTER_EXTENSION.createRestClusterClient();
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            JobDetailsInfo details = clusterClient.getJobDetails(client.getJobID()).get();
            Optional<JobDetailsInfo.JobVertexDetailsInfo> writer =
                    details.getJobVertexInfos().stream()
                            .filter(vertex -> vertex.getName().contains("Writer"))
                            .findFirst();
            if (writer.isPresent()) {
                Map<ExecutionState, Integer> states = writer.get().getTasksPerState();
                if (states.getOrDefault(ExecutionState.FINISHED, 0) == finished
                        && states.getOrDefault(ExecutionState.RUNNING, 0) == running) {
                    return;
                }
            }
            Thread.sleep(200L);
        }
        throw new AssertionError(
                "Writer tasks did not reach expected states; actual=" + writerTaskStates(client));
    }

    private String writerTaskStates(JobClient client) throws Exception {
        RestClusterClient<?> clusterClient = MINI_CLUSTER_EXTENSION.createRestClusterClient();
        JobDetailsInfo details = clusterClient.getJobDetails(client.getJobID()).get();
        StringBuilder states = new StringBuilder();
        for (JobDetailsInfo.JobVertexDetailsInfo vertex : details.getJobVertexInfos()) {
            if (vertex.getName().contains("Writer")) {
                states.append(vertex.getName()).append('=').append(vertex.getTasksPerState());
            }
        }
        return states.toString();
    }

    private long triggerCheckpoint(MiniCluster miniCluster, JobID jobId) throws Exception {
        return miniCluster
                .triggerCheckpoint(jobId, CheckpointType.CONFIGURED)
                .get(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
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

    private FileStoreTable createCoordinatorCommitTable(String tableName) throws Exception {
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.executeSql(
                "CREATE CATALOG failovercat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG failovercat");
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, data STRING) WITH ("
                        + "'bucket' = '-1', 'write-only' = 'true', "
                        + "'sink.coordinator-commit.enabled' = 'true')");
        return (FileStoreTable)
                ((FlinkCatalog) tEnv.getCatalog("failovercat").get())
                        .catalog()
                        .getTable(Identifier.create("default", tableName));
    }

    private JobClient startPartialSavepointJob(FileStoreTable table, String savepointPath)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("restart-strategy.type", "none");
        configuration.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        if (savepointPath != null) {
            SavepointRestoreSettings.toConfiguration(
                    SavepointRestoreSettings.forPath(savepointPath, false), configuration);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(SCRIPTED_PARALLELISM);
        env.enableCheckpointing(TimeUnit.HOURS.toMillis(1));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.configure(configuration, getClass().getClassLoader());
        DataStreamSource<RowData> source =
                env.fromSource(
                                new PartialSavepointRestoreSource(),
                                org.apache.flink.api.common.eventtime.WatermarkStrategy
                                        .noWatermarks(),
                                "partial-savepoint-restore-source")
                        .setParallelism(SCRIPTED_PARALLELISM);
        new FlinkSinkBuilder(table).forRowData(source).build();
        return env.executeAsync("partial-savepoint-restore");
    }

    private OperatorState loadWriterState(String savepointPath, boolean fullyFinished)
            throws Exception {
        CheckpointMetadata metadata = loadMetadata(savepointPath);
        // The source has no coordinator; the Paimon writer is the only coordinated operator here.
        for (OperatorState state : metadata.getOperatorStates()) {
            if (fullyFinished ? state.isFullyFinished() : state.getCoordinatorState() != null) {
                assertThat(state.isFullyFinished()).isEqualTo(fullyFinished);
                return state;
            }
        }
        throw new AssertionError("Expected writer state missing from savepoint " + savepointPath);
    }

    private CheckpointMetadata loadMetadata(String savepointPath) throws Exception {
        org.apache.flink.core.fs.Path metadataPath =
                new org.apache.flink.core.fs.Path(savepointPath, "_metadata");
        try (DataInputStream input =
                new DataInputStream(metadataPath.getFileSystem().open(metadataPath))) {
            return Checkpoints.loadCheckpointMetadata(
                    input, getClass().getClassLoader(), savepointPath);
        }
    }

    private void waitUntilCheckpointCommitted(
            JobClient client, FileStoreTable table, long checkpoint) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (client.getJobStatus().get() != JobStatus.RUNNING) {
                client.getJobExecutionResult().get(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                throw new AssertionError("Job ended before checkpoint commit " + checkpoint);
            }
            Snapshot latest = table.snapshotManager().latestSnapshot();
            if (latest != null && latest.commitIdentifier() >= checkpoint) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("No Paimon snapshot for checkpoint " + checkpoint);
    }

    private MiniCluster miniCluster(JobClient client) throws Exception {
        Field field = client.getClass().getDeclaredField("miniCluster");
        field.setAccessible(true);
        return (MiniCluster) field.get(client);
    }

    private void waitUntilRestarted(MiniCluster miniCluster, JobID jobId) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            WriterAttempts attempts = writerAttempts(miniCluster, jobId);
            if (attempts.running == 1 && attempts.finished == 1 && attempts.runningAttempt > 0) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError(
                "Writer vertex did not recover to one running and one finished subtask");
    }

    private void waitUntilBranchRestarted(MiniCluster miniCluster, JobID jobId) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (FullyFinishedControl.branchRestarted.get()
                    && miniCluster.getJobStatus(jobId).get() == JobStatus.RUNNING) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("Branch B did not restart after the injected failure");
    }

    private void waitUntilVertexTasksRunning(
            MiniCluster miniCluster, JobID jobId, String vertexName, int expectedRunning)
            throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            for (AccessExecutionJobVertex vertex :
                    miniCluster.getExecutionGraph(jobId).get().getVerticesTopologically()) {
                if (vertex.getName().contains(vertexName)
                        && vertex.getAggregateState() == ExecutionState.RUNNING
                        && vertex.getTaskVertices().length == expectedRunning) {
                    return;
                }
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("Vertex " + vertexName + " did not reach RUNNING");
    }

    private WriterAttempts writerAttempts(MiniCluster miniCluster, JobID jobId) throws Exception {
        AccessExecutionJobVertex writer =
                miniCluster
                        .getExecutionGraph(jobId)
                        .get()
                        .getVerticesTopologically()
                        .iterator()
                        .next();
        for (AccessExecutionJobVertex vertex :
                miniCluster.getExecutionGraph(jobId).get().getVerticesTopologically()) {
            if (vertex.getName().contains("Writer")) {
                writer = vertex;
                break;
            }
        }
        int finished = 0;
        int running = 0;
        int finishedAttempt = -1;
        int runningAttempt = -1;
        for (AccessExecutionVertex task : writer.getTaskVertices()) {
            if (task.getExecutionState() == ExecutionState.FINISHED) {
                finished++;
                finishedAttempt = task.getCurrentExecutionAttempt().getAttemptNumber();
            } else if (task.getExecutionState() == ExecutionState.RUNNING) {
                running++;
                runningAttempt = task.getCurrentExecutionAttempt().getAttemptNumber();
            }
        }
        return new WriterAttempts(finished, running, finishedAttempt, runningAttempt);
    }

    private void waitUntilRowCount(FileStoreTable table, long expected) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (readRowCount(table) == expected) {
                return;
            }
            Thread.sleep(100L);
        }
        assertThat(readRowCount(table)).isEqualTo(expected);
    }

    private void waitUntilJobRunning(JobClient client) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            JobStatus status = client.getJobStatus().get(5, TimeUnit.SECONDS);
            if (status == JobStatus.RUNNING) {
                return;
            }
            if (status.isTerminalState()) {
                throw new AssertionError("Job terminated before it started: " + status);
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("Job did not reach RUNNING");
    }

    private void waitUntilRowsCommitted(RunningJob runningJob) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (readRowCount(runningJob.table) > 0) {
                return;
            }
            runningJob.checkNotTerminated();
            Thread.sleep(500);
        }
        assertThat(readRowCount(runningJob.table))
                .describedAs(describeWriterMetrics(runningJob.jobId))
                .isGreaterThan(0L);
    }

    private static class RunningJob {

        private final FileStoreTable table;
        private final JobClient client;
        private final JobID jobId;

        private RunningJob(FileStoreTable table, JobClient client) {
            this.table = table;
            this.client = client;
            this.jobId = client.getJobID();
        }

        private void checkNotTerminated() throws Exception {
            JobStatus status = client.getJobStatus().get(30, TimeUnit.SECONDS);
            if (status == JobStatus.FAILED) {
                client.getJobExecutionResult().get(30, TimeUnit.SECONDS);
            }
            assertThat(status.isTerminalState()).describedAs("job status: %s", status).isFalse();
        }

        private void cancel() throws Exception {
            client.cancel().get(30, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!client.getJobStatus().get(5, TimeUnit.SECONDS).isTerminalState()) {
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("Canceled job did not reach a terminal state");
                }
                Thread.sleep(100L);
            }
        }
    }

    private static class WriterAttempts {
        private final int finished;
        private final int running;
        private final int finishedAttempt;
        private final int runningAttempt;

        private WriterAttempts(int finished, int running, int finishedAttempt, int runningAttempt) {
            this.finished = finished;
            this.running = running;
            this.finishedAttempt = finishedAttempt;
            this.runningAttempt = runningAttempt;
        }
    }

    private static class PartialFinishControl {
        private static CountDownLatch secondCheckpointCompleted;
        private static CountDownLatch baselineCheckpointCompleted;
        private static CountDownLatch allowW0EndInput;
        private static CountDownLatch failAfterC2;
        private static CountDownLatch postFailoverRecordEmitted;
        private static CountDownLatch readersStarted;
        private static CountDownLatch w0EndInputReached;
        private static CompletableFuture<Void> w0Availability;
        private static CompletableFuture<Void> failureAvailability;
        private static AtomicBoolean firstRunningRecord;
        private static AtomicBoolean failureInjected;
        private static AtomicBoolean allowFailure;

        private static void reset() {
            secondCheckpointCompleted = new CountDownLatch(1);
            baselineCheckpointCompleted = new CountDownLatch(1);
            allowW0EndInput = new CountDownLatch(1);
            failAfterC2 = new CountDownLatch(1);
            postFailoverRecordEmitted = new CountDownLatch(1);
            readersStarted = new CountDownLatch(SCRIPTED_PARALLELISM);
            w0EndInputReached = new CountDownLatch(1);
            w0Availability = new CompletableFuture<>();
            failureAvailability = new CompletableFuture<>();
            firstRunningRecord = new AtomicBoolean();
            failureInjected = new AtomicBoolean();
            allowFailure = new AtomicBoolean();
        }
    }

    private enum RestoreBehavior {
        EMPTY_CHECKPOINT,
        NEW_RECORD,
        IMMEDIATE_END_INPUT
    }

    private static class PartialSavepointControl {
        private static CountDownLatch allowW0EndInput;
        private static CountDownLatch w0EndInputReached;
        private static CompletableFuture<Void> firstAvailability;
        private static CompletableFuture<Void> restoredAvailability;
        private static AtomicBoolean restoring;
        private static RestoreBehavior behavior;
        private static AtomicBoolean finishRestored;
        private static CountDownLatch restoredRecordEmitted;
        private static CountDownLatch restoredEndInputReached;

        private static void reset() {
            allowW0EndInput = new CountDownLatch(1);
            w0EndInputReached = new CountDownLatch(1);
            firstAvailability = new CompletableFuture<>();
            restoredAvailability = new CompletableFuture<>();
            restoring = new AtomicBoolean();
            finishRestored = new AtomicBoolean();
            restoredRecordEmitted = new CountDownLatch(1);
            restoredEndInputReached = new CountDownLatch(SCRIPTED_PARALLELISM);
        }

        private static void beginRestore(RestoreBehavior restoreBehavior) {
            behavior = restoreBehavior;
            finishRestored.set(restoreBehavior == RestoreBehavior.IMMEDIATE_END_INPUT);
            restoring.set(true);
        }
    }

    private static class FullyFinishedControl {
        private static AtomicInteger readersStarted;
        private static CountDownLatch releaseEndInput;
        private static CountDownLatch endInputReached;
        private static CompletableFuture<Void> availability;
        private static CompletableFuture<Void> failureAvailability;
        private static AtomicBoolean allowFailure;
        private static AtomicBoolean failureInjected;
        private static AtomicBoolean branchRestarted;

        private static void reset() {
            readersStarted = new AtomicInteger();
            releaseEndInput = new CountDownLatch(1);
            endInputReached = new CountDownLatch(SCRIPTED_PARALLELISM);
            availability = new CompletableFuture<>();
            failureAvailability = new CompletableFuture<>();
            allowFailure = new AtomicBoolean();
            failureInjected = new AtomicBoolean();
            branchRestarted = new AtomicBoolean();
        }
    }

    /**
     * On the first submission W0 writes one row and ends while W1 remains live. The second
     * submission emits no records from either subtask, so its first checkpoint is an exact probe
     * for restored ordinary empty contributions.
     */
    private static class PartialSavepointRestoreSource
            extends AbstractNonCoordinatedSource<RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<RowData, SimpleSourceSplit> createReader(SourceReaderContext context) {
            return new AbstractNonCoordinatedSourceReader<RowData>() {
                private final boolean restoredSubmission = PartialSavepointControl.restoring.get();
                private boolean emitted;
                private boolean endInputReported;

                @Override
                public InputStatus pollNext(ReaderOutput<RowData> output) {
                    if (restoredSubmission) {
                        if (context.getIndexOfSubtask() == 0
                                && !emitted
                                && PartialSavepointControl.behavior == RestoreBehavior.NEW_RECORD) {
                            output.collect(GenericRowData.of(1, StringData.fromString("restored")));
                            emitted = true;
                            PartialSavepointControl.restoredRecordEmitted.countDown();
                            return InputStatus.MORE_AVAILABLE;
                        }
                        if (PartialSavepointControl.finishRestored.get()) {
                            if (!endInputReported) {
                                endInputReported = true;
                                PartialSavepointControl.restoredEndInputReached.countDown();
                            }
                            return InputStatus.END_OF_INPUT;
                        }
                        return InputStatus.NOTHING_AVAILABLE;
                    }
                    if (context.getIndexOfSubtask() != 0) {
                        return InputStatus.NOTHING_AVAILABLE;
                    }
                    if (!emitted) {
                        output.collect(GenericRowData.of(0, StringData.fromString("terminal")));
                        emitted = true;
                        return InputStatus.MORE_AVAILABLE;
                    }
                    if (PartialSavepointControl.allowW0EndInput.getCount() == 0) {
                        if (!endInputReported) {
                            endInputReported = true;
                            PartialSavepointControl.w0EndInputReached.countDown();
                        }
                        return InputStatus.END_OF_INPUT;
                    }
                    return InputStatus.NOTHING_AVAILABLE;
                }

                @Override
                public CompletableFuture<Void> isAvailable() {
                    if (restoredSubmission) {
                        if (PartialSavepointControl.finishRestored.get()
                                || (context.getIndexOfSubtask() == 0
                                        && !emitted
                                        && PartialSavepointControl.behavior
                                                == RestoreBehavior.NEW_RECORD)) {
                            return CompletableFuture.completedFuture(null);
                        }
                        return PartialSavepointControl.restoredAvailability;
                    }
                    if (context.getIndexOfSubtask() == 0 && !emitted) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (context.getIndexOfSubtask() != 0) {
                        return PartialSavepointControl.restoredAvailability;
                    }
                    return PartialSavepointControl.firstAvailability;
                }
            };
        }
    }

    private static class FullyFinishedSource extends AbstractNonCoordinatedSource<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<RowData, SimpleSourceSplit> createReader(SourceReaderContext context) {
            return new AbstractNonCoordinatedSourceReader<RowData>() {
                private boolean emitted;

                @Override
                public void start() {
                    FullyFinishedControl.readersStarted.incrementAndGet();
                }

                @Override
                public InputStatus pollNext(ReaderOutput<RowData> output) {
                    if (!emitted) {
                        output.collect(
                                GenericRowData.of(
                                        context.getIndexOfSubtask(),
                                        StringData.fromString("fully-finished")));
                        emitted = true;
                        return InputStatus.MORE_AVAILABLE;
                    }
                    if (FullyFinishedControl.releaseEndInput.getCount() == 0) {
                        FullyFinishedControl.endInputReached.countDown();
                        return InputStatus.END_OF_INPUT;
                    }
                    return InputStatus.NOTHING_AVAILABLE;
                }

                @Override
                public CompletableFuture<Void> isAvailable() {
                    return FullyFinishedControl.releaseEndInput.getCount() == 0
                            ? CompletableFuture.completedFuture(null)
                            : FullyFinishedControl.availability;
                }
            };
        }
    }

    private static class FailingBranchSource extends AbstractNonCoordinatedSource<Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<Integer, SimpleSourceSplit> createReader(SourceReaderContext context) {
            return new AbstractNonCoordinatedSourceReader<Integer>() {
                private boolean firstPoll = true;

                @Override
                public InputStatus pollNext(ReaderOutput<Integer> output) {
                    if (firstPoll) {
                        firstPoll = false;
                        if (FullyFinishedControl.failureInjected.get()) {
                            FullyFinishedControl.branchRestarted.set(true);
                        }
                        return InputStatus.NOTHING_AVAILABLE;
                    }
                    if (FullyFinishedControl.allowFailure.get()
                            && FullyFinishedControl.failureInjected.compareAndSet(false, true)) {
                        throw new RuntimeException("Injected Branch B failure after C2");
                    }
                    return InputStatus.NOTHING_AVAILABLE;
                }

                @Override
                public CompletableFuture<Void> isAvailable() {
                    return !FullyFinishedControl.allowFailure.get()
                                    && !FullyFinishedControl.failureInjected.get()
                            ? FullyFinishedControl.failureAvailability
                            : CompletableFuture.completedFuture(null);
                }
            };
        }
    }

    /** W0 ends, while W1 remains active and fails only after C2 has completed. */
    private static class PartiallyFinishedFailoverSource
            extends AbstractNonCoordinatedSource<RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<RowData, SimpleSourceSplit> createReader(
                SourceReaderContext sourceReaderContext) {
            return new Reader(sourceReaderContext.getIndexOfSubtask());
        }

        private static class Reader extends AbstractNonCoordinatedSourceReader<RowData> {
            private final int subtask;
            private boolean terminalRowEmitted;
            private boolean postFailoverRowEmitted;
            private long completedCheckpoints;

            private Reader(int subtask) {
                this.subtask = subtask;
                PartialFinishControl.readersStarted.countDown();
            }

            @Override
            public InputStatus pollNext(ReaderOutput<RowData> output) {
                if (subtask == 0) {
                    if (!terminalRowEmitted) {
                        output.collect(GenericRowData.of(0, StringData.fromString("finished")));
                        terminalRowEmitted = true;
                        return InputStatus.MORE_AVAILABLE;
                    }
                    if (PartialFinishControl.allowW0EndInput.getCount() == 0) {
                        PartialFinishControl.w0EndInputReached.countDown();
                        return InputStatus.END_OF_INPUT;
                    }
                    return InputStatus.NOTHING_AVAILABLE;
                }
                if (!PartialFinishControl.firstRunningRecord.getAndSet(true)) {
                    output.collect(GenericRowData.of(1, StringData.fromString("before-failover")));
                    return InputStatus.MORE_AVAILABLE;
                }
                if (PartialFinishControl.failureInjected.get() && !postFailoverRowEmitted) {
                    output.collect(GenericRowData.of(2, StringData.fromString("after-failover")));
                    postFailoverRowEmitted = true;
                    PartialFinishControl.postFailoverRecordEmitted.countDown();
                    return InputStatus.NOTHING_AVAILABLE;
                }
                if (PartialFinishControl.allowFailure.get()
                        && !PartialFinishControl.failureInjected.get()) {
                    if (PartialFinishControl.failureInjected.compareAndSet(false, true)) {
                        throw new RuntimeException("Injected failure after completed C2");
                    }
                }
                return InputStatus.NOTHING_AVAILABLE;
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                if (subtask == 0 && PartialFinishControl.allowW0EndInput.getCount() > 0) {
                    return PartialFinishControl.w0Availability;
                }
                if (subtask == 1
                        && !PartialFinishControl.allowFailure.get()
                        && !PartialFinishControl.failureInjected.get()) {
                    return PartialFinishControl.failureAvailability;
                }
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) {
                if (subtask == 1) {
                    completedCheckpoints++;
                    if (completedCheckpoints == 3) {
                        PartialFinishControl.secondCheckpointCompleted.countDown();
                    }
                } else if (subtask == 0 && checkpointId > 0) {
                    PartialFinishControl.baselineCheckpointCompleted.countDown();
                }
            }
        }
    }

    /** Ends subtask 0 while subtask 1 remains active for the assertion window. */
    private static class AsymmetricEndInputSource extends AbstractNonCoordinatedSource<RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<RowData, SimpleSourceSplit> createReader(
                SourceReaderContext sourceReaderContext) {
            return new Reader(sourceReaderContext.getIndexOfSubtask());
        }

        private static class Reader extends AbstractNonCoordinatedSourceReader<RowData> {

            private final int subtask;
            private boolean emitted;

            private Reader(int subtask) {
                this.subtask = subtask;
            }

            @Override
            public InputStatus pollNext(ReaderOutput<RowData> output) throws InterruptedException {
                if (subtask == 0 && !emitted) {
                    output.collect(GenericRowData.of(0, StringData.fromString("early")));
                    emitted = true;
                    return InputStatus.MORE_AVAILABLE;
                }
                if (subtask == 0) {
                    return InputStatus.END_OF_INPUT;
                }
                Thread.sleep(50L);
                return InputStatus.MORE_AVAILABLE;
            }
        }
    }

    /** Emits one row per subtask, then delays END_INPUT until a checkpoint can be triggered. */
    private static class EndInputSource extends AbstractNonCoordinatedSource<RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<RowData, SimpleSourceSplit> createReader(
                SourceReaderContext sourceReaderContext) {
            return new Reader(sourceReaderContext.getIndexOfSubtask());
        }

        private static class Reader extends AbstractNonCoordinatedSourceReader<RowData> {

            private final int subtask;
            private boolean emitted;

            private Reader(int subtask) {
                this.subtask = subtask;
            }

            @Override
            public InputStatus pollNext(ReaderOutput<RowData> output) throws InterruptedException {
                if (!emitted) {
                    output.collect(
                            GenericRowData.of(
                                    subtask, StringData.fromString("subtask-" + subtask)));
                    emitted = true;
                    return InputStatus.MORE_AVAILABLE;
                }
                Thread.sleep(1_000L);
                return InputStatus.END_OF_INPUT;
            }
        }
    }

    /**
     * Scripted source that emits controlled ({@link Watermark}, {@link
     * org.apache.flink.api.connector.source.SourceOutput#markIdle()}) sequences on a per-subtask
     * basis, so the same input drives both commit paths deterministically without depending on
     * datagen timing. Subtask-0 and subtask-1 follow the {@code Scenario}'s script; each step emits
     * a watermark or marks the split idle, sleeps briefly to let checkpoints run, then the source
     * stays available forever so the job is only stopped by the outer cancel.
     */
    private static class IdleWatermarkScriptedSource extends AbstractNonCoordinatedSource<RowData> {

        private static final long serialVersionUID = 1L;

        enum Scenario {
            /** Both subtasks stay active; snapshot watermark equals the smaller of the two. */
            ALL_ACTIVE(300L),
            /**
             * Subtask-0 emits a small watermark then goes idle; subtask-1 keeps emitting bigger
             * watermarks. The idle subtask must not hold the snapshot back.
             */
            PARTIAL_IDLE(700L),
            /**
             * Both subtasks emit a watermark and go idle. Once every input is idle, Flink's valve
             * flushes {@code max} over all channels — so the snapshot watermark advances to the
             * larger of the two, then stays put (not regresses back to {@code Long.MIN_VALUE}).
             */
            ALL_IDLE(600L);

            final long expectedWatermark;

            Scenario(long expectedWatermark) {
                this.expectedWatermark = expectedWatermark;
            }
        }

        private final Scenario scenario;

        IdleWatermarkScriptedSource(Scenario scenario) {
            this.scenario = scenario;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<RowData, SimpleSourceSplit> createReader(
                SourceReaderContext sourceReaderContext) {
            return new Reader(scenario, sourceReaderContext.getIndexOfSubtask());
        }

        private static class Reader extends AbstractNonCoordinatedSourceReader<RowData> {

            private final Scenario scenario;
            private final int subtaskIndex;

            private int step;

            Reader(Scenario scenario, int subtaskIndex) {
                this.scenario = scenario;
                this.subtaskIndex = subtaskIndex;
            }

            @Override
            public InputStatus pollNext(ReaderOutput<RowData> output) throws InterruptedException {
                if (step == 0) {
                    // Emit one record so the writer has something to commit and thus produces a
                    // snapshot carrying the watermark for us to observe.
                    output.collect(
                            GenericRowData.of(
                                    subtaskIndex, StringData.fromString("s" + subtaskIndex)));
                }
                switch (scenario) {
                    case ALL_ACTIVE:
                        return driveAllActive(output);
                    case PARTIAL_IDLE:
                        return drivePartialIdle(output);
                    case ALL_IDLE:
                        return driveAllIdle(output);
                    default:
                        throw new IllegalStateException("Unknown scenario " + scenario);
                }
            }

            private InputStatus driveAllActive(ReaderOutput<RowData> output)
                    throws InterruptedException {
                // Both subtasks keep emitting increasing watermarks. Min across subtasks = 300.
                long watermark = subtaskIndex == 0 ? 300L : 500L;
                output.emitWatermark(new Watermark(watermark));
                Thread.sleep(200);
                step++;
                return InputStatus.MORE_AVAILABLE;
            }

            private InputStatus drivePartialIdle(ReaderOutput<RowData> output)
                    throws InterruptedException {
                if (subtaskIndex == 0) {
                    if (step == 0) {
                        output.emitWatermark(new Watermark(100L));
                        Thread.sleep(200);
                        output.markIdle();
                    }
                    Thread.sleep(200);
                    step++;
                    return InputStatus.MORE_AVAILABLE;
                }
                // subtask-1 keeps advancing; peers at 700 by the second step.
                long watermark = 500L + Math.min(step, 1) * 200L;
                output.emitWatermark(new Watermark(watermark));
                Thread.sleep(200);
                step++;
                return InputStatus.MORE_AVAILABLE;
            }

            private InputStatus driveAllIdle(ReaderOutput<RowData> output)
                    throws InterruptedException {
                if (step == 0) {
                    long watermark = subtaskIndex == 0 ? 400L : 600L;
                    output.emitWatermark(new Watermark(watermark));
                    Thread.sleep(200);
                    output.markIdle();
                }
                Thread.sleep(200);
                step++;
                return InputStatus.MORE_AVAILABLE;
            }
        }
    }
}
