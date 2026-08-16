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
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
                                    .setConfiguration(
                                            reporter.addToConfiguration(new Configuration()))
                                    .build());

    @TempDir Path tempPath;

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
        String tableName = coordinatorCommitEnabled ? "T_COORDINATOR_COMMIT" : "T_DEFAULT_COMMIT";
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "200 ms");

        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '"
                        + tempPath
                        + "' )");
        tEnv.executeSql("USE CATALOG mycat");
        String coordinatorCommitOption =
                coordinatorCommitEnabled
                        ? ", 'sink.coordinator-commit.enabled' = 'true', 'write-only' = 'true'"
                        : "";
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableName
                        + " (id INT, data STRING) WITH ("
                        + "'bucket' = '-1'"
                        + coordinatorCommitOption
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

    private void waitUntilRowsCommitted(RunningJob runningJob) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (readRowCount(runningJob.table) > 0) {
                return;
            }
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

        private void cancel() throws Exception {
            client.cancel().get(30, TimeUnit.SECONDS);
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
