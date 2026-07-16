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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
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
}
