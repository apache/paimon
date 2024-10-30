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
import org.apache.paimon.table.BucketMode;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case for flink source / sink restore from savepoint. */
public class FlinkJobRecoveryITCase extends CatalogITCaseBase {

    @BeforeEach
    @Override
    public void before() throws IOException {
        super.before();

        // disable checkpoint
        sEnv.getConfig()
                .getConfiguration()
                .set(
                        CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)
                .removeConfig(CheckpointingOptions.CHECKPOINTING_INTERVAL);

        // insert source data
        batchSql("INSERT INTO source_table1 VALUES (1, 'test-1', '20241030')");
        batchSql("INSERT INTO source_table1 VALUES (2, 'test-2', '20241030')");
        batchSql("INSERT INTO source_table1 VALUES (3, 'test-3', '20241030')");
        batchSql(
                "INSERT INTO source_table2 VALUES (4, 'test-4', '20241031'), (5, 'test-5', '20241031'), (6, 'test-6', '20241031')");
    }

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                String.format(
                        "CREATE CATALOG `fs_catalog` WITH ('type'='paimon', 'warehouse'='%s')",
                        path),
                "CREATE TABLE IF NOT EXISTS `source_table1` (k INT, f1 STRING, dt STRING) WITH ('bucket'='1', 'bucket-key'='k')",
                "CREATE TABLE IF NOT EXISTS `source_table2` (k INT, f1 STRING, dt STRING) WITH ('bucket'='1', 'bucket-key'='k')");
    }

    @ParameterizedTest
    @EnumSource(BucketMode.class)
    @Timeout(300)
    public void testRestoreFromSavepointWithJobGraphChange(BucketMode bucketMode) throws Exception {
        createTargetTable("target_table", bucketMode);
        String beforeRecoverSql =
                "INSERT INTO `target_table` /*+ OPTIONS('sink.operator-uid.suffix'='test-uid') */ SELECT * FROM source_table1 /*+ OPTIONS('source.operator-uid.suffix'='test-uid') */";
        String beforeRecoverCheckSql = "SELECT * FROM target_table";
        List<Row> beforeRecoverExpectedRows =
                Arrays.asList(
                        Row.of(1, "test-1", "20241030"),
                        Row.of(2, "test-2", "20241030"),
                        Row.of(3, "test-3", "20241030"));
        String afterRecoverSql =
                "INSERT INTO `target_table` /*+ OPTIONS('sink.operator-uid.suffix'='test-uid') */ (SELECT * FROM source_table1 /*+ OPTIONS('source.operator-uid.suffix'='test-uid') */ UNION ALL SELECT * FROM source_table2)";
        String afterRecoverCheckSql = "SELECT * FROM target_table";
        List<Row> afterRecoverExpectedRows =
                Arrays.asList(
                        Row.of(1, "test-1", "20241030"),
                        Row.of(2, "test-2", "20241030"),
                        Row.of(3, "test-3", "20241030"),
                        Row.of(4, "test-4", "20241031"),
                        Row.of(5, "test-5", "20241031"),
                        Row.of(6, "test-6", "20241031"));
        testRecoverFromSavepoint(
                beforeRecoverSql,
                beforeRecoverCheckSql,
                beforeRecoverExpectedRows,
                afterRecoverSql,
                afterRecoverCheckSql,
                afterRecoverExpectedRows,
                Collections.emptyList(),
                Collections.emptyMap());
    }

    @Test
    @Timeout(300)
    public void testRestoreFromSavepointWithIgnoreSourceState() throws Exception {
        createTargetTable("target_table", BucketMode.HASH_FIXED);
        String beforeRecoverSql = "INSERT INTO `target_table` SELECT * FROM source_table1";
        String beforeRecoverCheckSql = "SELECT * FROM target_table";
        List<Row> beforeRecoverExpectedRows =
                Arrays.asList(
                        Row.of(1, "test-1", "20241030"),
                        Row.of(2, "test-2", "20241030"),
                        Row.of(3, "test-3", "20241030"));
        String afterRecoverSql =
                "INSERT INTO `target_table` SELECT * FROM source_table2 /*+ OPTIONS('source.operator-uid.suffix'='test-uid') */";
        String afterRecoverCheckSql = "SELECT * FROM target_table";
        List<Row> afterRecoverExpectedRows =
                Arrays.asList(
                        Row.of(1, "test-1", "20241030"),
                        Row.of(2, "test-2", "20241030"),
                        Row.of(3, "test-3", "20241030"),
                        Row.of(4, "test-4", "20241031"),
                        Row.of(5, "test-5", "20241031"),
                        Row.of(6, "test-6", "20241031"));
        testRecoverFromSavepoint(
                beforeRecoverSql,
                beforeRecoverCheckSql,
                beforeRecoverExpectedRows,
                afterRecoverSql,
                afterRecoverCheckSql,
                afterRecoverExpectedRows,
                Collections.emptyList(),
                Collections.singletonMap(
                        StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key(), "true"));
    }

    @Test
    @Timeout(300)
    public void testRestoreFromSavepointWithIgnoreSinkState() throws Exception {
        createTargetTable("target_table", BucketMode.HASH_FIXED);
        createTargetTable("target_table2", BucketMode.HASH_FIXED);

        String beforeRecoverSql = "INSERT INTO `target_table` SELECT * FROM source_table1";
        String beforeRecoverCheckSql = "SELECT * FROM target_table";
        List<Row> beforeRecoverExpectedRows =
                Arrays.asList(
                        Row.of(1, "test-1", "20241030"),
                        Row.of(2, "test-2", "20241030"),
                        Row.of(3, "test-3", "20241030"));
        String afterRecoverSql =
                "INSERT INTO `target_table2` /*+ OPTIONS('sink.operator-uid.suffix'='test-uid') */ SELECT * FROM source_table1";
        String afterRecoverCheckSql = "SELECT * FROM target_table2";
        List<Row> afterRecoverExpectedRows =
                Arrays.asList(
                        Row.of(7, "test-7", "20241030"),
                        Row.of(8, "test-8", "20241030"),
                        Row.of(9, "test-9", "20241030"));
        String updateSql =
                "INSERT INTO source_table1 VALUES (7, 'test-7', '20241030'), (8, 'test-8', '20241030'), (9, 'test-9', '20241030')";
        testRecoverFromSavepoint(
                beforeRecoverSql,
                beforeRecoverCheckSql,
                beforeRecoverExpectedRows,
                afterRecoverSql,
                afterRecoverCheckSql,
                afterRecoverExpectedRows,
                Collections.singletonList(updateSql),
                Collections.singletonMap(
                        StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key(), "true"));
    }

    private void testRecoverFromSavepoint(
            String beforeRecoverSql,
            String beforeRecoverCheckSql,
            List<Row> beforeRecoverExpectedRows,
            String afterRecoverSql,
            String afterRecoverCheckSql,
            List<Row> afterRecoverExpectedRows,
            List<String> updateSql,
            Map<String, String> recoverOptions)
            throws Exception {

        JobClient jobClient = sEnv.executeSql(beforeRecoverSql).getJobClient().get();
        String checkpointPath =
                triggerCheckpointAndWaitForWrites(jobClient, beforeRecoverExpectedRows.size());
        jobClient.cancel().get();

        List<Row> rows = batchSql(beforeRecoverCheckSql);
        assertThat(rows.size()).isEqualTo(beforeRecoverExpectedRows.size());
        assertThat(rows).containsExactlyInAnyOrder(beforeRecoverExpectedRows.toArray(new Row[0]));

        for (String sql : updateSql) {
            batchSql(sql);
        }

        Configuration config =
                sEnv.getConfig()
                        .getConfiguration()
                        .set(StateRecoveryOptions.SAVEPOINT_PATH, checkpointPath);
        for (Map.Entry<String, String> entry : recoverOptions.entrySet()) {
            config.setString(entry.getKey(), entry.getValue());
        }

        jobClient = sEnv.executeSql(afterRecoverSql).getJobClient().get();
        triggerCheckpointAndWaitForWrites(jobClient, afterRecoverExpectedRows.size());
        jobClient.cancel().get();

        rows = batchSql(afterRecoverCheckSql);
        assertThat(rows.size()).isEqualTo(afterRecoverExpectedRows.size());
        assertThat(rows).containsExactlyInAnyOrder(afterRecoverExpectedRows.toArray(new Row[0]));
    }

    private void createTargetTable(String tableName, BucketMode bucketMode) {
        switch (bucketMode) {
            case HASH_FIXED:
                batchSql(
                        String.format(
                                "CREATE TABLE IF NOT EXISTS `%s` (k INT, f1 STRING, pt STRING, PRIMARY KEY(k, pt) NOT ENFORCED) WITH ('bucket'='2', 'commit.force-create-snapshot'='true')",
                                tableName));
                return;
            case HASH_DYNAMIC:
                batchSql(
                        String.format(
                                "CREATE TABLE IF NOT EXISTS `%s` (k INT, f1 STRING, pt STRING, PRIMARY KEY(k, pt) NOT ENFORCED) WITH ('bucket'='-1', 'commit.force-create-snapshot'='true')",
                                tableName));
                return;
            case CROSS_PARTITION:
                batchSql(
                        String.format(
                                "CREATE TABLE IF NOT EXISTS `%s` (k INT, f1 STRING, pt STRING, PRIMARY KEY(k) NOT ENFORCED) WITH ('bucket'='-1', 'commit.force-create-snapshot'='true')",
                                tableName));
                return;
            case BUCKET_UNAWARE:
                batchSql(
                        String.format(
                                "CREATE TABLE IF NOT EXISTS `%s` (k INT, f1 STRING, pt STRING) WITH ('bucket'='-1', 'commit.force-create-snapshot'='true')",
                                tableName));
                return;
            default:
                throw new IllegalArgumentException("Unsupported bucket mode: " + bucketMode);
        }
    }

    private Snapshot waitForNewSnapshot(String tableName, long initialSnapshot)
            throws InterruptedException {
        Snapshot snapshot = findLatestSnapshot(tableName);
        while (snapshot == null || snapshot.id() == initialSnapshot) {
            Thread.sleep(2000L);
            snapshot = findLatestSnapshot(tableName);
        }
        return snapshot;
    }

    @SuppressWarnings("unchecked")
    private <T> T reflectGetField(Object instance, String fieldName)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(instance);
    }

    private String triggerCheckpointAndWaitForWrites(JobClient jobClient, long totalReocrds)
            throws Exception {
        MiniCluster miniCluster = reflectGetField(jobClient, "miniCluster");
        JobID jobID = jobClient.getJobID();
        JobStatus jobStatus = jobClient.getJobStatus().get();
        while (jobStatus == JobStatus.INITIALIZING || jobStatus == JobStatus.CREATED) {
            Thread.sleep(2000L);
            jobStatus = jobClient.getJobStatus().get();
        }

        if (jobStatus != JobStatus.RUNNING) {
            throw new IllegalStateException("Job status is not RUNNING");
        }

        AtomicBoolean allTaskRunning = new AtomicBoolean(false);
        while (!allTaskRunning.get()) {
            allTaskRunning.set(true);
            Thread.sleep(2000L);
            miniCluster
                    .getExecutionGraph(jobID)
                    .thenAccept(
                            eg ->
                                    eg.getAllExecutionVertices()
                                            .forEach(
                                                    ev -> {
                                                        if (ev.getExecutionState()
                                                                != ExecutionState.RUNNING) {
                                                            allTaskRunning.set(false);
                                                        }
                                                    }))
                    .get();
        }

        String checkpointPath = miniCluster.triggerCheckpoint(jobID).get();
        Snapshot snapshot = waitForNewSnapshot("target_table", -1L);
        while (snapshot.totalRecordCount() < totalReocrds) {
            checkpointPath = miniCluster.triggerCheckpoint(jobID).get();
            snapshot = waitForNewSnapshot("target_table", snapshot.id());
        }

        return checkpointPath;
    }
}
