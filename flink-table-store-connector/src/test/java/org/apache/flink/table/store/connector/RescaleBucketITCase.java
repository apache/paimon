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

package org.apache.flink.table.store.connector;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.types.Row;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.store.CoreOptions.BUCKET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for overwrite data layout after changing num of bucket. */
public class RescaleBucketITCase extends CatalogITCaseBase {

    private final String alterTableSql = "ALTER TABLE %s SET ('bucket' = '%d')";

    private final String rescaleOverwriteSql = "INSERT OVERWRITE %s SELECT * FROM %s";

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                String.format(
                        "CREATE CATALOG `fs_catalog` WITH ('type' = 'table-store', 'warehouse' = '%s')",
                        path),
                "CREATE TABLE IF NOT EXISTS `fs_catalog`.`default`.`T1` (f0 INT) WITH ('bucket' = '2')");
    }

    @Test
    public void testRescaleCatalogTable() {
        innerTest("fs_catalog", "T1");
    }

    @Test
    public void testSuspendAndRecoverAfterRescaleOverwrite() throws Exception {
        // register a companion table T4 for T3
        executeBoth(
                Arrays.asList(
                        "USE CATALOG fs_catalog",
                        "CREATE TEMPORARY TABLE IF NOT EXISTS `S0` (f0 INT) WITH ('connector' = 'datagen')",
                        "CREATE TABLE IF NOT EXISTS `T3` (f0 INT) WITH ('bucket' = '2')",
                        "CREATE TABLE IF NOT EXISTS `T4` (f0 INT)"));
        SchemaManager schemaManager = new SchemaManager(getTableDirectory("T3"));
        assertLatestSchema(schemaManager, 0L, 2);

        String streamSql =
                "EXECUTE STATEMENT SET BEGIN\n "
                        + "INSERT INTO `T3` SELECT * FROM `S0`;\n "
                        + "INSERT INTO `T4` SELECT * FROM `S0`;\n"
                        + "END";

        sEnv.getConfig().getConfiguration().set(SavepointConfigOptions.SAVEPOINT_PATH, path);
        ClusterClient<?> client = MINI_CLUSTER_RESOURCE.getClusterClient();

        // step1: run streaming insert
        JobID jobId = startJobAndCommitSnapshot(streamSql, null);

        // step2: stop with savepoint
        stopJobSafely(client, jobId);

        final Snapshot snapshotBeforeRescale = findLatestSnapshot("T3");
        assertThat(snapshotBeforeRescale).isNotNull();
        assertSnapshotSchema(schemaManager, snapshotBeforeRescale.schemaId(), 0L, 2);
        List<Row> committedData = batchSql("SELECT * FROM T3");

        // step3: check bucket num
        batchSql(alterTableSql, "T3", 4);
        assertLatestSchema(schemaManager, 1L, 4);

        // step4: rescale data layout according to the new bucket num
        batchSql(rescaleOverwriteSql, "T3", "T3");
        Snapshot snapshotAfterRescale = findLatestSnapshot("T3");
        assertThat(snapshotAfterRescale).isNotNull();
        assertThat(snapshotAfterRescale.id()).isEqualTo(snapshotBeforeRescale.id() + 1);
        assertThat(snapshotAfterRescale.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);
        assertSnapshotSchema(schemaManager, snapshotAfterRescale.schemaId(), 1L, 4);
        assertThat(batchSql("SELECT * FROM T3")).containsExactlyInAnyOrderElementsOf(committedData);

        // step5: resume streaming job
        JobID resumedJobId = startJobAndCommitSnapshot(streamSql, snapshotAfterRescale.id());
        // stop job
        stopJobSafely(client, resumedJobId);

        // check snapshot and schema
        Snapshot lastSnapshot = findLatestSnapshot("T3");
        assertThat(lastSnapshot).isNotNull();
        SnapshotManager snapshotManager = new SnapshotManager(getTableDirectory("T3"));
        for (long snapshotId = lastSnapshot.id();
                snapshotId > snapshotAfterRescale.id();
                snapshotId--) {
            assertSnapshotSchema(
                    schemaManager, snapshotManager.snapshot(snapshotId).schemaId(), 1L, 4);
        }
        // check data
        assertThat(batchSql("SELECT * FROM T3"))
                .containsExactlyInAnyOrderElementsOf(batchSql("SELECT * FROM T4"));
    }

    private void waitForTheNextSnapshot(@Nullable Long initSnapshotId) throws InterruptedException {
        Snapshot snapshot = findLatestSnapshot("T3");
        while (snapshot == null || new Long(snapshot.id()).equals(initSnapshotId)) {
            Thread.sleep(2000L);
            snapshot = findLatestSnapshot("T3");
        }
    }

    private JobID startJobAndCommitSnapshot(String sql, @Nullable Long initSnapshotId)
            throws Exception {
        JobID jobId = sEnv.executeSql(sql).getJobClient().get().getJobID();
        // let job run until the first snapshot is finished
        waitForTheNextSnapshot(initSnapshotId);
        return jobId;
    }

    private void stopJobSafely(ClusterClient<?> client, JobID jobId)
            throws ExecutionException, InterruptedException {
        client.stopWithSavepoint(jobId, true, path, SavepointFormatType.DEFAULT);
        while (!client.getJobStatus(jobId).get().isGloballyTerminalState()) {
            Thread.sleep(2000L);
        }
    }

    private void assertLatestSchema(
            SchemaManager schemaManager, long expectedSchemaId, int expectedBucketNum) {
        assertThat(schemaManager.latest()).isPresent();
        TableSchema tableSchema = schemaManager.latest().get();
        assertThat(tableSchema.id()).isEqualTo(expectedSchemaId);
        assertThat(tableSchema.options())
                .containsEntry(BUCKET.key(), String.valueOf(expectedBucketNum));
    }

    private void assertSnapshotSchema(
            SchemaManager schemaManager,
            long schemaIdFromSnapshot,
            long expectedSchemaId,
            int expectedBucketNum) {
        assertThat(schemaIdFromSnapshot).isEqualTo(expectedSchemaId);
        TableSchema tableSchema = schemaManager.schema(schemaIdFromSnapshot);
        assertThat(tableSchema.options())
                .containsEntry(BUCKET.key(), String.valueOf(expectedBucketNum));
    }

    private void innerTest(String catalogName, String tableName) {
        String useCatalogSql = "USE CATALOG %s";
        batchSql(useCatalogSql, catalogName);
        String insertSql = "INSERT INTO %s VALUES (1), (2), (3), (4), (5)";
        batchSql(insertSql, tableName);
        Snapshot snapshot = findLatestSnapshot(tableName);
        assertThat(snapshot).isNotNull();

        SchemaManager schemaManager = new SchemaManager(getTableDirectory(tableName));
        assertSnapshotSchema(schemaManager, snapshot.schemaId(), 0L, 2);

        // for managed table schema id remains unchanged, for catalog table id increase from 0 to 1
        batchSql(alterTableSql, tableName, 4);
        assertLatestSchema(schemaManager, 1L, 4);

        // check read is not influenced
        List<Row> expected = Arrays.asList(Row.of(1), Row.of(2), Row.of(3), Row.of(4), Row.of(5));
        assertThat(batchSql("SELECT * FROM %s", tableName))
                .containsExactlyInAnyOrderElementsOf(expected);

        // check write without rescale
        assertThatThrownBy(() -> batchSql("INSERT INTO %s VALUES (6)", tableName))
                .getRootCause()
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "Try to write table with a new bucket num 4, but the previous bucket num is 2. "
                                + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.");

        batchSql(rescaleOverwriteSql, tableName, tableName);
        snapshot = findLatestSnapshot(tableName);
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.id()).isEqualTo(2L);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);
        assertSnapshotSchema(schemaManager, snapshot.schemaId(), 1L, 4);
        assertThat(batchSql("SELECT * FROM %s", tableName))
                .containsExactlyInAnyOrderElementsOf(expected);

        // insert new data
        batchSql("INSERT INTO %s VALUES(6)", tableName);
        expected = Arrays.asList(Row.of(1), Row.of(2), Row.of(3), Row.of(4), Row.of(5), Row.of(6));
        assertThat(batchSql("SELECT * FROM %s", tableName))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private void executeBoth(List<String> sqlList) {
        sqlList.forEach(
                sql -> {
                    sEnv.executeSql(sql);
                    tEnv.executeSql(sql);
                });
    }
}
