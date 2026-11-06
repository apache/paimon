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
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.SnapshotTest.newSnapshotManager;
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
                        "CREATE CATALOG `fs_catalog` WITH ('type' = 'paimon', 'warehouse' = '%s')",
                        path),
                "CREATE TABLE IF NOT EXISTS `fs_catalog`.`default`.`T1` (f0 INT) WITH ('bucket' = '2', 'bucket-key' = 'f0')");
    }

    @Test
    public void testRescaleCatalogTable() {
        innerTest("fs_catalog", "T1");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRescalePreservesGuardOnlyPartitions(boolean disableCapture) {
        batchSql("USE CATALOG fs_catalog");
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        batchSql(
                "CREATE TABLE guarded_rescale (pt STRING, id INT, seq INT, op STRING, "
                        + "PRIMARY KEY (pt, id) NOT ENFORCED) PARTITIONED BY (pt) WITH ("
                        + "'bucket' = '2', 'sequence.field' = 'seq', 'rowkind.field' = 'op', "
                        + "'write-only' = 'true', 'delete-ordering-guards.enabled' = 'true')");
        batchSql(
                "INSERT INTO guarded_rescale VALUES "
                        + "('p1', 1, 4, '-D'), ('p2', 2, 4, '-D'), ('p2', 3, 3, '+I')");
        batchSql(
                "CALL sys.compact(`table` => 'default.guarded_rescale', "
                        + "compact_strategy => 'full', options => 'write-only=false')");
        PrimaryKeyFileStoreTable table =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(
                                LocalFileIO.create(), getTableDirectory("guarded_rescale"));
        assertThat(table.store().newIndexFileHandler().scan("DELETE_ORDERING_GUARD")).isNotEmpty();
        assertThat(batchSql("SELECT * FROM guarded_rescale"))
                .containsExactly(Row.of("p2", 3, 3, "+I"));
        batchSql(
                "ALTER TABLE guarded_rescale SET ('bucket' = '5'"
                        + (disableCapture ? ", 'delete-ordering-guards.enabled' = 'false'" : "")
                        + ")");
        batchSql(
                "CALL sys.rescale(`table` => 'default.guarded_rescale', "
                        + "`partition` => 'pt=p1', scan_parallelism => 2, sink_parallelism => 2)");
        table =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(
                                LocalFileIO.create(), getTableDirectory("guarded_rescale"));
        assertThat(table.newSnapshotReader().withDeleteOrderingGuardBuckets().read().dataSplits())
                .filteredOn(split -> split.partition().getString(0).toString().equals("p1"))
                .isNotEmpty()
                .allSatisfy(
                        split -> {
                            assertThat(split.totalBuckets()).isEqualTo(5);
                            assertThat(split.dataFiles()).isEmpty();
                        });
        long beforeStale = table.snapshotManager().latestSnapshotId();
        batchSql("INSERT INTO guarded_rescale VALUES ('p1', 1, 2, '+I')");
        assertThat(batchSql("SELECT * FROM guarded_rescale WHERE pt = 'p1'")).isEmpty();
        long afterStale = table.snapshotManager().latestSnapshotId();
        assertThat(
                        batchSql(
                                "SELECT id, seq FROM guarded_rescale /*+ OPTIONS("
                                        + "'incremental-between'='"
                                        + beforeStale
                                        + ","
                                        + afterStale
                                        + "', 'incremental-between-scan-mode'='diff') */ WHERE pt = 'p1'"))
                .isEmpty();
        batchSql("INSERT INTO guarded_rescale VALUES ('p1', 1, 5, '+I')");
        assertThat(batchSql("SELECT * FROM guarded_rescale WHERE pt = 'p1'"))
                .containsExactly(Row.of("p1", 1, 5, "+I"));
        long afterFresh = table.snapshotManager().latestSnapshotId();
        assertThat(
                        batchSql(
                                "SELECT id, seq FROM guarded_rescale /*+ OPTIONS("
                                        + "'incremental-between'='"
                                        + beforeStale
                                        + ","
                                        + afterFresh
                                        + "', 'incremental-between-scan-mode'='diff') */ WHERE pt = 'p1'"))
                .containsExactly(Row.of(1, 5));
        batchSql(
                "CALL sys.rescale(`table` => 'default.guarded_rescale', "
                        + "bucket_num => 5, `partition` => 'pt=p2')");
        batchSql("INSERT INTO guarded_rescale VALUES ('p2', 2, 2, '+I')");
        assertThat(batchSql("SELECT * FROM guarded_rescale"))
                .containsExactlyInAnyOrder(Row.of("p1", 1, 5, "+I"), Row.of("p2", 3, 3, "+I"));
    }

    @Test
    public void testSuspendAndRecoverAfterRescaleOverwrite() throws Exception {
        // register a companion table T4 for T3
        executeBoth(
                Arrays.asList(
                        "USE CATALOG fs_catalog",
                        "CREATE TEMPORARY TABLE IF NOT EXISTS `S0` (f0 INT) WITH ('connector' = 'datagen')",
                        "CREATE TABLE IF NOT EXISTS `T3` (f0 INT) WITH ('bucket' = '2', 'bucket-key' = 'f0')",
                        "CREATE TABLE IF NOT EXISTS `T4` (f0 INT)"));
        SchemaManager schemaManager =
                new FileSystemSchemaManager(LocalFileIO.create(), getTableDirectory("T3"));
        assertLatestSchema(schemaManager, 0L, 2);

        String streamSql =
                "EXECUTE STATEMENT SET BEGIN\n "
                        + "INSERT INTO `T3` SELECT * FROM `S0`;\n "
                        + "INSERT INTO `T4` SELECT * FROM `S0`;\n"
                        + "END";

        // step1: run streaming insert
        JobClient jobClient = startJobAndCommitSnapshot(streamSql, null);

        // step2: stop with savepoint
        String savepointPath = stopJobSafely(jobClient);

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
        // use config string to stay compatible with flink 1.19-
        sEnv.getConfig()
                .getConfiguration()
                .setString("execution.state-recovery.path", savepointPath);
        JobClient resumedJobClient =
                startJobAndCommitSnapshot(streamSql, snapshotAfterRescale.id());
        // stop job
        stopJobSafely(resumedJobClient);

        // check snapshot and schema
        Snapshot lastSnapshot = findLatestSnapshot("T3");
        assertThat(lastSnapshot).isNotNull();
        SnapshotManager snapshotManager =
                newSnapshotManager(LocalFileIO.create(), getTableDirectory("T3"));
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

    private JobClient startJobAndCommitSnapshot(String sql, @Nullable Long initSnapshotId)
            throws Exception {
        JobClient jobClient = sEnv.executeSql(sql).getJobClient().get();
        // let job run until the first snapshot is finished
        waitForTheNextSnapshot(initSnapshotId);
        return jobClient;
    }

    private String stopJobSafely(JobClient client) throws ExecutionException, InterruptedException {
        CompletableFuture<String> savepointPath =
                client.stopWithSavepoint(true, path, SavepointFormatType.DEFAULT);
        while (!client.getJobStatus().get().isGloballyTerminalState()) {
            Thread.sleep(2000L);
        }
        return savepointPath.get();
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

        SchemaManager schemaManager =
                new FileSystemSchemaManager(LocalFileIO.create(), getTableDirectory(tableName));
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
                .rootCause()
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        "Try to write table with a new bucket num 4, but the previous bucket num is 2. "
                                + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.");

        if (ThreadLocalRandom.current().nextBoolean()) {
            batchSql(rescaleOverwriteSql, tableName, tableName);
        } else {
            tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
            batchSql(String.format("CALL sys.rescale(`table` => 'default.%s')", tableName));
        }

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
