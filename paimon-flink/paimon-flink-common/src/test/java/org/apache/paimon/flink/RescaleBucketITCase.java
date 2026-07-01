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
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
                new SchemaManager(LocalFileIO.create(), getTableDirectory("T3"));
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
                new SchemaManager(LocalFileIO.create(), getTableDirectory(tableName));
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

    @Test
    public void testRescaleSinglePartitionViaInsertOverwrite() throws Exception {
        // Create a partitioned primary-key table with 2 buckets.
        // Primary-key tables are sensitive to bucket routing: each row must land in the
        // bucket determined by hash(pk) % totalBuckets, otherwise merging during reads
        // produces duplicates (LSM finds the same key in different buckets and returns both).
        batchSql("USE CATALOG fs_catalog");
        batchSql("DROP TABLE IF EXISTS `TP`");
        batchSql(
                "CREATE TABLE `TP` "
                        + "(dt STRING, f0 INT, PRIMARY KEY (dt, f0) NOT ENFORCED) "
                        + "PARTITIONED BY (dt) "
                        + "WITH ('bucket' = '2')");

        // Insert data into two partitions
        batchSql("INSERT INTO TP VALUES ('p1', 1), ('p1', 2), ('p1', 3)");
        batchSql("INSERT INTO TP VALUES ('p2', 4), ('p2', 5), ('p2', 6)");

        // Alter bucket count to 4
        batchSql(alterTableSql, "TP", 4);

        // INSERT OVERWRITE only partition p1 via the SQL path.
        // The SQL path goes through FlinkTableSinkBase which does NOT wrap the table in
        // OverwriteFileStoreTable. As a result, table.createRowKeyExtractor() calls
        // PartitionBucketMapping.loadFromTable() which reads the old per-partition bucket
        // count from the manifest (2 for p1) and uses it for row routing (hashing).
        // Rows land in buckets [0,1] but files are stamped totalBuckets=4.
        // A subsequent upsert into p1 routes rows using the new 4-bucket mapping — the
        // same key may appear in two different files with different bucket assignments,
        // causing duplicate reads.
        batchSql("INSERT OVERWRITE TP PARTITION (dt = 'p1') SELECT f0 FROM TP WHERE dt = 'p1'");

        // Verify the overwrite preserved data correctly
        assertThat(batchSql("SELECT * FROM TP WHERE dt = 'p1'"))
                .containsExactlyInAnyOrder(Row.of("p1", 1), Row.of("p1", 2), Row.of("p1", 3));
        assertThat(batchSql("SELECT * FROM TP WHERE dt = 'p2'"))
                .containsExactlyInAnyOrder(Row.of("p2", 4), Row.of("p2", 5), Row.of("p2", 6));

        // Now upsert (update) existing rows in p1 — this uses the new 4-bucket mapping.
        // If the overwrite used the old 2-bucket hashing, the original row for key (p1,1)
        // lands in bucket hash(1)%2, but the update lands in bucket hash(1)%4.
        // The LSM reader finds two files for key (p1,1) in different buckets and returns
        // both — causing duplicate rows.
        batchSql("INSERT INTO TP VALUES ('p1', 1), ('p1', 2), ('p1', 3)");
        assertThat(batchSql("SELECT * FROM TP WHERE dt = 'p1'"))
                .as(
                        "After rescale overwrite + upsert of same keys, each key must appear "
                                + "exactly once. If INSERT OVERWRITE used the old 2-bucket "
                                + "routing instead of the new 4-bucket routing (via "
                                + "OverwriteFileStoreTable), the same key will exist in two "
                                + "different buckets causing duplicate rows on read.")
                .containsExactlyInAnyOrder(Row.of("p1", 1), Row.of("p1", 2), Row.of("p1", 3));

        // Also verify the files written by INSERT OVERWRITE are stamped with the new bucket count
        FileStoreTable fileStoreTable = paimonTable("TP");
        Iterator<ManifestEntry> it =
                fileStoreTable
                        .newSnapshotReader()
                        .withPartitionFilter(Collections.singletonMap("dt", "p1"))
                        .onlyReadRealBuckets()
                        .readFileIterator();
        assertThat(it.hasNext()).isTrue();
        while (it.hasNext()) {
            ManifestEntry entry = it.next();
            assertThat(entry.totalBuckets())
                    .as("Files in partition p1 must be stamped with the new bucket count (4)")
                    .isEqualTo(4);
            assertThat(entry.bucket()).as("Bucket index must be in range [0, 3]").isBetween(0, 3);
        }

        batchSql("USE CATALOG default_catalog");
    }

    @Test
    public void testWriteToEmptyBucketAfterRescaleKeepsPartitionBucketCount() throws Exception {
        // End-to-end reproduction of the FileSystemWriteRestore empty-bucket scenario.
        //
        // The scenario occurs when a partition has a bucket count that differs from
        // the table-level default, AND at least one of that partition's buckets
        // has NO existing data files. A subsequent write that lands in the
        // empty bucket must keep the partition's bucket count, not silently
        // adopt the table default.
        //
        // Final state we want before the bug-triggering write (Step 5):
        //   - table default bucket : 2
        //   - partition p1 bucket  : 4 (held over from a prior rescale+overwrite)
        //   - partition p1 bucket=X: has data files, totalBuckets = 4
        //   - partition p1 bucket=Y: NO data files
        //
        // The Step 5 write fills bucket=Y in p1. Post-fix, the new file MUST
        // be stamped totalBuckets=4. Before the fix, FileSystemWriteRestore
        // returned null for the empty bucket, the writer fell back to the
        // table default (2), and the new file was stamped totalBuckets=2,
        // corrupting p1's bucket layout.

        // Step 1: create a partitioned PK table with bucket=2 and seed one row
        // into p1.
        batchSql(
                "CREATE TABLE IF NOT EXISTS `fs_catalog`.`default`.`TPE` "
                        + "(dt STRING, f0 INT, PRIMARY KEY (dt, f0) NOT ENFORCED) "
                        + "PARTITIONED BY (dt) "
                        + "WITH ('bucket' = '2')");
        batchSql("USE CATALOG fs_catalog");
        batchSql("INSERT INTO TPE VALUES ('p1', 1)");
        // Also seed partition p2 — this acts as a positive control: p2 is
        // never rescaled and must always be stamped with the current table
        // default bucket count.
        batchSql("INSERT INTO TPE VALUES ('p2', 1)");

        // Step 2: rescale the table default to 4.
        batchSql(alterTableSql, "TPE", 4);

        // Step 3: INSERT OVERWRITE p1 — this rewrites p1's files using the
        // current table default (4), so afterwards p1's files are stamped
        // totalBuckets=4. With only a single row in p1, at least one of p1's
        // four buckets is guaranteed to be empty.
        batchSql("INSERT OVERWRITE TPE PARTITION (dt = 'p1') SELECT f0 FROM TPE WHERE dt = 'p1'");

        // Step 4 (NEW): rescale the table default BACK to 2, while p1 keeps
        // its 4-bucket layout. Now the per-partition bucket count (4) diverges
        // from the table default (2) — the precondition for the bug.
        batchSql(alterTableSql, "TPE", 2);

        // Step 5: insert more rows into p1. PartitionBucketMapping reads the
        // manifest and routes these rows using p1's actual bucket count (4).
        // Several rows are inserted so that — with high probability — at
        // least one lands in the bucket that was previously empty, exercising
        // the FileSystemWriteRestore empty-bucket code path.
        // Also insert another row into p2 — p2 has never been rescaled and
        // must remain stamped with the current table default bucket count (2).
        batchSql(
                "INSERT INTO TPE VALUES "
                        + "('p1', 2), ('p1', 3), ('p1', 4), ('p1', 5), ('p1', 6), "
                        + "('p2', 2), ('p2', 3), ('p2', 4), ('p2', 5), ('p2', 6)");

        // Sanity check: each PK appears exactly once. Duplicates would indicate
        // that the new rows landed in an inconsistent bucket layout (e.g. some
        // files stamped totalBuckets=2 and others stamped totalBuckets=4),
        // causing the LSM reader to find the same PK in two different buckets.
        assertThat(batchSql("SELECT * FROM TPE"))
                .as(
                        "Each PK must appear exactly once across all partitions. Duplicates "
                                + "indicate rows landed in inconsistent bucket layouts.")
                .containsExactlyInAnyOrder(
                        Row.of("p1", 1),
                        Row.of("p1", 2),
                        Row.of("p1", 3),
                        Row.of("p1", 4),
                        Row.of("p1", 5),
                        Row.of("p1", 6),
                        Row.of("p2", 1),
                        Row.of("p2", 2),
                        Row.of("p2", 3),
                        Row.of("p2", 4),
                        Row.of("p2", 5),
                        Row.of("p2", 6));

        // Strong assertion (p1 — the rescaled partition): every file must be
        // stamped totalBuckets=4 (the partition's actual bucket count), NOT 2
        // (the new table default). This is the precise condition the fix
        // guarantees for partitions whose bucket count differs from the table
        // default.
        FileStoreTable fileStoreTable = paimonTable("TPE");
        Iterator<ManifestEntry> p1Iter =
                fileStoreTable
                        .newSnapshotReader()
                        .withPartitionFilter(Collections.singletonMap("dt", "p1"))
                        .onlyReadRealBuckets()
                        .readFileIterator();
        assertThat(p1Iter.hasNext()).isTrue();
        while (p1Iter.hasNext()) {
            ManifestEntry entry = p1Iter.next();
            assertThat(entry.totalBuckets())
                    .as(
                            "Files in partition p1 must keep the partition's bucket count (4). "
                                    + "If 2, FileSystemWriteRestore failed to fall back to "
                                    + "PartitionBucketMapping for buckets that had no existing "
                                    + "files at restore time, and stamped new files with the "
                                    + "table-level default bucket count instead.")
                    .isEqualTo(4);
            assertThat(entry.bucket()).as("Bucket index must be in range [0, 3]").isBetween(0, 3);
        }

        // Positive control (p2 — never rescaled): every file must be stamped
        // with the current table default (2). This guards against an
        // over-eager fix that would erroneously route ALL writes through
        // PartitionBucketMapping even when no per-partition override exists.
        Iterator<ManifestEntry> p2Iter =
                fileStoreTable
                        .newSnapshotReader()
                        .withPartitionFilter(Collections.singletonMap("dt", "p2"))
                        .onlyReadRealBuckets()
                        .readFileIterator();
        assertThat(p2Iter.hasNext()).isTrue();
        java.util.List<ManifestEntry> p2Entries = new java.util.ArrayList<>();
        while (p2Iter.hasNext()) {
            p2Entries.add(p2Iter.next());
        }
        // Diagnostic: dump every manifest entry in p2 so we can see what each
        // file is stamped with (FileKind, bucket, totalBuckets, file name,
        // snapshot/sequence info via the file meta).
        System.out.println("=== p2 manifest entries (" + p2Entries.size() + " total) ===");
        for (int i = 0; i < p2Entries.size(); i++) {
            ManifestEntry entry = p2Entries.get(i);
            System.out.printf(
                    "  [%d] kind=%s level=%d bucket=%d totalBuckets=%d file=%s "
                            + "minSeq=%d maxSeq=%d rowCount=%d%n",
                    i,
                    entry.kind(),
                    entry.level(),
                    entry.bucket(),
                    entry.totalBuckets(),
                    entry.file().fileName(),
                    entry.file().minSequenceNumber(),
                    entry.file().maxSequenceNumber(),
                    entry.file().rowCount());
        }
        for (ManifestEntry entry : p2Entries) {
            assertThat(entry.totalBuckets())
                    .as(
                            "Files in partition p2 must use the current table default bucket "
                                    + "count (2). p2 was never rescaled, so its files should "
                                    + "always reflect the table-level default.")
                    .isEqualTo(2);
            assertThat(entry.bucket()).as("Bucket index must be in range [0, 1]").isBetween(0, 1);
        }
    }

    private void executeBoth(List<String> sqlList) {
        sqlList.forEach(
                sql -> {
                    sEnv.executeSql(sql);
                    tEnv.executeSql(sql);
                });
    }
}
