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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.fs.local.LocalFileIOLoader;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test case for append-only managed table. */
public class AppendOnlyTableITCase extends CatalogITCaseBase {
    @TempDir Path tempExternalPath1;
    @TempDir Path tempExternalPath2;

    @Test
    public void testCreateUnawareBucketTableWithBucketKey() {
        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "CREATE TABLE pk_table (id INT, data STRING) "
                                                + "WITH ('bucket' = '-1', 'bucket-key' = 'id')"))
                .hasRootCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage(
                        "Cannot define 'bucket-key' with bucket = -1, please remove the 'bucket-key' setting or specify a bucket number.");
    }

    @Test
    public void testCreateUnawareBucketTableWithFullCompaction() {
        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "CREATE TABLE pk_table (id INT, data STRING) "
                                                + "WITH ('bucket' = '-1','full-compaction.delta-commits'='10')"))
                .hasRootCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage(
                        "AppendOnlyTable of unaware or dynamic bucket does not support 'full-compaction.delta-commits'");
    }

    @Test
    public void testReadEmpty() {
        assertThat(batchSql("SELECT * FROM append_table")).isEmpty();
    }

    @Test
    public void testReadWrite() {
        batchSql("INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB')");

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

        rows = batchSql("SELECT data from append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("AAA"), Row.of("BBB"));
    }

    @Test
    public void testReadWriteWithExternalPathRoundRobinStrategy1() {
        String externalPaths =
                TraceableFileIO.SCHEME
                        + "://"
                        + tempExternalPath1.toString()
                        + ","
                        + LocalFileIOLoader.SCHEME
                        + "://"
                        + tempExternalPath2.toString();
        batchSql(
                "ALTER TABLE append_table SET ('data-file.external-paths' = '"
                        + externalPaths
                        + "')");
        batchSql(
                "ALTER TABLE append_table SET ('data-file.external-paths.strategy' = 'round-robin')");

        batchSql("INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB')");
        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

        rows = batchSql("SELECT data from append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("AAA"), Row.of("BBB"));

        batchSql("INSERT INTO append_table VALUES (3, 'CCC')");
        rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(3);
        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "CCC"));

        batchSql("INSERT INTO append_table VALUES (4, 'DDD')");
        rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "CCC"), Row.of(4, "DDD"));
    }

    @Test
    public void testReadWriteWithExternalPathRoundRobinStrategy2() {
        batchSql("INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB')");
        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

        rows = batchSql("SELECT data from append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("AAA"), Row.of("BBB"));

        String externalPaths =
                TraceableFileIO.SCHEME
                        + "://"
                        + tempExternalPath1.toString()
                        + ","
                        + LocalFileIOLoader.SCHEME
                        + "://"
                        + tempExternalPath2.toString();
        batchSql(
                "ALTER TABLE append_table SET ('data-file.external-paths' = '"
                        + externalPaths
                        + "')");
        batchSql(
                "ALTER TABLE append_table SET ('data-file.external-paths.strategy' = 'round-robin')");

        batchSql("INSERT INTO append_table VALUES (3, 'CCC')");
        rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(3);
        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "CCC"));

        batchSql("INSERT INTO append_table VALUES (4, 'DDD')");
        rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "CCC"), Row.of(4, "DDD"));
    }

    @Test
    public void testReadWriteWithExternalPathSpecificFSStrategy() {
        String externalPaths = TraceableFileIO.SCHEME + "://" + tempExternalPath1.toString();
        batchSql(
                "ALTER TABLE append_table SET ('data-file.external-paths' = '"
                        + externalPaths
                        + "')");
        batchSql(
                "ALTER TABLE append_table SET ('data-file.external-paths.strategy' = 'specific-fs')");
        batchSql(
                "ALTER TABLE append_table SET ('data-file.external-paths.specific-fs' = 'traceable')");

        batchSql("INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB')");
        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

        rows = batchSql("SELECT data from append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("AAA"), Row.of("BBB"));

        batchSql("INSERT INTO append_table VALUES (3, 'CCC')");
        rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(3);
        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "CCC"));

        batchSql("INSERT INTO append_table VALUES (4, 'DDD')");
        rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "CCC"), Row.of(4, "DDD"));
    }

    @Test
    public void testReadWriteWithExternalPathNoneStrategy() {
        String externalPaths = TraceableFileIO.SCHEME + "://" + tempExternalPath1.toString();
        batchSql(
                "ALTER TABLE append_table SET ('data-file.external-paths' = '"
                        + externalPaths
                        + "')");
        batchSql("ALTER TABLE append_table SET ('data-file.external-paths.strategy' = 'none')");

        batchSql("INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB')");
        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

        rows = batchSql("SELECT data from append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("AAA"), Row.of("BBB"));

        batchSql("INSERT INTO append_table VALUES (3, 'CCC')");
        rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(3);
        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "CCC"));

        batchSql("INSERT INTO append_table VALUES (4, 'DDD')");
        rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "CCC"), Row.of(4, "DDD"));
    }

    @Test
    public void testTableReadWriteWithChangedExternalPath() throws Exception {
        String externalPaths1 = TraceableFileIO.SCHEME + "://" + tempExternalPath1.toString();
        String externalPaths2 = TraceableFileIO.SCHEME + "://" + tempExternalPath2.toString();

        int bucket = ThreadLocalRandom.current().nextBoolean() ? 1 : -1;
        String bucketKey = bucket == 1 ? "'bucket-key' = 'k'," : "";
        sEnv.executeSql(
                "CREATE TABLE T2 ( k INT, v INT) "
                        + "WITH ( "
                        + "'bucket' = '"
                        + bucket
                        + "',"
                        + bucketKey
                        + "'sink.writer-refresh-detectors' = 'external-paths',"
                        + "'data-file.external-paths' = '"
                        + externalPaths1
                        + "',"
                        + "'data-file.external-paths.strategy' = 'round-robin'"
                        + ")");

        // Create a datagen source table
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE datagen_source (k INT, v INT) WITH ("
                        + "'connector' = 'datagen', "
                        + "'rows-per-second' = '1', "
                        + "'fields.k.kind' = 'sequence', "
                        + "'fields.k.start' = '1', "
                        + "'fields.k.end' = '5', "
                        + "'fields.v.kind' = 'sequence', "
                        + "'fields.v.start' = '1', "
                        + "'fields.v.end' = '5'"
                        + ")");

        CloseableIterator<Row> it = streamSqlIter("SELECT * FROM T2");

        // Insert data from datagen source into T2
        sEnv.executeSql("INSERT INTO T2 SELECT * FROM datagen_source");
        sEnv.executeSql(
                "ALTER TABLE T2 SET ( "
                        + "'data-file.external-paths' = '"
                        + externalPaths2
                        + "'"
                        + ")");

        // Read and verify the data
        List<String> resultRows = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            resultRows.add(it.next().toString());
        }

        assertThat(resultRows)
                .containsExactlyInAnyOrder(
                        "+I[1, 1]", "+I[2, 2]", "+I[3, 3]", "+I[4, 4]", "+I[5, 5]");

        LocalFileIO fileIO = LocalFileIO.create();
        assertThat(fileIO.exists(new org.apache.paimon.fs.Path(tempExternalPath1.toString())))
                .isTrue();
        assertThat(fileIO.exists(new org.apache.paimon.fs.Path(tempExternalPath2.toString())))
                .isTrue();
        assertThat(
                        fileIO.listStatus(
                                        new org.apache.paimon.fs.Path(tempExternalPath1.toString()))
                                .length)
                .isGreaterThanOrEqualTo(1);
        assertThat(
                        fileIO.listStatus(
                                        new org.apache.paimon.fs.Path(tempExternalPath2.toString()))
                                .length)
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testReadUnawareBucketTableWithRebalanceShuffle() throws Exception {
        batchSql(
                "CREATE TABLE append_scalable_table (id INT, data STRING) "
                        + "WITH ('bucket' = '-1', 'consumer-id' = 'test', 'consumer.expiration-time' = '365 d', 'target-file-size' = '1 B', 'source.split.target-size' = '1 B', 'scan.parallelism' = '4')");
        batchSql("INSERT INTO append_scalable_table VALUES (1, 'AAA'), (2, 'BBB')");
        batchSql("INSERT INTO append_scalable_table VALUES (1, 'AAA'), (2, 'BBB')");
        batchSql("INSERT INTO append_scalable_table VALUES (1, 'AAA'), (2, 'BBB')");
        batchSql("INSERT INTO append_scalable_table VALUES (1, 'AAA'), (2, 'BBB')");

        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter(("SELECT id FROM append_scalable_table")));
        assertThat(iterator.collect(2)).containsExactlyInAnyOrder(Row.of(1), Row.of(2));
        iterator.close();
    }

    @Test
    public void testReadPartitionOrder() {
        setParallelism(1);
        batchSql("INSERT INTO part_table VALUES (1, 'AAA', 'part-1')");
        batchSql("INSERT INTO part_table VALUES (2, 'BBB', 'part-2')");
        batchSql("INSERT INTO part_table VALUES (3, 'CCC', 'part-3')");

        assertThat(batchSql("SELECT * FROM part_table"))
                .containsExactly(
                        Row.of(1, "AAA", "part-1"),
                        Row.of(2, "BBB", "part-2"),
                        Row.of(3, "CCC", "part-3"));
    }

    @Test
    public void testSkipDedup() {
        batchSql("INSERT INTO append_table VALUES (1, 'AAA'), (1, 'AAA'), (2, 'BBB'), (3, 'BBB')");

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "BBB"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(1), Row.of(2), Row.of(3));

        rows = batchSql("SELECT data FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("AAA"), Row.of("AAA"), Row.of("BBB"), Row.of("BBB"));
    }

    @Test
    public void testIngestFromSource() {
        List<Row> input =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, "AAA"),
                        Row.ofKind(RowKind.INSERT, 1, "AAA"),
                        Row.ofKind(RowKind.INSERT, 1, "BBB"),
                        Row.ofKind(RowKind.INSERT, 2, "AAA"));

        String id = TestValuesTableFactory.registerData(input);
        batchSql(
                "CREATE TEMPORARY TABLE source (id INT, data STRING) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);

        batchSql("INSERT INTO append_table SELECT * FROM source");

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(1, "AAA"), Row.of(1, "BBB"), Row.of(2, "AAA"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(1), Row.of(1), Row.of(2));

        rows = batchSql("SELECT data FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("AAA"), Row.of("AAA"), Row.of("BBB"), Row.of("AAA"));
    }

    @Test
    public void testAutoCompaction() {
        batchSql("ALTER TABLE append_table SET ('compaction.min.file-num' = '4')");

        assertAutoCompaction(
                "INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB')",
                1L,
                Snapshot.CommitKind.APPEND);
        assertAutoCompaction(
                "INSERT INTO append_table VALUES (3, 'CCC'), (4, 'DDD')",
                2L,
                Snapshot.CommitKind.APPEND);
        assertAutoCompaction(
                "INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC'), (4, 'DDD')",
                3L,
                Snapshot.CommitKind.APPEND);
        assertAutoCompaction(
                "INSERT INTO append_table VALUES (5, 'EEE'), (6, 'FFF')",
                5L,
                Snapshot.CommitKind.COMPACT);
        assertAutoCompaction(
                "INSERT INTO append_table VALUES (7, 'HHH'), (8, 'III')",
                6L,
                Snapshot.CommitKind.APPEND);
        assertAutoCompaction(
                "INSERT INTO append_table VALUES (9, 'JJJ'), (10, 'KKK')",
                7L,
                Snapshot.CommitKind.APPEND);
        assertAutoCompaction(
                "INSERT INTO append_table VALUES (11, 'LLL'), (12, 'MMM')",
                9L,
                Snapshot.CommitKind.COMPACT);

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(16);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"),
                        Row.of(2, "BBB"),
                        Row.of(3, "CCC"),
                        Row.of(4, "DDD"),
                        Row.of(1, "AAA"),
                        Row.of(2, "BBB"),
                        Row.of(3, "CCC"),
                        Row.of(4, "DDD"),
                        Row.of(5, "EEE"),
                        Row.of(6, "FFF"),
                        Row.of(7, "HHH"),
                        Row.of(8, "III"),
                        Row.of(9, "JJJ"),
                        Row.of(10, "KKK"),
                        Row.of(11, "LLL"),
                        Row.of(12, "MMM"));
    }

    @Test
    public void testAutoCluster() {
        batchSql("ALTER TABLE append_table SET ('num-sorted-run.compaction-trigger' = '3')");
        batchSql("ALTER TABLE append_table SET ('num-levels' = '6')");
        batchSql("ALTER TABLE append_table SET ('bucket-append-ordered' = 'false')");
        batchSql("ALTER TABLE append_table SET ('clustering.columns' = 'data')");
        batchSql("ALTER TABLE append_table SET ('clustering.strategy' = 'order')");
        batchSql("ALTER TABLE append_table SET ('clustering.incremental' = 'true')");

        assertAutoCompaction(
                "INSERT INTO append_table VALUES (1, '9')", 1L, Snapshot.CommitKind.APPEND);
        assertAutoCompaction(
                "INSERT INTO append_table VALUES (2, '8')", 2L, Snapshot.CommitKind.APPEND);
        assertAutoCompaction(
                "INSERT INTO append_table VALUES (3, '7')", 4L, Snapshot.CommitKind.COMPACT);
        assertAutoCompaction(
                "INSERT INTO append_table VALUES (4, '6')", 5L, Snapshot.CommitKind.APPEND);
        assertAutoCompaction(
                "INSERT INTO append_table VALUES (5, '5')", 7L, Snapshot.CommitKind.COMPACT);

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(5);
        assertThat(rows)
                .containsExactly(
                        Row.of(5, "5"),
                        Row.of(4, "6"),
                        Row.of(3, "7"),
                        Row.of(2, "8"),
                        Row.of(1, "9"));
    }

    @Test
    public void testRejectDelete() {
        testRejectChanges(RowKind.DELETE);
    }

    @Test
    public void testRejectUpdateBefore() {
        testRejectChanges(RowKind.UPDATE_BEFORE);
    }

    @Test
    public void testRejectUpdateAfter() {
        testRejectChanges(RowKind.UPDATE_BEFORE);
    }

    @Test
    public void testComplexType() {
        batchSql("INSERT INTO complex_table VALUES (1, CAST(NULL AS MAP<INT, INT>))");
        assertThat(batchSql("SELECT * FROM complex_table")).containsExactly(Row.of(1, null));
    }

    @Test
    public void testNestedTypeDDL() {
        assertThrows(
                RuntimeException.class,
                () ->
                        batchSql(
                                "CREATE TABLE IF NOT EXISTS nested_table (id INT, data MAP<INT, INT>) WITH ('bucket' = '1', 'bucket-key'='id,data')"),
                "nested type can not in bucket-key, in your table these key are [data]");
    }

    @Test
    public void testTimestampLzType() {
        sql("CREATE TABLE t_table (id INT, data TIMESTAMP_LTZ(3))");
        batchSql("INSERT INTO t_table VALUES (1, TIMESTAMP '2023-02-03 20:20:20')");
        assertThat(batchSql("SELECT * FROM t_table"))
                .containsExactly(
                        Row.of(
                                1,
                                LocalDateTime.parse("2023-02-03T20:20:20")
                                        .atZone(ZoneId.systemDefault())
                                        .toInstant()));
    }

    @Test
    public void testDynamicOptions() throws Exception {
        sql("CREATE TABLE T (id INT)");
        batchSql("INSERT INTO T VALUES (1)");
        sEnv.getConfig()
                .getConfiguration()
                .setString(
                        "paimon.*.*.T." + CoreOptions.SCAN_MODE.key(),
                        CoreOptions.StartupMode.LATEST.toString());
        BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM T");

        // wait streaming job start
        Thread.sleep(2000);

        sql("INSERT INTO T VALUES (2)");
        // Only fetch latest snapshot is, dynamic option worked
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(2));
    }

    @Test
    public void testReadWriteBranch() throws Exception {
        // create table
        sql("CREATE TABLE T (id INT)");
        // insert data
        batchSql("INSERT INTO T VALUES (1)");
        // create tag
        paimonTable("T").createTag("tag1", 1);
        // create branch
        paimonTable("T").createBranch("branch1", "tag1");
        // insert data to branch
        batchSql("INSERT INTO T/*+ OPTIONS('branch' = 'branch1') */ VALUES (2)");
        List<Row> rows = batchSql("select * from T /*+ OPTIONS('branch' = 'branch1') */");
        assertThat(rows).containsExactlyInAnyOrder(Row.of(2), Row.of(1));
    }

    @Test
    public void testBranchNotExist() throws Exception {
        // create table
        sql("CREATE TABLE T (id INT)");
        // insert data
        batchSql("INSERT INTO T VALUES (1)");
        // create tag
        paimonTable("T").createTag("tag1", 1);
        // create branch
        paimonTable("T").createBranch("branch1", "tag1");
        // call the FileSystemCatalog.getDataTableSchema() function
        assertThatThrownBy(() -> paimonTable("T$branch_branch2"))
                .isInstanceOf(Catalog.TableNotExistException.class)
                .hasMessage("Table %s does not exist.", "default.T$branch_branch2");
    }

    @Test
    public void testStreamReadOverwriteTable() throws Exception {
        // create table
        sql(
                "CREATE TABLE T (id INT, p STRING) PARTITIONED BY (p) with ("
                        + "'streaming-read-append-overwrite' = 'true')");
        BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM T");
        // insert data
        batchSql("INSERT OVERWRITE T PARTITION(p = '2024') VALUES (1)");
        Assertions.assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(1, "2024"));
        batchSql("INSERT OVERWRITE T PARTITION(p = '2024') VALUES (2)");
        Assertions.assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(2, "2024"));
        iterator.close();
    }

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS append_table (id INT, data STRING) WITH ('bucket' = '1', 'bucket-key'='id')",
                "CREATE TABLE IF NOT EXISTS part_table (id INT, data STRING, dt STRING) PARTITIONED BY (dt) WITH ('bucket' = '1', 'bucket-key'='id')",
                "CREATE TABLE IF NOT EXISTS complex_table (id INT, data MAP<INT, INT>) WITH ('bucket' = '1', 'bucket-key'='id')");
    }

    private void testRejectChanges(RowKind kind) {
        List<Row> input = Collections.singletonList(Row.ofKind(kind, 1, "AAA"));

        String id = TestValuesTableFactory.registerData(input);
        batchSql(
                "CREATE TEMPORARY TABLE source (id INT, data STRING) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);

        assertThatThrownBy(() -> batchSql("INSERT INTO append_table SELECT * FROM source"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Append only writer can not accept row with RowKind %s", kind);
    }

    private void assertAutoCompaction(
            String sql, long expectedSnapshotId, Snapshot.CommitKind expectedCommitKind) {
        batchSql(sql);
        Snapshot snapshot = findLatestSnapshot("append_table");
        assertThat(snapshot.id()).isEqualTo(expectedSnapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(expectedCommitKind);
    }
}
