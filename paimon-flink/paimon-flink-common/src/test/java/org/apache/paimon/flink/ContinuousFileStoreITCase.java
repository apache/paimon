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
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SQL ITCase for continuous file store. */
public class ContinuousFileStoreITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS T1 (a STRING, b STRING, c STRING) WITH ('bucket' = '1', 'bucket-key' = 'a')",
                "CREATE TABLE IF NOT EXISTS T2 (a STRING, b STRING, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + " WITH ('changelog-producer'='input', 'bucket' = '1')");
    }

    @Test
    public void testSourceReuseWithScanPushDown() {
        // source can be reused with projection applied
        sEnv.executeSql("CREATE TEMPORARY TABLE print1 (a STRING) WITH ('connector'='print')");
        sEnv.executeSql("CREATE TEMPORARY TABLE print2 (b STRING) WITH ('connector'='print')");

        StatementSet statementSet = sEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO print1 SELECT a FROM T1");
        statementSet.addInsertSql("INSERT INTO print2 SELECT b FROM T1");
        assertThat(statementSet.compilePlan().explain()).contains("Reused");

        // source cannot be reused with filter or limit applied
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE new_print1 (a STRING, b STRING, c STRING) WITH ('connector'='print')");
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE new_print2 (a STRING, b STRING, c STRING) WITH ('connector'='print')");

        statementSet = sEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO new_print1 SELECT * FROM T1 WHERE a = 'Apache'");
        statementSet.addInsertSql("INSERT INTO new_print2 SELECT * FROM T1");
        assertThat(statementSet.compilePlan().explain()).doesNotContain("Reused");

        statementSet = sEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO new_print1 SELECT * FROM T1 LIMIT 5");
        statementSet.addInsertSql("INSERT INTO new_print2 SELECT * FROM T1");
        assertThat(statementSet.compilePlan().explain()).doesNotContain("Reused");
    }

    @Test
    public void testWithoutPrimaryKey() throws Exception {
        testSimple("T1");
    }

    @Test
    public void testWithPrimaryKey() throws Exception {
        testSimple("T2");
    }

    @Test
    public void testProjectionWithoutPrimaryKey() throws Exception {
        testProjection("T1");
    }

    @Test
    public void testProjectionWithPrimaryKey() throws Exception {
        testProjection("T2");
    }

    @Test
    public void testConsumerId() throws Exception {
        String table = "T2";
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM %s /*+ OPTIONS('consumer-id'='me','consumer.expiration-time'='3h') */",
                                table));

        batchSql("INSERT INTO %s VALUES ('1', '2', '3'), ('4', '5', '6')", table);
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));

        List<Row> result;
        do {
            result = sql("SELECT * FROM %s$consumers", table);
            if (!result.isEmpty()) {
                break;
            }
            Thread.sleep(1000);
        } while (true);
        iterator.close();

        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM %s /*+ OPTIONS('consumer-id'='me','consumer.expiration-time'='3h') */",
                                table));
        batchSql("INSERT INTO %s VALUES ('7', '8', '9')", table);
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("7", "8", "9"));
        iterator.close();
    }

    @Test
    public void testConsumerIdInBatch() throws Exception {
        String table = "T2";

        batchSql("INSERT INTO %s VALUES ('1', '2', '3'), ('4', '5', '6')", table);
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM %s /*+ OPTIONS('consumer-id'='me','consumer.expiration-time'='3h') */",
                                table));

        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));

        Thread.sleep(1000);
        iterator.close();

        batchSql("INSERT INTO %s VALUES ('7', '8', '9')", table);
        // ignore the consumer id in batch mode
        assertThat(
                        sql(
                                "SELECT * FROM %s /*+ OPTIONS('consumer-id'='me','consumer.expiration-time'='3h') */",
                                table))
                .containsExactlyInAnyOrder(
                        Row.of("1", "2", "3"), Row.of("4", "5", "6"), Row.of("7", "8", "9"));
    }

    @Test
    @Timeout(120)
    public void testSnapshotWatermark() throws Exception {
        streamSqlIter(
                "CREATE TEMPORARY TABLE gen (a STRING, b STRING, c STRING,"
                        + " dt AS NOW(), WATERMARK FOR dt AS dt) WITH ('connector'='datagen')");
        CloseableIterator<Row> insert1 = streamSqlIter("INSERT INTO T2 SELECT a, b, c FROM gen");
        sql("CREATE TABLE WT (a STRING, b STRING, c STRING, PRIMARY KEY (a) NOT ENFORCED)");
        CloseableIterator<Row> insert2 =
                streamSqlIter(
                        "INSERT INTO WT SELECT * FROM T2 /*+ OPTIONS('consumer-id'='me','consumer.expiration-time'='3h') */");
        while (true) {
            Set<Long> watermarks =
                    sql("SELECT `watermark` FROM WT$snapshots").stream()
                            .map(r -> (Long) r.getField(0))
                            .collect(Collectors.toSet());
            if (watermarks.size() > 1) {
                insert1.close();
                insert2.close();
                return;
            }
            Thread.sleep(1000);
        }
    }

    private void testSimple(String table) throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM %s", table));

        batchSql("INSERT INTO %s VALUES ('1', '2', '3'), ('4', '5', '6')", table);
        batchSql("INSERT INTO %s VALUES ('7', '8', '9')", table);
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));

        //        batchSql("INSERT INTO %s VALUES ('7', '8', '9')", table);
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("7", "8", "9"));
        iterator.close();
    }

    private void testProjection(String table) throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT b, c FROM %s", table));

        batchSql("INSERT INTO %s VALUES ('1', '2', '3'), ('4', '5', '6')", table);
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("2", "3"), Row.of("5", "6"));

        batchSql("INSERT INTO %s VALUES ('7', '8', '9')", table);
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("8", "9"));
        iterator.close();
    }

    @Test
    public void testContinuousLatest() throws Exception {
        batchSql("INSERT INTO T1 VALUES ('1', '2', '3'), ('4', '5', '6')");

        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter("SELECT * FROM T1 /*+ OPTIONS('log.scan'='latest') */"));

        batchSql("INSERT INTO T1 VALUES ('7', '8', '9'), ('10', '11', '12')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("7", "8", "9"), Row.of("10", "11", "12"));
        iterator.close();
    }

    @Test
    public void testContinuousLatestStartingFromEmpty() throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter("SELECT * FROM T1 /*+ OPTIONS('scan.mode'='latest') */"));

        sql("INSERT INTO T1 VALUES ('1', 'Hello', 'World')");
        sql("INSERT INTO T1 VALUES ('2', 'Apache', 'Paimon')");
        sql("INSERT INTO T1 VALUES ('3', 'C', 'c')");

        assertThat(iterator.collect(3))
                .containsExactlyInAnyOrder(
                        Row.of("1", "Hello", "World"),
                        Row.of("2", "Apache", "Paimon"),
                        Row.of("3", "C", "c"));
    }

    @Test
    public void testContinuousFromTimestamp() throws Exception {
        String sql =
                "SELECT * FROM T1 /*+ OPTIONS('log.scan'='from-timestamp', 'log.scan.timestamp-millis'='%s') */";

        // empty table
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(streamSqlIter(sql, 0));
        batchSql("INSERT INTO T1 VALUES ('1', '2', '3'), ('4', '5', '6')");
        batchSql("INSERT INTO T1 VALUES ('7', '8', '9'), ('10', '11', '12')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));
        iterator.close();

        SnapshotManager snapshotManager =
                new SnapshotManager(LocalFileIO.create(), getTableDirectory("T1"));
        List<Snapshot> snapshots =
                new ArrayList<>(ImmutableList.copyOf(snapshotManager.snapshots()));
        snapshots.sort(Comparator.comparingLong(Snapshot::timeMillis));
        Snapshot first = snapshots.get(0);
        Snapshot second = snapshots.get(1);

        // before second snapshot
        iterator = BlockingIterator.of(streamSqlIter(sql, second.timeMillis() - 1));
        batchSql("INSERT INTO T1 VALUES ('13', '14', '15')");
        assertThat(iterator.collect(3))
                .containsExactlyInAnyOrder(
                        Row.of("7", "8", "9"), Row.of("10", "11", "12"), Row.of("13", "14", "15"));
        iterator.close();

        // from second snapshot
        iterator = BlockingIterator.of(streamSqlIter(sql, second.timeMillis()));
        assertThat(iterator.collect(3))
                .containsExactlyInAnyOrder(
                        Row.of("7", "8", "9"), Row.of("10", "11", "12"), Row.of("13", "14", "15"));
        iterator.close();

        // after second snapshot
        iterator = BlockingIterator.of(streamSqlIter(sql, second.timeMillis() + 1));
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("13", "14", "15"));
        iterator.close();

        // from start
        iterator = BlockingIterator.of(streamSqlIter(sql, first.timeMillis() - 1));
        assertThat(iterator.collect(5))
                .containsExactlyInAnyOrder(
                        Row.of("1", "2", "3"),
                        Row.of("4", "5", "6"),
                        Row.of("7", "8", "9"),
                        Row.of("10", "11", "12"),
                        Row.of("13", "14", "15"));
        iterator.close();

        // from end
        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                sql,
                                snapshotManager
                                                .snapshot(snapshotManager.latestSnapshotId())
                                                .timeMillis()
                                        + 1));
        batchSql("INSERT INTO T1 VALUES ('16', '17', '18')");
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("16", "17", "18"));
        iterator.close();
    }

    @Test
    public void testLackStartupTimestamp() {
        assertThatThrownBy(
                        () ->
                                streamSqlIter(
                                        "SELECT * FROM T1 /*+ OPTIONS('log.scan'='from-timestamp') */"))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "must set only one key in [scan.timestamp-millis,scan.timestamp] when you use from-timestamp for scan.mode");
    }

    @Test
    public void testConfigureStartupTimestamp() throws Exception {
        // Configure 'log.scan.timestamp-millis' without 'log.scan'.
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T1 /*+ OPTIONS('log.scan.timestamp-millis'='%s') */",
                                0));
        batchSql("INSERT INTO T1 VALUES ('1', '2', '3'), ('4', '5', '6')");
        batchSql("INSERT INTO T1 VALUES ('7', '8', '9'), ('10', '11', '12')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));
        iterator.close();

        // Configure 'log.scan.timestamp-millis' with 'log.scan=latest'.
        assertThatThrownBy(
                        () ->
                                streamSqlIter(
                                        "SELECT * FROM T1 /*+ OPTIONS('log.scan'='latest', 'log.scan.timestamp-millis'='%s') */",
                                        0))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "scan.timestamp-millis must be null when you use latest for scan.mode");
    }

    @Test
    public void testConfigureStartupSnapshot() throws Exception {
        // Configure 'scan.snapshot-id' without 'scan.mode'.
        batchSql("INSERT INTO T1 VALUES ('1', '2', '3'), ('4', '5', '6')");
        batchSql("INSERT INTO T1 VALUES ('7', '8', '9'), ('10', '11', '12')");
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T1 /*+ OPTIONS('scan.snapshot-id'='%s') */", 1));
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));
        iterator.close();

        // Start from earliest snapshot
        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T1 /*+ OPTIONS('scan.snapshot-id'='%s') */", 0));
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));
        iterator.close();

        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T1 /*+ OPTIONS('scan.snapshot-id'='%s') */", 1));
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));
        iterator.close();

        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T1 /*+ OPTIONS('scan.snapshot-id'='%s') */", 2));
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("7", "8", "9"), (Row.of("10", "11", "12")));
        iterator.close();

        // Configure 'scan.snapshot-id' with 'scan.mode=latest'.
        assertThatThrownBy(
                        () ->
                                streamSqlIter(
                                        "SELECT * FROM T1 /*+ OPTIONS('scan.mode'='latest', 'scan.snapshot-id'='%s') */",
                                        0))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "scan.snapshot-id must be null when you use latest for scan.mode");
    }

    @Test
    public void testConfigureStartupSnapshotFull() throws Exception {
        // Configure 'scan.snapshot-id' with 'scan.mode'='from-snapshot-full'.
        batchSql("INSERT INTO T1 VALUES ('1', '2', '3'), ('4', '5', '6')");
        batchSql("INSERT INTO T1 VALUES ('7', '8', '9'), ('10', '11', '12')");

        // Start from earliest snapshot
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T1 /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='%s') */",
                                1));
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));
        iterator.close();

        batchSql("INSERT INTO T1 VALUES ('13', '14', '15')");

        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T1 /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='%s') */",
                                2));
        assertThat(iterator.collect(4))
                .containsExactlyInAnyOrder(
                        Row.of("1", "2", "3"),
                        Row.of("4", "5", "6"),
                        Row.of("7", "8", "9"),
                        Row.of("10", "11", "12"));
        iterator.close();

        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T1 /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='%s') */",
                                2));
        assertThat(iterator.collect(5))
                .containsExactlyInAnyOrder(
                        Row.of("1", "2", "3"),
                        Row.of("4", "5", "6"),
                        Row.of("7", "8", "9"),
                        Row.of("10", "11", "12"),
                        Row.of("13", "14", "15"));
        iterator.close();
    }

    @Test
    public void testIgnoreOverwrite() throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM T1"));

        batchSql("INSERT INTO T1 VALUES ('1', '2', '3'), ('4', '5', '6')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));

        // should ignore this overwrite
        batchSql("INSERT OVERWRITE T1 VALUES ('7', '8', '9')");

        batchSql("INSERT INTO T1 VALUES ('9', '10', '11')");
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("9", "10", "11"));
        iterator.close();
    }

    @Test
    public void testUnsupportedUpsert() {
        assertThatThrownBy(
                        () ->
                                streamSqlIter(
                                        "SELECT * FROM T1 /*+ OPTIONS('log.changelog-mode'='upsert') */"))
                .hasCauseInstanceOf(ValidationException.class)
                .hasRootCauseMessage(
                        "File store continuous reading does not support upsert changelog mode.");
    }

    @Test
    public void testUnsupportedEventual() {
        assertThatThrownBy(
                        () ->
                                streamSqlIter(
                                        "SELECT * FROM T1 /*+ OPTIONS('log.consistency'='eventual') */"))
                .hasCauseInstanceOf(ValidationException.class)
                .hasRootCauseMessage(
                        "File store continuous reading does not support eventual consistency mode.");
    }

    @Test
    public void testFlinkMemoryPool() {
        // Check if the configuration is effective
        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "INSERT INTO %s /*+ OPTIONS('sink.use-managed-memory-allocator'='true', 'sink.managed.writer-buffer-memory'='0M') */ "
                                                + "VALUES ('1', '2', '3'), ('4', '5', '6')",
                                        "T1"))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Weights for operator scope use cases must be greater than 0.");

        batchSql(
                "INSERT INTO %s /*+ OPTIONS('sink.use-managed-memory-allocator'='true', 'sink.managed.writer-buffer-memory'='1M') */ "
                        + "VALUES ('1', '2', '3'), ('4', '5', '6')",
                "T1");
        assertThat(batchSql("SELECT * FROM T1").size()).isEqualTo(2);
    }

    @Test
    public void testDynamicPartitionPruningNotWork() throws Exception {
        // dim table
        sql("CREATE TABLE dim (x INT PRIMARY KEY NOT ENFORCED, y STRING, z INT)");
        sql("INSERT INTO dim VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2)");

        // partitioned fact table
        sql(
                "CREATE TABLE fact (a INT, b BIGINT, c STRING, p INT, `proctime` AS PROCTIME(), PRIMARY KEY (a, p) NOT ENFORCED) PARTITIONED BY (p)\n");
        sql(
                "INSERT INTO fact PARTITION (p = 1) VALUES (10, 100, 'aaa'), (11, 101, 'bbb'), (12, 102, 'ccc')");
        sql(
                "INSERT INTO fact PARTITION (p = 2) VALUES (20, 200, 'aaa'), (21, 201, 'bbb'), (22, 202, 'ccc')");
        sql(
                "INSERT INTO fact PARTITION (p = 3) VALUES (30, 300, 'aaa'), (31, 301, 'bbb'), (32, 302, 'ccc')");

        String joinSql = "SELECT a, b, c, p, x, y FROM fact INNER JOIN dim ON x = p and z = 1";

        // check dynamic partition pruning isn't working
        assertThat(sEnv.explainSql(joinSql)).doesNotContain("DynamicFilteringDataCollector");

        // check results
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(streamSqlIter(joinSql));
        assertThat(iterator.collect(6))
                .containsExactlyInAnyOrder(
                        Row.of(10, 100L, "aaa", 1, 1, "a"),
                        Row.of(11, 101L, "bbb", 1, 1, "a"),
                        Row.of(12, 102L, "ccc", 1, 1, "a"),
                        Row.of(20, 200L, "aaa", 2, 2, "b"),
                        Row.of(21, 201L, "bbb", 2, 2, "b"),
                        Row.of(22, 202L, "ccc", 2, 2, "b"));
        iterator.close();
    }

    @Test
    public void testIgnoreDelete() throws Exception {
        sql(
                "CREATE TABLE ignore_delete (pk INT PRIMARY KEY NOT ENFORCED, v STRING) "
                        + "WITH ('merge-engine' = 'deduplicate', 'ignore-delete' = 'true')");

        BlockingIterator<Row, Row> iterator =
                streamSqlBlockIter(
                        "SELECT * FROM ignore_delete /*+ OPTIONS('continuous.discovery-interval' = '1s', 'scan.snapshot-id' = '1') */");

        sql("INSERT INTO ignore_delete VALUES (1, 'A'), (2, 'B')");
        sql("DELETE FROM ignore_delete WHERE pk = 1");
        sql("INSERT INTO ignore_delete VALUES (1, 'B')");

        // no -D[1, 'A'] but exist -U[1, 'A']
        assertThat(iterator.collect(4))
                .containsExactly(
                        Row.ofKind(RowKind.INSERT, 1, "A"),
                        Row.ofKind(RowKind.INSERT, 2, "B"),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, "A"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, "B"));
        iterator.close();
    }

    @Test
    public void testScanFromOldSchema() throws Exception {
        sql("CREATE TABLE select_old (f0 INT PRIMARY KEY NOT ENFORCED, f1 STRING)");

        sql("INSERT INTO select_old VALUES (1, 'a'), (2, 'b')");

        Thread.sleep(1_000);
        long timestamp = System.currentTimeMillis();

        sql("ALTER TABLE select_old ADD f2 STRING");
        sql("INSERT INTO select_old VALUES (3, 'c', 'C')");

        // this way will initialize source with the latest schema
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM select_old /*+ OPTIONS('scan.timestamp-millis'='%s') */",
                                timestamp));
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(3, "c", "C"));
        iterator.close();

        // this way will initialize source with time-travelled schema
        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM select_old FOR SYSTEM_TIME AS OF TIMESTAMP '%s'",
                                DateTimeUtils.formatTimestamp(
                                        DateTimeUtils.toInternal(timestamp, 0), 0)));
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(3, "c"));
    }

    @ParameterizedTest(name = "changelog-producer = {0}")
    @ValueSource(strings = {"none", "input"})
    public void testScanFromChangelog(String changelogProducer) throws Exception {
        batchSql(
                "CREATE TABLE IF NOT EXISTS T3 (a STRING, b STRING, c STRING, PRIMARY KEY (a) NOT ENFORCED)\n"
                        + " WITH ('changelog-producer'='%s', 'bucket' = '1', \n"
                        + " 'snapshot.num-retained.max' = '2',\n"
                        + " 'snapshot.num-retained.min' = '1',\n"
                        + " 'changelog.num-retained.max' = '3',\n"
                        + " 'changelog.num-retained.min' = '1'\n"
                        + ")",
                changelogProducer);

        batchSql("INSERT INTO T3 VALUES ('1', '2', '3')");
        batchSql("INSERT INTO T3 VALUES ('4', '5', '6')");
        batchSql("INSERT INTO T3 VALUES ('7', '8', '9')");
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T3 /*+ OPTIONS('scan.snapshot-id'='%s') */", 0));
        assertThat(iterator.collect(3))
                .containsExactlyInAnyOrder(
                        Row.of("1", "2", "3"), Row.of("4", "5", "6"), Row.of("7", "8", "9"));
        iterator.close();

        batchSql("INSERT INTO T3 VALUES ('10', '11', '12')");

        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T3 /*+ OPTIONS('scan.snapshot-id'='%s') */", 0));
        assertThat(iterator.collect(3))
                .containsExactlyInAnyOrder(
                        Row.of("4", "5", "6"), Row.of("7", "8", "9"), Row.of("10", "11", "12"));
        iterator.close();

        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T3 /*+ OPTIONS('scan.snapshot-id'='%s') */", 4));
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("10", "11", "12"));
        iterator.close();
    }

    @Test
    public void testAvroRetractNotNullField() {
        List<Row> input =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, "A"), Row.ofKind(RowKind.DELETE, 1, "A"));
        String id = TestValuesTableFactory.registerData(input);
        sEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE source (pk INT PRIMARY KEY NOT ENFORCED, a STRING) "
                                + "WITH ('connector'='values', 'bounded'='true', 'data-id'='%s', "
                                + "'changelog-mode' = 'I,D,UA,UB')",
                        id));

        sql(
                "CREATE TABLE avro_sink (pk INT PRIMARY KEY NOT ENFORCED, a STRING NOT NULL) "
                        + " WITH ('file.format' = 'avro', 'merge-engine' = 'aggregation')");

        assertThatThrownBy(
                        () -> sEnv.executeSql("INSERT INTO avro_sink select * from source").await())
                .satisfies(
                        anyCauseMatches(
                                RuntimeException.class,
                                "Caught NullPointerException, the possible reason is you have set following options together"));
    }
}
