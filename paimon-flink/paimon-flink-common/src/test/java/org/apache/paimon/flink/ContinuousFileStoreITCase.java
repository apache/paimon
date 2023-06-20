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
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** SQL ITCase for continuous file store. */
@ExtendWith(ParameterizedTestExtension.class)
public class ContinuousFileStoreITCase extends CatalogITCaseBase {

    private final boolean changelogFile;

    public ContinuousFileStoreITCase(boolean changelogFile) {
        this.changelogFile = changelogFile;
    }

    @Parameters(name = "changelogFile-{0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    @Override
    protected List<String> ddl() {
        String options =
                changelogFile
                        ? " WITH('write-mode'='change-log','changelog-producer'='input')"
                        : "";
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS T1 (a STRING, b STRING, c STRING)" + options,
                "CREATE TABLE IF NOT EXISTS T2 (a STRING, b STRING, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + options);
    }

    @TestTemplate
    public void testWithoutPrimaryKey() throws Exception {
        testSimple("T1");
    }

    @TestTemplate
    public void testWithPrimaryKey() throws Exception {
        testSimple("T2");
    }

    @TestTemplate
    public void testProjectionWithoutPrimaryKey() throws Exception {
        testProjection("T1");
    }

    @TestTemplate
    public void testProjectionWithPrimaryKey() throws Exception {
        testProjection("T2");
    }

    @TestTemplate
    public void testConsumerId() throws Exception {
        String table = "T2";
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM %s /*+ OPTIONS('consumer-id'='me') */", table));

        batchSql("INSERT INTO %s VALUES ('1', '2', '3'), ('4', '5', '6')", table);
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));

        Thread.sleep(1000);
        iterator.close();

        iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM %s /*+ OPTIONS('consumer-id'='me') */", table));
        batchSql("INSERT INTO %s VALUES ('7', '8', '9')", table);
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("7", "8", "9"));
        iterator.close();
    }

    @TestTemplate
    @Timeout(120)
    public void testSnapshotWatermark() throws Exception {
        streamSqlIter(
                "CREATE TEMPORARY TABLE gen (a STRING, b STRING, c STRING,"
                        + " dt AS NOW(), WATERMARK FOR dt AS dt) WITH ('connector'='datagen')");
        CloseableIterator<Row> insert1 = streamSqlIter("INSERT INTO T2 SELECT a, b, c FROM gen");
        sql("CREATE TABLE WT (a STRING, b STRING, c STRING, PRIMARY KEY (a) NOT ENFORCED)");
        CloseableIterator<Row> insert2 =
                streamSqlIter("INSERT INTO WT SELECT * FROM T2 /*+ OPTIONS('consumer-id'='me') */");
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
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));

        batchSql("INSERT INTO %s VALUES ('7', '8', '9')", table);
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

    @TestTemplate
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

    @TestTemplate
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

    @TestTemplate
    public void testLackStartupTimestamp() {
        assertThatThrownBy(
                        () ->
                                streamSqlIter(
                                        "SELECT * FROM T1 /*+ OPTIONS('log.scan'='from-timestamp') */"))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "scan.timestamp-millis can not be null when you use from-timestamp for scan.mode");
    }

    @TestTemplate
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

    @TestTemplate
    public void testConfigureStartupSnapshot() throws Exception {
        // Configure 'scan.snapshot-id' without 'scan.mode'.
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T1 /*+ OPTIONS('scan.snapshot-id'='%s') */", 1));
        batchSql("INSERT INTO T1 VALUES ('1', '2', '3'), ('4', '5', '6')");
        batchSql("INSERT INTO T1 VALUES ('7', '8', '9'), ('10', '11', '12')");
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

    @TestTemplate
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

    @TestTemplate
    public void testUnsupportedUpsert() {
        assertThatThrownBy(
                        () ->
                                streamSqlIter(
                                        "SELECT * FROM T1 /*+ OPTIONS('log.changelog-mode'='upsert') */"))
                .hasCauseInstanceOf(ValidationException.class)
                .hasRootCauseMessage(
                        "File store continuous reading does not support upsert changelog mode.");
    }

    @TestTemplate
    public void testUnsupportedEventual() {
        assertThatThrownBy(
                        () ->
                                streamSqlIter(
                                        "SELECT * FROM T1 /*+ OPTIONS('log.consistency'='eventual') */"))
                .hasCauseInstanceOf(ValidationException.class)
                .hasRootCauseMessage(
                        "File store continuous reading does not support eventual consistency mode.");
    }

    @TestTemplate
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
        assertEquals(batchSql("SELECT * FROM T1").size(), 2);
    }
}
