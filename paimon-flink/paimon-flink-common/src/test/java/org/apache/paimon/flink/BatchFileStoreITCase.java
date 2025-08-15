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
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for batch file store. */
public class BatchFileStoreITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return singletonList("CREATE TABLE IF NOT EXISTS T (a INT, b INT, c INT)");
    }

    @Test
    public void testFullCompactionNoDv() throws Catalog.TableNotExistException {
        sql(
                "CREATE TEMPORARY TABLE GEN (a INT) WITH ("
                        + "'connector'='datagen', "
                        + "'number-of-rows'='1000', "
                        + "'fields.a.kind'='sequence', "
                        + "'fields.a.start'='0', "
                        + "'fields.a.end'='1000')");
        sql(
                "CREATE TABLE T1 (a INT PRIMARY KEY NOT ENFORCED, b STRING, c STRING) WITH ("
                        + "'bucket' = '1', "
                        + "'file.format' = 'avro', "
                        + "'file.compression' = 'null', "
                        + "'deletion-vectors.enabled' = 'true')");
        batchSql("INSERT INTO T1 SELECT a, 'unknown', 'unknown' FROM GEN");

        // first insert, producing dv files
        batchSql("INSERT INTO T1 VALUES (1, '22', '33')");
        FileStoreTable table = paimonTable("T1");
        Snapshot snapshot = table.latestSnapshot().get();
        assertThat(deletionVectors(table, snapshot)).hasSize(1);
        assertThat(batchSql("SELECT * FROM T1 WHERE a = 1")).containsExactly(Row.of(1, "22", "33"));

        // second insert, producing no dv files
        batchSql("ALTER TABLE T1 SET ('compaction.total-size-threshold' = '1m')");
        batchSql("INSERT INTO T1 VALUES (1, '44', '55')");
        snapshot = table.latestSnapshot().get();
        assertThat(deletionVectors(table, snapshot)).hasSize(0);
        assertThat(batchSql("SELECT * FROM T1 WHERE a = 1")).containsExactly(Row.of(1, "44", "55"));

        // third insert, producing no dv files, same index manifest
        batchSql("INSERT INTO T1 VALUES (1, '66', '77')");
        assertThat(table.latestSnapshot().get().indexManifest())
                .isEqualTo(snapshot.indexManifest());
        assertThat(batchSql("SELECT * FROM T1 WHERE a = 1")).containsExactly(Row.of(1, "66", "77"));
    }

    private Map<String, DeletionVector> deletionVectors(FileStoreTable table, Snapshot snapshot) {
        assertThat(snapshot.indexManifest()).isNotNull();
        List<IndexManifestEntry> indexManifestEntries =
                table.indexManifestFileReader().read(snapshot.indexManifest());
        assertThat(indexManifestEntries.size()).isEqualTo(1);
        IndexFileMeta indexFileMeta = indexManifestEntries.get(0).indexFile();
        return table.store()
                .newIndexFileHandler()
                .readAllDeletionVectors(singletonList(indexFileMeta));
    }

    @Test
    public void testWriteRestoreCoordinator() {
        batchSql(
                "CREATE TABLE IF NOT EXISTS PK (a INT PRIMARY KEY NOT ENFORCED, b INT, c INT) WITH ('bucket' = '2')");
        batchSql("ALTER TABLE PK SET ('sink.writer-coordinator.enabled' = 'true')");
        batchSql("INSERT INTO PK VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333)");
        batchSql("INSERT INTO PK VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333)");
        assertThat(batchSql("SELECT * FROM PK"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111), Row.of(2, 22, 222), Row.of(3, 33, 333));
    }

    @Test
    public void testWriteRestoreCoordinatorWithPageSize() {
        batchSql(
                "CREATE TABLE IF NOT EXISTS PK (a INT PRIMARY KEY NOT ENFORCED, b INT, c INT) WITH ('bucket' = '2')");
        batchSql("ALTER TABLE PK SET ('sink.writer-coordinator.enabled' = 'true')");
        batchSql("ALTER TABLE PK SET ('sink.writer-coordinator.page-size' = '10 b')");
        batchSql("INSERT INTO PK VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333)");
        batchSql("INSERT INTO PK VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333)");
        assertThat(batchSql("SELECT * FROM PK"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111), Row.of(2, 22, 222), Row.of(3, 33, 333));
    }

    @Test
    public void testWriteRestoreCoordinatorDv() {
        batchSql(
                "CREATE TABLE IF NOT EXISTS PK (a INT PRIMARY KEY NOT ENFORCED, b INT, c INT) WITH ("
                        + "'bucket' = '2', 'deletion-vectors.enabled' = 'true')");
        batchSql("ALTER TABLE PK SET ('sink.writer-coordinator.enabled' = 'true')");
        batchSql("INSERT INTO PK VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333)");
        batchSql("INSERT INTO PK VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333)");
        assertThat(batchSql("SELECT * FROM PK"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111), Row.of(2, 22, 222), Row.of(3, 33, 333));
    }

    @Test
    public void testWriteRestoreCoordinatorDb() {
        batchSql(
                "CREATE TABLE IF NOT EXISTS PK (a INT PRIMARY KEY NOT ENFORCED, b INT, c INT) WITH ("
                        + "'bucket' = '-1', 'dynamic-bucket.target-row-num' = '1')");
        batchSql("ALTER TABLE PK SET ('sink.writer-coordinator.enabled' = 'true')");
        batchSql("INSERT INTO PK VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333)");
        batchSql("INSERT INTO PK VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333)");
        assertThat(batchSql("SELECT * FROM PK"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111), Row.of(2, 22, 222), Row.of(3, 33, 333));
    }

    @Test
    public void testAQEWithDynamicBucket() {
        batchSql("CREATE TABLE IF NOT EXISTS D_T (a INT PRIMARY KEY NOT ENFORCED, b INT, c INT)");
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");
        batchSql("INSERT INTO D_T SELECT a, b, c FROM T GROUP BY a,b,c");
        assertThat(batchSql("SELECT * FROM D_T"))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));
    }

    @Test
    public void testOverwriteEmpty() {
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");
        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));
        batchSql("INSERT OVERWRITE T SELECT * FROM T WHERE 1 <> 1");
        assertThat(batchSql("SELECT * FROM T")).isEmpty();
    }

    @Test
    public void testTimeTravelRead() throws Exception {
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");
        long time1 = System.currentTimeMillis();

        Thread.sleep(10);
        batchSql("INSERT INTO T VALUES (3, 33, 333), (4, 44, 444)");
        long time2 = System.currentTimeMillis();

        Thread.sleep(10);
        batchSql("INSERT INTO T VALUES (5, 55, 555), (6, 66, 666)");
        long time3 = System.currentTimeMillis();

        Thread.sleep(10);
        batchSql("INSERT INTO T VALUES (7, 77, 777), (8, 88, 888)");

        paimonTable("T").createTag("tag2", 2);

        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='1') */"))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));

        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='1') */"))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));

        assertThatThrownBy(() -> batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='0') */"))
                .satisfies(
                        anyCauseMatches(
                                Exception.class,
                                "The specified scan snapshotId 0 is out of available snapshotId range [1, 4]."));

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='0') */"))
                .satisfies(
                        anyCauseMatches(
                                Exception.class,
                                "The specified scan snapshotId 0 is out of available snapshotId range [1, 4]."));

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM T /*+ OPTIONS('scan.timestamp-millis'='%s') */",
                                        time1)))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));

        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.file-creation-time-millis'='%s') */",
                                time1))
                .containsExactlyInAnyOrder(
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444),
                        Row.of(5, 55, 555),
                        Row.of(6, 66, 666),
                        Row.of(7, 77, 777),
                        Row.of(8, 88, 888));

        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='2') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444));
        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='2') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444));
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM T /*+ OPTIONS('scan.timestamp-millis'='%s') */",
                                        time2)))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444));

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM T /*+ OPTIONS('scan.file-creation-time-millis'='%s') */",
                                        time2)))
                .containsExactlyInAnyOrder(
                        Row.of(5, 55, 555), Row.of(6, 66, 666),
                        Row.of(7, 77, 777), Row.of(8, 88, 888));

        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='3') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444),
                        Row.of(5, 55, 555),
                        Row.of(6, 66, 666));
        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='3') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444),
                        Row.of(5, 55, 555),
                        Row.of(6, 66, 666));
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM T /*+ OPTIONS('scan.timestamp-millis'='%s') */",
                                        time3)))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444),
                        Row.of(5, 55, 555),
                        Row.of(6, 66, 666));

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM T /*+ OPTIONS('scan.file-creation-time-millis'='%s') */",
                                        time3)))
                .containsExactlyInAnyOrder(Row.of(7, 77, 777), Row.of(8, 88, 888));

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        String.format(
                                                "SELECT * FROM T /*+ OPTIONS('scan.timestamp-millis'='%s', 'scan.snapshot-id'='1') */",
                                                time3)))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "[scan.snapshot-id] must be null when you set [scan.timestamp-millis,scan.timestamp]");

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='full', 'scan.snapshot-id'='1') */"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "%s must be null when you use latest-full for scan.mode",
                        CoreOptions.SCAN_SNAPSHOT_ID.key());

        // travel to tag
        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.tag-name'='tag2') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444));

        assertThatThrownBy(
                        () -> batchSql("SELECT * FROM T /*+ OPTIONS('scan.tag-name'='unknown') */"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Tag 'unknown' doesn't exist.");
    }

    @Test
    @Timeout(120)
    public void testTimeTravelReadWithWatermark() throws Exception {
        streamSqlIter(
                "CREATE TEMPORARY TABLE gen (a STRING, b STRING, c STRING,"
                        + " dt AS NOW(), WATERMARK FOR dt AS dt) WITH ('connector'='datagen')");
        sql(
                "CREATE TABLE WT (a STRING, b STRING, c STRING, dt TIMESTAMP, PRIMARY KEY (a) NOT ENFORCED)");
        CloseableIterator<Row> insert = streamSqlIter("INSERT INTO WT SELECT * FROM gen ");
        List<Long> watermarks;
        while (true) {
            watermarks =
                    sql("SELECT `watermark` FROM WT$snapshots").stream()
                            .map(r -> (Long) r.getField("watermark"))
                            .collect(Collectors.toList());
            if (watermarks.size() > 1) {
                insert.close();
                break;
            }
            Thread.sleep(1000);
        }
        Long maxWatermark = watermarks.get(watermarks.size() - 1);
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM WT /*+ OPTIONS('scan.watermark'='%d') */",
                                        maxWatermark)))
                .isNotEmpty();
    }

    @Test
    public void testTimeTravelReadWithSnapshotExpiration() throws Exception {
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");

        paimonTable("T").createTag("tag1", 1);

        batchSql("INSERT INTO T VALUES (3, 33, 333), (4, 44, 444)");

        // expire snapshot 1
        Map<String, String> expireOptions = new HashMap<>();
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        FileStoreTable table = (FileStoreTable) paimonTable("T");
        table.copy(expireOptions).newCommit("").expireSnapshots();
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(1);

        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.tag-name'='tag1') */"))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));
    }

    @Test
    public void testIncrementBetweenReadWithSnapshotExpiration() throws Exception {
        String tableName = "T";
        batchSql(String.format("INSERT INTO %s VALUES (1, 11, 111)", tableName));

        paimonTable(tableName).createTag("tag1", 1);

        batchSql(String.format("INSERT INTO %s VALUES (2, 22, 222)", tableName));
        paimonTable(tableName).createTag("tag2", 2);
        batchSql(String.format("INSERT INTO %s VALUES (3, 33, 333)", tableName));
        paimonTable(tableName).createTag("tag3", 3);

        // expire snapshot 1
        Map<String, String> expireOptions = new HashMap<>();
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        FileStoreTable table = (FileStoreTable) paimonTable(tableName);
        table.copy(expireOptions).newCommit("").expireSnapshots();
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(1);

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM %s /*+ OPTIONS('incremental-between' = 'tag1,tag2', 'deletion-vectors.enabled' = 'true') */",
                                        tableName)))
                .containsExactlyInAnyOrder(Row.of(2, 22, 222));
    }

    @Test
    public void testSortSpillMerge() {
        sql(
                "CREATE TABLE IF NOT EXISTS KT (a INT PRIMARY KEY NOT ENFORCED, b STRING) WITH ('sort-spill-threshold'='2')");
        sql("INSERT INTO KT VALUES (1, '1')");
        sql("INSERT INTO KT VALUES (1, '2')");
        sql("INSERT INTO KT VALUES (1, '3')");
        sql("INSERT INTO KT VALUES (1, '4')");
        sql("INSERT INTO KT VALUES (1, '5')");
        sql("INSERT INTO KT VALUES (1, '6')");
        sql("INSERT INTO KT VALUES (1, '7')");

        // select all
        assertThat(sql("SELECT * FROM KT")).containsExactlyInAnyOrder(Row.of(1, "7"));

        // select projection
        assertThat(sql("SELECT b FROM KT")).containsExactlyInAnyOrder(Row.of("7"));
    }

    @Test
    public void testTruncateTable() {
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");
        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));
        List<Row> truncateResult = batchSql("TRUNCATE TABLE T");
        assertThat(truncateResult.size()).isEqualTo(1);
        assertThat(truncateResult.get(0)).isEqualTo(Row.ofKind(RowKind.INSERT, "OK"));
        assertThat(batchSql("SELECT * FROM T").isEmpty()).isTrue();
    }

    /** NOTE: only supports INNER JOIN. */
    @Test
    public void testDynamicPartitionPruning() {
        // dim table
        sql("CREATE TABLE dim (x INT PRIMARY KEY NOT ENFORCED, y STRING, z INT)");
        sql("INSERT INTO dim VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2)");

        // partitioned fact table
        sql(
                "CREATE TABLE fact (a INT, b BIGINT, c STRING, p INT, PRIMARY KEY (a, p) NOT ENFORCED) PARTITIONED BY (p)");
        sql(
                "INSERT INTO fact PARTITION (p = 1) VALUES (10, 100, 'aaa'), (11, 101, 'bbb'), (12, 102, 'ccc')");
        sql(
                "INSERT INTO fact PARTITION (p = 2) VALUES (20, 200, 'aaa'), (21, 201, 'bbb'), (22, 202, 'ccc')");
        sql(
                "INSERT INTO fact PARTITION (p = 3) VALUES (30, 300, 'aaa'), (31, 301, 'bbb'), (32, 302, 'ccc')");

        String joinSql =
                "SELECT a, b, c, p, x, y FROM fact INNER JOIN dim ON x = p and z = 1 ORDER BY a";
        String joinSqlSwapFactDim =
                "SELECT a, b, c, p, x, y FROM dim INNER JOIN fact ON x = p and z = 1 ORDER BY a";
        String expectedResult =
                "[+I[10, 100, aaa, 1, 1, a], +I[11, 101, bbb, 1, 1, a], +I[12, 102, ccc, 1, 1, a], "
                        + "+I[20, 200, aaa, 2, 2, b], +I[21, 201, bbb, 2, 2, b], +I[22, 202, ccc, 2, 2, b]]";

        // check dynamic partition pruning is working
        assertThat(tEnv.explainSql(joinSql)).contains("DynamicFilteringDataCollector");
        assertThat(tEnv.explainSql(joinSqlSwapFactDim)).contains("DynamicFilteringDataCollector");

        // check results
        assertThat(sql(joinSql).toString()).isEqualTo(expectedResult);
        assertThat(sql(joinSqlSwapFactDim).toString()).isEqualTo(expectedResult);
    }

    @Test
    public void testDynamicPartitionPruningOnTwoFactTables() {
        // dim table
        sql("CREATE TABLE dim (x INT PRIMARY KEY NOT ENFORCED, y STRING, z INT)");
        sql("INSERT INTO dim VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2)");

        // two partitioned fact tables
        sql(
                "CREATE TABLE fact1 (a INT, b BIGINT, c STRING, p INT, PRIMARY KEY (a, p) NOT ENFORCED) PARTITIONED BY (p)");
        sql(
                "INSERT INTO fact1 PARTITION (p = 1) VALUES (10, 100, 'aaa'), (11, 101, 'bbb'), (12, 102, 'ccc')");
        sql(
                "INSERT INTO fact1 PARTITION (p = 2) VALUES (20, 200, 'aaa'), (21, 201, 'bbb'), (22, 202, 'ccc')");
        sql(
                "INSERT INTO fact1 PARTITION (p = 3) VALUES (30, 300, 'aaa'), (31, 301, 'bbb'), (32, 302, 'ccc')");

        sql(
                "CREATE TABLE fact2 (a INT, b BIGINT, c STRING, p INT, PRIMARY KEY (a, p) NOT ENFORCED) PARTITIONED BY (p)");
        sql(
                "INSERT INTO fact2 PARTITION (p = 1) VALUES (40, 100, 'aaa'), (41, 101, 'bbb'), (42, 102, 'ccc')");
        sql(
                "INSERT INTO fact2 PARTITION (p = 2) VALUES (50, 200, 'aaa'), (51, 201, 'bbb'), (52, 202, 'ccc')");
        sql(
                "INSERT INTO fact2 PARTITION (p = 3) VALUES (60, 300, 'aaa'), (61, 301, 'bbb'), (62, 302, 'ccc')");

        // two fact sources share the same dynamic filter
        String joinSql =
                "SELECT * FROM (\n"
                        + "SELECT a, b, c, p, x, y FROM fact1 INNER JOIN dim ON x = p AND z = 1\n"
                        + "UNION ALL\n"
                        + "SELECT a, b, c, p, x, y FROM fact2 INNER JOIN dim ON x = p AND z = 1)\n"
                        + "ORDER BY a";
        assertThat(tEnv.explainSql(joinSql))
                .containsOnlyOnce("DynamicFilteringDataCollector(fields=[x])(reuse_id=");
        assertThat(sql(joinSql).toString())
                .isEqualTo(
                        "[+I[10, 100, aaa, 1, 1, a], +I[11, 101, bbb, 1, 1, a], +I[12, 102, ccc, 1, 1, a], "
                                + "+I[20, 200, aaa, 2, 2, b], +I[21, 201, bbb, 2, 2, b], +I[22, 202, ccc, 2, 2, b], "
                                + "+I[40, 100, aaa, 1, 1, a], +I[41, 101, bbb, 1, 1, a], +I[42, 102, ccc, 1, 1, a], "
                                + "+I[50, 200, aaa, 2, 2, b], +I[51, 201, bbb, 2, 2, b], +I[52, 202, ccc, 2, 2, b]]");

        // two fact sources use different dynamic filters
        joinSql =
                "SELECT * FROM (\n"
                        + "SELECT a, b, c, p, x, y FROM fact1 INNER JOIN dim ON x = p AND z = 1\n"
                        + "UNION ALL\n"
                        + "SELECT a, b, c, p, x, y FROM fact2 INNER JOIN dim ON x = p AND z = 2)\n"
                        + "ORDER BY a";
        String expected2 =
                "[+I[10, 100, aaa, 1, 1, a], +I[11, 101, bbb, 1, 1, a], +I[12, 102, ccc, 1, 1, a], "
                        + "+I[20, 200, aaa, 2, 2, b], +I[21, 201, bbb, 2, 2, b], +I[22, 202, ccc, 2, 2, b], "
                        + "+I[60, 300, aaa, 3, 3, c], +I[61, 301, bbb, 3, 3, c], +I[62, 302, ccc, 3, 3, c]]";
        assertThat(tEnv.explainSql(joinSql)).contains("DynamicFilteringDataCollector");
        assertThat(sql(joinSql).toString()).isEqualTo(expected2);
    }

    @Test
    public void testRowKindField() {
        sql(
                "CREATE TABLE R_T (pk INT PRIMARY KEY NOT ENFORCED, v INT, rf STRING) "
                        + "WITH ('rowkind.field'='rf')");
        sql("INSERT INTO R_T VALUES (1, 1, '+I')");
        assertThat(sql("SELECT * FROM R_T")).containsExactly(Row.of(1, 1, "+I"));
        sql("INSERT INTO R_T VALUES (1, 2, '-D')");
        assertThat(sql("SELECT * FROM R_T")).isEmpty();
    }

    @Test
    public void testIgnoreDelete() {
        sql(
                "CREATE TABLE ignore_delete (pk INT PRIMARY KEY NOT ENFORCED, v STRING) "
                        + "WITH ('merge-engine' = 'deduplicate', 'ignore-delete' = 'true', 'bucket' = '1')");

        sql("INSERT INTO ignore_delete VALUES (1, 'A')");
        assertThat(sql("SELECT * FROM ignore_delete")).containsExactly(Row.of(1, "A"));

        sql("DELETE FROM ignore_delete WHERE pk = 1");
        assertThat(sql("SELECT * FROM ignore_delete")).containsExactly(Row.of(1, "A"));

        sql("INSERT INTO ignore_delete VALUES (1, 'B')");
        assertThat(sql("SELECT * FROM ignore_delete")).containsExactly(Row.of(1, "B"));
    }

    @Test
    public void testIgnoreDeleteWithRowKindField() {
        sql(
                "CREATE TABLE ignore_delete (pk INT PRIMARY KEY NOT ENFORCED, v STRING, kind STRING) "
                        + "WITH ('merge-engine' = 'deduplicate', 'ignore-delete' = 'true', 'bucket' = '1', 'rowkind.field' = 'kind')");

        sql("INSERT INTO ignore_delete VALUES (1, 'A', '+I')");
        assertThat(sql("SELECT * FROM ignore_delete")).containsExactly(Row.of(1, "A", "+I"));

        sql("INSERT INTO ignore_delete VALUES (1, 'A', '-D')");
        assertThat(sql("SELECT * FROM ignore_delete")).containsExactly(Row.of(1, "A", "+I"));

        sql("INSERT INTO ignore_delete VALUES (1, 'B', '+I')");
        assertThat(sql("SELECT * FROM ignore_delete")).containsExactly(Row.of(1, "B", "+I"));
    }

    @Test
    public void testDeleteWithPkLookup() throws Exception {
        sql(
                "CREATE TABLE ignore_delete (pk INT PRIMARY KEY NOT ENFORCED, v STRING) "
                        + "WITH ('changelog-producer' = 'lookup', 'bucket' = '1')");
        BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM ignore_delete");

        sql("INSERT INTO ignore_delete VALUES (1, 'A'), (2, 'B')");
        sql("DELETE FROM ignore_delete WHERE pk = 1");
        sql("INSERT INTO ignore_delete VALUES (1, 'B')");

        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, "B"), Row.ofKind(RowKind.INSERT, 2, "B"));
        iterator.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "lookup", "input"})
    public void testDeletePartitionWithChangelog(String producer) throws Exception {
        sql(
                "CREATE TABLE delete_table (pt INT, pk INT, v STRING, PRIMARY KEY(pt, pk) NOT ENFORCED) PARTITIONED BY (pt)   "
                        + "WITH ('changelog-producer' = '"
                        + producer
                        + "', 'delete.force-produce-changelog'='true', 'bucket'='1')");
        BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM delete_table");

        sql("INSERT INTO delete_table VALUES (1, 1, 'A'), (2, 2, 'B')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, 1, "A"),
                        Row.ofKind(RowKind.INSERT, 2, 2, "B"));
        sql("DELETE FROM delete_table WHERE pt = 1");
        assertThat(iterator.collect(1))
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.DELETE, 1, 1, "A"));
        sql("INSERT INTO delete_table VALUES (1, 1, 'B')");

        assertThat(iterator.collect(1))
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, 1, 1, "B"));
        iterator.close();
    }

    @Test
    public void testScanFromOldSchema() throws InterruptedException {
        sql("CREATE TABLE select_old (f0 INT PRIMARY KEY NOT ENFORCED, f1 STRING)");

        sql("INSERT INTO select_old VALUES (1, 'a'), (2, 'b')");

        Thread.sleep(1_000);
        long timestamp = System.currentTimeMillis();

        sql("ALTER TABLE select_old ADD f2 STRING");
        sql("INSERT INTO select_old VALUES (3, 'c', 'C')");

        // this way will initialize source with the latest schema
        assertThat(
                        sql(
                                "SELECT * FROM select_old /*+ OPTIONS('scan.timestamp-millis'='%s') */",
                                timestamp))
                // old schema doesn't have column f2
                .containsExactlyInAnyOrder(Row.of(1, "a", null), Row.of(2, "b", null));

        // this way will initialize source with time-travelled schema
        assertThat(
                        sql(
                                "SELECT * FROM select_old FOR SYSTEM_TIME AS OF TIMESTAMP '%s'",
                                DateTimeUtils.formatTimestamp(
                                        DateTimeUtils.toInternal(timestamp, 0), 0)))
                .containsExactlyInAnyOrder(Row.of(1, "a"), Row.of(2, "b"));
    }

    @Test
    public void testCountStarAppend() {
        sql("CREATE TABLE count_append (f0 INT, f1 STRING)");
        sql("INSERT INTO count_append VALUES (1, 'a'), (2, 'b')");

        String sql = "SELECT COUNT(*) FROM count_append";
        assertThat(sql(sql)).containsOnly(Row.of(2L));
        validateCount1PushDown(sql);
    }

    @Test
    public void testCountStarPartAppend() {
        sql("CREATE TABLE count_part_append (f0 INT, f1 STRING, dt STRING) PARTITIONED BY (dt)");
        sql("INSERT INTO count_part_append VALUES (1, 'a', '1'), (1, 'a', '1'), (2, 'b', '2')");
        String sql = "SELECT COUNT(*) FROM count_part_append WHERE dt = '1'";

        assertThat(sql(sql)).containsOnly(Row.of(2L));
        validateCount1PushDown(sql);
    }

    @Test
    public void testCountStarAppendWithDv() {
        sql(
                String.format(
                        "CREATE TABLE count_append_dv (f0 INT, f1 STRING) WITH ('deletion-vectors.enabled' = 'true', "
                                + "'deletion-vectors.bitmap64' = '%s') ",
                        ThreadLocalRandom.current().nextBoolean()));
        sql("INSERT INTO count_append_dv VALUES (1, 'a'), (2, 'b')");

        String sql = "SELECT COUNT(*) FROM count_append_dv";
        assertThat(sql(sql)).containsOnly(Row.of(2L));
        validateCount1PushDown(sql);
    }

    @Test
    public void testCountStarPK() {
        sql(
                "CREATE TABLE count_pk (f0 INT PRIMARY KEY NOT ENFORCED, f1 STRING) WITH ('file.format' = 'avro')");
        sql("INSERT INTO count_pk VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
        sql("INSERT INTO count_pk VALUES (1, 'e')");

        String sql = "SELECT COUNT(*) FROM count_pk";
        assertThat(sql(sql)).containsOnly(Row.of(4L));
        validateCount1NotPushDown(sql);
    }

    @Test
    public void testCountStarPKDv() {
        sql(
                String.format(
                        "CREATE TABLE count_pk_dv (f0 INT PRIMARY KEY NOT ENFORCED, f1 STRING) WITH ("
                                + "'file.format' = 'avro', "
                                + "'deletion-vectors.enabled' = 'true', "
                                + "'deletion-vectors.bitmap64' = '%s')",
                        ThreadLocalRandom.current().nextBoolean()));
        sql("INSERT INTO count_pk_dv VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
        sql("INSERT INTO count_pk_dv VALUES (1, 'e')");

        String sql = "SELECT COUNT(*) FROM count_pk_dv";
        assertThat(sql(sql)).containsOnly(Row.of(4L));
        validateCount1PushDown(sql);
    }

    private void validateCount1PushDown(String sql) {
        Transformation<?> transformation = AbstractTestBase.translate(tEnv, sql);
        while (!transformation.getInputs().isEmpty()) {
            transformation = transformation.getInputs().get(0);
        }
        assertThat(transformation.getDescription()).contains("Count1AggFunction");
    }

    private void validateCount1NotPushDown(String sql) {
        Transformation<?> transformation = AbstractTestBase.translate(tEnv, sql);
        while (!transformation.getInputs().isEmpty()) {
            transformation = transformation.getInputs().get(0);
        }
        assertThat(transformation.getDescription()).doesNotContain("Count1AggFunction");
    }

    @Test
    public void testParquetRowDecimalAndTimestamp() {
        sql(
                "CREATE TABLE parquet_row_decimal(`row` ROW<f0 DECIMAL(2,1)>) WITH ('file.format' = 'parquet')");
        sql("INSERT INTO parquet_row_decimal VALUES ( (ROW(1.2)) )");

        assertThat(sql("SELECT * FROM parquet_row_decimal"))
                .containsExactly(Row.of(Row.of(new BigDecimal("1.2"))));

        sql(
                "CREATE TABLE parquet_row_timestamp(`row` ROW<f0 TIMESTAMP(0)>) WITH ('file.format' = 'parquet')");
        sql("INSERT INTO parquet_row_timestamp VALUES ( (ROW(TIMESTAMP'2024-11-13 18:00:00')) )");

        assertThat(sql("SELECT * FROM parquet_row_timestamp"))
                .containsExactly(
                        Row.of(Row.of(DateTimeUtils.toLocalDateTime("2024-11-13 18:00:00", 0))));
    }

    @Test
    public void testScanBounded() {
        sql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");
        List<Row> result;
        try (CloseableIterator<Row> iter =
                sEnv.executeSql("SELECT * FROM T /*+ OPTIONS('scan.bounded'='true') */")
                        .collect()) {
            result = ImmutableList.copyOf(iter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));
    }

    @Test
    public void testIncrementTagQueryWithRescaleBucket() throws Exception {
        sql("CREATE TABLE test (a INT PRIMARY KEY NOT ENFORCED, b INT) WITH ('bucket' = '1')");
        Table table = paimonTable("test");

        sql("INSERT INTO test VALUES (1, 11), (2, 22)");
        sql("ALTER TABLE test SET ('bucket' = '2')");
        sql("INSERT OVERWRITE test SELECT * FROM test");
        sql("INSERT INTO test VALUES (3, 33)");

        table.createTag("2024-01-01", 1);
        table.createTag("2024-01-02", 3);

        List<String> incrementalOptions =
                Arrays.asList(
                        "'incremental-between'='2024-01-01,2024-01-02'",
                        "'incremental-to-auto-tag'='2024-01-02'");

        for (String option : incrementalOptions) {
            assertThatThrownBy(() -> sql("SELECT * FROM test /*+ OPTIONS (%s) */", option))
                    .satisfies(
                            anyCauseMatches(
                                    TimeTravelUtil.InconsistentTagBucketException.class,
                                    "The bucket number of two snapshots are different (1, 2), which is not supported in incremental diff query."));
        }
    }

    @Test
    public void testAggregationWithNullSequenceField() {
        sql(
                "CREATE TABLE test ("
                        + "  pk INT PRIMARY KEY NOT ENFORCED,"
                        + "  v STRING,"
                        + "  s0 INT,"
                        + "  s1 INT"
                        + ") WITH ("
                        + "  'merge-engine' = 'aggregation',"
                        + "  'sequence.field' = 's0,s1')");

        sql(
                "INSERT INTO test VALUES (1, 'A1', CAST (NULL AS INT), 1), (1, 'A2', 1, CAST (NULL AS INT))");
        assertThat(sql("SELECT * FROM test")).containsExactly(Row.of(1, "A2", 1, null));

        sql("INSERT INTO test VALUES (1, 'A3', 1, 0)");
        assertThat(sql("SELECT * FROM test")).containsExactly(Row.of(1, "A3", 1, 0));
    }

    @Test
    public void testScanWithSpecifiedPartitions() {
        sql("CREATE TABLE P (pt STRING, id INT, v INT) PARTITIONED BY (pt)");
        sql("CREATE TABLE Q (id INT)");
        sql(
                "INSERT INTO P VALUES ('a', 1, 10), ('a', 2, 20), ('b', 1, 11), ('b', 3, 31), ('c', 1, 12), ('c', 2, 22), ('c', 3, 32)");
        sql("INSERT INTO Q VALUES (1), (2)");
        String query =
                "SELECT Q.id, P.v FROM Q INNER JOIN P /*+ OPTIONS('scan.partitions' = 'pt=b;pt=c') */ ON Q.id = P.id ORDER BY Q.id, P.v";
        assertThat(sql(query)).containsExactly(Row.of(1, 11), Row.of(1, 12), Row.of(2, 22));
    }

    @Test
    public void testScanWithSpecifiedPartitionsWithFieldMapping() {
        sql("CREATE TABLE P (id INT, v INT, pt STRING) PARTITIONED BY (pt)");
        sql("CREATE TABLE Q (id INT)");
        sql(
                "INSERT INTO P VALUES (1, 10, 'a'), (2, 20, 'a'), (1, 11, 'b'), (3, 31, 'b'), (1, 12, 'c'), (2, 22, 'c'), (3, 32, 'c')");
        sql("INSERT INTO Q VALUES (1), (2)");
        String query =
                "SELECT Q.id, P.v FROM Q INNER JOIN P /*+ OPTIONS('scan.partitions' = 'pt=b;pt=c') */ ON Q.id = P.id ORDER BY Q.id, P.v";
        assertThat(sql(query)).containsExactly(Row.of(1, 11), Row.of(1, 12), Row.of(2, 22));
    }

    @Test
    public void testEmptyTableIncrementalBetweenTimestamp() {
        assertThat(sql("SELECT * FROM T /*+ OPTIONS('incremental-between-timestamp'='0,1') */"))
                .isEmpty();
    }

    @Test
    public void testIncrementScanMode() throws Exception {
        sql(
                "CREATE TABLE test_scan_mode (id INT PRIMARY KEY NOT ENFORCED, v STRING) WITH ('changelog-producer' = 'lookup')");

        // snapshot 1,2
        sql("INSERT INTO test_scan_mode VALUES (1, 'A')");
        // snapshot 3,4
        sql("INSERT INTO test_scan_mode VALUES (2, 'B')");

        // snapshot 5,6
        String dataId =
                TestValuesTableFactory.registerData(
                        singletonList(Row.ofKind(RowKind.DELETE, 2, "B")));
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE source (id INT, v STRING) "
                        + "WITH ('connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + dataId
                        + "')");
        sEnv.executeSql("INSERT INTO test_scan_mode SELECT * FROM source").await();

        //  snapshot 7,8
        sql("INSERT INTO test_scan_mode VALUES (3, 'C')");

        List<Row> result =
                sql(
                        "SELECT * FROM `test_scan_mode$audit_log` "
                                + "/*+ OPTIONS('incremental-between'='1,8','incremental-between-scan-mode'='diff') */");
        assertThat(result).containsExactlyInAnyOrder(Row.of("+I", 3, "C"));

        result =
                sql(
                        "SELECT * FROM `test_scan_mode$audit_log` "
                                + "/*+ OPTIONS('incremental-between'='1,8','incremental-between-scan-mode'='delta') */");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of("+I", 2, "B"), Row.of("-D", 2, "B"), Row.of("+I", 3, "C"));
    }

    @Test
    public void testAuditLogTableWithComputedColumn() throws Exception {
        sql("CREATE TABLE test_table (a int, b int, c AS a + b);");
        String ddl = sql("SHOW CREATE TABLE `test_table$audit_log`").get(0).getFieldAs(0);
        assertThat(ddl).contains("`c` AS `a` + `b`");

        sql("INSERT INTO test_table VALUES (1, 1)");
        assertThat(sql("SELECT * FROM `test_table$audit_log`"))
                .containsExactly(Row.of("+I", 1, 1, 2));
    }

    @Test
    public void testBinlogTableWithComputedColumn() {
        sql("CREATE TABLE test_table (a int, b int, c AS a + b);");
        String ddl = sql("SHOW CREATE TABLE `test_table$binlog`").get(0).getFieldAs(0);
        assertThat(ddl).doesNotContain("`c` AS `a` + `b`");

        sql("INSERT INTO test_table VALUES (1, 1)");
        assertThat(sql("SELECT * FROM `test_table$binlog`"))
                .containsExactly(Row.of("+I", new Integer[] {1}, new Integer[] {1}));
    }

    @Test
    public void testBinlogTableWithProjection() {
        sql("CREATE TABLE test_table (a int, b string);");
        sql("INSERT INTO test_table VALUES (1, 'A')");
        assertThat(sql("SELECT * FROM `test_table$binlog`"))
                .containsExactly(Row.of("+I", new Integer[] {1}, new String[] {"A"}));
        assertThat(sql("SELECT b FROM `test_table$binlog`"))
                .containsExactly(Row.of((Object) new String[] {"A"}));
        assertThat(sql("SELECT rowkind, b FROM `test_table$binlog`"))
                .containsExactly(Row.of("+I", new String[] {"A"}));
    }

    @Test
    public void testBatchReadSourceWithSnapshot() {
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333), (4, 44, 444)");
        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='1', 'scan.dedicated-split-generation'='true') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444));

        batchSql("INSERT INTO T VALUES (5, 55, 555), (6, 66, 666)");
        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.dedicated-split-generation'='true') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444),
                        Row.of(5, 55, 555),
                        Row.of(6, 66, 666));

        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.dedicated-split-generation'='true') */ limit 2"))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));
    }

    @Test
    public void testBatchReadSourceWithoutSnapshot() {
        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.dedicated-split-generation'='true') */"))
                .hasSize(0);
    }
}
