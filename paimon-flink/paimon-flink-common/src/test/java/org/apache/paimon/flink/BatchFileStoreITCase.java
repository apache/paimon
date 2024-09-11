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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for batch file store. */
public class BatchFileStoreITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList("CREATE TABLE IF NOT EXISTS T (a INT, b INT, c INT)");
    }

    @Test
    public void testAQEWithWriteManifest() {
        batchSql("ALTER TABLE T SET ('write-manifest-cache' = '1 mb')");
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");
        assertThatThrownBy(() -> batchSql("INSERT INTO T SELECT a, b, c FROM T GROUP BY a,b,c"))
                .hasMessageContaining(
                        "Paimon Sink with [Write Manifest Cache] does not support Flink's Adaptive Parallelism mode.");

        // work fine
        batchSql(
                "INSERT INTO T /*+ OPTIONS('sink.parallelism'='1') */ SELECT a, b, c FROM T GROUP BY a,b,c");

        // work fine too
        batchSql("ALTER TABLE T SET ('write-manifest-cache' = '0 b')");
        batchSql("INSERT INTO T SELECT a, b, c FROM T GROUP BY a,b,c");
    }

    @Test
    public void testAQEWithDynamicBucket() {
        batchSql("CREATE TABLE IF NOT EXISTS D_T (a INT PRIMARY KEY NOT ENFORCED, b INT, c INT)");
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");
        assertThatThrownBy(() -> batchSql("INSERT INTO D_T SELECT a, b, c FROM T GROUP BY a,b,c"))
                .hasMessageContaining(
                        "Paimon Sink with [Dynamic Bucket Mode] does not support Flink's Adaptive Parallelism mode.");

        // work fine
        batchSql(
                "INSERT INTO D_T /*+ OPTIONS('sink.parallelism'='1') */ SELECT a, b, c FROM T GROUP BY a,b,c");
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
                                IllegalArgumentException.class,
                                "The specified scan snapshotId 0 is out of available snapshotId range [1, 4]."));

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='0') */"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
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
}
