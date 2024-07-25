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

import org.apache.paimon.utils.BlockingIterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for table with branches using SQL. */
public class BranchSqlITCase extends CatalogITCaseBase {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testAlterTable() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '" + tempDir + "' )");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql(
                "CREATE TABLE t ( pt INT, k INT, v STRING, PRIMARY KEY (pt, k) NOT ENFORCED ) "
                        + "PARTITIONED BY (pt) WITH ( 'bucket' = '2' )");

        tEnv.executeSql(
                        "INSERT INTO t VALUES (1, 10, 'apple'), (1, 20, 'banana'), (2, 10, 'cat'), (2, 20, 'dog')")
                .await();
        tEnv.executeSql("CALL sys.create_branch('default.t', 'test', 1)");
        tEnv.executeSql("INSERT INTO t VALUES (1, 10, 'APPLE'), (2, 20, 'DOG'), (2, 30, 'horse')")
                .await();

        tEnv.executeSql("ALTER TABLE `t$branch_test` ADD (v2 INT)").await();
        tEnv.executeSql(
                        "INSERT INTO `t$branch_test` VALUES "
                                + "(1, 10, 'cherry', 100), (2, 20, 'bird', 200), (2, 40, 'wolf', 400)")
                .await();

        assertThat(collectResult(tEnv, "SELECT * FROM t"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, APPLE]",
                        "+I[1, 20, banana]",
                        "+I[2, 30, horse]",
                        "+I[2, 10, cat]",
                        "+I[2, 20, DOG]");
        assertThat(collectResult(tEnv, "SELECT * FROM t$branch_test"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, cherry, 100]",
                        "+I[1, 20, banana, null]",
                        "+I[2, 10, cat, null]",
                        "+I[2, 20, bird, 200]",
                        "+I[2, 40, wolf, 400]");
    }

    @Test
    public void testBranchOptionsTable() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '" + tempDir + "' )");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql(
                "CREATE TABLE t ( pt INT, k INT, v STRING, PRIMARY KEY (pt, k) NOT ENFORCED ) "
                        + "PARTITIONED BY (pt) WITH ( 'bucket' = '2' )");

        tEnv.executeSql("CALL sys.create_branch('default.t', 'test')");

        tEnv.executeSql("ALTER TABLE t SET ('snapshot.time-retained' = '5 h')");
        tEnv.executeSql("ALTER TABLE t$branch_test SET ('snapshot.time-retained' = '1 h')");

        assertThat(collectResult(tEnv, "SELECT * FROM t$options"))
                .containsExactlyInAnyOrder("+I[bucket, 2]", "+I[snapshot.time-retained, 5 h]");
        assertThat(collectResult(tEnv, "SELECT * FROM t$branch_test$options"))
                .containsExactlyInAnyOrder("+I[bucket, 2]", "+I[snapshot.time-retained, 1 h]");
    }

    @Test
    public void testBranchSnapshotsTable() {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");
        sql("CALL sys.create_branch('default.t', 'b1',1)");
        List<Row> result1 = sql("SELECT snapshot_id, schema_id, commit_kind FROM t$snapshots");
        assertThat(result1).containsExactly(Row.of(1L, 0L, "APPEND"));
        List<Row> result2 =
                sql("SELECT snapshot_id, schema_id, commit_kind FROM t$branch_b1$snapshots");
        assertThat(result2).containsExactly(Row.of(1L, 0L, "APPEND"));

        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        sql("INSERT INTO t$branch_b1 VALUES (5, 6)");

        List<Row> result3 =
                sql("SELECT snapshot_id, schema_id, commit_kind FROM t$branch_b1$snapshots");
        assertThat(result3)
                .containsExactly(
                        Row.of(1L, 0L, "APPEND"),
                        Row.of(2L, 0L, "APPEND"),
                        Row.of(3L, 0L, "APPEND"));
    }

    @Test
    public void testBranchSchemasTable() {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");
        sql("CALL sys.create_branch('default.t', 'b1')");
        List<Row> result = sql("SELECT schema_id FROM t$branch_b1$schemas order by schema_id");
        assertThat(result.toString()).isEqualTo("[+I[0]]");
        sql("ALTER TABLE t$branch_b1 SET ('snapshot.time-retained' = '5 h')");

        List<Row> result1 = sql("SELECT schema_id FROM t$branch_b1$schemas order by schema_id");
        assertThat(result1.toString()).isEqualTo("[+I[0], +I[1]]");
    }

    @Test
    public void testBranchAuditLogTable() {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");
        //        sql("INSERT INTO t VALUES (3, 4)");
        List<Row> res = sql("SELECT * FROM t$audit_log");
        //        assertThat(res.toString()).isEqualTo("[+I[+I, 1, 2]]");
        assertThat(res).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "+I", 1, 2));

        sql("CALL sys.create_branch('default.t', 'b1')");
        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        List<Row> result = sql("SELECT * FROM t$branch_b1$audit_log");
        assertThat(result.toString()).isEqualTo("[+I[+I, 3, 4]]");
    }

    @Test
    public void testBranchReadOptimizedTable() {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");
        List<Row> res = sql("SELECT * FROM t$ro");
        //        assertThat(res.toString()).isEqualTo("[+I[+I, 1, 2]]");
        assertThat(res).containsExactly(Row.of(1, 2));

        sql("CALL sys.create_branch('default.t', 'b1')");
        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        List<Row> result = sql("SELECT * FROM t$branch_b1$ro");
        assertThat(result.toString()).isEqualTo("[+I[3, 4]]");
    }

    @Test
    public void testBranchFilesTable() {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");
        List<Row> res = sql("SELECT min_value_stats FROM t$files");
        //        assertThat(res.toString()).isEqualTo("[+I[+I, 1, 2]]");
        //        assertThat(res).containsExactly(Row.of(1, 2));

        sql("CALL sys.create_branch('default.t', 'b1',1)");
        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        sql("INSERT INTO t$branch_b1 VALUES (5, 6)");
        List<Row> result = sql("SELECT min_value_stats FROM t$branch_b1$files");
        assertThat(result)
                .containsExactly(Row.of("{a=1, b=2}"), Row.of("{a=3, b=4}"), Row.of("{a=5, b=6}"));
        //        assertThat(result.toString()).isEqualTo("[+I[3, 4]]");
    }

    @Test
    public void testBranchTagsTable() throws Exception {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");
        paimonTable("t").createTag("tag1", 1);
        List<Row> res = sql("SELECT tag_name,snapshot_id,record_count FROM t$tags");
        assertThat(res.toString()).isEqualTo("[+I[tag1, 1, 1]]");

        sql("CALL sys.create_branch('default.t', 'b1',1)");
        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        paimonTable("t$branch_b1").createTag("tag2", 2);
        List<Row> result = sql("SELECT tag_name,snapshot_id,record_count FROM t$branch_b1$tags");
        assertThat(result.toString()).isEqualTo("[+I[tag2, 2, 2]]");
    }

    @Test
    public void testBranchConsumersTable() throws Exception {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2), (3,4)");

        sql("CALL sys.create_branch('default.t', 'b1',1)");
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM t$branch_b1 /*+ OPTIONS('consumer-id'='id1','consumer.expiration-time'='3h') */"));
        assertThat(iterator.collect(2)).containsExactlyInAnyOrder(Row.of(1, 2), Row.of(3, 4));
        sql("INSERT INTO t$branch_b1 VALUES (5, 6), (7, 8)");
        assertThat(iterator.collect(2)).containsExactlyInAnyOrder(Row.of(5, 6), Row.of(7, 8));
        iterator.close();
        List<Row> result = sql("SELECT * FROM t$branch_b1$consumers");
        assertThat(result.toString()).isEqualTo("[+I[id1, 3]]");
    }

    @Test
    public void testBranchManifestsTable() {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");

        sql("CALL sys.create_branch('default.t', 'b1',1)");
        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        List<Row> result = sql("SELECT schema_id, file_name, file_size FROM t$branch_b1$manifests");
        assertThat(result.size()).isEqualTo(2);
        result.forEach(
                row -> {
                    assertThat((long) row.getField(0)).isEqualTo(0L);
                    assertThat(StringUtils.startsWith((String) row.getField(1), "manifest"))
                            .isTrue();
                    assertThat((long) row.getField(2)).isGreaterThan(0L);
                });
    }

    @Test
    public void testBranchPartitionsTable() {
        sql("CREATE TABLE t (a INT, b INT,c STRING) PARTITIONED BY (a)");
        assertThat(sql("SELECT * FROM t$partitions")).isEmpty();

        sql("INSERT INTO t VALUES (1, 2, 'x')");
        sql("INSERT INTO t VALUES (1, 4, 'S2'), (2, 2, 'S1'), (2, 2, 'S1')");
        sql("INSERT INTO t VALUES (1, 4, 'S3'), (2, 2, 'S4')");

        sql("CALL sys.create_branch('default.t', 'b1')");
        sql("INSERT INTO t$branch_b1 VALUES (1, 4, 'S2'), (2, 2, 'S1'), (2, 2, 'S5')");
        sql("INSERT INTO t$branch_b1 VALUES (1, 4, 'S3'), (2, 2, 'S4')");

        List<Row> result = sql("SELECT `partition`, record_count, file_count FROM t$partitions");
        assertThat(result).containsExactlyInAnyOrder(Row.of("[1]", 3L, 3L), Row.of("[2]", 3L, 2L));
        result = sql("SELECT `partition`, record_count, file_count FROM t$branch_b1$partitions");
        assertThat(result).containsExactlyInAnyOrder(Row.of("[1]", 2L, 2L), Row.of("[2]", 3L, 2L));
    }

    private List<String> collectResult(TableEnvironment tEnv, String sql) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add(it.next().toString());
            }
        }
        return result;
    }
}
