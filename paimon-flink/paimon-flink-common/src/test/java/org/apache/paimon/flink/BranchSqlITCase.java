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

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for table with branches using SQL. */
public class BranchSqlITCase extends CatalogITCaseBase {

    @Test
    public void testAlterBranchTable() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        sql(
                "INSERT INTO T VALUES"
                        + " (1, 10, 'apple'),"
                        + " (1, 20, 'banana'),"
                        + " (2, 10, 'cat'),"
                        + " (2, 20, 'dog')");

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");

        sql("CALL sys.create_branch('default.T', 'test', 'tag1')");

        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.schema().fields().size()).isEqualTo(3);

        sql(
                "INSERT INTO T VALUES"
                        + " (1, 10, 'APPLE'),"
                        + " (2, 20, 'DOG'),"
                        + " (2, 30, 'horse')");

        // Add v2 column for branch table.
        sql("ALTER TABLE `T$branch_test` ADD (v2 INT)");

        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.schema().fields().size()).isEqualTo(4);

        sql(
                "INSERT INTO `T$branch_test` VALUES "
                        + "(1, 10, 'cherry', 100)"
                        + ", (2, 20, 'bird', 200)"
                        + ", (2, 40, 'wolf', 400)");

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, APPLE]",
                        "+I[1, 20, banana]",
                        "+I[2, 30, horse]",
                        "+I[2, 10, cat]",
                        "+I[2, 20, DOG]");

        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, cherry, 100]",
                        "+I[1, 20, banana, null]",
                        "+I[2, 10, cat, null]",
                        "+I[2, 20, bird, 200]",
                        "+I[2, 40, wolf, 400]");
    }

    @Test
    public void testCreateBranchFromTag() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        // snapshot 1.
        sql("INSERT INTO T VALUES" + " (1, 10, 'apple')," + " (1, 20, 'banana')");
        // snapshot 2.
        sql("INSERT INTO T VALUES" + " (2, 10, 'cat')," + " (2, 20, 'dog')");

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");
        sql("CALL sys.create_tag('default.T', 'tag2', 2)");

        sql("CALL sys.create_branch('default.T', 'test', 'tag1')");
        sql("CALL sys.create_branch('default.T', 'test2', 'tag2')");

        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.tagManager().tagExists("tag1")).isEqualTo(true);

        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder("+I[1, 10, apple]", "+I[1, 20, banana]");

        FileStoreTable branchTable2 = paimonTable("T$branch_test2");
        assertThat(branchTable2.tagManager().tagExists("tag2")).isEqualTo(true);

        assertThat(collectResult("SELECT * FROM T$branch_test2"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, apple]",
                        "+I[1, 20, banana]",
                        "+I[2, 10, cat]",
                        "+I[2, 20, dog]");
    }

    @Test
    public void testCreateEmptyBranch() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        // snapshot 1.
        sql("INSERT INTO T VALUES(1, 10, 'apple')");

        // snapshot 2.
        sql("INSERT INTO T VALUES(1, 20, 'dog')");

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, 10, apple]", "+I[1, 20, dog]");

        // create en empty branch.
        sql("CALL sys.create_branch('default.T', 'empty_branch')");

        sql("INSERT INTO `T$branch_empty_branch` VALUES (3, 30, 'banana')");

        assertThat(collectResult("SELECT * FROM T$branch_empty_branch"))
                .containsExactlyInAnyOrder("+I[3, 30, banana]");
    }

    @Test
    public void testDeleteBranchTable() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        // snapshot 1.
        sql("INSERT INTO T VALUES(1, 10, 'apple')");

        // snapshot 2.
        sql("INSERT INTO T VALUES(1, 20, 'dog')");

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");

        sql("CALL sys.create_tag('default.T', 'tag2', 2)");

        sql("CALL sys.create_branch('default.T', 'test', 'tag1')");
        sql("CALL sys.create_branch('default.T', 'test2', 'tag2')");

        assertThat(collectResult("SELECT branch_name, created_from_tag FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[test, tag1]", "+I[test2, tag2]");

        sql("CALL sys.delete_branch('default.T', 'test')");

        assertThat(collectResult("SELECT branch_name, created_from_tag FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[test2, tag2]");
    }

    @Test
    public void testBranchFastForward() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        sql("INSERT INTO T VALUES (1, 10, 'hunter')");
        sql("INSERT INTO T VALUES (1, 20, 'hunter')");
        sql("INSERT INTO T VALUES (1, 30, 'hunter')");

        checkSnapshots(snapshotManager, 1, 3);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, hunter]", "+I[1, 20, hunter]", "+I[1, 30, hunter]");

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");

        sql("CALL sys.create_branch('default.T', 'test', 'tag1')");

        sql("INSERT INTO `T$branch_test` VALUES (2, 10, 'hunterX')");

        checkSnapshots(paimonTable("T$branch_test").snapshotManager(), 1, 2);

        // query branch data.
        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]", "+I[2, 10, hunterX]");

        sql("CALL sys.fast_forward('default.T', 'test')");

        // Branch `test` replaces the main branch.
        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]", "+I[2, 10, hunterX]");

        checkSnapshots(snapshotManager, 1, 2);
    }

    @Test
    public void testFallbackBranchBatchRead() throws Exception {
        sql(
                "CREATE TABLE t ( pt INT NOT NULL, k INT NOT NULL, v STRING ) PARTITIONED BY (pt) WITH ( 'bucket' = '-1' )");
        sql("INSERT INTO t VALUES (1, 10, 'apple'), (1, 20, 'banana')");

        sql("CALL sys.create_branch('default.t', 'pk')");
        sql("ALTER TABLE `t$branch_pk` SET ( 'primary-key' = 'pt, k', 'bucket' = '2' )");
        sql("ALTER TABLE t SET ( 'scan.fallback-branch' = 'pk' )");

        sql("INSERT INTO `t$branch_pk` VALUES (1, 20, 'cat'), (1, 30, 'dog')");
        assertThat(collectResult("SELECT v, k FROM t"))
                .containsExactlyInAnyOrder("+I[apple, 10]", "+I[banana, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder("+I[cat, 20]", "+I[dog, 30]");

        sql("INSERT INTO `t$branch_pk` VALUES (2, 10, 'tiger'), (2, 20, 'wolf')");
        assertThat(collectResult("SELECT v, k FROM t"))
                .containsExactlyInAnyOrder(
                        "+I[apple, 10]", "+I[banana, 20]", "+I[tiger, 10]", "+I[wolf, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder(
                        "+I[cat, 20]", "+I[dog, 30]", "+I[tiger, 10]", "+I[wolf, 20]");
        assertThat(collectResult("SELECT v, k FROM t WHERE pt = 1"))
                .containsExactlyInAnyOrder("+I[apple, 10]", "+I[banana, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk` WHERE pt = 1"))
                .containsExactlyInAnyOrder("+I[cat, 20]", "+I[dog, 30]");
        assertThat(collectResult("SELECT v, k FROM t WHERE pt = 2"))
                .containsExactlyInAnyOrder("+I[tiger, 10]", "+I[wolf, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk` WHERE pt = 2"))
                .containsExactlyInAnyOrder("+I[tiger, 10]", "+I[wolf, 20]");

        sql("INSERT INTO `t$branch_pk` VALUES (2, 10, 'lion')");
        assertThat(collectResult("SELECT v, k FROM t"))
                .containsExactlyInAnyOrder(
                        "+I[apple, 10]", "+I[banana, 20]", "+I[lion, 10]", "+I[wolf, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder(
                        "+I[cat, 20]", "+I[dog, 30]", "+I[lion, 10]", "+I[wolf, 20]");

        sql("INSERT OVERWRITE t PARTITION (pt = 1) VALUES (10, 'pear'), (20, 'mango')");
        assertThat(collectResult("SELECT v, k FROM t"))
                .containsExactlyInAnyOrder(
                        "+I[pear, 10]", "+I[mango, 20]", "+I[lion, 10]", "+I[wolf, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder(
                        "+I[cat, 20]", "+I[dog, 30]", "+I[lion, 10]", "+I[wolf, 20]");

        sql("ALTER TABLE t RESET ( 'scan.fallback-branch' )");
        assertThat(collectResult("SELECT v, k FROM t"))
                .containsExactlyInAnyOrder("+I[pear, 10]", "+I[mango, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder(
                        "+I[cat, 20]", "+I[dog, 30]", "+I[lion, 10]", "+I[wolf, 20]");
    }

    @Test
    public void testDifferentRowTypes() throws Exception {
        sql(
                "CREATE TABLE t ( pt INT NOT NULL, k INT NOT NULL, v STRING ) PARTITIONED BY (pt) WITH ( 'bucket' = '-1' )");
        sql("CALL sys.create_branch('default.t', 'pk')");
        sql("ALTER TABLE `t$branch_pk` SET ( 'primary-key' = 'pt, k', 'bucket' = '2' )");
        sql("ALTER TABLE `t$branch_pk` ADD (v2 INT)");
        sql("INSERT INTO t VALUES (1, 10, 'apple')");
        sql("INSERT INTO `t$branch_pk` VALUES (1, 10, 'cat', 100)");

        sql("ALTER TABLE t SET ( 'scan.fallback-branch' = 'pk' )");
        assertThatThrownBy(() -> sql("SELECT * FROM t"))
                .hasMessageContaining("Branch main and pk does not have the same row type");

        sql("ALTER TABLE t RESET ( 'scan.fallback-branch' )");
        assertThat(collectResult("SELECT v, k FROM t")).containsExactlyInAnyOrder("+I[apple, 10]");
        assertThat(collectResult("SELECT v, v2, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder("+I[cat, 100, 10]");
    }

    @Test
    public void testBranchOptionsTable() throws Exception {
        sql(
                "CREATE TABLE t ( pt INT, k INT, v STRING, PRIMARY KEY (pt, k) NOT ENFORCED ) "
                        + "PARTITIONED BY (pt) WITH ( 'bucket' = '2' )");

        sql("CALL sys.create_branch('default.t', 'test')");

        sql("ALTER TABLE t SET ('snapshot.time-retained' = '5 h')");
        sql("ALTER TABLE t$branch_test SET ('snapshot.time-retained' = '1 h')");

        assertThat(collectResult("SELECT * FROM t$options"))
                .containsExactlyInAnyOrder(
                        "+I[bucket, 2]",
                        "+I[snapshot.time-retained, 5 h]",
                        "+I[scan.infer-parallelism, false]");
        assertThat(collectResult("SELECT * FROM t$branch_test$options"))
                .containsExactlyInAnyOrder(
                        "+I[bucket, 2]",
                        "+I[snapshot.time-retained, 1 h]",
                        "+I[scan.infer-parallelism, false]");
    }

    @Test
    public void testBranchSchemasTable() throws Exception {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");
        sql("CALL sys.create_branch('default.t', 'b1')");
        assertThat(collectResult("SELECT schema_id FROM t$branch_b1$schemas order by schema_id"))
                .containsExactlyInAnyOrder("+I[0]");

        sql("ALTER TABLE t$branch_b1 SET ('snapshot.time-retained' = '5 h')");
        assertThat(collectResult("SELECT schema_id FROM t$branch_b1$schemas order by schema_id"))
                .containsExactlyInAnyOrder("+I[0]", "+I[1]");
    }

    @Test
    public void testBranchAuditLogTable() throws Exception {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");
        assertThat(collectResult("SELECT * FROM t$audit_log"))
                .containsExactlyInAnyOrder("+I[+I, 1, 2]");

        sql("CALL sys.create_branch('default.t', 'b1')");
        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        assertThat(collectResult("SELECT * FROM t$branch_b1$audit_log"))
                .containsExactlyInAnyOrder("+I[+I, 3, 4]");
    }

    @Test
    public void testBranchReadOptimizedTable() throws Exception {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");
        assertThat(collectResult("SELECT * FROM t$ro")).containsExactlyInAnyOrder("+I[1, 2]");

        sql("CALL sys.create_branch('default.t', 'b1')");
        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        assertThat(collectResult("SELECT * FROM t$branch_b1$ro"))
                .containsExactlyInAnyOrder("+I[3, 4]");
    }

    @Test
    public void testBranchFilesTable() throws Exception {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");

        sql("CALL sys.create_branch('default.t', 'b1')");
        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        sql("INSERT INTO t$branch_b1 VALUES (5, 6)");

        assertThat(collectResult("SELECT min_value_stats FROM t$files"))
                .containsExactlyInAnyOrder("+I[{a=1, b=2}]");
        assertThat(collectResult("SELECT min_value_stats FROM t$branch_b1$files"))
                .containsExactlyInAnyOrder("+I[{a=3, b=4}]", "+I[{a=5, b=6}]");
    }

    @Test
    public void testBranchTagsTable() throws Exception {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");
        paimonTable("t").createTag("tag1", 1);

        sql("CALL sys.create_branch('default.t', 'b1','tag1')");
        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        paimonTable("t$branch_b1").createTag("tag2", 2);

        assertThat(collectResult("SELECT tag_name,snapshot_id,record_count FROM t$tags"))
                .containsExactlyInAnyOrder("+I[tag1, 1, 1]");
        assertThat(collectResult("SELECT tag_name,snapshot_id,record_count FROM t$branch_b1$tags"))
                .containsExactlyInAnyOrder("+I[tag1, 1, 1]", "+I[tag2, 2, 2]");
    }

    @Test
    public void testBranchConsumersTable() throws Exception {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2), (3,4)");

        sql("CALL sys.create_branch('default.t', 'b1')");
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM t$branch_b1 /*+ OPTIONS('consumer-id'='id1','consumer.expiration-time'='3h') */"));
        sql("INSERT INTO t$branch_b1 VALUES (5, 6), (7, 8)");
        assertThat(iterator.collect(2)).containsExactlyInAnyOrder(Row.of(5, 6), Row.of(7, 8));
        iterator.close();

        assertThat(collectResult("SELECT * FROM t$consumers")).isEmpty();
        assertThat(collectResult("SELECT * FROM t$branch_b1$consumers"))
                .containsExactlyInAnyOrder("+I[id1, 2]");
    }

    @Test
    public void testBranchManifestsTable() {
        sql("CREATE TABLE t (a INT, b INT)");
        sql("INSERT INTO t VALUES (1, 2)");

        sql("CALL sys.create_branch('default.t', 'b1')");
        sql("INSERT INTO t$branch_b1 VALUES (3, 4)");
        sql("INSERT INTO t$branch_b1 VALUES (5, 6)");

        List<Row> res = sql("SELECT schema_id, file_name, file_size FROM t$manifests");
        assertThat(res).hasSize(1);

        res = sql("SELECT schema_id, file_name, file_size FROM t$branch_b1$manifests");
        assertThat(res).hasSize(2);
        res.forEach(
                row -> {
                    assertThat((long) row.getField(0)).isEqualTo(0L);
                    assertThat(StringUtils.startsWith((String) row.getField(1), "manifest"))
                            .isTrue();
                    assertThat((long) row.getField(2)).isGreaterThan(0L);
                });
    }

    @Test
    public void testBranchPartitionsTable() throws Exception {
        sql("CREATE TABLE t (a INT, b INT,c STRING) PARTITIONED BY (a)");
        assertThat(sql("SELECT * FROM t$partitions")).isEmpty();

        sql("INSERT INTO t VALUES (1, 2, 'x')");
        sql("INSERT INTO t VALUES (1, 4, 'S2'), (2, 2, 'S1'), (2, 2, 'S1')");
        sql("INSERT INTO t VALUES (1, 4, 'S3'), (2, 2, 'S4')");

        sql("CALL sys.create_branch('default.t', 'b1')");
        sql("INSERT INTO t$branch_b1 VALUES (1, 4, 'S2'), (2, 2, 'S1'), (2, 2, 'S5')");
        sql("INSERT INTO t$branch_b1 VALUES (1, 4, 'S3'), (2, 2, 'S4')");

        assertThat(collectResult("SELECT `partition`, record_count, file_count FROM t$partitions"))
                .containsExactlyInAnyOrder("+I[[1], 3, 3]", "+I[[2], 3, 2]");
        assertThat(
                        collectResult(
                                "SELECT `partition`, record_count, file_count FROM t$branch_b1$partitions"))
                .containsExactlyInAnyOrder("+I[[1], 2, 2]", "+I[[2], 3, 2]");
    }

    @Test
    public void testCannotSetEmptyFallbackBranch() {
        String errMsg =
                "Cannot set 'scan.fallback-branch' = 'test' because the branch 'test' isn't existed.";
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE t1 (a INT, b INT) WITH ('scan.fallback-branch' = 'test')"))
                .satisfies(anyCauseMatches(IllegalArgumentException.class, errMsg));

        assertThatThrownBy(
                        () -> {
                            sql("CREATE TABLE t2 (a INT, b INT)");
                            sql("ALTER TABLE t2 SET ('scan.fallback-branch' = 'test')");
                        })
                .satisfies(anyCauseMatches(IllegalArgumentException.class, errMsg));
    }

    private List<String> collectResult(String sql) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add(it.next().toString());
            }
        }
        return result;
    }

    private void checkSnapshots(SnapshotManager sm, int earliest, int latest) throws IOException {
        assertThat(sm.snapshotCount()).isEqualTo(latest - earliest + 1);
        assertThat(sm.earliestSnapshotId()).isEqualTo(earliest);
        assertThat(sm.latestSnapshotId()).isEqualTo(latest);
    }
}
