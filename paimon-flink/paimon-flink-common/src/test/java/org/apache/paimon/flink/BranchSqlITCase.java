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
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        assertThat(collectResult("SELECT branch_name, created_from_snapshot FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[test, 1]", "+I[test2, 2]");

        sql("CALL sys.delete_branch('default.T', 'test')");

        assertThat(collectResult("SELECT branch_name, created_from_snapshot FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[test2, 2]");
    }

    @Test
    public void testBranchManagerGetBranchSnapshotsList() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        sql("INSERT INTO T VALUES (1, 10, 'hxh')");
        sql("INSERT INTO T VALUES (1, 20, 'hxh')");
        sql("INSERT INTO T VALUES (1, 30, 'hxh')");

        FileStoreTable table = paimonTable("T");
        checkSnapshots(table.snapshotManager(), 1, 3);

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");
        sql("CALL sys.create_tag('default.T', 'tag2', 2)");
        sql("CALL sys.create_tag('default.T', 'tag3', 3)");

        sql("CALL sys.create_branch('default.T', 'test1', 'tag1')");
        sql("CALL sys.create_branch('default.T', 'test2', 'tag2')");
        sql("CALL sys.create_branch('default.T', 'test3', 'tag3')");

        assertThat(collectResult("SELECT created_from_snapshot FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[1]", "+I[2]", "+I[3]");
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
    public void testDifferentRowTypes() {
        sql(
                "CREATE TABLE t ( pt INT NOT NULL, k INT NOT NULL, v STRING ) PARTITIONED BY (pt) WITH ( 'bucket' = '-1' )");
        sql("CALL sys.create_branch('default.t', 'pk')");
        sql("ALTER TABLE `t$branch_pk` SET ( 'primary-key' = 'pt, k', 'bucket' = '2' )");
        sql("ALTER TABLE `t$branch_pk` ADD (v2 INT)");
        sql("ALTER TABLE t SET ( 'scan.fallback-branch' = 'pk' )");

        assertThatThrownBy(() -> sql("INSERT INTO t VALUES (1, 10, 'apple')"))
                .hasMessageContaining("Branch main and pk does not have the same row type");
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
