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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.privilege.NoPrivilegeException;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Ensure that the legacy multiply overloaded CALL with positional arguments can be invoked. */
public class ProcedurePositionalArgumentsITCase extends CatalogITCaseBase {

    @Test
    public void testCompactDatabaseAndTable() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " pt INT,"
                        + " PRIMARY KEY (k, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");

        assertThatCode(() -> sql("CALL sys.compact('default.T')")).doesNotThrowAnyException();
        assertThatCode(() -> sql("CALL sys.compact('default.T', 'pt=1')"))
                .doesNotThrowAnyException();
        assertThatCode(() -> sql("CALL sys.compact('default.T', 'pt=1', '', '')"))
                .doesNotThrowAnyException();
        assertThatCode(() -> sql("CALL sys.compact('default.T', '', '', '', 'sink.parallelism=1')"))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                sql(
                                        "CALL sys.compact('default.T', '', '', '', 'sink.parallelism=1','pt=1')"))
                .doesNotThrowAnyException();
        assertThatCode(() -> sql("CALL sys.compact('default.T', '', 'zorder', 'k', '','','5s')"))
                .message()
                .contains("sort compact do not support 'partition_idle_time'.");

        assertThatCode(() -> sql("CALL sys.compact_database('default')"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testUserPrivileges() throws Exception {
        sql(
                String.format(
                        "CREATE CATALOG mycat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ")",
                        path));
        sql("USE CATALOG mycat");
        sql("CREATE DATABASE mydb");
        sql("CREATE DATABASE mydb2");
        sql(
                "CREATE TABLE mydb.T1 (\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")");
        sql("INSERT INTO mydb.T1 VALUES (1, 10), (2, 20), (3, 30)");
        sql("CALL sys.init_file_based_privilege('root-passwd')");

        sql(
                String.format(
                        "CREATE CATALOG anonymouscat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ")",
                        path));

        sql("USE CATALOG anonymouscat");
        assertNoPrivilege(() -> sql("INSERT INTO mydb.T1 VALUES (1, 11), (2, 21)"));
        assertNoPrivilege(() -> collect("SELECT * FROM mydb.T1 ORDER BY k"));

        sql(
                String.format(
                        "CREATE CATALOG rootcat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s',\n"
                                + "  'user' = 'root',\n"
                                + "  'password' = 'root-passwd'\n"
                                + ")",
                        path));
        sql("USE CATALOG rootcat");
        sql(
                "CREATE TABLE mydb2.T2 (\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")");
        sql("INSERT INTO mydb2.T2 VALUES (100, 1000), (200, 2000), (300, 3000)");
        sql("CALL sys.create_privileged_user('test', 'test-passwd')");
        sql("CALL sys.grant_privilege_to_user('test', 'CREATE_TABLE', 'mydb')");
        sql("CALL sys.grant_privilege_to_user('test', 'SELECT', 'mydb')");
        sql("CALL sys.grant_privilege_to_user('test', 'INSERT', 'mydb')");

        sql(
                String.format(
                        "CREATE CATALOG testcat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s',\n"
                                + "  'user' = 'test',\n"
                                + "  'password' = 'test-passwd'\n"
                                + ")",
                        path));
        sql("USE CATALOG testcat");
        sql("INSERT INTO mydb.T1 VALUES (1, 12), (2, 22)");
        assertThat(collect("SELECT * FROM mydb.T1 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(1, 12), Row.of(2, 22), Row.of(3, 30)));
        sql("CREATE TABLE mydb.S1 ( a INT, b INT )");
        sql("INSERT INTO mydb.S1 VALUES (1, 100), (2, 200), (3, 300)");
        assertThat(collect("SELECT * FROM mydb.S1 ORDER BY a"))
                .isEqualTo(Arrays.asList(Row.of(1, 100), Row.of(2, 200), Row.of(3, 300)));
        assertNoPrivilege(() -> sql("DROP TABLE mydb.T1"));
        assertNoPrivilege(() -> sql("ALTER TABLE mydb.T1 RENAME TO mydb.T2"));
        assertNoPrivilege(() -> sql("DROP TABLE mydb.S1"));
        assertNoPrivilege(() -> sql("ALTER TABLE mydb.S1 RENAME TO mydb.S2"));
        assertNoPrivilege(() -> sql("CREATE DATABASE anotherdb"));
        assertNoPrivilege(() -> sql("DROP DATABASE mydb CASCADE"));
        assertNoPrivilege(() -> sql("CALL sys.create_privileged_user('test2', 'test2-passwd')"));

        sql("USE CATALOG rootcat");
        sql("CALL sys.create_privileged_user('test2', 'test2-passwd')");
        sql("CALL sys.grant_privilege_to_user('test2', 'SELECT', 'mydb2')");
        sql("CALL sys.grant_privilege_to_user('test2', 'INSERT', 'mydb', 'T1')");
        sql("CALL sys.grant_privilege_to_user('test2', 'SELECT', 'mydb', 'S1')");
        sql("CALL sys.grant_privilege_to_user('test2', 'CREATE_DATABASE')");

        sql(
                String.format(
                        "CREATE CATALOG test2cat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s',\n"
                                + "  'user' = 'test2',\n"
                                + "  'password' = 'test2-passwd'\n"
                                + ")",
                        path));
        sql("USE CATALOG test2cat");
        sql("INSERT INTO mydb.T1 VALUES (1, 13), (2, 23)");
        assertNoPrivilege(() -> collect("SELECT * FROM mydb.T1 ORDER BY k"));
        assertNoPrivilege(() -> sql("CREATE TABLE mydb.S2 ( a INT, b INT )"));
        assertNoPrivilege(() -> sql("INSERT INTO mydb.S1 VALUES (1, 100), (2, 200), (3, 300)"));
        assertThat(collect("SELECT * FROM mydb.S1 ORDER BY a"))
                .isEqualTo(Arrays.asList(Row.of(1, 100), Row.of(2, 200), Row.of(3, 300)));
        assertNoPrivilege(
                () -> sql("INSERT INTO mydb2.T2 VALUES (100, 1001), (200, 2001), (300, 3001)"));
        assertThat(collect("SELECT * FROM mydb2.T2 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(100, 1000), Row.of(200, 2000), Row.of(300, 3000)));
        sql("CREATE DATABASE anotherdb");
        assertNoPrivilege(() -> sql("DROP TABLE mydb.T1"));
        assertNoPrivilege(() -> sql("ALTER TABLE mydb.T1 RENAME TO mydb.T2"));
        assertNoPrivilege(() -> sql("DROP TABLE mydb.S1"));
        assertNoPrivilege(() -> sql("ALTER TABLE mydb.S1 RENAME TO mydb.S2"));
        assertNoPrivilege(() -> sql("DROP DATABASE mydb CASCADE"));
        assertNoPrivilege(() -> sql("CALL sys.create_privileged_user('test3', 'test3-passwd')"));

        sql("USE CATALOG rootcat");
        assertThat(collect("SELECT * FROM mydb.T1 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(1, 13), Row.of(2, 23), Row.of(3, 30)));
        sql("CALL sys.revoke_privilege_from_user('test2', 'SELECT')");
        sql("CALL sys.drop_privileged_user('test')");

        sql("USE CATALOG testcat");
        Exception e =
                assertThrows(Exception.class, () -> collect("SELECT * FROM mydb.T1 ORDER BY k"));
        assertThat(e).hasRootCauseMessage("User test not found, or password incorrect.");

        sql("USE CATALOG test2cat");
        assertNoPrivilege(() -> collect("SELECT * FROM mydb.S1 ORDER BY a"));
        assertNoPrivilege(() -> collect("SELECT * FROM mydb2.T2 ORDER BY k"));
        sql("INSERT INTO mydb.T1 VALUES (1, 14), (2, 24)");

        sql("USE CATALOG rootcat");
        assertThat(collect("SELECT * FROM mydb.T1 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(1, 14), Row.of(2, 24), Row.of(3, 30)));
        sql("DROP DATABASE mydb CASCADE");
        sql("DROP DATABASE mydb2 CASCADE");
    }

    private List<Row> collect(String sql) throws Exception {
        List<Row> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add(it.next());
            }
        }
        return result;
    }

    private void assertNoPrivilege(Executable executable) {
        Exception e = assertThrows(Exception.class, executable);
        if (e.getCause() != null) {
            assertThat(e).hasRootCauseInstanceOf(NoPrivilegeException.class);
        } else {
            assertThat(e).isInstanceOf(NoPrivilegeException.class);
        }
    }

    @Test
    public void testExpirePartitionsProcedure() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");
        sql("INSERT INTO T VALUES ('1', '2024-06-01')");
        sql("INSERT INTO T VALUES ('2', '9024-06-01')");
        assertThat(read(table)).containsExactlyInAnyOrder("1:2024-06-01", "2:9024-06-01");
        sql("CALL sys.expire_partitions('default.T', '1 d', 'yyyy-MM-dd', '$dt', 'values-time')");
        assertThat(read(table)).containsExactlyInAnyOrder("2:9024-06-01");
    }

    private List<String> read(FileStoreTable table) throws IOException {
        List<String> ret = new ArrayList<>();
        table.newRead()
                .createReader(table.newScan().plan().splits())
                .forEachRemaining(row -> ret.add(row.getString(0) + ":" + row.getString(1)));
        return ret;
    }

    @Test
    public void testCreateDeleteTag() {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        sql("insert into T values('k', '2024-01-01')");
        sql("insert into T values('k2', '2024-01-02')");

        sql("CALL sys.create_tag('default.T', 'tag1')");

        assertThat(
                        sql("select * from T /*+ OPTIONS('scan.tag-name'='tag1') */").stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder("+I[k2, 2024-01-02]", "+I[k, 2024-01-01]");

        sql("CALL sys.rollback_to('default.T', 'tag1')");

        assertThat(sql("select * from T").stream().map(Row::toString))
                .containsExactlyInAnyOrder("+I[k2, 2024-01-02]", "+I[k, 2024-01-01]");

        sql("CALL sys.delete_tag('default.T', 'tag1')");

        assertThatThrownBy(
                        () ->
                                sql("select * from T /*+ OPTIONS('scan.tag-name'='tag1') */")
                                        .stream()
                                        .map(Row::toString))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Tag 'tag1' doesn't exist.");
    }

    @Test
    public void testCreateDeleteAndForwardBranch() throws Exception {
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

        assertThat(collectToString("SELECT branch_name, created_from_snapshot FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[test, 1]", "+I[test2, 2]");

        sql("CALL sys.delete_branch('default.T', 'test')");

        assertThat(collectToString("SELECT branch_name, created_from_snapshot FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[test2, 2]");

        sql("CALL sys.fast_forward('default.T', 'test2')");

        // Branch `test` replaces the main branch.
        assertThat(collectToString("SELECT * FROM `T`"))
                .containsExactlyInAnyOrder("+I[1, 10, apple]", "+I[1, 20, dog]");
    }

    private List<String> collectToString(String sql) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add(it.next().toString());
            }
        }
        return result;
    }

    @Test
    public void testPartitionMarkDone() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " pt INT,"
                        + " PRIMARY KEY (k, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");

        assertThatCode(() -> sql("CALL sys.mark_partition_done('default.T', 'pt = 0')"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testMergeInto() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " pt INT,"
                        + " PRIMARY KEY (k, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        sql(
                "CREATE TABLE S ("
                        + " k INT,"
                        + " v INT,"
                        + " pt INT,"
                        + " PRIMARY KEY (k, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");

        assertThatCode(
                        () ->
                                sql(
                                        "CALL sys.merge_into('default.T', 'TT', '', 'S', 'TT.k = S.k', '', '', '', '', 'S.v IS NULL')"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testMigrateProcedures() {
        sql(
                "CREATE TABLE S ("
                        + " k INT,"
                        + " v INT,"
                        + " pt INT,"
                        + " PRIMARY KEY (k, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");

        assertThatThrownBy(() -> sql("CALL sys.migrate_database('hive', 'default', '')"))
                .hasMessageContaining("Only support Hive Catalog.");
        assertThatThrownBy(() -> sql("CALL sys.migrate_table('hive', 'default.T', '')"))
                .hasMessageContaining("Only support Hive Catalog.");
        assertThatThrownBy(() -> sql("CALL sys.migrate_file('hive', 'default.T', 'default.S')"))
                .hasMessageContaining("Only support Hive Catalog.");
    }

    @Test
    public void testQueryService() {
        sql(
                "CREATE TABLE DIM (k INT PRIMARY KEY NOT ENFORCED, v INT) WITH ('bucket' = '2', 'continuous.discovery-interval' = '1ms')");
        assertThatCode(
                        () -> {
                            CloseableIterator<Row> service =
                                    streamSqlIter("CALL sys.query_service('default.DIM', 2)");
                            service.close();
                        })
                .doesNotThrowAnyException();
    }

    @Test
    public void testRemoveOrphanFiles() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " pt INT,"
                        + " PRIMARY KEY (k, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        assertThatCode(() -> sql("CALL sys.remove_orphan_files('default.T')"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testRepair() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " pt INT,"
                        + " PRIMARY KEY (k, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        assertThatThrownBy(() -> sql("CALL sys.repair('default.T')"))
                .hasStackTraceContaining("Catalog.repairTable");
    }

    @Test
    public void testResetConsumer() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " pt INT,"
                        + " PRIMARY KEY (k, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        assertThatCode(() -> sql("CALL sys.reset_consumer('default.T', 'myid')"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testRewriteFileIndex() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " pt INT,"
                        + " PRIMARY KEY (k, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        assertThatCode(() -> sql("CALL sys.rewrite_file_index('default.T', 'pt = 0')"))
                .doesNotThrowAnyException();
    }
}
