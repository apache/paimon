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

package org.apache.paimon.flink.procedure.privilege;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.privilege.FileBasedPrivilegeManager;
import org.apache.paimon.privilege.NoPrivilegeException;
import org.apache.paimon.privilege.PrivilegedCatalog;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for privilege related procedures. */
public class PrivilegeProcedureITCase extends AbstractTestBase {

    private String path;

    @BeforeEach
    public void beforeEach() {
        path = getTempDirPath();
    }

    @Test
    public void testUserPrivileges() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG mycat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ")",
                        path));
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql("CREATE DATABASE mydb");
        tEnv.executeSql("CREATE DATABASE mydb2");
        tEnv.executeSql(
                "CREATE TABLE mydb.T1 (\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")");
        tEnv.executeSql("INSERT INTO mydb.T1 VALUES (1, 10), (2, 20), (3, 30)").await();
        tEnv.executeSql("CALL sys.init_file_based_privilege('root-passwd')");

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG anonymouscat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ")",
                        path));

        org.apache.flink.table.catalog.Catalog catalog = tEnv.getCatalog("anonymouscat").get();
        assertThat(catalog).isInstanceOf(FlinkCatalog.class);
        Catalog paimonCatalog = ((FlinkCatalog) catalog).catalog();
        assertThat(paimonCatalog).isInstanceOf(PrivilegedCatalog.class);
        PrivilegedCatalog privilegedCatalog = (PrivilegedCatalog) paimonCatalog;
        assertThat(privilegedCatalog.wrapped()).isInstanceOf(FileSystemCatalog.class);
        assertThat(privilegedCatalog.privilegeManager())
                .isInstanceOf(FileBasedPrivilegeManager.class);
        assertThat(privilegedCatalog.privilegeManager().privilegeEnabled()).isTrue();

        tEnv.executeSql("USE CATALOG anonymouscat");
        assertNoPrivilege(
                () -> tEnv.executeSql("INSERT INTO mydb.T1 VALUES (1, 11), (2, 21)").await());
        assertNoPrivilege(() -> collect(tEnv, "SELECT * FROM mydb.T1 ORDER BY k"));
        assertNoPrivilege(() -> tEnv.executeSql("CREATE TABLE mydb.S1 ( a INT, b INT )"));
        assertNoPrivilege(() -> tEnv.executeSql("DROP TABLE mydb.T1"));
        assertNoPrivilege(() -> tEnv.executeSql("ALTER TABLE mydb.T1 RENAME TO mydb.T2"));
        assertNoPrivilege(() -> tEnv.executeSql("CREATE DATABASE anotherdb"));
        assertNoPrivilege(() -> tEnv.executeSql("DROP DATABASE mydb CASCADE"));
        assertNoPrivilege(
                () -> tEnv.executeSql("CALL sys.create_privileged_user('test2', 'test2-passwd')"));

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG rootcat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s',\n"
                                + "  'user' = 'root',\n"
                                + "  'password' = 'root-passwd'\n"
                                + ")",
                        path));
        tEnv.executeSql("USE CATALOG rootcat");
        tEnv.executeSql(
                "CREATE TABLE mydb2.T2 (\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")");
        tEnv.executeSql("INSERT INTO mydb2.T2 VALUES (100, 1000), (200, 2000), (300, 3000)")
                .await();
        tEnv.executeSql("CALL sys.create_privileged_user('test', 'test-passwd')");
        tEnv.executeSql("CALL sys.grant_privilege_to_user('test', 'CREATE_TABLE', 'mydb')");
        tEnv.executeSql("CALL sys.grant_privilege_to_user('test', 'SELECT', 'mydb')");
        tEnv.executeSql("CALL sys.grant_privilege_to_user('test', 'INSERT', 'mydb')");

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG testcat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s',\n"
                                + "  'user' = 'test',\n"
                                + "  'password' = 'test-passwd'\n"
                                + ")",
                        path));
        tEnv.executeSql("USE CATALOG testcat");
        tEnv.executeSql("INSERT INTO mydb.T1 VALUES (1, 12), (2, 22)").await();
        assertThat(collect(tEnv, "SELECT * FROM mydb.T1 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(1, 12), Row.of(2, 22), Row.of(3, 30)));
        tEnv.executeSql("CREATE TABLE mydb.S1 ( a INT, b INT )");
        tEnv.executeSql("INSERT INTO mydb.S1 VALUES (1, 100), (2, 200), (3, 300)").await();
        assertThat(collect(tEnv, "SELECT * FROM mydb.S1 ORDER BY a"))
                .isEqualTo(Arrays.asList(Row.of(1, 100), Row.of(2, 200), Row.of(3, 300)));
        assertNoPrivilege(() -> tEnv.executeSql("DROP TABLE mydb.T1"));
        assertNoPrivilege(() -> tEnv.executeSql("ALTER TABLE mydb.T1 RENAME TO mydb.T2"));
        assertNoPrivilege(() -> tEnv.executeSql("DROP TABLE mydb.S1"));
        assertNoPrivilege(() -> tEnv.executeSql("ALTER TABLE mydb.S1 RENAME TO mydb.S2"));
        assertNoPrivilege(() -> tEnv.executeSql("CREATE DATABASE anotherdb"));
        assertNoPrivilege(() -> tEnv.executeSql("DROP DATABASE mydb CASCADE"));
        assertNoPrivilege(
                () -> tEnv.executeSql("CALL sys.create_privileged_user('test2', 'test2-passwd')"));

        tEnv.executeSql("USE CATALOG rootcat");
        tEnv.executeSql("CALL sys.create_privileged_user('test2', 'test2-passwd')");
        tEnv.executeSql("CALL sys.grant_privilege_to_user('test2', 'SELECT', 'mydb2')");
        tEnv.executeSql("CALL sys.grant_privilege_to_user('test2', 'INSERT', 'mydb', 'T1')");
        tEnv.executeSql("CALL sys.grant_privilege_to_user('test2', 'SELECT', 'mydb', 'S1')");
        tEnv.executeSql("CALL sys.grant_privilege_to_user('test2', 'CREATE_DATABASE')");

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG test2cat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s',\n"
                                + "  'user' = 'test2',\n"
                                + "  'password' = 'test2-passwd'\n"
                                + ")",
                        path));
        tEnv.executeSql("USE CATALOG test2cat");
        tEnv.executeSql("INSERT INTO mydb.T1 VALUES (1, 13), (2, 23)").await();
        assertNoPrivilege(() -> collect(tEnv, "SELECT * FROM mydb.T1 ORDER BY k"));
        assertNoPrivilege(() -> tEnv.executeSql("CREATE TABLE mydb.S2 ( a INT, b INT )"));
        assertNoPrivilege(
                () ->
                        tEnv.executeSql("INSERT INTO mydb.S1 VALUES (1, 100), (2, 200), (3, 300)")
                                .await());
        assertThat(collect(tEnv, "SELECT * FROM mydb.S1 ORDER BY a"))
                .isEqualTo(Arrays.asList(Row.of(1, 100), Row.of(2, 200), Row.of(3, 300)));
        assertNoPrivilege(
                () ->
                        tEnv.executeSql(
                                        "INSERT INTO mydb2.T2 VALUES (100, 1001), (200, 2001), (300, 3001)")
                                .await());
        assertThat(collect(tEnv, "SELECT * FROM mydb2.T2 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(100, 1000), Row.of(200, 2000), Row.of(300, 3000)));
        tEnv.executeSql("CREATE DATABASE anotherdb");
        assertNoPrivilege(() -> tEnv.executeSql("DROP TABLE mydb.T1"));
        assertNoPrivilege(() -> tEnv.executeSql("ALTER TABLE mydb.T1 RENAME TO mydb.T2"));
        assertNoPrivilege(() -> tEnv.executeSql("DROP TABLE mydb.S1"));
        assertNoPrivilege(() -> tEnv.executeSql("ALTER TABLE mydb.S1 RENAME TO mydb.S2"));
        assertNoPrivilege(() -> tEnv.executeSql("DROP DATABASE mydb CASCADE"));
        assertNoPrivilege(
                () -> tEnv.executeSql("CALL sys.create_privileged_user('test3', 'test3-passwd')"));

        tEnv.executeSql("USE CATALOG rootcat");
        assertThat(collect(tEnv, "SELECT * FROM mydb.T1 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(1, 13), Row.of(2, 23), Row.of(3, 30)));
        tEnv.executeSql("CALL sys.revoke_privilege_from_user('test2', 'SELECT')");
        tEnv.executeSql("CALL sys.drop_privileged_user('test')");

        tEnv.executeSql("USE CATALOG testcat");
        Exception e =
                assertThrows(
                        Exception.class, () -> collect(tEnv, "SELECT * FROM mydb.T1 ORDER BY k"));
        assertThat(e).hasRootCauseMessage("User test not found, or password incorrect.");

        tEnv.executeSql("USE CATALOG test2cat");
        assertNoPrivilege(() -> collect(tEnv, "SELECT * FROM mydb.S1 ORDER BY a"));
        assertNoPrivilege(() -> collect(tEnv, "SELECT * FROM mydb2.T2 ORDER BY k"));
        tEnv.executeSql("INSERT INTO mydb.T1 VALUES (1, 14), (2, 24)").await();

        tEnv.executeSql("USE CATALOG rootcat");
        assertThat(collect(tEnv, "SELECT * FROM mydb.T1 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(1, 14), Row.of(2, 24), Row.of(3, 30)));
        tEnv.executeSql("DROP DATABASE mydb CASCADE");
        tEnv.executeSql("DROP DATABASE mydb2 CASCADE");
    }

    @Test
    public void testDropUser() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        initializeSingleUserTest(tEnv);

        tEnv.executeSql("USE CATALOG rootcat");
        tEnv.executeSql("CALL sys.drop_privileged_user('test')");
        tEnv.executeSql("CALL sys.create_privileged_user('test', 'test-passwd')");

        tEnv.executeSql("USE CATALOG testcat");
        assertNoPrivilege(() -> collect(tEnv, "SELECT * FROM mydb.T1 ORDER BY k"));
        assertNoPrivilege(
                () -> tEnv.executeSql("INSERT INTO mydb.T1 VALUES (1, 12), (2, 22)").await());
    }

    @Test
    public void testDropObject() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        initializeSingleUserTest(tEnv);

        tEnv.executeSql("USE CATALOG rootcat");
        tEnv.executeSql("DROP TABLE mydb.T1");
        tEnv.executeSql(
                "CREATE TABLE mydb.T1 (\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")");

        tEnv.executeSql("USE CATALOG testcat");
        assertNoPrivilege(() -> collect(tEnv, "SELECT * FROM mydb.T1 ORDER BY k"));
        assertNoPrivilege(
                () -> tEnv.executeSql("INSERT INTO mydb.T1 VALUES (1, 12), (2, 22)").await());
    }

    @Test
    public void testRenameObject() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        initializeSingleUserTest(tEnv);

        tEnv.executeSql("USE CATALOG rootcat");
        tEnv.executeSql("ALTER TABLE mydb.T1 RENAME TO mydb.T2");

        tEnv.executeSql("USE CATALOG testcat");
        assertThat(collect(tEnv, "SELECT * FROM mydb.T2 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(1, 11), Row.of(2, 21), Row.of(3, 30)));
        tEnv.executeSql("INSERT INTO mydb.T2 VALUES (1, 12), (2, 22)").await();
        assertThat(collect(tEnv, "SELECT * FROM mydb.T2 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(1, 12), Row.of(2, 22), Row.of(3, 30)));
    }

    private void initializeSingleUserTest(TableEnvironment tEnv) throws Exception {
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG mycat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'\n"
                                + ")",
                        path));
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql("CREATE DATABASE mydb");
        tEnv.executeSql(
                "CREATE TABLE mydb.T1 (\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")");
        tEnv.executeSql("INSERT INTO mydb.T1 VALUES (1, 10), (2, 20), (3, 30)").await();
        tEnv.executeSql("CALL sys.init_file_based_privilege('root-passwd')");

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG rootcat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s',\n"
                                + "  'user' = 'root',\n"
                                + "  'password' = 'root-passwd'\n"
                                + ")",
                        path));
        tEnv.executeSql("USE CATALOG rootcat");
        tEnv.executeSql("CALL sys.create_privileged_user('test', 'test-passwd')");
        tEnv.executeSql("CALL sys.grant_privilege_to_user('test', 'SELECT', 'mydb', 'T1')");
        tEnv.executeSql("CALL sys.grant_privilege_to_user('test', 'INSERT', 'mydb', 'T1')");

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG testcat WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s',\n"
                                + "  'user' = 'test',\n"
                                + "  'password' = 'test-passwd'\n"
                                + ")",
                        path));
        tEnv.executeSql("USE CATALOG testcat");
        assertThat(collect(tEnv, "SELECT * FROM mydb.T1 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(1, 10), Row.of(2, 20), Row.of(3, 30)));
        tEnv.executeSql("INSERT INTO mydb.T1 VALUES (1, 11), (2, 21)").await();
        assertThat(collect(tEnv, "SELECT * FROM mydb.T1 ORDER BY k"))
                .isEqualTo(Arrays.asList(Row.of(1, 11), Row.of(2, 21), Row.of(3, 30)));
    }

    private List<Row> collect(TableEnvironment tEnv, String sql) throws Exception {
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
}
