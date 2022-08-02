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

package org.apache.flink.table.store.tests;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for reading table store from Hive.
 *
 * <p>NOTE: This test runs a complete Hadoop cluster in Docker, which requires a lot of memory. If
 * you're running this test locally, make sure that the memory limit of your Docker is at least 8GB.
 */
public class HiveE2eTest extends E2eTestBase {

    private static final String TABLE_STORE_HIVE_CONNECTOR_JAR_NAME =
            "flink-table-store-hive-connector.jar";

    public HiveE2eTest() {
        super(false, true);
    }

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        getHive()
                .execInContainer(
                        "/bin/bash",
                        "-c",
                        "mkdir /opt/hive/auxlib && cp /jars/"
                                + TABLE_STORE_HIVE_CONNECTOR_JAR_NAME
                                + " /opt/hive/auxlib");
    }

    @Test
    public void testReadExternalTable() throws Exception {
        String tableStorePkDdl =
                "CREATE TABLE IF NOT EXISTS table_store_pk (\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string,\n"
                        + "  PRIMARY KEY (a, b) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'bucket' = '2',\n"
                        + "  'root-path' = '%s'\n"
                        + ");";
        String tableStorePkPath = HDFS_ROOT + "/" + UUID.randomUUID().toString() + ".store";
        tableStorePkDdl = String.format(tableStorePkDdl, tableStorePkPath);
        runSql(
                "INSERT INTO table_store_pk VALUES "
                        + "(1, 10, 'Hi'), "
                        + "(1, 100, 'Hi Again'), "
                        + "(2, 20, 'Hello'), "
                        + "(3, 30, 'Table'), "
                        + "(4, 40, 'Store');",
                tableStorePkDdl);

        String externalTablePkDdl =
                "CREATE EXTERNAL TABLE IF NOT EXISTS table_store_pk\n"
                        + "STORED BY 'org.apache.flink.table.store.hive.TableStoreHiveStorageHandler'\n"
                        + "LOCATION '"
                        + tableStorePkPath
                        + "/default_catalog.catalog/default_database.db/table_store_pk';\n";

        checkQueryResult(
                externalTablePkDdl + "SELECT * FROM table_store_pk ORDER BY b;",
                "1\t10\tHi\n"
                        + "2\t20\tHello\n"
                        + "3\t30\tTable\n"
                        + "4\t40\tStore\n"
                        + "1\t100\tHi Again\n");
        checkQueryResult(
                externalTablePkDdl + "SELECT b, a FROM table_store_pk ORDER BY b;",
                "10\t1\n" + "20\t2\n" + "30\t3\n" + "40\t4\n" + "100\t1\n");
        checkQueryResult(
                externalTablePkDdl + "SELECT * FROM table_store_pk WHERE a > 1 ORDER BY b;",
                "2\t20\tHello\n" + "3\t30\tTable\n" + "4\t40\tStore\n");
        checkQueryResult(
                externalTablePkDdl
                        + "SELECT a, SUM(b), MIN(c) FROM table_store_pk GROUP BY a ORDER BY a;",
                "1\t110\tHi\n" + "2\t20\tHello\n" + "3\t30\tTable\n" + "4\t40\tStore\n");
        checkQueryResult(
                externalTablePkDdl
                        + "SELECT T1.a, T1.b, T2.b FROM table_store_pk T1 JOIN table_store_pk T2 "
                        + "ON T1.a = T2.a WHERE T1.a <= 2 ORDER BY T1.a, T1.b, T2.b;",
                "1\t10\t10\n" + "1\t10\t100\n" + "1\t100\t10\n" + "1\t100\t100\n" + "2\t20\t20\n");
    }

    @Test
    public void testFlinkWriteAndHiveRead() throws Exception {
        String sql =
                String.join(
                        "\n",
                        "CREATE CATALOG my_hive WITH (",
                        "  'type' = 'table-store',",
                        "  'metastore' = 'hive',",
                        "  'uri' = 'thrift://hive-metastore:9083',",
                        "  'warehouse' = '"
                                + HDFS_ROOT
                                + "/"
                                + UUID.randomUUID().toString()
                                + ".warehouse'",
                        ");",
                        "",
                        "USE CATALOG my_hive;",
                        "",
                        "CREATE TABLE T (",
                        "  a int,",
                        "  b bigint,",
                        "  c string",
                        ") WITH (",
                        "  'bucket' = '2'",
                        ");",
                        "",
                        "INSERT INTO T VALUES "
                                + "(1, 10, 'Hi'), "
                                + "(1, 100, 'Hi Again'), "
                                + "(2, 20, 'Hello'), "
                                + "(3, 30, 'Table'), "
                                + "(4, 40, 'Store');");
        runSql(sql);

        checkQueryResult(
                "SELECT * FROM t ORDER BY b;",
                "1\t10\tHi\n"
                        + "2\t20\tHello\n"
                        + "3\t30\tTable\n"
                        + "4\t40\tStore\n"
                        + "1\t100\tHi Again\n");
        checkQueryResult(
                "SELECT b, a FROM t ORDER BY b;",
                "10\t1\n" + "20\t2\n" + "30\t3\n" + "40\t4\n" + "100\t1\n");
        checkQueryResult(
                "SELECT * FROM t WHERE a > 1 ORDER BY b;",
                "2\t20\tHello\n" + "3\t30\tTable\n" + "4\t40\tStore\n");
        checkQueryResult(
                "SELECT a, SUM(b), MIN(c) FROM t GROUP BY a ORDER BY a;",
                "1\t110\tHi\n" + "2\t20\tHello\n" + "3\t30\tTable\n" + "4\t40\tStore\n");
        checkQueryResult(
                "SELECT T1.a, T1.b, T2.b FROM t T1 JOIN t T2 "
                        + "ON T1.a = T2.a WHERE T1.a <= 2 ORDER BY T1.a, T1.b, T2.b;",
                "1\t10\t10\n" + "1\t10\t100\n" + "1\t100\t10\n" + "1\t100\t100\n" + "2\t20\t20\n");
    }

    private void checkQueryResult(String query, String expected) throws Exception {
        writeSharedFile("pk.hql", query);
        Container.ExecResult execResult =
                getHive()
                        .execInContainer(
                                "/opt/hive/bin/hive",
                                "--hiveconf",
                                "hive.root.logger=INFO,console",
                                "-f",
                                TEST_DATA_DIR + "/pk.hql");
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when running hive sql.");
        }
        assertThat(execResult.getStdout()).isEqualTo(expected);
    }

    private ContainerState getHive() {
        return environment.getContainerByServiceName("hive-server_1").get();
    }

    private void runSql(String sql, String... ddls) throws Exception {
        runSql(
                "SET 'execution.runtime-mode' = 'batch';\n"
                        + "SET 'table.dml-sync' = 'true';\n"
                        + String.join("\n", ddls)
                        + "\n"
                        + sql);
    }
}
