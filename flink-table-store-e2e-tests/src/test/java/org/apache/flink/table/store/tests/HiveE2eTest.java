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

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import java.util.UUID;

/**
 * Tests for reading table store from Hive.
 *
 * <p>NOTE: This test runs a complete Hadoop cluster in Docker, which requires a lot of memory. If
 * you're running this test locally, make sure that the memory limit of your Docker is at least 8GB.
 */
public class HiveE2eTest extends E2eTestBase {

    private static final String ADD_JAR_HQL =
            "ADD JAR " + TEST_DATA_DIR + "/" + TABLE_STORE_HIVE_JAR_NAME + ";";

    public HiveE2eTest() {
        super(false, true);
    }

    @Test
    public void testReadExternalTable() throws Exception {
        // TODO write data directly to HDFS after FLINK-27562 is solved
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
        String tableStorePkPath = TEST_DATA_DIR + "/" + UUID.randomUUID().toString() + ".store";
        tableStorePkDdl = String.format(tableStorePkDdl, tableStorePkPath);
        runSql(
                "INSERT INTO table_store_pk VALUES "
                        + "(1, 10, 'Hi'), "
                        + "(2, 20, 'Hello'), "
                        + "(3, 30, 'Table'), "
                        + "(4, 40, 'Store');",
                tableStorePkDdl);

        String externalTablePkDdl =
                "CREATE EXTERNAL TABLE IF NOT EXISTS table_store_pk\n"
                        + "STORED BY 'org.apache.flink.table.store.hive.TableStoreHiveStorageHandler'\n"
                        + "LOCATION '"
                        // hive cannot read from local path
                        + HDFS_ROOT
                        + tableStorePkPath
                        + "/default_catalog.catalog/default_database.db/table_store_pk';";
        writeTestData(
                "pk.hql",
                // same default database name as Flink
                ADD_JAR_HQL
                        + "\n"
                        + externalTablePkDdl
                        + "\n"
                        + "SELECT b, a, c FROM table_store_pk ORDER BY b;");

        ContainerState hive = getHive();
        hive.execInContainer("hdfs", "dfs", "-mkdir", "-p", HDFS_ROOT + TEST_DATA_DIR);
        hive.execInContainer(
                "hdfs", "dfs", "-copyFromLocal", tableStorePkPath, HDFS_ROOT + tableStorePkPath);
        Container.ExecResult execResult =
                hive.execInContainer(
                        "/opt/hive/bin/hive",
                        "--hiveconf",
                        "hive.root.logger=INFO,console",
                        "-f",
                        TEST_DATA_DIR + "/pk.hql");
        System.out.println(execResult.getStdout());
        // System.out.println(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when running hive sql.");
        }
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
