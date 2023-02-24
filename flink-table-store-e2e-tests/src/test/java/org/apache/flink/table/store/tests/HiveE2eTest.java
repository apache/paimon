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
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import java.util.UUID;

/**
 * Tests for reading table store from Hive.
 *
 * <p>NOTE: This test runs a complete Hadoop cluster in Docker, which requires a lot of memory. If
 * you're running this test locally, make sure that the memory limit of your Docker is at least 8GB.
 */
@DisabledIfSystemProperty(named = "test.flink.version", matches = "1.14.*")
public class HiveE2eTest extends E2eReaderTestBase {

    private static final String TABLE_STORE_HIVE_CONNECTOR_JAR_NAME =
            "flink-table-store-hive-connector.jar";

    public HiveE2eTest() {
        super(false, true, false);
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
        final String table = "table_store_pk";
        String tableStorePkPath = HDFS_ROOT + "/" + UUID.randomUUID() + ".store";
        String tableStorePkDdl =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  PRIMARY KEY (a, b) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'bucket' = '2'\n"
                                + ");",
                        table);
        runSql(
                createInsertSql(table),
                createCatalogSql("table_store", tableStorePkPath),
                tableStorePkDdl);

        String externalTablePkDdl =
                String.format(
                        "CREATE EXTERNAL TABLE IF NOT EXISTS %s\n"
                                + "STORED BY 'org.apache.flink.table.store.hive.TableStoreHiveStorageHandler'\n"
                                + "LOCATION '%s/default.db/%s';\n",
                        table, tableStorePkPath, table);

        checkQueryResults(table, this::executeQuery, externalTablePkDdl);
    }

    @Test
    public void testFlinkWriteAndHiveRead() throws Exception {
        final String warehouse = HDFS_ROOT + "/" + UUID.randomUUID() + ".warehouse";
        final String table = "t";
        runSql(
                String.join(
                        "\n",
                        createCatalogSql(
                                "my_hive",
                                warehouse,
                                "'metastore' = 'hive'",
                                "'uri' = 'thrift://hive-metastore:9083'"),
                        createTableSql(table),
                        createInsertSql(table)));
        checkQueryResults(table, this::executeQuery);
    }

    private String executeQuery(String sql) throws Exception {
        Container.ExecResult execResult =
                getHive()
                        .execInContainer(
                                "/opt/hive/bin/hive",
                                "--hiveconf",
                                "hive.root.logger=INFO,console",
                                "-f",
                                TEST_DATA_DIR + "/" + sql);
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when running hive sql.");
        }
        return execResult.getStdout();
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
