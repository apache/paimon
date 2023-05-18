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

package org.apache.paimon.tests.cdc;

import org.apache.paimon.flink.action.cdc.mysql.MySqlContainer;
import org.apache.paimon.flink.action.cdc.mysql.MySqlVersion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.sql.Connection;
import java.sql.Statement;

/** E2e test for MySql CDC with Hive catalog aiming to test whether ignore case is effective. */
public class MySqlIgnoreCaseE2EeTest extends MySqlCdcE2eTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlIgnoreCaseE2EeTest.class);

    private String catalogDdl;
    private String useCatalogCmd;
    private String useDatabaseCmd;

    protected MySqlIgnoreCaseE2EeTest() {
        super(MySqlVersion.V5_7, true);
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        setupHiveConnector();

        catalogDdl =
                String.format(
                        "CREATE CATALOG ts_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'warehouse' = '%s',\n"
                                + "    'metastore' = 'hive',\n"
                                + "    'uri' = 'thrift://hive-metastore:9083'"
                                + ");",
                        warehousePath);
        useCatalogCmd = "USE CATALOG ts_catalog;";
        useDatabaseCmd = "USE test_db;";
    }

    @Disabled("one test is enough")
    @Test
    public void testSyncTable() {}

    @Test
    public void testSyncDatabase() throws Exception {
        String runActionCommand =
                String.join(
                        " ",
                        "bin/flink",
                        "run",
                        "-D",
                        "execution.checkpointing.interval=1s",
                        "--detached",
                        "lib/paimon-flink-action.jar",
                        "mysql-sync-database",
                        "--warehouse",
                        warehousePath,
                        "--database",
                        "test_db",
                        "--mysql-conf",
                        "hostname=mysql-1",
                        "--mysql-conf",
                        String.format("port=%d", MySqlContainer.MYSQL_PORT),
                        "--mysql-conf",
                        String.format("username='%s'", mySqlContainer.getUsername()),
                        "--mysql-conf",
                        String.format("password='%s'", mySqlContainer.getPassword()),
                        "--mysql-conf",
                        "database-name='paimon_ignore_CASE'",
                        "--catalog-conf",
                        "metastore=hive",
                        "--catalog-conf",
                        "uri=thrift://hive-metastore:9083",
                        "--table-conf",
                        "bucket=2");
        Container.ExecResult execResult =
                jobManager.execInContainer("su", "flink", "-c", runActionCommand);
        if (execResult.getExitCode() != 0) {
            LOG.info(execResult.getStdout());
            LOG.info(execResult.getStderr());
        }

        checkTableSchema();

        // check sync schema changes and records
        try (Connection conn = getMySqlConnection();
                Statement statement = conn.createStatement()) {
            checkSyncDatabase(statement);
        }
    }

    private void checkTableSchema() throws Exception {
        String insert = "INSERT INTO result1 SELECT fields FROM t\\$schemas;";

        String jobId =
                runSql(
                        insert,
                        catalogDdl,
                        useCatalogCmd,
                        useDatabaseCmd,
                        createResultSink("result1", "fields STRING"));

        checkResult(
                "[{\"id\":0,\"name\":\"k\",\"type\":\"INT NOT NULL\",\"description\":\"\"},"
                        + "{\"id\":1,\"name\":\"uppercase_v0\",\"type\":\"VARCHAR(20)\",\"description\":\"\"}]");
        clearCurrentResults();
        cancelJob(jobId);
    }

    private void checkSyncDatabase(Statement statement) throws Exception {
        statement.executeUpdate("USE paimon_ignore_CASE");
        statement.executeUpdate("INSERT INTO T VALUES (1, 'Hi')");

        statement.executeUpdate("ALTER TABLE T MODIFY COLUMN UPPERCASE_V0 VARCHAR(30)");
        statement.executeUpdate("INSERT INTO T VALUES (2, 'Paimon')");

        statement.executeUpdate("ALTER TABLE T ADD COLUMN UPPERCASE_V1 DOUBLE");
        statement.executeUpdate("INSERT INTO T VALUES (3, 'TEST', 0.5)");

        String jobId =
                runSql(
                        "INSERT INTO result2 SELECT k, uppercase_v0, uppercase_v1 FROM t;",
                        catalogDdl,
                        useCatalogCmd,
                        useDatabaseCmd,
                        createResultSink("result2", "k INT, v0 VARCHAR(30), v1 DOUBLE"));
        checkResult("1, Hi, null", "2, Paimon, null", "3, TEST, 0.5");
        clearCurrentResults();
        cancelJob(jobId);
    }
}
