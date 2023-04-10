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
import org.apache.paimon.tests.E2eTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.UUID;
import java.util.stream.Stream;

/** E2e tests for {@link org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableAction}. */
public abstract class MySqlCdcE2eTestBase extends E2eTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCdcE2eTestBase.class);

    private static final String USER = "paimonuser";
    private static final String PASSWORD = "paimonpw";
    private static final String DATABASE_NAME = "paimon_test";

    private final MySqlVersion mySqlVersion;
    private MySqlContainer mySqlContainer;

    private String warehousePath;
    private String catalogDdl;
    private String useCatalogCmd;

    protected MySqlCdcE2eTestBase(MySqlVersion mySqlVersion) {
        this.mySqlVersion = mySqlVersion;
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        mySqlContainer = createMySqlContainer(mySqlVersion);
        Startables.deepStart(Stream.of(mySqlContainer)).join();

        warehousePath = TEST_DATA_DIR + "/" + UUID.randomUUID() + ".store";
        catalogDdl =
                String.format(
                        "CREATE CATALOG ts_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'warehouse' = '%s'\n"
                                + ");",
                        warehousePath);

        useCatalogCmd = "USE CATALOG ts_catalog;";
    }

    private MySqlContainer createMySqlContainer(MySqlVersion version) {
        return (MySqlContainer)
                new MySqlContainer(version)
                        .withConfigurationOverride("mysql/my.cnf")
                        .withSetupSQL("mysql/setup.sql")
                        .withUsername(USER)
                        .withPassword(PASSWORD)
                        .withDatabaseName(DATABASE_NAME)
                        .withLogConsumer(new Slf4jLogConsumer(LOG))
                        // connect with docker-compose.yaml
                        .withNetwork(network)
                        .withNetworkAliases("mysql-1");
    }

    @AfterEach
    public void after() {
        mySqlContainer.stop();
        super.after();
    }

    @Test
    public void testSyncTable() throws Exception {
        String runActionCommand =
                String.join(
                        " ",
                        "bin/flink",
                        "run",
                        "-c",
                        "org.apache.paimon.flink.action.FlinkActions",
                        "-D",
                        "execution.checkpointing.interval=1s",
                        "--detached",
                        "lib/paimon-flink.jar",
                        "mysql-sync-table",
                        "--warehouse",
                        warehousePath,
                        "--database",
                        "default",
                        "--table",
                        "ts_table",
                        "--partition-keys",
                        "pt",
                        "--primary-keys",
                        "pt,_id",
                        "--sink-parallelism",
                        "2",
                        "--mysql-conf",
                        "hostname=mysql-1",
                        "--mysql-conf",
                        String.format("port=%d", MySqlContainer.MYSQL_PORT),
                        "--mysql-conf",
                        String.format("username='%s'", mySqlContainer.getUsername()),
                        "--mysql-conf",
                        String.format("password='%s'", mySqlContainer.getPassword()),
                        "--mysql-conf",
                        String.format("database-name='%s'", DATABASE_NAME),
                        "--mysql-conf",
                        "table-name='schema_evolution_.+'",
                        "--paimon-conf",
                        "bucket=2");
        Container.ExecResult execResult =
                jobManager.execInContainer("su", "flink", "-c", runActionCommand);
        LOG.info(execResult.getStdout());
        LOG.info(execResult.getStderr());

        try (Connection conn =
                DriverManager.getConnection(
                        String.format(
                                "jdbc:mysql://%s:%s/",
                                mySqlContainer.getHost(), mySqlContainer.getDatabasePort()),
                        mySqlContainer.getUsername(),
                        mySqlContainer.getPassword())) {
            try (Statement statement = conn.createStatement()) {
                testSyncTableImpl(statement);
            }
        }
    }

    private void testSyncTableImpl(Statement statement) throws Exception {
        statement.executeUpdate("USE paimon_test");

        statement.executeUpdate("INSERT INTO schema_evolution_1 VALUES (1, 1, 'one')");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_2 VALUES (1, 2, 'two'), (2, 4, 'four')");

        String jobId =
                runSql(
                        "INSERT INTO result1 SELECT * FROM ts_table;",
                        catalogDdl,
                        useCatalogCmd,
                        "",
                        createResultSink("result1", "pt INT, _id INT, v1 VARCHAR(10)"));
        checkResult("1, 1, one", "1, 2, two", "2, 4, four");
        clearCurrentResults();
        Container.ExecResult execResult = jobManager.execInContainer("bin/flink", "cancel", jobId);
        LOG.info(execResult.getStdout());
        LOG.info(execResult.getStderr());

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v2 INT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (2, 3, 'three', 30), (1, 5, 'five', 50)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v2 INT");
        statement.executeUpdate("INSERT INTO schema_evolution_2 VALUES (1, 6, 'six', 60)");

        jobId =
                runSql(
                        "INSERT INTO result2 SELECT * FROM ts_table;",
                        catalogDdl,
                        useCatalogCmd,
                        "",
                        createResultSink("result2", "pt INT, _id INT, v1 VARCHAR(10), v2 INT"));
        checkResult(
                "1, 1, one, null",
                "1, 2, two, null",
                "2, 3, three, 30",
                "2, 4, four, null",
                "1, 5, five, 50",
                "1, 6, six, 60");
        clearCurrentResults();
        jobManager.execInContainer("bin/flink", "cancel", jobId);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 MODIFY COLUMN v2 BIGINT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (2, 7, 'seven', 70000000000)");
        statement.executeUpdate("UPDATE schema_evolution_1 SET v2 = 30000000000 WHERE _id = 3");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 MODIFY COLUMN v2 BIGINT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_2 VALUES (2, 8, 'eight', 80000000000)");

        jobId =
                runSql(
                        "INSERT INTO result3 SELECT * FROM ts_table;",
                        catalogDdl,
                        useCatalogCmd,
                        "",
                        createResultSink("result3", "pt INT, _id INT, v1 VARCHAR(10), v2 BIGINT"));
        checkResult(
                "1, 1, one, null",
                "1, 2, two, null",
                "2, 3, three, 30000000000",
                "2, 4, four, null",
                "1, 5, five, 50",
                "1, 6, six, 60",
                "2, 7, seven, 70000000000",
                "2, 8, eight, 80000000000");
        clearCurrentResults();
        jobManager.execInContainer("bin/flink", "cancel", jobId);
    }

    private String runSql(String sql, String... ddls) throws Exception {
        return runSql(String.join("\n", ddls) + "\n" + sql);
    }
}
