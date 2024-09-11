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
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * E2e tests for {@link org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableAction} and {@link
 * org.apache.paimon.flink.action.cdc.mysql.MySqlSyncDatabaseAction}.
 */
public abstract class MySqlCdcE2eTestBase extends E2eTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCdcE2eTestBase.class);

    private static final String USER = "paimonuser";
    private static final String PASSWORD = "paimonpw";

    protected static final String ACTION_SYNC_TABLE = "mysql-sync-table";

    protected static final String ACTION_SYNC_DATABASE = "mysql-sync-database";

    private final MySqlVersion mySqlVersion;
    protected MySqlContainer mySqlContainer;

    protected String warehousePath;
    protected String catalogDdl;
    protected String useCatalogCmd;

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
        runAction(
                ACTION_SYNC_TABLE,
                "pt",
                "pt,_id",
                null,
                ImmutableMap.of(),
                ImmutableMap.of(
                        "database-name", "paimon_sync_table", "table-name", "schema_evolution_.+"),
                ImmutableMap.of("bucket", "2"));

        try (Connection conn = getMySqlConnection();
                Statement statement = conn.createStatement()) {
            testSyncTableImpl(statement);
        }
    }

    private void testSyncTableImpl(Statement statement) throws Exception {
        statement.executeUpdate("USE paimon_sync_table");

        statement.executeUpdate("INSERT INTO schema_evolution_1 VALUES (1, 1, 'one')");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_2 VALUES (1, 2, 'two'), (2, 4, 'four')");

        String jobId =
                runBatchSql(
                        "INSERT INTO result1 SELECT * FROM ts_table;",
                        catalogDdl,
                        useCatalogCmd,
                        "",
                        createResultSink("result1", "pt INT, _id INT, v1 VARCHAR(10)"));
        checkResult("1, 1, one", "1, 2, two", "2, 4, four");
        clearCurrentResults();
        cancelJob(jobId);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v2 INT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (2, 3, 'three', 30), (1, 5, 'five', 50)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v2 INT");
        statement.executeUpdate("INSERT INTO schema_evolution_2 VALUES (1, 6, 'six', 60)");

        jobId =
                runBatchSql(
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
        cancelJob(jobId);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 MODIFY COLUMN v2 BIGINT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (2, 7, 'seven', 70000000000)");
        statement.executeUpdate("UPDATE schema_evolution_1 SET v2 = 30000000000 WHERE _id = 3");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 MODIFY COLUMN v2 BIGINT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_2 VALUES (2, 8, 'eight', 80000000000)");

        jobId =
                runBatchSql(
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
        cancelJob(jobId);
    }

    @Test
    public void testSyncDatabase() throws Exception {
        runAction(
                ACTION_SYNC_DATABASE,
                null,
                null,
                null,
                ImmutableMap.of(),
                ImmutableMap.of("database-name", "paimon_sync_database"),
                ImmutableMap.of("bucket", "2"));

        try (Connection conn = getMySqlConnection();
                Statement statement = conn.createStatement()) {
            testSyncDatabaseImpl(statement);
        }
    }

    private void testSyncDatabaseImpl(Statement statement) throws Exception {
        statement.executeUpdate("USE paimon_sync_database");

        statement.executeUpdate("INSERT INTO t1 VALUES (1, 10)");
        statement.executeUpdate("INSERT INTO t2 VALUES (2, 'two', 20)");

        String jobId =
                runBatchSql(
                        "INSERT INTO result1 SELECT * FROM t1;",
                        catalogDdl,
                        useCatalogCmd,
                        "",
                        createResultSink("result1", "k INT, v INT"));
        checkResult("1, 10");
        clearCurrentResults();
        cancelJob(jobId);

        jobId =
                runBatchSql(
                        "INSERT INTO result2 SELECT * FROM t2;",
                        catalogDdl,
                        useCatalogCmd,
                        "",
                        createResultSink("result2", "k1 INT, k2 VARCHAR(10), v1 INT"));
        checkResult("2, two, 20");
        clearCurrentResults();
        cancelJob(jobId);

        statement.executeUpdate("ALTER TABLE t1 MODIFY COLUMN v BIGINT");
        statement.executeUpdate("INSERT INTO t1 VALUES (3, 3000000000000)");
        statement.executeUpdate("ALTER TABLE t2 ADD COLUMN v2 DOUBLE");
        statement.executeUpdate("INSERT INTO t2 VALUES (4, 'four', 40, 40.5)");

        jobId =
                runBatchSql(
                        "INSERT INTO result3 SELECT * FROM t1;",
                        catalogDdl,
                        useCatalogCmd,
                        "",
                        createResultSink("result3", "k INT, v BIGINT"));
        checkResult("1, 10", "3, 3000000000000");
        clearCurrentResults();
        cancelJob(jobId);

        jobId =
                runBatchSql(
                        "INSERT INTO result4 SELECT * FROM t2;",
                        catalogDdl,
                        useCatalogCmd,
                        "",
                        createResultSink("result4", "k1 INT, k2 VARCHAR(10), v1 INT, v2 DOUBLE"));
        checkResult("2, two, 20, null", "4, four, 40, 40.5");
        clearCurrentResults();
        cancelJob(jobId);
    }

    protected Connection getMySqlConnection() throws Exception {
        return DriverManager.getConnection(
                String.format(
                        "jdbc:mysql://%s:%s/",
                        mySqlContainer.getHost(), mySqlContainer.getDatabasePort()),
                mySqlContainer.getUsername(),
                mySqlContainer.getPassword());
    }

    protected void cancelJob(String jobId) throws Exception {
        jobManager.execInContainer("bin/flink", "cancel", jobId);
    }

    protected void runAction(
            String action,
            @Nullable String partitionKeys,
            @Nullable String primaryKeys,
            @Nullable String typeMappingOptions,
            Map<String, String> computedColumn,
            Map<String, String> mysqlConf,
            Map<String, String> tableConf)
            throws Exception {

        String partitionKeysStr =
                StringUtils.isNullOrWhitespaceOnly(partitionKeys)
                        ? ""
                        : "--partition-keys " + partitionKeys;
        String primaryKeysStr =
                StringUtils.isNullOrWhitespaceOnly(primaryKeys)
                        ? ""
                        : "--primary-keys " + primaryKeys;
        String typeMappingStr =
                StringUtils.isNullOrWhitespaceOnly(typeMappingOptions)
                        ? ""
                        : "--type-mapping " + typeMappingOptions;
        String tableStr = action.equals(ACTION_SYNC_TABLE) ? "--table ts_table" : "";

        List<String> computedColumns =
                computedColumn.keySet().stream()
                        .map(key -> String.format("%s=%s", key, computedColumn.get(key)))
                        .flatMap(s -> Stream.of("--computed-column", s))
                        .collect(Collectors.toList());

        List<String> mysqlConfs =
                mysqlConf.keySet().stream()
                        .map(key -> String.format("%s=%s", key, mysqlConf.get(key)))
                        .flatMap(s -> Stream.of("--mysql-conf", s))
                        .collect(Collectors.toList());

        List<String> tableConfs =
                tableConf.keySet().stream()
                        .map(key -> String.format("%s=%s", key, tableConf.get(key)))
                        .flatMap(s -> Stream.of("--table-conf", s))
                        .collect(Collectors.toList());

        String runActionCommand =
                String.join(
                        " ",
                        "bin/flink",
                        "run",
                        "-D",
                        "execution.checkpointing.interval=1s",
                        "--detached",
                        "lib/paimon-flink-action.jar",
                        action,
                        "--warehouse",
                        warehousePath,
                        "--database",
                        "default",
                        tableStr,
                        partitionKeysStr,
                        primaryKeysStr,
                        typeMappingStr,
                        "--mysql-conf",
                        "hostname=mysql-1",
                        "--mysql-conf",
                        String.format("port=%d", MySqlContainer.MYSQL_PORT),
                        "--mysql-conf",
                        String.format("username='%s'", mySqlContainer.getUsername()),
                        "--mysql-conf",
                        String.format("password='%s'", mySqlContainer.getPassword()));

        runActionCommand +=
                " "
                        + String.join(" ", computedColumns)
                        + " "
                        + String.join(" ", mysqlConfs)
                        + " "
                        + String.join(" ", tableConfs);

        Container.ExecResult execResult =
                jobManager.execInContainer("su", "flink", "-c", runActionCommand);
        LOG.info(execResult.getStdout());
        LOG.info(execResult.getStderr());
    }
}
