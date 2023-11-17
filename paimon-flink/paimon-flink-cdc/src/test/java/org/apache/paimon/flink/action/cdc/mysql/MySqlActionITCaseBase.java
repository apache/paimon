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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;

import org.junit.jupiter.api.AfterAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/** Base test class for {@link org.apache.paimon.flink.action.Action}s related to MySQL. */
public class MySqlActionITCaseBase extends CdcActionITCaseBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlActionITCaseBase.class);

    protected static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V5_7);
    private static final String USER = "paimonuser";
    private static final String PASSWORD = "paimonpw";

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        MYSQL_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return (MySqlContainer)
                new MySqlContainer(version)
                        .withConfigurationOverride("mysql/my.cnf")
                        .withUsername(USER)
                        .withPassword(PASSWORD)
                        .withEnv("TZ", "America/Los_Angeles")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    protected static void start() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    protected Statement getStatement() throws SQLException {
        Connection conn =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword());
        return conn.createStatement();
    }

    protected Map<String, String> getBasicMySqlConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("hostname", MYSQL_CONTAINER.getHost());
        config.put("port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        config.put("username", USER);
        config.put("password", PASSWORD);
        // see mysql/my.cnf in test resources
        config.put("server-time-zone", ZoneId.of("America/New_York").toString());
        // Configure after upgrading to version 2.4.1;
        // Refer:
        // https://github.com/ververica/flink-cdc-connectors/blob/release-2.4/flink-connector-mysql-cdc/src/main/java/com/ververica/cdc/connectors/mysql/debezium/reader/BinlogSplitReader.java#L272
        config.put("scan.newly-added-table.enabled", "false");
        return config;
    }

    protected MySqlSyncTableActionBuilder syncTableActionBuilder(Map<String, String> mySqlConfig) {
        return new MySqlSyncTableActionBuilder(mySqlConfig);
    }

    protected MySqlSyncDatabaseActionBuilder syncDatabaseActionBuilder(
            Map<String, String> mySqlConfig) {
        return new MySqlSyncDatabaseActionBuilder(mySqlConfig);
    }

    /** Builder to build {@link MySqlSyncTableAction} from action arguments. */
    protected class MySqlSyncTableActionBuilder
            extends SyncTableActionBuilder<MySqlSyncTableAction> {

        public MySqlSyncTableActionBuilder(Map<String, String> mySqlConfig) {
            super(MySqlSyncTableAction.class, mySqlConfig);
        }
    }

    /** Builder to build {@link MySqlSyncDatabaseAction} from action arguments. */
    protected class MySqlSyncDatabaseActionBuilder
            extends SyncDatabaseActionBuilder<MySqlSyncDatabaseAction> {

        public MySqlSyncDatabaseActionBuilder(Map<String, String> mySqlConfig) {
            super(MySqlSyncDatabaseAction.class, mySqlConfig);
        }
    }
}
