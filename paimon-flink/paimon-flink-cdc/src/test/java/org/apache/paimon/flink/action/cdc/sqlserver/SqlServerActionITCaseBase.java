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

package org.apache.paimon.flink.action.cdc.sqlserver;

import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;

import org.junit.jupiter.api.AfterAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Base test class for {@link org.apache.paimon.flink.action.Action}s related to SqlServer. */
public class SqlServerActionITCaseBase extends CdcActionITCaseBase {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerActionITCaseBase.class);
    public static final MSSQLServerContainer MSSQL_SERVER_CONTAINER = createSqlServerContainer();
    private static final String PASSWORD = "Password!";

    public static final String ENABLE_TABLE_CDC_WITH_CUSTOM_CAPTURE =
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'%s', @capture_instance = N'%s', @role_name = NULL, @supports_net_changes = 0, @captured_column_list = %s";
    public static final String DISABLE_TABLE_CDC =
            "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'#', @capture_instance = 'all'";
    private static final String STATEMENTS_PLACEHOLDER = "#";

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        MSSQL_SERVER_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    private static MSSQLServerContainer createSqlServerContainer() {
        return (MSSQLServerContainer)
                new MSSQLServerContainerExtend("mcr.microsoft.com/mssql/server:2019-latest")
                        .withPassword(PASSWORD)
                        .withEnv("MSSQL_AGENT_ENABLED", "true")
                        .withEnv("MSSQL_PID", "Standard")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    protected static void start() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MSSQL_SERVER_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    protected static Statement getStatement() throws SQLException {
        Connection conn =
                DriverManager.getConnection(
                        MSSQL_SERVER_CONTAINER.getJdbcUrl(),
                        MSSQL_SERVER_CONTAINER.getUsername(),
                        MSSQL_SERVER_CONTAINER.getPassword());
        return conn.createStatement();
    }

    protected Map<String, String> getBasicSqlServerConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("hostname", MSSQL_SERVER_CONTAINER.getHost());
        config.put("username", MSSQL_SERVER_CONTAINER.getUsername());
        config.put("password", MSSQL_SERVER_CONTAINER.getPassword());
        config.put(SqlServerSourceOptions.SCHEMA_NAME.key(), "dbo");
        config.put("port", this.getPort().toString());
        return config;
    }

    protected SqlServerSyncTableActionBuilder syncTableActionBuilder(
            Map<String, String> sqlServerConfig) {
        return new SqlServerSyncTableActionBuilder(sqlServerConfig);
    }

    protected SqlServerSyncDatabaseActionBuilder syncDatabaseActionBuilder(
            Map<String, String> sqlServerConfig) {
        return new SqlServerSyncDatabaseActionBuilder(sqlServerConfig);
    }

    /** Builder to build {@link SqlServerSyncTableAction} from action arguments. */
    protected class SqlServerSyncTableActionBuilder
            extends SyncTableActionBuilder<SqlServerSyncTableAction> {

        public SqlServerSyncTableActionBuilder(Map<String, String> sqlServerConfig) {
            super(SqlServerSyncTableAction.class, sqlServerConfig);
        }
    }

    public Integer getPort() {
        return MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT);
    }

    /** Builder to build {@link SqlServerSyncDatabaseAction} from action arguments. */
    protected class SqlServerSyncDatabaseActionBuilder
            extends SyncDatabaseActionBuilder<SqlServerSyncDatabaseAction> {

        public SqlServerSyncDatabaseActionBuilder(Map<String, String> sqlServerConfig) {
            super(SqlServerSyncDatabaseAction.class, sqlServerConfig);
        }
    }

    public static void enableTableCdc(Statement connection, String tableName, String captureName)
            throws SQLException {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(captureName);
        String enableCdcForTableStmt =
                String.format(ENABLE_TABLE_CDC_WITH_CUSTOM_CAPTURE, tableName, captureName, "NULL");
        connection.execute(enableCdcForTableStmt);
    }

    /**
     * Disables CDC for a table for which it was enabled before.
     *
     * @param name the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void disableTableCdc(Statement connection, String name) throws SQLException {
        Objects.requireNonNull(name);
        String disableCdcForTableStmt = DISABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, name);
        connection.execute(disableCdcForTableStmt);
    }
}
