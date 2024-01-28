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

package org.apache.paimon.flink.action.cdc.oracle;

import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;

import com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions;
import org.junit.jupiter.api.AfterAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/** Base test class for {@link org.apache.paimon.flink.action.Action}s related to Oracle. */
public class OracleActionITCaseBase extends CdcActionITCaseBase {

    private static final Logger LOG = LoggerFactory.getLogger(OracleActionITCaseBase.class);

    public static final String DEFAULT_DB = "oracle";

    private static final String USER = "dbzuser";
    private static final String PASSWORD = "dbz";

    private static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static final String INTER_CONTAINER_ORACLE_ALIAS = "oracle";
    public static final String ORACLE_IMAGE = "goodboy008/oracle-19.3.0-ee";
    private static OracleContainer oracle;
    public static final String ORACLE_DATABASE = "ORCLCDB";
    public static final String ORACLE_SCHEMA = "DEBEZIUM";
    public static final String CONNECTOR_USER = "dbzuser";
    public static final String CONNECTOR_PWD = "dbz";
    public static final String TEST_USER = "debezium";
    public static final String TEST_PWD = "dbz";
    public static final String TOP_SECRET = "top_secret";

    public static final OracleContainer ORACLE_CONTAINER =
            new OracleContainer(
                            DockerImageName.parse("goodboy008/oracle-19.3.0-ee").withTag("non-cdb"))
                    .withUsername(USER)
                    .withPassword(PASSWORD)
                    .withDatabaseName(ORACLE_DATABASE)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected static void start() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        ORACLE_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    protected Statement getStatement(String databaseName) throws SQLException {
        Connection conn =
                DriverManager.getConnection(ORACLE_CONTAINER.getJdbcUrl(), TEST_USER, TEST_PWD);
        return conn.createStatement();
    }

    protected Statement getStatementDBA() throws SQLException {
        Connection conn =
                DriverManager.getConnection(ORACLE_CONTAINER.getJdbcUrl(), "sys as sysdba", "top_secret");
        return conn.createStatement();
    }

    protected Map<String, String> getBasicOracleConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(OracleSourceOptions.HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        config.put(
                OracleSourceOptions.PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
//        config.put(OracleSourceOptions.USERNAME.key(), TEST_USER);
//        config.put(OracleSourceOptions.PASSWORD.key(), TEST_PWD);
        config.put(OracleSourceOptions.USERNAME.key(), ORACLE_CONTAINER.getUsername());
        config.put(OracleSourceOptions.PASSWORD.key(), ORACLE_CONTAINER.getPassword());
        config.put(OracleSourceOptions.DATABASE_NAME.key(), ORACLE_CONTAINER.getDatabaseName());
        return config;
    }

    protected OracleActionITCaseBase.OracleSyncTableActionBuilder syncTableActionBuilder(
            Map<String, String> oracleConfig) {
        return new OracleActionITCaseBase.OracleSyncTableActionBuilder(oracleConfig);
    }

    /** Builder to build {@link OracleSyncTableAction} from action arguments. */
    protected class OracleSyncTableActionBuilder
            extends SyncTableActionBuilder<OracleSyncTableAction> {

        public OracleSyncTableActionBuilder(Map<String, String> oracleConfig) {
            super(OracleSyncTableAction.class, oracleConfig);
        }
    }
}
