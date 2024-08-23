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

package org.apache.paimon.flink.action.cdc.postgres;

import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;

import org.apache.flink.cdc.connectors.postgres.source.PostgresConnectionPoolFactory;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceOptions;
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
import java.util.Random;
import java.util.stream.Stream;

/** Base test class for {@link org.apache.paimon.flink.action.Action}s related to PostgreSQL. */
public class PostgresActionITCaseBase extends CdcActionITCaseBase {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresActionITCaseBase.class);

    public static final String DEFAULT_DB = "postgres";

    private static final String USER = "paimonuser";
    private static final String PASSWORD = "paimonpw";

    // use newer version of postgresql image to support pgoutput plugin
    // when testing postgres 13, only 13-alpine supports both amd64 and arm64
    protected static final DockerImageName PG_IMAGE =
            DockerImageName.parse("postgres:13").asCompatibleSubstituteFor("postgres");

    protected static final PostgresContainer POSTGRES_CONTAINER =
            new PostgresContainer(PG_IMAGE)
                    .withDatabaseName(DEFAULT_DB)
                    .withUsername(USER)
                    .withPassword(PASSWORD)
                    .withEnv("TZ", "America/Los_Angeles")
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "wal_level=logical",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "max_wal_senders=20");

    protected static void start() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(POSTGRES_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        POSTGRES_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    protected Statement getStatement(String databaseName) throws SQLException {
        String jdbcUrl =
                String.format(
                        PostgresConnectionPoolFactory.JDBC_URL_PATTERN,
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getDatabasePort(),
                        databaseName);
        Connection conn =
                DriverManager.getConnection(
                        jdbcUrl,
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword());
        return conn.createStatement();
    }

    protected Map<String, String> getBasicPostgresConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(PostgresSourceOptions.HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        config.put(
                PostgresSourceOptions.PG_PORT.key(),
                String.valueOf(POSTGRES_CONTAINER.getDatabasePort()));
        config.put(PostgresSourceOptions.USERNAME.key(), USER);
        config.put(PostgresSourceOptions.PASSWORD.key(), PASSWORD);
        config.put(PostgresSourceOptions.SLOT_NAME.key(), getSlotName());
        config.put(PostgresSourceOptions.DECODING_PLUGIN_NAME.key(), "pgoutput");
        return config;
    }

    protected String getSlotName() {
        final Random random = new Random();
        int id = random.nextInt(10000);
        return "paimon_" + id;
    }

    protected PostgresSyncTableActionBuilder syncTableActionBuilder(
            Map<String, String> postgresConfig) {
        return new PostgresSyncTableActionBuilder(postgresConfig);
    }

    /** Builder to build {@link PostgresSyncTableAction} from action arguments. */
    protected class PostgresSyncTableActionBuilder
            extends SyncTableActionBuilder<PostgresSyncTableAction> {

        public PostgresSyncTableActionBuilder(Map<String, String> postgresConfig) {
            super(PostgresSyncTableAction.class, postgresConfig);
        }
    }
}
