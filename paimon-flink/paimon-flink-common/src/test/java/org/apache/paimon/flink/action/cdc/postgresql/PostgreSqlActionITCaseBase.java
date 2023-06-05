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

package org.apache.paimon.flink.action.cdc.postgresql;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for {@link org.apache.paimon.flink.action.Action}s related to PsotgreSQL. */
public class PostgreSqlActionITCaseBase extends ActionITCaseBase {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlActionITCaseBase.class);

    protected static final PostgreSqlContainer POSTGRE_SQL_CONTAINER = createPostgreSqlContainer(PostgreSqlVersion.V_12);

    private static final String USER = "postgres";
    private static final String PASSWORD = "password";

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(POSTGRE_SQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        POSTGRE_SQL_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    private static PostgreSqlContainer createPostgreSqlContainer(PostgreSqlVersion postgreSqlVersion) {
        PostgreSqlContainer postgresContainer =  (PostgreSqlContainer) new PostgreSqlContainer(postgreSqlVersion)
                .withSetupSQL("postgresql/setup.sql")
                .withUsername(USER)
                .withPassword(PASSWORD)
                .withEnv("TZ", "America/Los_Angeles")
                .withLogConsumer(new Slf4jLogConsumer(LOG));

       /* postgresContainer.withCopyFileToContainer(
                MountableFile.forClasspathResource("postgresql/postgresql.conf"),
                "/var/lib/postgresql/data");*/

        postgresContainer.withCommand("postgres -c wal_level=logical");

        postgresContainer.withInitScript("postgresql/setup.sql");

        return postgresContainer;
    }

    protected void waitForResult(
            List<String> expected, FileStoreTable table, RowType rowType, List<String> primaryKeys)
            throws Exception {
        assertThat(table.schema().primaryKeys()).isEqualTo(primaryKeys);

        // wait for table schema to become our expected schema
        while (true) {
            if (rowType.getFieldCount() == table.schema().fields().size()) {
                int cnt = 0;
                for (int i = 0; i < table.schema().fields().size(); i++) {
                    DataField field = table.schema().fields().get(i);
                    boolean sameName = field.name().equals(rowType.getFieldNames().get(i));
                    boolean sameType = field.type().equals(rowType.getFieldTypes().get(i));
                    if (sameName && sameType) {
                        cnt++;
                    }
                }
                if (cnt == rowType.getFieldCount()) {
                    break;
                }
            }
            table = table.copyWithLatestSchema();
            Thread.sleep(100);
        }

        // wait for data to become expected
        List<String> sortedExpected = new ArrayList<>(expected);
        Collections.sort(sortedExpected);
        while (true) {
            ReadBuilder readBuilder = table.newReadBuilder();
            TableScan.Plan plan = readBuilder.newScan().plan();
            List<String> result =
                    getResult(
                            readBuilder.newRead(),
                            plan == null ? Collections.emptyList() : plan.splits(),
                            rowType);
            List<String> sortedActual = new ArrayList<>(result);
            Collections.sort(sortedActual);
            if (sortedExpected.equals(sortedActual)) {
                break;
            }
            Thread.sleep(100);
        }
    }

    protected Map<String, String> getBasicPostgreSqlConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("hostname", POSTGRE_SQL_CONTAINER.getHost());
        config.put("port", String.valueOf(POSTGRE_SQL_CONTAINER.getDatabasePort()));
        config.put("username", USER);
        config.put("password", PASSWORD);
        return config;
    }

    protected void waitJobRunning(JobClient client) throws Exception {
        while (true) {
            JobStatus status = client.getJobStatus().get();
            if (status == JobStatus.RUNNING) {
                break;
            }
            Thread.sleep(100);
        }
    }

    public static void main(String[] args) throws SQLException {
        PostgreSqlContainer postgreSqlContainer = createPostgreSqlContainer(PostgreSqlVersion.V_12);
        postgreSqlContainer.withInitScript("postgresql/setup.sql");
        //Startables.deepStart(Stream.of(postgreSqlContainer)).join();
        postgreSqlContainer.start();
        Connection connection = postgreSqlContainer.createConnection("");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'paimon_sync_database_including';");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }
}
