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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.action.cdc.DatabaseSyncMode;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.flink.action.cdc.DatabaseSyncMode.COMBINED;
import static org.apache.paimon.flink.action.cdc.DatabaseSyncMode.DIVIDED;
import static org.assertj.core.api.Assertions.assertThat;

/** Test if the table list in {@link MySqlSyncDatabaseAction} is correct. */
public class MySqlSyncDatabaseTableListITCase extends MySqlActionITCaseBase {

    @BeforeAll
    public static void startContainers() {
        MYSQL_CONTAINER.withSetupSQL("mysql/tablelist_test_setup.sql");
        start();
    }

    @Test
    @Timeout(120)
    public void testActionRunResult() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", ".*shard_.*");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        Map<String, String> tableConfig = getBasicTableConfig();
        DatabaseSyncMode mode = ThreadLocalRandom.current().nextBoolean() ? DIVIDED : COMBINED;
        MySqlSyncDatabaseAction action =
                new MySqlSyncDatabaseAction(
                        mySqlConfig,
                        warehouse,
                        database,
                        false,
                        false,
                        null,
                        null,
                        "t.+|s.+",
                        "ta|sa",
                        Collections.emptyMap(),
                        tableConfig,
                        mode);
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        try (Statement statement = getStatement()) {
            Catalog catalog = catalog();
            List<String> tables = waitingAllTables(catalog, 10, 10);
            assertThat(tables)
                    .containsExactlyInAnyOrder(
                            "shard_1_t11",
                            "shard_1_t2",
                            "shard_1_t3",
                            "shard_1_taa",
                            "shard_1_s2",
                            "shard_2_t1",
                            "shard_2_t22",
                            "shard_2_t3",
                            "shard_2_tb",
                            "x_shard_1_t1");

            // test newly added tables
            if (mode == COMBINED) {
                // case 1: new tables in existed database
                statement.executeUpdate("USE shard_2");
                // ignored: ta
                statement.executeUpdate(
                        "CREATE TABLE ta (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                // captured: s3
                statement.executeUpdate(
                        "CREATE TABLE s3 (k INT, name VARCHAR(100), PRIMARY KEY (k))");

                // case 2: new tables in new captured database
                statement.executeUpdate("CREATE DATABASE shard_3");
                statement.executeUpdate("USE shard_3");
                // ignored: ta, m
                statement.executeUpdate(
                        "CREATE TABLE ta (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                statement.executeUpdate(
                        "CREATE TABLE m (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                // captured: tab
                statement.executeUpdate(
                        "CREATE TABLE tab (k INT, name VARCHAR(100), PRIMARY KEY (k))");

                // case 3: new tables in new ignored database
                statement.executeUpdate("CREATE DATABASE what");
                statement.executeUpdate("USE what");
                // ignored: ta
                statement.executeUpdate(
                        "CREATE TABLE ta (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                // match including pattern but ignored: s4
                statement.executeUpdate(
                        "CREATE TABLE s4 (k INT, name VARCHAR(100), PRIMARY KEY (k))");

                tables = waitingAllTables(catalog, 12, 10);

                assertThat(tables)
                        .containsExactlyInAnyOrder(
                                // old
                                "shard_1_t11",
                                "shard_1_t2",
                                "shard_1_t3",
                                "shard_1_taa",
                                "shard_1_s2",
                                "shard_2_t1",
                                "shard_2_t22",
                                "shard_2_t3",
                                "shard_2_tb",
                                "x_shard_1_t1",
                                // new
                                "shard_2_s3",
                                "shard_3_tab");
            }
        }
    }

    private List<String> waitingAllTables(Catalog catalog, int numberOfTables, int maxAttempt)
            throws Exception {
        List<String> tables;
        int attempt = 0;
        do {
            Thread.sleep(5_000);
            tables = catalog.listTables(database);
        } while (tables.size() < numberOfTables && ++attempt < maxAttempt);

        return tables;
    }
}
