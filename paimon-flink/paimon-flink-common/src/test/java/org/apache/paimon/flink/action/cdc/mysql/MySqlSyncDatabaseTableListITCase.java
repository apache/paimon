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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
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

    // TODO it's more convenient to check table without merging shards
    @Test
    @Timeout(60)
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
                        // TODO refactor
                        true,
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

        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl("shard_1"),
                                MYSQL_CONTAINER.getUsername(),
                                MYSQL_CONTAINER.getPassword());
                Statement statement = conn.createStatement()) {
            Catalog catalog = catalog();
            assertThat(catalog.listTables(database))
                    .containsExactlyInAnyOrder("t1", "t11", "t2", "t22", "t3", "taa", "tb", "s2");

            RowType rowType =
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(100)},
                            new String[] {"k", "name"});
            List<String> pk = Collections.singletonList("k");

            waitForResult(
                    Arrays.asList("+I[2, shard_2.t1]", "+I[3, x_shard_1.t1]"),
                    getFileStoreTable("t1"),
                    rowType,
                    pk);

            waitForResult(
                    Collections.singletonList("+I[1, shard_1.t11]"),
                    getFileStoreTable("t11"),
                    rowType,
                    pk);

            waitForResult(
                    Collections.singletonList("+I[1, shard_1.t2]"),
                    getFileStoreTable("t2"),
                    rowType,
                    pk);

            waitForResult(
                    Collections.singletonList("+I[2, shard_2.t22]"),
                    getFileStoreTable("t22"),
                    rowType,
                    pk);

            waitForResult(
                    Arrays.asList("+I[1, shard_1.t3]", "+I[2, shard_2.t3]"),
                    getFileStoreTable("t3"),
                    rowType,
                    pk);

            waitForResult(
                    Collections.singletonList("+I[1, shard_1.taa]"),
                    getFileStoreTable("taa"),
                    rowType,
                    pk);

            waitForResult(
                    Collections.singletonList("+I[2, shard_2.tb]"),
                    getFileStoreTable("tb"),
                    rowType,
                    pk);

            waitForResult(
                    Collections.singletonList("+I[1, shard_1.s2]"),
                    getFileStoreTable("s2"),
                    rowType,
                    pk);

            // test newly added tables
            if (mode == COMBINED) {
                // case 1: new tables in existed database
                statement.executeUpdate("USE shard_2");
                // ignored: ta
                statement.executeUpdate(
                        "CREATE TABLE ta (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                statement.executeUpdate("INSERT INTO ta VALUES (10, 'shard_2.ta')");

                // captured: s3
                statement.executeUpdate(
                        "CREATE TABLE s3 (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                statement.executeUpdate("INSERT INTO s3 VALUES (10, 'shard_2.s3')");

                // case 2: new tables in new captured database
                statement.executeUpdate("CREATE DATABASE shard_3");
                statement.executeUpdate("USE shard_3");
                // ignored: ta, m
                statement.executeUpdate(
                        "CREATE TABLE ta (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                statement.executeUpdate("INSERT INTO ta VALUES (10, 'shard_3.ta')");

                statement.executeUpdate(
                        "CREATE TABLE m (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                statement.executeUpdate("INSERT INTO m VALUES (10, 'shard_3.m')");

                // captured: tab
                statement.executeUpdate(
                        "CREATE TABLE tab (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                statement.executeUpdate("INSERT INTO tab VALUES (10, 'shard_3.tab')");

                // case 3: new tables in new ignored database
                statement.executeUpdate("CREATE DATABASE what");
                statement.executeUpdate("USE what");
                // ignored: ta
                statement.executeUpdate(
                        "CREATE TABLE ta (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                statement.executeUpdate("INSERT INTO ta VALUES (10, 'what.ta')");

                // match including pattern but ignored: s4
                statement.executeUpdate(
                        "CREATE TABLE s4 (k INT, name VARCHAR(100), PRIMARY KEY (k))");
                statement.executeUpdate("INSERT INTO s4 VALUES (10, 'what.s4')");

                Thread.sleep(5_000);

                assertThat(catalog.listTables(database))
                        .containsExactlyInAnyOrder(
                                "t1", "t11", "t2", "t22", "t3", "taa", "tb", "s2", "s3", "tab");

                waitForResult(
                        Collections.singletonList("+I[10, shard_2.s3]"),
                        getFileStoreTable("s3"),
                        rowType,
                        pk);

                waitForResult(
                        Collections.singletonList("+I[10, shard_3.tab]"),
                        getFileStoreTable("tab"),
                        rowType,
                        pk);
            }
        }
    }
}
