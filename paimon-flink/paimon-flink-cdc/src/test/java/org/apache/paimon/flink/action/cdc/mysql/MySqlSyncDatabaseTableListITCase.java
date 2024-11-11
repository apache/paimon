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

import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;

/** Test if the table list in {@link MySqlSyncDatabaseAction} is correct. */
public class MySqlSyncDatabaseTableListITCase extends MySqlActionITCaseBase {

    @BeforeAll
    public static void startContainers() {
        MYSQL_CONTAINER.withSetupSQL("mysql/tablelist_test_setup.sql");
        start();
    }

    @Test
    @Timeout(60)
    public void testActionRunResult() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put(
                "database-name",
                ThreadLocalRandom.current().nextBoolean()
                        ? ".*shard_.*"
                        : "shard_1|shard_2|shard_3|x_shard_1");

        MultiTablesSinkMode mode = ThreadLocalRandom.current().nextBoolean() ? DIVIDED : COMBINED;
        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .mergeShards(false)
                        .withMode(mode.configString())
                        .includingTables("t.+|s.+")
                        .excludingTables("ta|sa")
                        .build();
        runActionWithDefaultEnv(action);

        assertExactlyExistTables(
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

        // test newly created tables
        if (mode == COMBINED) {
            try (Statement statement = getStatement()) {
                FileStoreTable t2 = getFileStoreTable("shard_1_t2");
                RowType rowTypeT2 =
                        RowType.of(
                                new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(100)},
                                new String[] {"k", "name"});
                List<String> primaryKeysT2 = Collections.singletonList("k");

                // ensure the job steps into incremental phase
                waitForResult(Collections.singletonList("+I[1, A]"), t2, rowTypeT2, primaryKeysT2);
                statement.executeUpdate("USE shard_1");
                statement.executeUpdate("INSERT INTO t2 VALUES (2, 'B')");
                waitForResult(Arrays.asList("+I[1, A]", "+I[2, B]"), t2, rowTypeT2, primaryKeysT2);

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

                waitingTables("shard_2_s3", "shard_3_tab");
            }
        }
    }
}
