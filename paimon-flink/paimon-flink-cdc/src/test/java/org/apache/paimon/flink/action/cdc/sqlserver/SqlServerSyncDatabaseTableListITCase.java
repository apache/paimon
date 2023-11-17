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

import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;

/** Test if the table list in {@link SqlServerSyncDatabaseAction} is correct. */
public class SqlServerSyncDatabaseTableListITCase extends SqlServerActionITCaseBase {

    @BeforeAll
    public static void startContainers() {
        MSSQL_SERVER_CONTAINER.withInitScript("sqlserver/tablelist_test_setup.sql");
        start();
    }

    @Test
    @Timeout(120)
    public void testActionRunResult() throws Exception {
        Map<String, String> sqlserverConfig = getBasicSqlServerConfig();
        sqlserverConfig.put("database-name", "shard_database");
        sqlserverConfig.put(
                "schema-name",
                ThreadLocalRandom.current().nextBoolean()
                        ? ".*shard_.*"
                        : "shard_1|shard_2|shard_3|x_shard_1");
        MultiTablesSinkMode mode = ThreadLocalRandom.current().nextBoolean() ? DIVIDED : COMBINED;
        SqlServerSyncDatabaseAction action =
                syncDatabaseActionBuilder(sqlserverConfig)
                        .withTableConfig(getBasicTableConfig())
                        .mergeShards(false)
                        .withMode(mode.configString())
                        .includingTables("t.+|s.+")
                        .excludingTables("ta|sa")
                        .build();
        runActionWithDefaultEnv(action);

        assertExactlyExistTables(
                "shard_database_shard_1_t11",
                "shard_database_shard_1_t2",
                "shard_database_shard_1_t3",
                "shard_database_shard_1_taa",
                "shard_database_shard_1_s2",
                "shard_database_shard_2_t1",
                "shard_database_shard_2_t22",
                "shard_database_shard_2_t3",
                "shard_database_shard_2_tb",
                "shard_database_x_shard_1_t1");

        try (Statement statement = getStatement()) {
            // ensure the job steps into incremental phase
            statement.executeUpdate("USE shard_database");
            statement.executeUpdate("INSERT INTO shard_1.t2 VALUES (1, 'A')");
        }
        waitForResult(
                Collections.singletonList("+I[1, A]"),
                getFileStoreTable("shard_database_shard_1_t2"),
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(100)},
                        new String[] {"k", "name"}),
                Collections.singletonList("k"));
    }
}
