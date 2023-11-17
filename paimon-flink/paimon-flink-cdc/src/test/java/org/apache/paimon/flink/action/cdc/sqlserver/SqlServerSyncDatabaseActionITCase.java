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

import org.apache.paimon.catalog.FileSystemCatalogOptions;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link SqlServerSyncDatabaseActionITCase}. */
public class SqlServerSyncDatabaseActionITCase extends SqlServerActionITCaseBase {

    @BeforeAll
    public static void startContainers() {
        MSSQL_SERVER_CONTAINER.withInitScript("sqlserver/sync_database_setup.sql");
        start();
    }

    @Test
    @Timeout(120)
    public void testSyncSqlServerDatabase() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", "paimon_sync_database");

        SqlServerSyncDatabaseAction action =
                syncDatabaseActionBuilder(sqlServerConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            testSyncSqlServerDatabaseImpl(statement);
        }
    }

    private void testSyncSqlServerDatabaseImpl(Statement statement) throws Exception {
        FileStoreTable table1 = getFileStoreTable("t1");
        FileStoreTable table2 = getFileStoreTable("t2");
        statement.executeUpdate("USE paimon_sync_database");
        statement.executeUpdate("INSERT INTO dbo.t1 VALUES (1, 'one')");
        statement.executeUpdate("INSERT INTO dbo.t2 VALUES (2, 'two', 20, 200)");
        statement.executeUpdate("INSERT INTO dbo.t1 VALUES (3, 'three')");
        statement.executeUpdate("INSERT INTO dbo.t2 VALUES (4, 'four', 40, 400)");
        statement.executeUpdate("INSERT INTO dbo.t3 VALUES (-1)");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k", "v1"});
        List<String> primaryKeys1 = Collections.singletonList("k");
        List<String> expected = Arrays.asList("+I[1, one]", "+I[3, three]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10).notNull(),
                            DataTypes.INT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"k1", "k2", "v1", "v2"});
        List<String> primaryKeys2 = Arrays.asList("k1", "k2");
        expected = Arrays.asList("+I[2, two, 20, 200]", "+I[4, four, 40, 400]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }

    @Test
    public void testSpecifiedSqlServerTable() {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", "paimon_sync_database");
        sqlServerConfig.put("table-name", "my_table");
        SqlServerSyncDatabaseAction action = syncDatabaseActionBuilder(sqlServerConfig).build();
        assertThatThrownBy(action::run).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Timeout(60)
    public void testIgnoreIncompatibleTables() throws Exception {
        // create an incompatible table
        createFileStoreTable(
                "incompatible",
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"k", "v1"}),
                Collections.emptyList(),
                Collections.singletonList("k"),
                Collections.emptyMap());

        // try synchronization
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", "paimon_sync_database_ignore_incompatible");
        SqlServerSyncDatabaseAction action =
                syncDatabaseActionBuilder(sqlServerConfig)
                        .withTableConfig(getBasicTableConfig())
                        .ignoreIncompatible(true)
                        .build();
        runActionWithDefaultEnv(action);

        // validate `compatible` can be synchronized
        try (Statement statement = getStatement()) {
            FileStoreTable table = getFileStoreTable("compatible");

            statement.executeUpdate("USE paimon_sync_database_ignore_incompatible");
            statement.executeUpdate("INSERT INTO compatible VALUES (2, 'two', 20, 200)");
            statement.executeUpdate("INSERT INTO compatible VALUES (4, 'four', 40, 400)");

            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(10).notNull(),
                                DataTypes.INT(),
                                DataTypes.BIGINT()
                            },
                            new String[] {"k1", "k2", "v1", "v2"});
            List<String> primaryKeys2 = Arrays.asList("k1", "k2");
            List<String> expected = Arrays.asList("+I[2, two, 20, 200]", "+I[4, four, 40, 400]");
            waitForResult(expected, table, rowType, primaryKeys2);
        }
    }

    @Test
    @Timeout(120)
    public void testTableAffix() throws Exception {
        // create table t1
        createFileStoreTable(
                "test_prefix_t1_test_suffix",
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.VARCHAR(10)},
                        new String[] {"k1", "v0"}),
                Collections.emptyList(),
                Collections.singletonList("k1"),
                Collections.emptyMap());

        // try synchronization
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", "paimon_sync_database_affix");

        SqlServerSyncDatabaseAction action =
                syncDatabaseActionBuilder(sqlServerConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withTablePrefix("test_prefix_")
                        .withTableSuffix("_test_suffix")
                        // test including check with affix
                        .includingTables(ThreadLocalRandom.current().nextBoolean() ? "t1|t2" : ".*")
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            testTableAffixImpl(statement);
        }
    }

    private void testTableAffixImpl(Statement statement) throws Exception {
        FileStoreTable table1 = getFileStoreTable("test_prefix_t1_test_suffix");
        FileStoreTable table2 = getFileStoreTable("test_prefix_t2_test_suffix");

        statement.executeUpdate("USE paimon_sync_database_affix");

        statement.executeUpdate("INSERT INTO t1 VALUES (1, 'one')");
        statement.executeUpdate("INSERT INTO t2 VALUES (2, 'two')");
        statement.executeUpdate("INSERT INTO t1 VALUES (3, 'three')");
        statement.executeUpdate("INSERT INTO t2 VALUES (4, 'four')");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k1", "v0"});
        List<String> primaryKeys1 = Collections.singletonList("k1");
        List<String> expected = Arrays.asList("+I[1, one]", "+I[3, three]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k2", "v0"});
        List<String> primaryKeys2 = Collections.singletonList("k2");
        expected = Arrays.asList("+I[2, two]", "+I[4, four]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }

    @Test
    @Timeout(60)
    public void testIncludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "paimon_sync_database_including",
                "flink|paimon.+",
                null,
                Arrays.asList("flink", "paimon_1", "paimon_2"),
                Collections.singletonList("ignored"));
    }

    @Test
    @Timeout(60)
    public void testExcludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "paimon_sync_database_excluding",
                null,
                "flink|paimon.+",
                Collections.singletonList("sync"),
                Arrays.asList("flink", "paimon_1", "paimon_2"));
    }

    @Test
    @Timeout(60)
    public void testIncludingAndExcludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "paimon_sync_database_in_excluding",
                "flink|paimon.+",
                "paimon_1",
                Arrays.asList("flink", "paimon_2"),
                Arrays.asList("paimon_1", "test"));
    }

    private void includingAndExcludingTablesImpl(
            String databaseName,
            @Nullable String includingTables,
            @Nullable String excludingTables,
            List<String> existedTables,
            List<String> notExistedTables)
            throws Exception {
        // try synchronization
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", databaseName);
        SqlServerSyncDatabaseAction action =
                syncDatabaseActionBuilder(sqlServerConfig)
                        .withTableConfig(getBasicTableConfig())
                        .includingTables(includingTables)
                        .excludingTables(excludingTables)
                        .build();
        runActionWithDefaultEnv(action);

        // check paimon tables
        assertExactlyExistTables(existedTables);
        assertTableNotExists(notExistedTables);
    }

    @Test
    @Timeout(60)
    public void testIgnoreCase() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", "paimon_ignore_CASE");

        SqlServerSyncDatabaseAction action =
                syncDatabaseActionBuilder(sqlServerConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        FileSystemCatalogOptions.CASE_SENSITIVE.key(), "false"))
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        // check table schema
        FileStoreTable table = getFileStoreTable("t");
        assertThat(JsonSerdeUtil.toFlatJson(table.schema().fields()))
                .isEqualTo(
                        "[{\"id\":0,\"name\":\"k\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"uppercase_v0\",\"type\":\"VARCHAR(20)\"}]");
    }

    @Test
    public void testCatalogAndTableConfig() {
        SqlServerSyncDatabaseAction action =
                syncDatabaseActionBuilder(getBasicSqlServerConfig())
                        .withCatalogConfig(Collections.singletonMap("catalog-key", "catalog-value"))
                        .withTableConfig(Collections.singletonMap("table-key", "table-value"))
                        .build();

        assertThat(action.catalogConfig()).containsEntry("catalog-key", "catalog-value");
        assertThat(action.tableConfig())
                .containsExactlyEntriesOf(Collections.singletonMap("table-key", "table-value"));
    }

    @Test
    @Timeout(120)
    public void testMetadataColumns() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", "metadata");

        MultiTablesSinkMode mode = ThreadLocalRandom.current().nextBoolean() ? DIVIDED : COMBINED;
        SqlServerSyncDatabaseAction action =
                syncDatabaseActionBuilder(sqlServerConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withMode(mode.configString())
                        .withMetadataColumn(Arrays.asList("table_name", "database_name"))
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE metadata");
            statement.executeUpdate("INSERT INTO t1 VALUES (1, 'db1_1')");
            statement.executeUpdate("INSERT INTO t1 VALUES (2, 'db1_2')");

            statement.executeUpdate("INSERT INTO t1 VALUES (3, 'db2_3')");
            statement.executeUpdate("INSERT INTO t1 VALUES (4, 'db2_4')");

            FileStoreTable table = getFileStoreTable("t1");
            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(10),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull()
                            },
                            new String[] {"k", "v1", "table_name", "database_name"});
            waitForResult(
                    Arrays.asList(
                            "+I[1, db1_1, t1, metadata]",
                            "+I[2, db1_2, t1, metadata]",
                            "+I[3, db2_3, t1, metadata]",
                            "+I[4, db2_4, t1, metadata]"),
                    table,
                    rowType,
                    Collections.singletonList("k"));

            statement.executeUpdate("INSERT INTO t2 VALUES (1, 'db1_1')");
            statement.executeUpdate("INSERT INTO t2 VALUES (2, 'db1_2')");
            statement.executeUpdate("INSERT INTO t2 VALUES (3, 'db1_3')");
            statement.executeUpdate("INSERT INTO t2 VALUES (4, 'db1_4')");
            table = getFileStoreTable("t2");
            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(10),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull()
                            },
                            new String[] {"k", "v1", "table_name", "database_name"});
            waitForResult(
                    Arrays.asList(
                            "+I[1, db1_1, t2, metadata]",
                            "+I[2, db1_2, t2, metadata]",
                            "+I[3, db1_3, t2, metadata]",
                            "+I[4, db1_4, t2, metadata]"),
                    table,
                    rowType,
                    Collections.singletonList("k"));
        }
    }

    @Test
    @Timeout(120)
    public void testSyncMultipleShards() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        // test table list
        sqlServerConfig.put("database-name", "database_shard");
        sqlServerConfig.put(
                "schema-name",
                ThreadLocalRandom.current().nextBoolean() ? "schema_.*" : "schema_1|schema_2");
        MultiTablesSinkMode mode = ThreadLocalRandom.current().nextBoolean() ? DIVIDED : COMBINED;
        SqlServerSyncDatabaseAction action =
                syncDatabaseActionBuilder(sqlServerConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withMode(mode.configString())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            // test insert into t1
            statement.executeUpdate("USE database_shard");
            statement.executeUpdate("INSERT INTO schema_1.t1 VALUES (1, 'db1_1')");
            statement.executeUpdate("INSERT INTO schema_1.t1 VALUES (2, 'db1_2')");

            statement.executeUpdate("INSERT INTO schema_2.t1 VALUES (3, 'db2_3', 300)");
            statement.executeUpdate("INSERT INTO schema_2.t1 VALUES (4, 'db2_4', 400)");

            FileStoreTable table = getFileStoreTable("t1");
            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(), DataTypes.VARCHAR(20), DataTypes.BIGINT()
                            },
                            new String[] {"k", "v1", "v2"});
            waitForResult(
                    Arrays.asList(
                            "+I[1, db1_1, NULL]",
                            "+I[2, db1_2, NULL]",
                            "+I[3, db2_3, 300]",
                            "+I[4, db2_4, 400]"),
                    table,
                    rowType,
                    Collections.singletonList("k"));

            // test that database_shard_schema_2.t3 won't be synchronized
            statement.executeUpdate("USE database_shard");
            statement.executeUpdate("INSERT INTO schema_2.t3 VALUES (1, 'db2_1'), (2, 'db2_2')");
            statement.executeUpdate("INSERT INTO schema_1.t3 VALUES (3, 'db1_3'), (4, 'db1_4')");
            table = getFileStoreTable("t3");
            rowType =
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                            new String[] {"k", "v1"});
            waitForResult(
                    Arrays.asList("+I[3, db1_3]", "+I[4, db1_4]"),
                    table,
                    rowType,
                    Collections.singletonList("k"));
        }
    }

    @Test
    @Timeout(120)
    public void testSyncMultipleShardsWithoutMerging() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", "without_merging_shard");
        sqlServerConfig.put("schema-name", "schema_.*");

        MultiTablesSinkMode mode = ThreadLocalRandom.current().nextBoolean() ? DIVIDED : COMBINED;
        SqlServerSyncDatabaseAction action =
                syncDatabaseActionBuilder(sqlServerConfig)
                        .withTableConfig(getBasicTableConfig())
                        .mergeShards(false)
                        .withMode(mode.configString())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            Thread.sleep(5_000);

            assertExactlyExistTables(
                    "without_merging_shard_schema_1_t1",
                    "without_merging_shard_schema_1_t2",
                    "without_merging_shard_schema_2_t1");

            // test insert into without_merging_shard_1.t1
            statement.executeUpdate("USE without_merging_shard");
            statement.executeUpdate("INSERT INTO schema_1.t1 VALUES (1, 'db1_1'), (2, 'db1_2')");
            FileStoreTable table = getFileStoreTable("without_merging_shard_schema_1_t1");
            RowType rowType =
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                            new String[] {"k", "v1"});
            waitForResult(
                    Arrays.asList("+I[1, db1_1]", "+I[2, db1_2]"),
                    table,
                    rowType,
                    Collections.singletonList("k"));

            // test insert into without_merging_shard_2.t1
            statement.executeUpdate("USE without_merging_shard");
            statement.executeUpdate(
                    "INSERT INTO schema_2.t1 VALUES (3, 'db2_3', 300), (4, 'db2_4', 400)");
            table = getFileStoreTable("without_merging_shard_schema_2_t1");
            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(), DataTypes.VARCHAR(20), DataTypes.BIGINT()
                            },
                            new String[] {"k", "v1", "v2"});
            waitForResult(
                    Arrays.asList("+I[3, db2_3, 300]", "+I[4, db2_4, 400]"),
                    table,
                    rowType,
                    Collections.singletonList("k"));
        }
    }
}
