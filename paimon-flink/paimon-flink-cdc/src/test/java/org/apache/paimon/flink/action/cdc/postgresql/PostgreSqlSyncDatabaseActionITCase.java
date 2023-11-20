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

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link PostgreSqlSyncDatabaseAction}. */
public class PostgreSqlSyncDatabaseActionITCase extends PostgreSqlActionITCaseBase {

    @BeforeAll
    public static void init() {
        POSTGRE_SQL_CONTAINER.withInitScript("postgresql/sync_database_setup.sql");
        startContainers();
    }

    @Test
    @Timeout(180)
    public void testSyncDatabase() throws Exception {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "test_schema");
        postgreSqlConfig.put("decoding.plugin.name", "pgoutput");
        postgreSqlConfig.put("slot.name", "flink_replication_slot_01");

        PostgreSqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(postgreSqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();

        runActionWithDefaultEnv(action);

        assertExactlyExistTables(
                "test_table_01",
                "test_table_02",
                "test_table_03",
                "test_table_04",
                "test_table_05",
                "test_table_06");

        try (Statement statement = getStatement()) {
            insertData(statement);
            assertResult();
        }
    }

    private void insertData(Statement statement) throws SQLException {
        statement.executeUpdate("SET search_path TO test_schema;");
        statement.executeUpdate("INSERT INTO test_table_01 VALUES (1, 'a1')");
        statement.executeUpdate("INSERT INTO test_table_02 VALUES (2, 'a2')");
        statement.executeUpdate("INSERT INTO test_table_01 VALUES (3, 'a3')");
        statement.executeUpdate("INSERT INTO test_table_02 VALUES (4, 'a4')");
    }

    private void assertResult() throws Exception {
        FileStoreTable t1 = getFileStoreTable("test_table_01");
        FileStoreTable t2 = getFileStoreTable("test_table_02");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k1", "v1"});
        List<String> primaryKeys1 = Collections.singletonList("k1");
        List<String> expected = Arrays.asList("+I[1, a1]", "+I[3, a3]");
        waitForResult(expected, t1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k2", "v2"});
        List<String> primaryKeys2 = Collections.singletonList("k2");
        expected = Arrays.asList("+I[2, a2]", "+I[4, a4]");
        waitForResult(expected, t2, rowType2, primaryKeys2);
    }

    @Test
    public void testSpecifiedTable() {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "test_schema");
        postgreSqlConfig.put("slot.name", "flink_replication_slot_02");
        postgreSqlConfig.put("table-name", "test_table_01");

        PostgreSqlSyncDatabaseAction action = syncDatabaseActionBuilder(postgreSqlConfig).build();

        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "table-name cannot be set for postgresql-sync-database. "
                                + "If you want to sync several PostgreSQL tables into one Paimon table, "
                                + "use postgresql-sync-table instead.");
    }

    @Test
    public void testInvalidSchema() {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "test_schema_01");

        PostgreSqlSyncDatabaseAction action = syncDatabaseActionBuilder(postgreSqlConfig).build();

        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "No tables found in PostgreSQL schema test_schema_01, or PostgreSQL schema does not exist.");
    }

    @Test
    @Timeout(60)
    public void testIncludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "test_db",
                "test_schema",
                "test_table_01|test_table_02|test_table_03",
                null,
                Arrays.asList("test_table_01", "test_table_02", "test_table_03"),
                Collections.singletonList("test_table_04"));
    }

    @Test
    @Timeout(60)
    public void testExcludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "test_db",
                "test_schema",
                null,
                "test_table_04|test_table_05|test_table_06",
                Arrays.asList("test_table_01", "test_table_02", "test_table_03"),
                Arrays.asList("test_table_04", "test_table_05", "test_table_06"));
    }

    @Test
    @Timeout(60)
    public void testIncludingAndExcludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "test_db",
                "paimon_sync_database_in_excluding",
                "flink|paimon.+",
                "paimon_1",
                Arrays.asList("flink", "paimon_2"),
                Arrays.asList("paimon_1", "test"));
    }

    private void includingAndExcludingTablesImpl(
            String databaseName,
            String schema,
            @Nullable String includingTables,
            @Nullable String excludingTables,
            List<String> existedTables,
            List<String> notExistedTables)
            throws Exception {

        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", databaseName);
        postgreSqlConfig.put("schema-name", schema);
        postgreSqlConfig.put("slot.name", "flink_replication_slot_03");

        PostgreSqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(postgreSqlConfig)
                        .includingTables(includingTables)
                        .excludingTables(excludingTables)
                        .build();

        runActionWithDefaultEnv(action);

        assertExactlyExistTables(existedTables);
        assertTableNotExists(notExistedTables);
    }

    @Test
    @Timeout(60)
    public void testTableAffix() throws Exception {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "paimon_sync_database_affix");

        PostgreSqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(postgreSqlConfig)
                        .withTablePrefix("test_prefix_")
                        .withTableSuffix("_test_suffix")
                        .build();

        runActionWithDefaultEnv(action);

        assertExactlyExistTables("test_prefix_t1_test_suffix", "test_prefix_t2_test_suffix");
    }

    @Test
    @Timeout(120)
    public void testIgnoreIncompatibleTables() throws Exception {
        createFileStoreTable(
                "incompatible",
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"k", "v1"}),
                Collections.emptyList(),
                Collections.singletonList("k"),
                Collections.emptyMap());

        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "paimon_sync_database_ignore_incompatible");
        postgreSqlConfig.put("decoding.plugin.name", "pgoutput");
        postgreSqlConfig.put("slot.name", "flink_replication_slot_04");

        PostgreSqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(postgreSqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .ignoreIncompatible(true)
                        .build();

        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            FileStoreTable table = getFileStoreTable("compatible");

            statement.executeUpdate("SET search_path TO paimon_sync_database_ignore_incompatible");
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
}
