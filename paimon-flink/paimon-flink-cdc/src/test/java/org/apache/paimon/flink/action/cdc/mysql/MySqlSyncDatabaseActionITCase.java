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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link MySqlSyncDatabaseAction}. */
public class MySqlSyncDatabaseActionITCase extends MySqlActionITCaseBase {

    @TempDir java.nio.file.Path tempDir;

    @BeforeAll
    public static void startContainers() {
        MYSQL_CONTAINER.withSetupSQL("mysql/sync_database_setup.sql");
        start();
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "paimon_sync_database");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            testSchemaEvolutionImpl(statement);
        }
    }

    private void testSchemaEvolutionImpl(Statement statement) throws Exception {
        FileStoreTable table1 = getFileStoreTable("t1");
        FileStoreTable table2 = getFileStoreTable("t2");

        statement.executeUpdate("USE paimon_sync_database");

        statement.executeUpdate("INSERT INTO t1 VALUES (1, 'one')");
        statement.executeUpdate("INSERT INTO t2 VALUES (2, 'two', 20, 200)");
        statement.executeUpdate("INSERT INTO t1 VALUES (3, 'three')");
        statement.executeUpdate("INSERT INTO t2 VALUES (4, 'four', 40, 400)");
        statement.executeUpdate("INSERT INTO t3 VALUES (-1)");

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

        statement.executeUpdate("ALTER TABLE t1 ADD COLUMN v2 INT");
        statement.executeUpdate("INSERT INTO t1 VALUES (5, 'five', 50)");
        statement.executeUpdate("ALTER TABLE t2 ADD COLUMN v3 VARCHAR(10)");
        statement.executeUpdate("INSERT INTO t2 VALUES (6, 'six', 60, 600, 'string_6')");
        statement.executeUpdate("INSERT INTO t1 VALUES (7, 'seven', 70)");
        statement.executeUpdate("INSERT INTO t2 VALUES (8, 'eight', 80, 800, 'string_8')");

        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                        },
                        new String[] {"k", "v1", "v2"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL]",
                        "+I[3, three, NULL]",
                        "+I[5, five, 50]",
                        "+I[7, seven, 70]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10).notNull(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"k1", "k2", "v1", "v2", "v3"});
        expected =
                Arrays.asList(
                        "+I[2, two, 20, 200, NULL]",
                        "+I[4, four, 40, 400, NULL]",
                        "+I[6, six, 60, 600, string_6]",
                        "+I[8, eight, 80, 800, string_8]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        statement.executeUpdate("ALTER TABLE t1 MODIFY COLUMN v2 BIGINT");
        statement.executeUpdate("INSERT INTO t1 VALUES (9, 'nine', 9000000000000)");
        statement.executeUpdate("ALTER TABLE t2 MODIFY COLUMN v3 VARCHAR(20)");
        statement.executeUpdate(
                "INSERT INTO t2 VALUES (10, 'ten', 100, 1000, 'long_long_string_10')");

        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.BIGINT()
                        },
                        new String[] {"k", "v1", "v2"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL]",
                        "+I[3, three, NULL]",
                        "+I[5, five, 50]",
                        "+I[7, seven, 70]",
                        "+I[9, nine, 9000000000000]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10).notNull(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.VARCHAR(20)
                        },
                        new String[] {"k1", "k2", "v1", "v2", "v3"});
        expected =
                Arrays.asList(
                        "+I[2, two, 20, 200, NULL]",
                        "+I[4, four, 40, 400, NULL]",
                        "+I[6, six, 60, 600, string_6]",
                        "+I[8, eight, 80, 800, string_8]",
                        "+I[10, ten, 100, 1000, long_long_string_10]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }

    @Test
    public void testSpecifiedMySqlTable() {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "paimon_sync_database");
        mySqlConfig.put("table-name", "my_table");

        MySqlSyncDatabaseAction action = syncDatabaseActionBuilder(mySqlConfig).build();

        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "table-name cannot be set for mysql_sync_database. "
                                + "If you want to sync several MySQL tables into one Paimon table, "
                                + "use mysql_sync_table instead.");
    }

    @Test
    public void testInvalidDatabase() {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "invalid");

        MySqlSyncDatabaseAction action = syncDatabaseActionBuilder(mySqlConfig).build();

        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "No tables found in MySQL database invalid, or MySQL database does not exist.");
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
                Collections.emptyList(),
                Collections.emptyMap());

        // try synchronization
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "paimon_sync_database_ignore_incompatible");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
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
    @Timeout(60)
    public void testTableAffix() throws Exception {
        // create table t1
        createFileStoreTable(
                "test_prefix_t1_test_suffix",
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.VARCHAR(10)},
                        new String[] {"k1", "v0"}),
                Collections.emptyList(),
                Collections.singletonList("k1"),
                Collections.emptyList(),
                Collections.emptyMap());

        // try synchronization
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "paimon_sync_database_affix");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
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

        statement.executeUpdate("ALTER TABLE t1 ADD COLUMN v1 INT");
        statement.executeUpdate("INSERT INTO t1 VALUES (5, 'five', 50)");
        statement.executeUpdate("ALTER TABLE t2 ADD COLUMN v1 VARCHAR(10)");
        statement.executeUpdate("INSERT INTO t2 VALUES (6, 'six', 's_6')");
        statement.executeUpdate("INSERT INTO t1 VALUES (7, 'seven', 70)");
        statement.executeUpdate("INSERT INTO t2 VALUES (8, 'eight', 's_8')");

        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                        },
                        new String[] {"k1", "v0", "v1"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL]",
                        "+I[3, three, NULL]",
                        "+I[5, five, 50]",
                        "+I[7, seven, 70]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.VARCHAR(10)
                        },
                        new String[] {"k2", "v0", "v1"});
        expected =
                Arrays.asList(
                        "+I[2, two, NULL]",
                        "+I[4, four, NULL]",
                        "+I[6, six, s_6]",
                        "+I[8, eight, s_8]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        statement.executeUpdate("ALTER TABLE t1 MODIFY COLUMN v1 BIGINT");
        statement.executeUpdate("INSERT INTO t1 VALUES (9, 'nine', 9000000000000)");
        statement.executeUpdate("ALTER TABLE t2 MODIFY COLUMN v1 VARCHAR(20)");
        statement.executeUpdate("INSERT INTO t2 VALUES (10, 'ten', 'long_s_10')");

        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.BIGINT()
                        },
                        new String[] {"k1", "v0", "v1"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL]",
                        "+I[3, three, NULL]",
                        "+I[5, five, 50]",
                        "+I[7, seven, 70]",
                        "+I[9, nine, 9000000000000]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.VARCHAR(20)
                        },
                        new String[] {"k2", "v0", "v1"});
        expected =
                Arrays.asList(
                        "+I[2, two, NULL]",
                        "+I[4, four, NULL]",
                        "+I[6, six, s_6]",
                        "+I[8, eight, s_8]",
                        "+I[10, ten, long_s_10]");
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
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", databaseName);

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
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
    public void testIgnoreCaseDivided() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "paimon_ignore_CASE_divided");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.CASE_SENSITIVE.key(), "false"))
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE paimon_ignore_CASE_divided");
            ignoreCaseTableCheck(statement, "T");
        }
    }

    @Test
    @Timeout(60)
    public void testIgnoreCaseCombined() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "paimon_ignore_CASE_combined");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.CASE_SENSITIVE.key(), "false"))
                        .withMode(COMBINED.configString())
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE paimon_ignore_CASE_combined");
            ignoreCaseTableCheck(statement, "T");

            // check new table
            statement.executeUpdate(
                    "CREATE TABLE T1 (k INT, UPPERCASE_V0 VARCHAR(20), PRIMARY KEY (k))");
            waitingTables("t1");
            ignoreCaseTableCheck(statement, "T1");
        }
    }

    private void ignoreCaseTableCheck(Statement statement, String tableName) throws Exception {
        FileStoreTable table = getFileStoreTable(tableName.toLowerCase());

        // check sync schema changes and records
        statement.executeUpdate("INSERT INTO " + tableName + " VALUES (1, 'Hi')");
        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(20)},
                        new String[] {"k", "uppercase_v0"});
        waitForResult(
                Collections.singletonList("+I[1, Hi]"),
                table,
                rowType1,
                Collections.singletonList("k"));

        statement.executeUpdate(
                "ALTER TABLE " + tableName + " MODIFY COLUMN UPPERCASE_V0 VARCHAR(30)");
        statement.executeUpdate("INSERT INTO " + tableName + " VALUES (2, 'Paimon')");
        RowType rowType2 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(30)},
                        new String[] {"k", "uppercase_v0"});
        waitForResult(
                Arrays.asList("+I[1, Hi]", "+I[2, Paimon]"),
                table,
                rowType2,
                Collections.singletonList("k"));

        statement.executeUpdate("ALTER TABLE " + tableName + " ADD COLUMN UPPERCASE_V1 DOUBLE");
        statement.executeUpdate("INSERT INTO " + tableName + " VALUES (3, 'Test', 0.5)");
        RowType rowType3 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(30), DataTypes.DOUBLE()
                        },
                        new String[] {"k", "uppercase_v0", "uppercase_v1"});
        waitForResult(
                Arrays.asList("+I[1, Hi, NULL]", "+I[2, Paimon, NULL]", "+I[3, Test, 0.5]"),
                table,
                rowType3,
                Collections.singletonList("k"));
    }

    @Test
    @Timeout(600)
    public void testNewlyAddedTables() throws Exception {
        testNewlyAddedTable(1, true, false, "paimon_sync_database_newly_added_tables");
    }

    @Test
    @Timeout(600)
    public void testNewlyAddedTableSingleTable() throws Exception {
        testNewlyAddedTable(1, false, false, "paimon_sync_database_newly_added_tables_1");
    }

    @Test
    @Timeout(600)
    public void testNewlyAddedTableMultipleTables() throws Exception {
        testNewlyAddedTable(3, false, false, "paimon_sync_database_newly_added_tables_2");
    }

    @Test
    @Timeout(600)
    public void testNewlyAddedTableSchemaChange() throws Exception {
        testNewlyAddedTable(1, false, true, "paimon_sync_database_newly_added_tables_3");
    }

    @Test
    @Timeout(600)
    public void testNewlyAddedTableSingleTableWithSavepoint() throws Exception {
        testNewlyAddedTable(1, true, true, "paimon_sync_database_newly_added_tables_4");
    }

    @Test
    @Timeout(120)
    public void testAddIgnoredTable() throws Exception {
        String mySqlDatabase = "paimon_sync_database_add_ignored_table";

        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", mySqlDatabase);

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .includingTables("t.+")
                        .excludingTables(".*a$")
                        .withMode(COMBINED.configString())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            FileStoreTable table1 = getFileStoreTable("t1");

            statement.executeUpdate("USE " + mySqlDatabase);
            statement.executeUpdate("INSERT INTO t1 VALUES (1, 'one')");
            statement.executeUpdate("INSERT INTO a VALUES (1, 'one')");

            // make sure the job steps into incremental phase
            RowType rowType =
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                            new String[] {"k", "v1"});
            List<String> primaryKeys = Collections.singletonList("k");
            waitForResult(Collections.singletonList("+I[1, one]"), table1, rowType, primaryKeys);

            // create new tables at runtime
            // synchronized table: t2, t22
            statement.executeUpdate("CREATE TABLE t2 (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
            statement.executeUpdate("INSERT INTO t2 VALUES (1, 'Hi')");

            statement.executeUpdate("CREATE TABLE t22 LIKE t2");
            statement.executeUpdate("INSERT INTO t22 VALUES (1, 'Hello')");

            // not synchronized tables: ta, t3, t4
            statement.executeUpdate("CREATE TABLE ta (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
            statement.executeUpdate("INSERT INTO ta VALUES (1, 'Apache')");
            statement.executeUpdate("CREATE TABLE t3 (k INT, v1 VARCHAR(10))");
            statement.executeUpdate("INSERT INTO t3 VALUES (1, 'Paimon')");
            statement.executeUpdate("CREATE TABLE t4 SELECT * FROM t2");

            statement.executeUpdate("INSERT INTO t1 VALUES (2, 'two')");
            waitForResult(Arrays.asList("+I[1, one]", "+I[2, two]"), table1, rowType, primaryKeys);

            // check tables
            assertExactlyExistTables("t1", "t2", "t22");
            assertTableNotExists("a", "ta", "t3", "t4");

            FileStoreTable newTable = getFileStoreTable("t2");
            waitForResult(Collections.singletonList("+I[1, Hi]"), newTable, rowType, primaryKeys);

            newTable = getFileStoreTable("t22");
            waitForResult(
                    Collections.singletonList("+I[1, Hello]"), newTable, rowType, primaryKeys);
        }
    }

    public void testNewlyAddedTable(
            int numOfNewlyAddedTables,
            boolean testSavepointRecovery,
            boolean testSchemaChange,
            String databaseName)
            throws Exception {
        JobClient client =
                buildSyncDatabaseActionWithNewlyAddedTables(databaseName, testSchemaChange);
        waitJobRunning(client);

        try (Statement statement = getStatement()) {
            testNewlyAddedTableImpl(
                    client,
                    statement,
                    numOfNewlyAddedTables,
                    testSavepointRecovery,
                    testSchemaChange,
                    databaseName);
        }
    }

    private void testNewlyAddedTableImpl(
            JobClient client,
            Statement statement,
            int newlyAddedTableCount,
            boolean testSavepointRecovery,
            boolean testSchemaChange,
            String databaseName)
            throws Exception {
        FileStoreTable table1 = getFileStoreTable("t1");
        FileStoreTable table2 = getFileStoreTable("t2");

        statement.executeUpdate("USE " + databaseName);

        statement.executeUpdate("INSERT INTO t1 VALUES (1, 'one')");
        statement.executeUpdate("INSERT INTO t2 VALUES (2, 'two', 20, 200)");
        statement.executeUpdate("INSERT INTO t1 VALUES (3, 'three')");
        statement.executeUpdate("INSERT INTO t2 VALUES (4, 'four', 40, 400)");
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

        // Create new tables at runtime. The Flink job is guaranteed to at incremental
        //    sync phase, because the newly added table will not be captured in snapshot
        //    phase.
        Map<String, List<Tuple2<Integer, String>>> recordsMap = new HashMap<>();
        List<String> newTablePrimaryKeys = Collections.singletonList("k");
        RowType newTableRowType =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k", "v1"});
        int newTableCount = 0;
        String newTableName = getNewTableName(newTableCount);

        createNewTable(statement, newTableName);
        statement.executeUpdate(
                String.format("INSERT INTO `%s`.`t2` VALUES (8, 'eight', 80, 800)", databaseName));
        List<Tuple2<Integer, String>> newTableRecords = getNewTableRecords();
        recordsMap.put(newTableName, newTableRecords);
        List<String> newTableExpected = getNewTableExpected(newTableRecords);
        insertRecordsIntoNewTable(statement, databaseName, newTableName, newTableRecords);

        // suspend the job and restart from savepoint
        if (testSavepointRecovery) {
            String savepoint =
                    client.stopWithSavepoint(
                                    false,
                                    tempDir.toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .join();
            assertThat(savepoint).isNotBlank();

            client =
                    buildSyncDatabaseActionWithNewlyAddedTables(
                            savepoint, databaseName, testSchemaChange);
            waitJobRunning(client);
        }

        // wait until table t2 contains the updated record, and then check
        //     for existence of first newly added table
        expected =
                Arrays.asList(
                        "+I[2, two, 20, 200]", "+I[4, four, 40, 400]", "+I[8, eight, 80, 800]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        FileStoreTable newTable = getFileStoreTable(newTableName);
        waitForResult(newTableExpected, newTable, newTableRowType, newTablePrimaryKeys);

        for (newTableCount = 1; newTableCount < newlyAddedTableCount; ++newTableCount) {
            // create new table
            newTableName = getNewTableName(newTableCount);
            createNewTable(statement, newTableName);

            Thread.sleep(5000L);

            // insert records
            newTableRecords = getNewTableRecords();
            recordsMap.put(newTableName, newTableRecords);
            insertRecordsIntoNewTable(statement, databaseName, newTableName, newTableRecords);
            newTable = getFileStoreTable(newTableName);
            newTableExpected = getNewTableExpected(newTableRecords);
            waitForResult(newTableExpected, newTable, newTableRowType, newTablePrimaryKeys);
        }

        ThreadLocalRandom random = ThreadLocalRandom.current();

        // pick a random newly added table and insert records
        int pick = random.nextInt(newlyAddedTableCount);
        String tableName = getNewTableName(pick);
        List<Tuple2<Integer, String>> records = recordsMap.get(tableName);
        records.add(Tuple2.of(80, "eighty"));
        newTable = getFileStoreTable(tableName);
        newTableExpected = getNewTableExpected(records);
        statement.executeUpdate(
                String.format(
                        "INSERT INTO `%s`.`%s` VALUES (80, 'eighty')", databaseName, tableName));

        waitForResult(newTableExpected, newTable, newTableRowType, newTablePrimaryKeys);

        // test schema change
        if (testSchemaChange) {
            pick = random.nextInt(newlyAddedTableCount);
            tableName = getNewTableName(pick);
            records = recordsMap.get(tableName);

            statement.executeUpdate(
                    String.format(
                            "ALTER TABLE `%s`.`%s` ADD COLUMN v2 INT", databaseName, tableName));
            statement.executeUpdate(
                    String.format(
                            "INSERT INTO `%s`.`%s` VALUES (100, 'hundred', 10000)",
                            databaseName, tableName));

            List<String> expectedRecords =
                    records.stream()
                            .map(tuple -> String.format("+I[%d, %s, NULL]", tuple.f0, tuple.f1))
                            .collect(Collectors.toList());
            expectedRecords.add("+I[100, hundred, 10000]");

            newTable = getFileStoreTable(tableName);
            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                            },
                            new String[] {"k", "v1", "v2"});
            waitForResult(expectedRecords, newTable, rowType, newTablePrimaryKeys);

            // test that catalog loader works
            assertThat(getFileStoreTable(tableName).options())
                    .containsEntry("alter-table-test", "true");
        }
    }

    private List<String> getNewTableExpected(List<Tuple2<Integer, String>> newTableRecords) {
        return newTableRecords.stream()
                .map(tuple -> String.format("+I[%d, %s]", tuple.f0, tuple.f1))
                .collect(Collectors.toList());
    }

    private List<Tuple2<Integer, String>> getNewTableRecords() {
        List<Tuple2<Integer, String>> records = new LinkedList<>();
        int count = ThreadLocalRandom.current().nextInt(10) + 1;
        for (int i = 0; i < count; i++) {
            records.add(Tuple2.of(i, "varchar_" + i));
        }
        return records;
    }

    private void insertRecordsIntoNewTable(
            Statement statement,
            String databaseName,
            String newTableName,
            List<Tuple2<Integer, String>> newTableRecords)
            throws SQLException {
        String sql =
                String.format(
                        "INSERT INTO `%s`.`%s` VALUES %s",
                        databaseName,
                        newTableName,
                        newTableRecords.stream()
                                .map(tuple -> String.format("(%d, '%s')", tuple.f0, tuple.f1))
                                .collect(Collectors.joining(", ")));
        statement.executeUpdate(sql);
    }

    private String getNewTableName(int newTableCount) {
        return "t_new_table_" + newTableCount;
    }

    private void createNewTable(Statement statement, String newTableName) throws SQLException {
        statement.executeUpdate(
                String.format(
                        "CREATE TABLE %s (k INT, v1 VARCHAR(10), PRIMARY KEY (k))", newTableName));
    }

    private JobClient buildSyncDatabaseActionWithNewlyAddedTables(
            String databaseName, boolean testSchemaChange) throws Exception {
        return buildSyncDatabaseActionWithNewlyAddedTables(null, databaseName, testSchemaChange);
    }

    private JobClient buildSyncDatabaseActionWithNewlyAddedTables(
            String savepointPath, String databaseName, boolean testSchemaChange) throws Exception {

        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", databaseName);
        mySqlConfig.put("scan.incremental.snapshot.chunk.size", "1");

        Map<String, String> catalogConfig =
                testSchemaChange
                        ? Collections.singletonMap(
                                CatalogOptions.METASTORE.key(), "test-alter-table")
                        : Collections.emptyMap();

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withCatalogConfig(catalogConfig)
                        .withTableConfig(getBasicTableConfig())
                        .includingTables("t.+")
                        .withMode(COMBINED.configString())
                        .build();
        action.withStreamExecutionEnvironment(env).build();

        if (Objects.nonNull(savepointPath)) {
            StreamGraph streamGraph = env.getStreamGraph();
            JobGraph jobGraph = streamGraph.getJobGraph();
            jobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath, true));
            return env.executeAsync(streamGraph);
        }
        return env.executeAsync();
    }

    @Test
    @Timeout(240)
    public void testSyncManyTableWithLimitedMemory() throws Exception {
        String databaseName = "many_table_sync_test";
        int newTableCount = 100;
        int recordsCount = 100;
        List<Tuple2<Integer, String>> newTableRecords = new ArrayList<>();
        List<String> expectedRecords = new ArrayList<>();

        for (int i = 0; i < recordsCount; i++) {
            newTableRecords.add(Tuple2.of(i, "string_" + i));
            expectedRecords.add(String.format("+I[%d, %s]", i, "string_" + i));
        }

        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", databaseName);
        mySqlConfig.put("scan.incremental.snapshot.chunk.size", "1");

        Map<String, String> tableConfig = getBasicTableConfig();
        tableConfig.put("sink.parallelism", "1");
        tableConfig.put(CoreOptions.WRITE_BUFFER_SIZE.key(), "4 mb");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(tableConfig)
                        .withMode(COMBINED.configString())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE " + databaseName);
            // wait checkpointing to step into incremental phase
            Thread.sleep(2_000);

            List<String> tables = new ArrayList<>();
            tables.add("a");
            for (int i = 0; i < newTableCount; i++) {
                tables.add("t" + i);
                Thread thread = new Thread(new SyncNewTableJob(i, statement, newTableRecords));
                thread.start();
            }

            waitingTables(tables);

            RowType newTableRowType =
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                            new String[] {"k", "v1"});
            List<String> newTablePrimaryKeys = Collections.singletonList("k");
            for (int i = 0; i < newTableCount; i++) {
                FileStoreTable newTable = getFileStoreTable("t" + i);
                waitForResult(expectedRecords, newTable, newTableRowType, newTablePrimaryKeys);
            }
        }
    }

    @Test
    @Timeout(60)
    public void testSyncMultipleShards() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();

        // test table list
        mySqlConfig.put(
                "database-name",
                ThreadLocalRandom.current().nextBoolean()
                        ? "database_shard_.*"
                        : "database_shard_1|database_shard_2");

        MultiTablesSinkMode mode = ThreadLocalRandom.current().nextBoolean() ? DIVIDED : COMBINED;
        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withMode(mode.configString())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            // test insert into t1
            statement.executeUpdate("INSERT INTO database_shard_1.t1 VALUES (1, 'db1_1')");
            statement.executeUpdate("INSERT INTO database_shard_1.t1 VALUES (2, 'db1_2')");

            statement.executeUpdate("INSERT INTO database_shard_2.t1 VALUES (3, 'db2_3', 300)");
            statement.executeUpdate("INSERT INTO database_shard_2.t1 VALUES (4, 'db2_4', 400)");

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

            // test schema evolution of t2
            statement.executeUpdate("ALTER TABLE database_shard_1.t2 ADD COLUMN v2 INT");
            statement.executeUpdate("ALTER TABLE database_shard_2.t2 ADD COLUMN v3 VARCHAR(10)");
            statement.executeUpdate("INSERT INTO database_shard_1.t2 VALUES (1, 1.1, 1)");
            statement.executeUpdate("INSERT INTO database_shard_1.t2 VALUES (2, 2.2, 2)");
            statement.executeUpdate("INSERT INTO database_shard_2.t2 VALUES (3, 3.3, 'db2_3')");
            statement.executeUpdate("INSERT INTO database_shard_2.t2 VALUES (4, 4.4, 'db2_4')");
            table = getFileStoreTable("t2");
            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.BIGINT().notNull(),
                                DataTypes.DOUBLE(),
                                DataTypes.INT(),
                                DataTypes.VARCHAR(10)
                            },
                            new String[] {"k", "v1", "v2", "v3"});
            waitForResult(
                    Arrays.asList(
                            "+I[1, 1.1, 1, NULL]",
                            "+I[2, 2.2, 2, NULL]",
                            "+I[3, 3.3, NULL, db2_3]",
                            "+I[4, 4.4, NULL, db2_4]"),
                    table,
                    rowType,
                    Collections.singletonList("k"));

            // test that database_shard_2.t3 won't be synchronized
            statement.executeUpdate(
                    "INSERT INTO database_shard_2.t3 VALUES (1, 'db2_1'), (2, 'db2_2')");
            statement.executeUpdate(
                    "INSERT INTO database_shard_1.t3 VALUES (3, 'db1_3'), (4, 'db1_4')");
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

            // test newly created table
            if (mode == COMBINED) {
                statement.executeUpdate(
                        "CREATE TABLE database_shard_1.t4 (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
                statement.executeUpdate("INSERT INTO database_shard_1.t4 VALUES (1, 'db1_1')");

                statement.executeUpdate(
                        "CREATE TABLE database_shard_2.t4 (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
                statement.executeUpdate("INSERT INTO database_shard_2.t4 VALUES (2, 'db2_2')");

                waitingTables("t4");

                table = getFileStoreTable("t4");
                rowType =
                        RowType.of(
                                new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                                new String[] {"k", "v1"});
                waitForResult(
                        Arrays.asList("+I[1, db1_1]", "+I[2, db2_2]"),
                        table,
                        rowType,
                        Collections.singletonList("k"));
            }
        }
    }

    @Test
    @Timeout(60)
    public void testSyncMultipleShardsWithoutMerging() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "without_merging_shard_.*");

        MultiTablesSinkMode mode = ThreadLocalRandom.current().nextBoolean() ? DIVIDED : COMBINED;
        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .mergeShards(false)
                        .withMode(mode.configString())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            Thread.sleep(5_000);

            assertExactlyExistTables(
                    "without_merging_shard_1_t1",
                    "without_merging_shard_1_t2",
                    "without_merging_shard_2_t1");

            // test insert into without_merging_shard_1.t1
            statement.executeUpdate(
                    "INSERT INTO without_merging_shard_1.t1 VALUES (1, 'db1_1'), (2, 'db1_2')");
            FileStoreTable table = getFileStoreTable("without_merging_shard_1_t1");
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
            statement.executeUpdate(
                    "INSERT INTO without_merging_shard_2.t1 VALUES (3, 'db2_3', 300), (4, 'db2_4', 400)");
            table = getFileStoreTable("without_merging_shard_2_t1");
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

            // test schema evolution of without_merging_shard_1.t2
            statement.executeUpdate("ALTER TABLE without_merging_shard_1.t2 ADD COLUMN v2 DOUBLE");
            statement.executeUpdate(
                    "INSERT INTO without_merging_shard_1.t2 VALUES (1, 'Apache', 1.1)");
            statement.executeUpdate(
                    "INSERT INTO without_merging_shard_1.t2 VALUES (2, 'Paimon', 2.2)");
            table = getFileStoreTable("without_merging_shard_1_t2");
            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.DOUBLE()
                            },
                            new String[] {"k", "v1", "v2"});
            waitForResult(
                    Arrays.asList("+I[1, Apache, 1.1]", "+I[2, Paimon, 2.2]"),
                    table,
                    rowType,
                    Collections.singletonList("k"));

            // test newly created table
            if (mode == COMBINED) {
                statement.executeUpdate(
                        "CREATE TABLE without_merging_shard_1.t3 (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
                statement.executeUpdate(
                        "INSERT INTO without_merging_shard_1.t3 VALUES (1, 'test')");

                statement.executeUpdate(
                        "CREATE TABLE without_merging_shard_2.t3 (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
                statement.executeUpdate(
                        "INSERT INTO without_merging_shard_2.t3 VALUES (2, 'test')");

                waitingTables("without_merging_shard_1_t3", "without_merging_shard_2_t3");

                table = getFileStoreTable("without_merging_shard_1_t3");
                rowType =
                        RowType.of(
                                new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                                new String[] {"k", "v1"});
                waitForResult(
                        Collections.singletonList("+I[1, test]"),
                        table,
                        rowType,
                        Collections.singletonList("k"));

                table = getFileStoreTable("without_merging_shard_2_t3");
                waitForResult(
                        Collections.singletonList("+I[2, test]"),
                        table,
                        rowType,
                        Collections.singletonList("k"));
            }
        }
    }

    @Test
    public void testMonitoredAndExcludedTablesWithMering() throws Exception {
        // create an incompatible table named t2
        createFileStoreTable(
                "t2",
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"k", "v1"}),
                Collections.emptyList(),
                Collections.singletonList("k"),
                Collections.emptyList(),
                Collections.emptyMap());

        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "monitored_and_excluded_shard_.*");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .ignoreIncompatible(true)
                        .withMode(COMBINED.configString())
                        .build();
        action.build();

        assertThat(action.monitoredTables())
                .containsOnly(
                        Identifier.create("monitored_and_excluded_shard_1", "t1"),
                        Identifier.create("monitored_and_excluded_shard_1", "t3"),
                        Identifier.create("monitored_and_excluded_shard_2", "t1"));

        assertThat(action.excludedTables())
                .containsOnly(
                        // t2 is merged, so all shards will be excluded
                        Identifier.create("monitored_and_excluded_shard_1", "t2"),
                        Identifier.create("monitored_and_excluded_shard_2", "t2"),
                        // non pk table
                        Identifier.create("monitored_and_excluded_shard_2", "t3"));
    }

    @Test
    @Timeout(60)
    public void testNewlyAddedTablesOptionsChange() throws Exception {
        try (Statement statement = getStatement()) {
            statement.execute("USE " + "newly_added_tables_option_schange");
            statement.executeUpdate("INSERT INTO t1 VALUES (1, 'one')");
            statement.executeUpdate("INSERT INTO t1 VALUES (3, 'three')");
        }

        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "newly_added_tables_option_schange");
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", "1");
        tableConfig.put("sink.parallelism", "1");

        MySqlSyncDatabaseAction action1 =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(tableConfig)
                        .withMode(COMBINED.configString())
                        .build();

        JobClient jobClient = runActionWithDefaultEnv(action1);

        waitingTables("t1");
        jobClient.cancel();

        tableConfig.put("sink.savepoint.auto-tag", "true");
        tableConfig.put("tag.num-retained-max", "5");
        tableConfig.put("tag.automatic-creation", "process-time");
        tableConfig.put("tag.creation-period", "hourly");
        tableConfig.put("tag.creation-delay", "600000");
        tableConfig.put("snapshot.time-retained", "1h");
        tableConfig.put("snapshot.num-retained.min", "5");
        tableConfig.put("snapshot.num-retained.max", "10");
        tableConfig.put("changelog-producer", "input");

        try (Statement statement = getStatement()) {
            statement.execute("USE " + "newly_added_tables_option_schange");
            statement.executeUpdate("CREATE TABLE t2 (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
            statement.executeUpdate("INSERT INTO t2 VALUES (1, 'Hi')");
        }

        MySqlSyncDatabaseAction action2 =
                syncDatabaseActionBuilder(mySqlConfig).withTableConfig(tableConfig).build();
        runActionWithDefaultEnv(action2);
        waitingTables("t2");

        Map<String, String> tableOptions = getFileStoreTable("t2").options();
        assertThat(tableOptions).containsAllEntriesOf(tableConfig).containsKey("path");
    }

    @Test
    public void testCatalogAndTableConfig() {
        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(getBasicMySqlConfig())
                        .withCatalogConfig(Collections.singletonMap("catalog-key", "catalog-value"))
                        .withTableConfig(Collections.singletonMap("table-key", "table-value"))
                        .build();

        assertThat(action.catalogConfig()).containsEntry("catalog-key", "catalog-value");
        assertThat(action.tableConfig())
                .containsExactlyEntriesOf(Collections.singletonMap("table-key", "table-value"));
    }

    @Test
    @Timeout(60)
    public void testMetadataColumns() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "metadata");

        MultiTablesSinkMode mode = ThreadLocalRandom.current().nextBoolean() ? DIVIDED : COMBINED;
        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withMode(mode.configString())
                        .withMetadataColumn(Arrays.asList("table_name", "database_name"))
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.executeUpdate("INSERT INTO metadata.t1 VALUES (1, 'db1_1')");
            statement.executeUpdate("INSERT INTO metadata.t1 VALUES (2, 'db1_2')");

            statement.executeUpdate("INSERT INTO metadata.t1 VALUES (3, 'db2_3')");
            statement.executeUpdate("INSERT INTO metadata.t1 VALUES (4, 'db2_4')");

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

            statement.executeUpdate("INSERT INTO metadata.t2 VALUES (1, 'db1_1')");
            statement.executeUpdate("INSERT INTO metadata.t2 VALUES (2, 'db1_2')");
            statement.executeUpdate("INSERT INTO metadata.t2 VALUES (3, 'db1_3')");
            statement.executeUpdate("INSERT INTO metadata.t2 VALUES (4, 'db1_4')");
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

            // test newly created table
            if (mode == COMBINED) {
                statement.execute("USE " + "metadata");
                statement.executeUpdate("CREATE TABLE t3 (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
                statement.executeUpdate("INSERT INTO t3 VALUES (1, 'Hi')");
                waitingTables("t3");
                table = getFileStoreTable("t3");
                waitForResult(
                        Collections.singletonList("+I[1, Hi, t3, metadata]"),
                        table,
                        rowType,
                        Collections.singletonList("k"));
            }
        }
    }

    @Test
    @Timeout(60)
    public void testSpecifyKeys() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "test_specify_keys");

        MultiTablesSinkMode mode = ThreadLocalRandom.current().nextBoolean() ? DIVIDED : COMBINED;
        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withMode(mode.configString())
                        .withPartitionKeys("part")
                        .withPrimaryKeys("k", "part")
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE test_specify_keys");
            testSpecifyKeysVerify1("t1", statement);
            testSpecifyKeysVerify2("t2", statement);

            // test newly created table
            if (mode == COMBINED) {
                statement.executeUpdate(
                        "CREATE TABLE t3 (k INT, part INT, v1 VARCHAR(10), PRIMARY KEY (k))");
                statement.executeUpdate("CREATE TABLE t4 (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
                waitingTables("t3", "t4");
                testSpecifyKeysVerify1("t3", statement);
                testSpecifyKeysVerify2("t4", statement);
            }
        }
    }

    private void testSpecifyKeysVerify1(String tableName, Statement statement) throws Exception {
        FileStoreTable table = getFileStoreTable(tableName);
        assertThat(table.partitionKeys()).containsExactly("part");
        assertThat(table.primaryKeys()).containsExactly("k", "part");

        statement.executeUpdate("INSERT INTO " + tableName + " VALUES(1, 1, 'A')");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10),
                        },
                        new String[] {"k", "part", "v1"});
        waitForResult(
                Collections.singletonList("+I[1, 1, A]"),
                table,
                rowType,
                Arrays.asList("k", "part"));
    }

    private void testSpecifyKeysVerify2(String tableName, Statement statement) throws Exception {
        FileStoreTable table = getFileStoreTable(tableName);
        assertThat(table.partitionKeys()).isEmpty();
        assertThat(table.primaryKeys()).containsExactly("k");

        statement.executeUpdate("INSERT INTO " + tableName + " VALUES(1, 'A')");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10),
                        },
                        new String[] {"k", "v1"});
        waitForResult(
                Collections.singletonList("+I[1, A]"),
                table,
                rowType,
                Collections.singletonList("k"));
    }

    private class SyncNewTableJob implements Runnable {

        private final int ith;
        private final Statement statement;
        private final List<Tuple2<Integer, String>> records;

        SyncNewTableJob(int ith, Statement statement, List<Tuple2<Integer, String>> records) {
            this.ith = ith;
            this.statement = statement;
            this.records = records;
        }

        @Override
        public void run() {
            String newTableName = "t" + ith;
            try {
                createNewTable(statement, newTableName);
                String sql =
                        String.format(
                                "INSERT INTO %s VALUES %s",
                                newTableName,
                                records.stream()
                                        .map(
                                                tuple ->
                                                        String.format(
                                                                "(%d, '%s')", tuple.f0, tuple.f1))
                                        .collect(Collectors.joining(", ")));
                statement.executeUpdate(sql);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
