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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** IT cases for {@link MySqlSyncDatabaseAction}. */
public class MySqlSyncDatabaseActionITCase extends MySqlActionITCaseBase {

    private static final String DATABASE_NAME = "paimon_sync_database";

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        MySqlSyncDatabaseAction action =
                new MySqlSyncDatabaseAction(
                        mySqlConfig,
                        warehouse,
                        database,
                        false,
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        try (Connection conn =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(DATABASE_NAME),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            try (Statement statement = conn.createStatement()) {
                testSchemaEvolutionImpl(statement);
            }
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
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "my_table");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MySqlSyncDatabaseAction action =
                new MySqlSyncDatabaseAction(
                        mySqlConfig,
                        warehouse,
                        database,
                        false,
                        Collections.emptyMap(),
                        Collections.emptyMap());

        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> action.build(env),
                        "Expecting IllegalArgumentException");
        assertThat(e)
                .hasMessage(
                        "table-name cannot be set for mysql-sync-database. "
                                + "If you want to sync several MySQL tables into one Paimon table, "
                                + "use mysql-sync-table instead.");
    }

    @Test
    public void testInvalidDatabase() {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "invalid");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MySqlSyncDatabaseAction action =
                new MySqlSyncDatabaseAction(
                        mySqlConfig,
                        warehouse,
                        database,
                        false,
                        Collections.emptyMap(),
                        Collections.emptyMap());

        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> action.build(env),
                        "Expecting IllegalArgumentException");
        assertThat(e)
                .hasMessage(
                        "No tables found in MySQL database invalid, or MySQL database does not exist.");
    }

    @Test
    @Timeout(60)
    public void testIgnoreIncompatibleTables() throws Exception {
        // create an incompatible table
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
        catalog.createDatabase(database, true);
        Identifier identifier = Identifier.create(database, "incompatible");
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.STRING())
                        .column("v1", DataTypes.STRING())
                        .primaryKey("k")
                        .build();
        catalog.createTable(identifier, schema, false);

        // try synchronization
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "paimon_sync_database_ignore_incompatible");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        MySqlSyncDatabaseAction action =
                new MySqlSyncDatabaseAction(
                        mySqlConfig,
                        warehouse,
                        database,
                        true,
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        // validate `compatible` can be synchronized
        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(DATABASE_NAME),
                                MYSQL_CONTAINER.getUsername(),
                                MYSQL_CONTAINER.getPassword());
                Statement statement = conn.createStatement()) {
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
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
        catalog.createDatabase(database, true);
        Identifier identifier = Identifier.create(database, "test_prefix_t1_test_suffix");
        Schema schema =
                Schema.newBuilder()
                        .column("k1", DataTypes.INT().notNull())
                        .column("v0", DataTypes.VARCHAR(10))
                        .primaryKey("k1")
                        .build();
        catalog.createTable(identifier, schema, false);

        // try synchronization
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "paimon_sync_database_affix");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        MySqlSyncDatabaseAction action =
                new MySqlSyncDatabaseAction(
                        mySqlConfig,
                        warehouse,
                        database,
                        false,
                        "test_prefix_",
                        "_test_suffix",
                        null,
                        null,
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(DATABASE_NAME),
                                MYSQL_CONTAINER.getUsername(),
                                MYSQL_CONTAINER.getPassword());
                Statement statement = conn.createStatement()) {
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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        MySqlSyncDatabaseAction action =
                new MySqlSyncDatabaseAction(
                        mySqlConfig,
                        warehouse,
                        database,
                        false,
                        null,
                        null,
                        includingTables,
                        excludingTables,
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        // check paimon tables
        assertTableExists(existedTables);
        assertTableNotExists(notExistedTables);
    }

    @Test
    @Timeout(600)
    public void testNewlyAddedTableSingleTable() throws Exception {
        testNewlyAddedTable(1, false, false);
    }

    @Test
    @Timeout(600)
    public void testNewlyAddedTableMultipleTables() throws Exception {
        testNewlyAddedTable(3, false, false);
    }

    @Test
    @Timeout(600)
    public void testNewlyAddedTableSchemaChange() throws Exception {
        testNewlyAddedTable(1, false, true);
    }

    public void testNewlyAddedTable(
            int numOfNewlyAddedTables, boolean testSavepointRecovery, boolean testSchemaChange)
            throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        MySqlSyncDatabaseAction action =
                new MySqlSyncDatabaseAction(
                        mySqlConfig,
                        warehouse,
                        database,
                        false,
                        null,
                        null,
                        "t.+",
                        null,
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        try (Connection conn =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(DATABASE_NAME),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            try (Statement statement = conn.createStatement()) {
                testNewlyAddedTableImpl(
                        statement, numOfNewlyAddedTables, testSavepointRecovery, testSchemaChange);
            }
        }
    }

    private void testNewlyAddedTableImpl(
            Statement statement,
            int newlyAddedTableCount,
            boolean testSavepointRecovery,
            boolean testSchemaChange)
            throws Exception {
        FileStoreTable table1 = getFileStoreTable("t1");
        FileStoreTable table2 = getFileStoreTable("t2");

        statement.executeUpdate("USE paimon_sync_database");

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
        statement.executeUpdate("INSERT INTO t2 VALUES (8, 'eight', 80, 800)");
        List<Tuple2<Integer, String>> newTableRecords = getNewTableRecords(newTableCount);
        recordsMap.put(newTableName, newTableRecords);
        List<String> newTableExpected =
                newTableRecords.stream()
                        .map(tuple -> String.format("+I[%d, %s]", tuple.f0, tuple.f1))
                        .collect(Collectors.toList());
        insertRecordsIntoNewTable(statement, newTableName, newTableRecords);

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
            newTableRecords = getNewTableRecords(newTableCount);
            recordsMap.put(newTableName, newTableRecords);
            insertRecordsIntoNewTable(statement, newTableName, newTableRecords);
            newTable = getFileStoreTable(newTableName);
            waitForResult(newTableExpected, newTable, newTableRowType, newTablePrimaryKeys);
        }

        // test schema change
        if (testSchemaChange) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            int pick = random.nextInt(newlyAddedTableCount);
            String tableName = getNewTableName(pick);
            List<Tuple2<Integer, String>> records = recordsMap.get(tableName);
            statement.executeUpdate(String.format("ALTER TABLE %s ADD COLUMN v2 INT", tableName));
            statement.executeUpdate(
                    String.format("INSERT INTO %s VALUES (100, 'hundred', 10000)", tableName));

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
            ;
            waitForResult(expectedRecords, newTable, rowType, newTablePrimaryKeys);
        }
    }

    private List<Tuple2<Integer, String>> getNewTableRecords(int newTableCount) {
        List<Tuple2<Integer, String>> records = new LinkedList<>();
        int count = ThreadLocalRandom.current().nextInt(10) + 1;
        for (int i = 0; i < count; i++) {
            records.add(Tuple2.of(i, "varchar_" + i));
        }
        return records;
    }

    private void insertRecordsIntoNewTable(
            Statement statement, String newTableName, List<Tuple2<Integer, String>> newTableRecords)
            throws SQLException {
        String sql =
                String.format(
                        "INSERT INTO %s VALUES %s",
                        newTableName,
                        newTableRecords.stream()
                                .map(tuple -> String.format("(%d, '%s')", tuple.f0, tuple.f1))
                                .collect(Collectors.joining(", ")));
        statement.executeUpdate(sql);
    }

    @NotNull
    private String getNewTableName(int newTableCount) {
        return "t_new_table_" + newTableCount;
    }

    private void createNewTable(Statement statement, String newTableName) throws SQLException {
        statement.executeUpdate(
                String.format(
                        "CREATE TABLE %s (k INT, v1 VARCHAR(10), PRIMARY KEY (k))", newTableName));
    }

    private FileStoreTable getFileStoreTable(String tableName) throws Exception {
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
        Identifier identifier = Identifier.create(database, tableName);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private void assertTableExists(List<String> tableNames) {
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
        for (String tableName : tableNames) {
            Identifier identifier = Identifier.create(database, tableName);
            assertThat(catalog.tableExists(identifier)).isTrue();
        }
    }

    private void assertTableNotExists(List<String> tableNames) {
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
        for (String tableName : tableNames) {
            Identifier identifier = Identifier.create(database, tableName);
            assertThat(catalog.tableExists(identifier)).isFalse();
        }
    }
}
