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

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link SqlServerSyncTableAction}. */
public class SqlServerSyncTableActionITCase extends SqlServerActionITCaseBase {

    private static final String DATABASE_NAME = "paimon_sync_table";

    private static AtomicLong seq = new AtomicLong(0);

    @BeforeAll
    public static void startContainers() {
        MSSQL_SERVER_CONTAINER.withInitScript("sqlserver/sync_table_setup.sql");
        start();
    }

    @Test
    @Timeout(300)
    @Disabled
    public void testSchemaEvolution() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put(SqlServerSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        sqlServerConfig.put(SqlServerSourceOptions.TABLE_NAME.key(), "schema_evolution_\\d+");

        SqlServerSyncTableAction action =
                syncTableActionBuilder(sqlServerConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.METASTORE.key(), "test-alter-table"))
                        .withTableConfig(getBasicTableConfig())
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
                        .build();
        runActionWithDefaultEnv(action);

        checkTableSchema(
                "[{\"id\":0,\"name\":\"pt\",\"type\":\"INT NOT NULL\"},"
                        + "{\"id\":1,\"name\":\"_id\",\"type\":\"INT NOT NULL\"},"
                        + "{\"id\":2,\"name\":\"v1\",\"type\":\"VARCHAR(10)\"}]");

        try (Statement statement = getStatement()) {
            testSchemaEvolutionImpl(statement);
        }
    }

    private void testSchemaEvolutionImpl(Statement statement) throws Exception {
        FileStoreTable table = getFileStoreTable();
        statement.execute("USE " + DATABASE_NAME);
        statement.executeUpdate("INSERT INTO schema_evolution_1 VALUES (1, 1, 'one')");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_2 VALUES (1, 2, 'two'), (2, 4, 'four')");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"pt", "_id", "v1"});
        List<String> primaryKeys = Arrays.asList("pt", "_id");
        List<String> expected = Arrays.asList("+I[1, 1, one]", "+I[1, 2, two]", "+I[2, 4, four]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD v2 INT");
        enableTableCdc(
                statement,
                "schema_evolution_1",
                String.format("%s_%s", "schema_evolution_1", seq.incrementAndGet()));
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (2, 3, 'three', 30), (1, 5, 'five', 50)");

        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD v2 INT");
        enableTableCdc(
                statement,
                "schema_evolution_2",
                String.format("%s_%s", "schema_evolution_2", seq.incrementAndGet()));

        statement.executeUpdate("INSERT INTO schema_evolution_2 VALUES (1, 6, 'six', 60)");
        statement.executeUpdate("UPDATE schema_evolution_2 SET v1 = 'second' WHERE _id = 2");

        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10),
                            DataTypes.INT()
                        },
                        new String[] {"pt", "_id", "v1", "v2"});
        expected =
                Arrays.asList(
                        "+I[1, 1, one, NULL]",
                        "+I[1, 2, second, NULL]",
                        "+I[2, 3, three, 30]",
                        "+I[2, 4, four, NULL]",
                        "+I[1, 5, five, 50]",
                        "+I[1, 6, six, 60]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ALTER COLUMN v2 BIGINT");
        enableTableCdc(
                statement,
                "schema_evolution_1",
                String.format("%_%", "schema_evolution_1", seq.incrementAndGet()));

        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (2, 7, 'seven', 70000000000)");
        statement.executeUpdate("DELETE FROM schema_evolution_1 WHERE _id = 5");
        statement.executeUpdate("UPDATE schema_evolution_1 SET v2 = 30000000000 WHERE _id = 3");

        statement.executeUpdate("ALTER TABLE schema_evolution_2 ALTER COLUMN v2 BIGINT");

        statement.executeUpdate(
                "INSERT INTO schema_evolution_2 VALUES (2, 8, 'eight', 80000000000)");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10),
                            DataTypes.BIGINT()
                        },
                        new String[] {"pt", "_id", "v1", "v2"});
        expected =
                Arrays.asList(
                        "+I[1, 1, one, NULL]",
                        "+I[1, 2, second, NULL]",
                        "+I[2, 3, three, 30000000000]",
                        "+I[2, 4, four, NULL]",
                        "+I[1, 6, six, 60]",
                        "+I[2, 7, seven, 70000000000]",
                        "+I[2, 8, eight, 80000000000]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD v3 NUMERIC(8, 3)");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD v4 BYTEA");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD v5 FLOAT");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 ALTER COLUMN v1 VARCHAR(20)");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (1, 9, 'nine', 90000000000, 99999.999, 'nine.bin', 9.9)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD v3 NUMERIC(8, 3)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD v4 BYTEA");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD v5 FLOAT");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ALTER COLUMN v1 VARCHAR(20)");
        statement.executeUpdate(
                "UPDATE schema_evolution_2 SET v1 = 'very long string' WHERE _id = 8");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(20),
                            DataTypes.BIGINT(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.VARBINARY(10),
                            DataTypes.FLOAT()
                        },
                        new String[] {"pt", "_id", "v1", "v2", "v3", "v4", "v5"});
        expected =
                Arrays.asList(
                        "+I[1, 1, one, NULL, NULL, NULL, NULL]",
                        "+I[1, 2, second, NULL, NULL, NULL, NULL]",
                        "+I[2, 3, three, 30000000000, NULL, NULL, NULL]",
                        "+I[2, 4, four, NULL, NULL, NULL, NULL]",
                        "+I[1, 6, six, 60, NULL, NULL, NULL]",
                        "+I[2, 7, seven, 70000000000, NULL, NULL, NULL]",
                        "+I[2, 8, very long string, 80000000000, NULL, NULL, NULL]",
                        "+I[1, 9, nine, 90000000000, 99999.999, [110, 105, 110, 101, 46, 98, 105, 110], 9.9]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ALTER COLUMN v5 DOUBLE PRECISION");
        statement.executeUpdate(
                "UPDATE schema_evolution_1 SET v4 = 'nine.bin.long', v5 = 9.00000000009 WHERE _id = 9");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ALTER COLUMN v5 DOUBLE PRECISION");
        statement.executeUpdate(
                "UPDATE schema_evolution_2 SET v4 = 'four.bin.long', v5 = 4.00000000004 WHERE _id = 4");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(20),
                            DataTypes.BIGINT(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.VARBINARY(20),
                            DataTypes.DOUBLE()
                        },
                        new String[] {"pt", "_id", "v1", "v2", "v3", "v4", "v5"});
        expected =
                Arrays.asList(
                        "+I[1, 1, one, NULL, NULL, NULL, NULL]",
                        "+I[1, 2, second, NULL, NULL, NULL, NULL]",
                        "+I[2, 3, three, 30000000000, NULL, NULL, NULL]",
                        "+I[2, 4, four, NULL, NULL, [102, 111, 117, 114, 46, 98, 105, 110, 46, 108, 111, 110, 103], 4.00000000004]",
                        "+I[1, 6, six, 60, NULL, NULL, NULL]",
                        "+I[2, 7, seven, 70000000000, NULL, NULL, NULL]",
                        "+I[2, 8, very long string, 80000000000, NULL, NULL, NULL]",
                        "+I[1, 9, nine, 90000000000, 99999.999, [110, 105, 110, 101, 46, 98, 105, 110, 46, 108, 111, 110, 103], 9.00000000009]");
        waitForResult(expected, table, rowType, primaryKeys);

        // test that catalog loader works
        assertThat(getFileStoreTable().options()).containsEntry("alter-table-test", "true");
    }

    @Test
    @Timeout(60)
    public void testSyncSqlServerTable() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", DATABASE_NAME);
        sqlServerConfig.put("table-name", "schema_evolution_\\d+");

        SqlServerSyncTableAction action =
                syncTableActionBuilder(sqlServerConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.METASTORE.key(), "test-alter-table"))
                        .withTableConfig(getBasicTableConfig())
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
                        .build();
        runActionWithDefaultEnv(action);

        checkTableSchema(
                "[{\"id\":0,\"name\":\"pt\",\"type\":\"INT NOT NULL\"},"
                        + "{\"id\":1,\"name\":\"_id\",\"type\":\"INT NOT NULL\"},"
                        + "{\"id\":2,\"name\":\"v1\",\"type\":\"VARCHAR(10)\"}]");

        try (Statement statement = getStatement()) {
            testSyncSqlServerTableImpl(statement);
        }
    }

    private void checkTableSchema(String excepted) throws Exception {
        FileStoreTable table = getFileStoreTable();
        assertThat(JsonSerdeUtil.toFlatJson(table.schema().fields())).isEqualTo(excepted);
    }

    private void testSyncSqlServerTableImpl(Statement statement) throws Exception {
        FileStoreTable table = getFileStoreTable();
        statement.executeUpdate("USE " + DATABASE_NAME);
        statement.executeUpdate("INSERT INTO schema_evolution_1 VALUES (1, 1, 'one')");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_2 VALUES (1, 2, 'two'), (2, 4, 'four')");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"pt", "_id", "v1"});
        List<String> primaryKeys = Arrays.asList("pt", "_id");
        List<String> expected = Arrays.asList("+I[1, 1, one]", "+I[1, 2, two]", "+I[2, 4, four]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(120)
    public void testAllTypes() throws Exception {
        // the first round checks for table creation
        // the second round checks for running the action on an existing table
        for (int i = 0; i < 2; i++) {
            testAllTypesOnce();
        }
    }

    private void testAllTypesOnce() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", DATABASE_NAME);
        sqlServerConfig.put("table-name", "all_types_table");

        SqlServerSyncTableAction action =
                syncTableActionBuilder(sqlServerConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
                        .build();
        JobClient client = runActionWithDefaultEnv(action);
        testAllTypesImpl();
        client.cancel().get();
    }

    private void testAllTypesImpl() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), // _id
                            DataTypes.DECIMAL(2, 1).notNull(), // pt
                            DataTypes.BOOLEAN(), // _bit1
                            DataTypes.TINYINT(), // _tinyint
                            DataTypes.SMALLINT(), // _smallint
                            DataTypes.INT(), // _int
                            DataTypes.BIGINT(), // _bigint
                            DataTypes.FLOAT(), // _float
                            DataTypes.DOUBLE(), // _real
                            DataTypes.DECIMAL(8, 0), // _numeric
                            DataTypes.DECIMAL(8, 3), // _numeric8_3
                            DataTypes.DECIMAL(8, 0), // _decimal
                            DataTypes.DECIMAL(38, 10), // _big_decimal
                            DataTypes.DECIMAL(10, 4), // _smallmoney
                            DataTypes.DECIMAL(19, 4), // _money
                            DataTypes.DECIMAL(19, 4), // _big_money
                            DataTypes.DATE(), // _date
                            DataTypes.TIME(3), // _time3
                            DataTypes.TIME(6), // _time6
                            DataTypes.TIME(7), // _time7
                            DataTypes.TIMESTAMP(3), // _datetime
                            DataTypes.TIMESTAMP(0), // _smalldatetime
                            DataTypes.TIMESTAMP(7), // _datetime2
                            DataTypes.TIMESTAMP(3), // _datetime2_3
                            DataTypes.TIMESTAMP(6), // _datetime2_6
                            DataTypes.TIMESTAMP(7), // _datetime2_7
                            DataTypes.CHAR(10), // _char
                            DataTypes.VARCHAR(20), // _varchar
                            DataTypes.STRING(), // _text
                            DataTypes.VARCHAR(10), // _nchar
                            DataTypes.VARCHAR(20), // _nvarchar
                            DataTypes.STRING(), // _ntext
                            DataTypes.STRING(), // _xml
                            DataTypes.STRING() // _datetimeoffset
                        },
                        new String[] {
                            "_id",
                            "pt",
                            "_bit1",
                            "_tinyint",
                            "_smallint",
                            "_int",
                            "_bigint",
                            "_float",
                            "_real",
                            "_numeric",
                            "_numeric8_3",
                            "_decimal",
                            "_big_decimal",
                            "_smallmoney",
                            "_money",
                            "_big_money",
                            "_date",
                            "_time3",
                            "_time6",
                            "_time7",
                            "_datetime",
                            "_smalldatetime",
                            "_datetime2",
                            "_datetime2_3",
                            "_datetime2_6",
                            "_datetime2_7",
                            "_char",
                            "_varchar",
                            "_text",
                            "_nchar",
                            "_nvarchar",
                            "_ntext",
                            "_xml",
                            "_datetimeoffset"
                        });
        FileStoreTable table = getFileStoreTable();
        List<String> expected =
                Arrays.asList(
                        "+I[1, 1.1, true, 1, 1000, 1000000, 10000000000, 3.14159, 3.14, 12345678, 12345.678, 12345678, 12345.6789123456, 12345.6700, 12345678.0000, 12345.6789, 19439, 37815123, 37815123, 37815123, 2023-09-30T10:30:15.123, 2023-03-23T14:30, 2023-09-30T10:30:15.123456700, 2023-09-30T10:30:15.123, 2023-09-30T10:30:15.123456, 2023-09-30T10:30:15.123456700, Paimon, Apache Paimon, Apache Paimon SQLServer TEXT Test Data, Paimon    , Apache Paimon, Apache Paimon NTEXT Long Test Data, NULL, 2023-02-01T10:00:00+05:00]",
                        "+I[2, 2.2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL]");
        waitForResult(expected, table, rowType, Arrays.asList("pt", "_id"));
    }

    @Test
    public void testIncompatibleSqlServerTable() {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", DATABASE_NAME);
        sqlServerConfig.put("table-name", "incompatible_field_\\d+");
        SqlServerSyncTableAction action = syncTableActionBuilder(sqlServerConfig).build();
        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Column v1 have different types when merging schemas."));
    }

    @Test
    public void testIncompatiblePaimonTable() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", DATABASE_NAME);
        sqlServerConfig.put("table-name", "incompatible_pk_\\d+");
        createFileStoreTable(
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"a", "b", "c"}),
                Collections.emptyList(),
                Collections.singletonList("a"),
                new HashMap<>());

        SqlServerSyncTableAction action =
                syncTableActionBuilder(sqlServerConfig).withPrimaryKeys("a").build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Paimon schema and source table schema are not compatible."));
    }

    @Test
    public void testInvalidPrimaryKey() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", DATABASE_NAME);
        sqlServerConfig.put("table-name", "schema_evolution_\\d+");

        SqlServerSyncTableAction action =
                syncTableActionBuilder(sqlServerConfig).withPrimaryKeys("pk").build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Specified primary key 'pk' does not exist in source tables or computed columns [pt, _id, v1]."));
    }

    @Test
    public void testNoPrimaryKey() {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", DATABASE_NAME);
        sqlServerConfig.put("table-name", "incompatible_pk_\\d+");

        SqlServerSyncTableAction action = syncTableActionBuilder(sqlServerConfig).build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Primary keys are not specified. "
                                        + "Also, can't infer primary keys from source table schemas because "
                                        + "source tables have no primary keys or have different primary keys."));
    }

    @Test
    @Timeout(120)
    public void testComputedColumn() throws Exception {
        // the first round checks for table creation
        // the second round checks for running the action on an existing table
        for (int i = 0; i < 2; i++) {
            innerTestComputedColumn(i == 0);
        }
    }

    private void innerTestComputedColumn(boolean executeSqlServer) throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put("database-name", DATABASE_NAME);
        sqlServerConfig.put("table-name", "test_computed_column");

        List<String> computedColumnDefs =
                Arrays.asList(
                        "_year_date=year(_date)",
                        "_year_datetime=year(_datetime)",
                        "_month_date=month(_date)",
                        "_month_datetime=month(_datetime)",
                        "_day_date=day(_date)",
                        "_day_datetime=day(_datetime)",
                        "_hour_date=hour(_date)",
                        "_hour_datetime=hour(_datetime)",
                        "_date_format_date=date_format(_date,yyyy)",
                        "_date_format_datetime=date_format(_datetime,yyyy-MM-dd)",
                        "_substring_date1=substring(_date,2)",
                        "_truncate_date=trUNcate(pk,2)"); // test case-insensitive too

        SqlServerSyncTableAction action =
                syncTableActionBuilder(sqlServerConfig)
                        .withPartitionKeys("_year_date")
                        .withPrimaryKeys("pk", "_year_date")
                        .withComputedColumnArgs(computedColumnDefs)
                        .build();
        runActionWithDefaultEnv(action);

        if (executeSqlServer) {
            try (Statement statement = getStatement()) {
                statement.execute("USE " + DATABASE_NAME);
                statement.executeUpdate(
                        "INSERT INTO test_computed_column VALUES (1, '2023-03-23', '2022-01-01 14:30')");
                statement.executeUpdate(
                        "INSERT INTO test_computed_column VALUES (2, '2023-03-23', null)");
            }
        }

        FileStoreTable table = getFileStoreTable();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.DATE(),
                            DataTypes.TIMESTAMP(3),
                            DataTypes.INT().notNull(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.INT().notNull()
                        },
                        new String[] {
                            "pk",
                            "_date",
                            "_datetime",
                            "_year_date",
                            "_year_datetime",
                            "_month_date",
                            "_month_datetime",
                            "_day_date",
                            "_day_datetime",
                            "_hour_date",
                            "_hour_datetime",
                            "_date_format_date",
                            "_date_format_datetime",
                            "_substring_date1",
                            "_truncate_date"
                        });
        List<String> expected =
                Arrays.asList(
                        "+I[1, 19439, 2022-01-01T14:30, 2023, 2022, 3, 1, 23, 1, 0, 14, 2023, 2022-01-01, 23-03-23, 0]",
                        "+I[2, 19439, NULL, 2023, NULL, 3, NULL, 23, NULL, 0, NULL, 2023, NULL, 23-03-23, 2]");
        waitForResult(expected, table, rowType, Arrays.asList("pk", "_year_date"));
    }

    @Test
    @Timeout(60)
    public void testSyncShards() throws Exception {
        Map<String, String> postgresConfig = getBasicSqlServerConfig();

        // test table list
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String schemaPattern = random.nextBoolean() ? "shard_.+" : "shard_1|shard_2";
        String tblPattern = random.nextBoolean() ? "t.+" : "t1|t2";
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), "shard_schema");
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), schemaPattern);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), tblPattern);

        SqlServerSyncTableAction action =
                syncTableActionBuilder(postgresConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pk", "pt")
                        .withComputedColumnArgs("pt=substring(_date,5)")
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.execute("USE shard_schema");
            statement.executeUpdate("INSERT INTO shard_1.t1 VALUES (1, '2023-07-30')");
            statement.executeUpdate("INSERT INTO shard_1.t2 VALUES (2, '2023-07-30')");
            statement.executeUpdate("INSERT INTO shard_2.t1 VALUES (3, '2023-07-31')");
            statement.executeUpdate("INSERT INTO shard_2.t2 VALUES (4, '2023-07-31')");
        }

        FileStoreTable table = getFileStoreTable();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10),
                            DataTypes.STRING().notNull()
                        },
                        new String[] {"pk", "_date", "pt"});
        waitForResult(
                Arrays.asList(
                        "+I[1, 2023-07-30, 07-30]",
                        "+I[2, 2023-07-30, 07-30]",
                        "+I[3, 2023-07-31, 07-31]",
                        "+I[4, 2023-07-31, 07-31]"),
                table,
                rowType,
                Arrays.asList("pk", "pt"));
    }

    @Test
    @Timeout(60)
    public void testOptionsChange() throws Exception {
        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();

        sqlServerConfig.put(SqlServerSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        sqlServerConfig.put(SqlServerSourceOptions.TABLE_NAME.key(), "test_options_change");
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", "1");
        tableConfig.put("sink.parallelism", "1");

        SqlServerSyncTableAction action1 =
                syncTableActionBuilder(sqlServerConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pk", "pt")
                        .withComputedColumnArgs("pt=substring(_date,5)")
                        .withTableConfig(tableConfig)
                        .build();
        JobClient jobClient = runActionWithDefaultEnv(action1);
        try (Statement statement = getStatement()) {
            statement.execute("USE " + DATABASE_NAME);
            statement.executeUpdate(
                    "INSERT INTO test_options_change VALUES (1, '2023-03-23', '2022-01-01 14:30')");
            statement.executeUpdate(
                    "INSERT INTO test_options_change VALUES (2, '2023-03-23', null)");
        }
        waitingTables(tableName);
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

        SqlServerSyncTableAction action2 =
                syncTableActionBuilder(sqlServerConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pk", "pt")
                        .withComputedColumnArgs("pt=substring(_date,5)")
                        .withTableConfig(tableConfig)
                        .build();
        runActionWithDefaultEnv(action2);

        Map<String, String> dynamicOptions = getFileStoreTable().options();
        assertThat(dynamicOptions).containsAllEntriesOf(tableConfig);
    }

    @Test
    @Timeout(120)
    public void testMetadataColumns() throws Exception {
        try (Statement statement = getStatement()) {
            statement.execute("USE table_metadata");
            statement.executeUpdate("INSERT INTO test_metadata_columns VALUES (1, '2023-07-30')");
            statement.executeUpdate("INSERT INTO test_metadata_columns VALUES (2, '2023-07-30')");
        }

        Map<String, String> sqlServerConfig = getBasicSqlServerConfig();
        sqlServerConfig.put(SqlServerSourceOptions.DATABASE_NAME.key(), "table_metadata");
        sqlServerConfig.put(SqlServerSourceOptions.TABLE_NAME.key(), "test_metadata_columns");

        SqlServerSyncTableAction action =
                syncTableActionBuilder(sqlServerConfig)
                        .withPrimaryKeys("pk")
                        .withMetadataColumns("table_name", "schema_name", "database_name", "op_ts")
                        .build();

        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10),
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING().notNull(),
                            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull()
                        },
                        new String[] {
                            "pk", "_date", "table_name", "schema_name", "database_name", "op_ts"
                        });

        waitForResult(
                Arrays.asList(
                        "+I[1, 2023-07-30, test_metadata_columns, dbo, table_metadata, 1970-01-01T00:00]",
                        "+I[2, 2023-07-30, test_metadata_columns, dbo, table_metadata, 1970-01-01T00:00]"),
                table,
                rowType,
                Collections.singletonList("pk"));
    }

    @Test
    public void testCatalogAndTableConfig() {
        SqlServerSyncTableAction action =
                syncTableActionBuilder(getBasicSqlServerConfig())
                        .withCatalogConfig(Collections.singletonMap("catalog-key", "catalog-value"))
                        .withTableConfig(Collections.singletonMap("table-key", "table-value"))
                        .build();
        assertThat(action.catalogConfig()).containsEntry("catalog-key", "catalog-value");
        assertThat(action.tableConfig())
                .containsExactlyEntriesOf(Collections.singletonMap("table-key", "table-value"));
    }

    private FileStoreTable getFileStoreTable() throws Exception {
        return getFileStoreTable(tableName);
    }
}
