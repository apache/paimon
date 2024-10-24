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

package org.apache.paimon.flink.action.cdc.postgres;

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link PostgresSyncTableAction}. */
public class PostgresSyncTableActionITCase extends PostgresActionITCaseBase {

    private static final String DATABASE_NAME = "paimon_sync_table";
    private static final String SCHEMA_NAME = "public";

    @BeforeAll
    public static void startContainers() {
        POSTGRES_CONTAINER.withSetupSQL("postgres/sync_table_setup.sql");
        start();
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), "schema_evolution_\\d+");

        PostgresSyncTableAction action =
                syncTableActionBuilder(postgresConfig)
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
                        + "{\"id\":2,\"name\":\"v1\",\"type\":\"STRING\"}]");

        try (Statement statement = getStatement(DATABASE_NAME)) {
            testSchemaEvolutionImpl(statement);
        }
    }

    private void checkTableSchema(String excepted) throws Exception {

        FileStoreTable table = getFileStoreTable();

        assertThat(JsonSerdeUtil.toFlatJson(table.schema().fields())).isEqualTo(excepted);
    }

    private void testSchemaEvolutionImpl(Statement statement) throws Exception {
        FileStoreTable table = getFileStoreTable();

        statement.executeUpdate("INSERT INTO schema_evolution_1 VALUES (1, 1, 'one')");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_2 VALUES (1, 2, 'two'), (2, 4, 'four')");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.INT().notNull(), DataTypes.STRING()
                        },
                        new String[] {"pt", "_id", "v1"});
        List<String> primaryKeys = Arrays.asList("pt", "_id");
        List<String> expected = Arrays.asList("+I[1, 1, one]", "+I[1, 2, two]", "+I[2, 4, four]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v2 INT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (2, 3, 'three', 30), (1, 5, 'five', 50)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v2 INT");
        statement.executeUpdate("INSERT INTO schema_evolution_2 VALUES (1, 6, 'six', 60)");
        statement.executeUpdate("UPDATE schema_evolution_2 SET v1 = 'second' WHERE _id = 2");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
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

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ALTER COLUMN v2 TYPE BIGINT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (2, 7, 'seven', 70000000000)");
        statement.executeUpdate("DELETE FROM schema_evolution_1 WHERE _id = 5");
        statement.executeUpdate("UPDATE schema_evolution_1 SET v2 = 30000000000 WHERE _id = 3");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ALTER COLUMN v2 TYPE BIGINT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_2 VALUES (2, 8, 'eight', 80000000000)");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
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

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v3 NUMERIC(8, 3)");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v4 BYTEA");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v5 FLOAT");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 ALTER COLUMN v1 TYPE VARCHAR(20)");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (1, 9, 'nine', 90000000000, 99999.999, 'nine.bin', 9.9)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v3 NUMERIC(8, 3)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v4 BYTEA");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v5 FLOAT");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ALTER COLUMN v1 TYPE VARCHAR(20)");
        statement.executeUpdate(
                "UPDATE schema_evolution_2 SET v1 = 'very long string' WHERE _id = 8");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.BIGINT(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.BYTES(),
                            DataTypes.DOUBLE()
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

        statement.executeUpdate(
                "ALTER TABLE schema_evolution_1 ALTER COLUMN v5 TYPE DOUBLE PRECISION");
        statement.executeUpdate(
                "UPDATE schema_evolution_1 SET v4 = 'nine.bin.long', v5 = 9.00000000009 WHERE _id = 9");
        statement.executeUpdate(
                "ALTER TABLE schema_evolution_2 ALTER COLUMN v5 TYPE DOUBLE PRECISION");
        statement.executeUpdate(
                "UPDATE schema_evolution_2 SET v4 = 'four.bin.long', v5 = 4.00000000004 WHERE _id = 4");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.BIGINT(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.BYTES(),
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
    public void testMultipleSchemaEvolutions() throws Exception {
        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), "schema_evolution_multiple");

        PostgresSyncTableAction action = syncTableActionBuilder(postgresConfig).build();
        runActionWithDefaultEnv(action);

        checkTableSchema(
                "[{\"id\":0,\"name\":\"_id\",\"type\":\"INT NOT NULL\"},"
                        + "{\"id\":1,\"name\":\"v1\",\"type\":\"STRING\"},"
                        + "{\"id\":2,\"name\":\"v2\",\"type\":\"INT\"},"
                        + "{\"id\":3,\"name\":\"v3\",\"type\":\"STRING\"}]");

        try (Statement statement = getStatement(DATABASE_NAME)) {
            testSchemaEvolutionMultipleImpl(statement);
        }
    }

    private void testSchemaEvolutionMultipleImpl(Statement statement) throws Exception {
        FileStoreTable table = getFileStoreTable();

        statement.executeUpdate(
                "INSERT INTO schema_evolution_multiple VALUES (1, 'one', 10, 'string_1')");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "v1", "v2", "v3"});
        List<String> primaryKeys = Collections.singletonList("_id");
        List<String> expected = Collections.singletonList("+I[1, one, 10, string_1]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate(
                "ALTER TABLE schema_evolution_multiple "
                        + "ADD COLUMN v4 INTEGER, "
                        + "ALTER COLUMN v1 TYPE VARCHAR(20),"
                        + "ADD COLUMN v5 DOUBLE PRECISION,"
                        + "ADD COLUMN v6 DECIMAL(5, 3),"
                        + "ADD COLUMN \"$% ^,& *(\" VARCHAR(10),"
                        + "ALTER COLUMN v2 TYPE BIGINT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_multiple VALUES "
                        + "(2, 'long_string_two', 2000000000000, 'string_2', 20, 20.5, 20.002, 'test_2')");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.BIGINT(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.DOUBLE(),
                            DataTypes.DECIMAL(5, 3),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "v1", "v2", "v3", "v4", "v5", "v6", "$% ^,& *("});
        expected =
                Arrays.asList(
                        "+I[1, one, 10, string_1, NULL, NULL, NULL, NULL]",
                        "+I[2, long_string_two, 2000000000000, string_2, 20, 20.5, 20.002, test_2]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(90)
    public void testAllTypes() throws Exception {
        // the first round checks for table creation
        // the second round checks for running the action on an existing table
        for (int i = 0; i < 2; i++) {
            testAllTypesOnce();
        }
    }

    private void testAllTypesOnce() throws Exception {
        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), "all_types_table");

        PostgresSyncTableAction action =
                syncTableActionBuilder(postgresConfig)
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
                            DataTypes.BINARY(2), // _bit
                            DataTypes.BINARY(Integer.MAX_VALUE / 8), // _bit_varying
                            DataTypes.BINARY(8), // _bit_varying1
                            DataTypes.BOOLEAN(), // _boolean
                            DataTypes.BOOLEAN(), // _bool
                            DataTypes.SMALLINT(), // _smallint
                            DataTypes.INT(), // _int
                            DataTypes.BIGINT(), // _bigint
                            DataTypes.SMALLINT().notNull(), // _small_serial
                            DataTypes.INT().notNull(), // _serial
                            DataTypes.BIGINT().notNull(), // _big_serial
                            DataTypes.DOUBLE(), // _float
                            DataTypes.FLOAT(), // _real
                            DataTypes.DOUBLE(), // _double_precision
                            DataTypes.DECIMAL(8, 3), // _numeric
                            DataTypes.DECIMAL(8, 0), // _decimal
                            DataTypes.DECIMAL(38, 10), // _big_decimal
                            DataTypes.DATE(), // _date
                            DataTypes.TIMESTAMP(6), // _timestamp
                            DataTypes.TIMESTAMP(6), // _timestamp0
                            DataTypes.TIME(6), // _time
                            DataTypes.TIME(6), // _time0
                            DataTypes.STRING(), // _char
                            DataTypes.STRING(), // _varchar
                            DataTypes.STRING(), // _text
                            DataTypes.BYTES(), // _bin
                            DataTypes.STRING(), // _json
                            DataTypes.ARRAY(DataTypes.STRING()) // _array
                        },
                        new String[] {
                            "_id",
                            "pt",
                            "_bit1",
                            "_bit",
                            "_bit_varying",
                            "_bit_varying1",
                            "_boolean",
                            "_bool",
                            "_smallint",
                            "_int",
                            "_bigint",
                            "_small_serial",
                            "_serial",
                            "_big_serial",
                            "_float",
                            "_real",
                            "_double_precision",
                            "_numeric",
                            "_decimal",
                            "_big_decimal",
                            "_date",
                            "_timestamp",
                            "_timestamp0",
                            "_time",
                            "_time0",
                            "_char",
                            "_varchar",
                            "_text",
                            "_bin",
                            "_json",
                            "_array",
                        });
        FileStoreTable table = getFileStoreTable();
        // BIT(11) data: 0B11111000111 -> 0B00000111_11000111
        String bits = Arrays.toString(new byte[] {(byte) 0B00000111, (byte) 0B11000111});
        List<String> expected =
                Arrays.asList(
                        "+I["
                                + "1, 1.1, "
                                + String.format("true, %s, [5], [2], ", bits)
                                + "true, true, "
                                + "1000, "
                                + "1000000, "
                                + "10000000000, 1, 2, 3, "
                                + "1.5, "
                                + "1.000001, "
                                + "1.000111, "
                                + "12345.110, "
                                + "11111, 2222222222222222300000001111.1234567890, "
                                + "19439, "
                                + "2023-03-23T14:30:05, 2023-03-23T00:00, "
                                + "36803000, 36803000, "
                                + "Paimon    , Apache Paimon, Apache Paimon PostgreSQL Test Data, "
                                + "[98, 121, 116, 101, 115], "
                                + "{\"a\": \"b\"}, "
                                + "[\"item1\", \"item2\"]"
                                + "]",
                        "+I["
                                + "2, 2.2, "
                                + "NULL, NULL, NULL, NULL, "
                                + "NULL, NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, 4, 5, 6, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, NULL, "
                                + "NULL, "
                                + "NULL, NULL, "
                                + "NULL, NULL, "
                                + "NULL, NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL"
                                + "]");
        waitForResult(expected, table, rowType, Arrays.asList("pt", "_id"));
    }

    @Test
    public void testIncompatibleTable() {
        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), "incompatible_field_\\d+");

        PostgresSyncTableAction action = syncTableActionBuilder(postgresConfig).build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Column v1 have different types when merging schemas.\n"
                                        + "Current table '{paimon_sync_table.incompatible_field_1}' field: `v1` TIMESTAMP(6)\n"
                                        + "To be merged table 'paimon_sync_table.incompatible_field_2' field: `v1` INT"));
    }

    @Test
    public void testIncompatiblePaimonTable() throws Exception {
        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), "incompatible_pk_\\d+");

        createFileStoreTable(
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"a", "b", "c"}),
                Collections.emptyList(),
                Collections.singletonList("a"),
                Collections.emptyList(),
                new HashMap<>());

        PostgresSyncTableAction action =
                syncTableActionBuilder(postgresConfig).withPrimaryKeys("a").build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Paimon schema and source table schema are not compatible."));
    }

    @Test
    public void testInvalidPrimaryKey() {
        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), "schema_evolution_\\d+");

        PostgresSyncTableAction action =
                syncTableActionBuilder(postgresConfig).withPrimaryKeys("pk").build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "For sink table "
                                        + tableName
                                        + ", not all specified primary keys '[pk]' exist in source tables or computed columns '[pt, _id, v1]'."));
    }

    @Test
    public void testNoPrimaryKey() {
        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), "incompatible_pk_\\d+");

        PostgresSyncTableAction action = syncTableActionBuilder(postgresConfig).build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Failed to set specified primary keys for sink table "
                                        + tableName
                                        + ". Also, can't infer primary keys from source table schemas because "
                                        + "source tables have no primary keys or have different primary keys."));
    }

    @Test
    @Timeout(60)
    public void testComputedColumn() throws Exception {
        // the first round checks for table creation
        // the second round checks for running the action on an existing table
        for (int i = 0; i < 2; i++) {
            innerTestComputedColumn(i == 0);
        }
    }

    private void innerTestComputedColumn(boolean execute) throws Exception {
        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), "test_computed_column");

        List<String> computedColumnDefs =
                Arrays.asList(
                        "_year_date=year(_date)",
                        "_year_timestamp=year(_timestamp)",
                        "_month_date=month(_date)",
                        "_month_timestamp=month(_timestamp)",
                        "_day_date=day(_date)",
                        "_day_timestamp=day(_timestamp)",
                        "_hour_date=hour(_date)",
                        "_hour_timestamp=hour(_timestamp)",
                        "_date_format_date=date_format(_date,yyyy)",
                        "_date_format_timestamp=date_format(_timestamp,yyyyMMdd)",
                        "_substring_date1=substring(_date,2)",
                        "_substring_date2=substring(_timestamp,5,10)",
                        "_truncate_date=trUNcate(pk,2)"); // test case-insensitive too

        PostgresSyncTableAction action =
                syncTableActionBuilder(postgresConfig)
                        .withPartitionKeys("_year_date")
                        .withPrimaryKeys("pk", "_year_date")
                        .withComputedColumnArgs(computedColumnDefs)
                        .build();
        runActionWithDefaultEnv(action);

        if (execute) {
            try (Statement statement = getStatement(DATABASE_NAME)) {
                statement.executeUpdate(
                        "INSERT INTO test_computed_column VALUES (1, '2023-03-23', '2021-09-15 15:00:10')");
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
                            DataTypes.TIMESTAMP(6),
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
                            DataTypes.STRING(),
                            DataTypes.INT().notNull()
                        },
                        new String[] {
                            "pk",
                            "_date",
                            "_timestamp",
                            "_year_date",
                            "_year_timestamp",
                            "_month_date",
                            "_month_timestamp",
                            "_day_date",
                            "_day_timestamp",
                            "_hour_date",
                            "_hour_timestamp",
                            "_date_format_date",
                            "_date_format_timestamp",
                            "_substring_date1",
                            "_substring_date2",
                            "_truncate_date"
                        });
        List<String> expected =
                Arrays.asList(
                        "+I[1, 19439, 2021-09-15T15:00:10, 2023, 2021, 3, 9, 23, 15, 0, 15, 2023, 20210915, 23-03-23, 09-15, 0]",
                        "+I[2, 19439, NULL, 2023, NULL, 3, NULL, 23, NULL, 0, NULL, 2023, NULL, 23-03-23, NULL, 2]");
        waitForResult(expected, table, rowType, Arrays.asList("pk", "_year_date"));
    }

    @Test
    @Timeout(60)
    public void testSyncShards() throws Exception {
        Map<String, String> postgresConfig = getBasicPostgresConfig();

        // test table list
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String schemaPattern = random.nextBoolean() ? "shard_.+" : "shard_1|shard_2";
        String tblPattern = random.nextBoolean() ? "t.+" : "t1|t2";
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), schemaPattern);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), tblPattern);

        PostgresSyncTableAction action =
                syncTableActionBuilder(postgresConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pk", "pt")
                        .withComputedColumnArgs("pt=substring(_date,5)")
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement(DATABASE_NAME)) {
            statement.execute("SET search_path TO shard_1");
            statement.executeUpdate("INSERT INTO t1 VALUES (1, '2023-07-30')");
            statement.executeUpdate("INSERT INTO t2 VALUES (2, '2023-07-30')");
            statement.execute("SET search_path TO shard_2");
            statement.executeUpdate("INSERT INTO t1 VALUES (3, '2023-07-31')");
            statement.executeUpdate("INSERT INTO t1 VALUES (4, '2023-07-31')");
        }

        FileStoreTable table = getFileStoreTable();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
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
        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), "test_options_change");

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", "1");
        tableConfig.put("sink.parallelism", "1");

        PostgresSyncTableAction action1 =
                syncTableActionBuilder(postgresConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pk", "pt")
                        .withComputedColumnArgs("pt=substring(_date,5)")
                        .withTableConfig(tableConfig)
                        .build();
        JobClient jobClient = runActionWithDefaultEnv(action1);
        try (Statement statement = getStatement(DATABASE_NAME)) {
            statement.executeUpdate(
                    "INSERT INTO test_options_change VALUES (1, '2023-03-23', '2021-09-15 15:00:10')");
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

        PostgresSyncTableAction action2 =
                syncTableActionBuilder(postgresConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pk", "pt")
                        .withComputedColumnArgs("pt=substring(_date,5)")
                        .withTableConfig(tableConfig)
                        .build();
        runActionWithDefaultEnv(action2);

        FileStoreTable table = getFileStoreTable();
        assertThat(table.options()).containsAllEntriesOf(tableConfig);
    }

    @Test
    @Timeout(60)
    public void testMetadataColumns() throws Exception {
        String tableName = "test_metadata_columns";
        try (Statement statement = getStatement(DATABASE_NAME)) {
            statement.executeUpdate("INSERT INTO test_metadata_columns VALUES (1, '2023-07-30')");
            statement.executeUpdate("INSERT INTO test_metadata_columns VALUES (2, '2023-07-30')");
        }
        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), tableName);

        PostgresSyncTableAction action =
                syncTableActionBuilder(postgresConfig)
                        .withPrimaryKeys("pk")
                        .withMetadataColumns("table_name", "database_name", "schema_name")
                        .build();

        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING().notNull()
                        },
                        new String[] {"pk", "_date", "table_name", "database_name", "schema_name"});
        waitForResult(
                Arrays.asList(
                        String.format(
                                "+I[1, 2023-07-30, %s, %s, %s]",
                                tableName, DATABASE_NAME, SCHEMA_NAME),
                        String.format(
                                "+I[2, 2023-07-30, %s, %s, %s]",
                                tableName, DATABASE_NAME, SCHEMA_NAME)),
                table,
                rowType,
                Collections.singletonList("pk"));
    }

    @Test
    public void testCatalogAndTableConfig() {
        PostgresSyncTableAction action =
                syncTableActionBuilder(getBasicPostgresConfig())
                        .withCatalogConfig(Collections.singletonMap("catalog-key", "catalog-value"))
                        .withTableConfig(Collections.singletonMap("table-key", "table-value"))
                        .build();

        assertThat(action.catalogConfig()).containsEntry("catalog-key", "catalog-value");
        assertThat(action.tableConfig())
                .containsExactlyEntriesOf(Collections.singletonMap("table-key", "table-value"));
    }

    @Test
    @Timeout(60)
    public void testColumnAlterInExistingTableWhenStartJob() throws Exception {
        String tableName = "test_exist_column_alter";
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");
        options.put("sink.parallelism", "1");

        RowType rowType =
                RowType.builder()
                        .field("pk", DataTypes.INT().notNull())
                        .field("a", DataTypes.BIGINT())
                        .field("b", DataTypes.STRING())
                        .build();

        createFileStoreTable(
                rowType,
                Collections.emptyList(),
                Collections.singletonList("pk"),
                Collections.emptyList(),
                options);

        Map<String, String> postgresConfig = getBasicPostgresConfig();
        postgresConfig.put(PostgresSourceOptions.DATABASE_NAME.key(), DATABASE_NAME);
        postgresConfig.put(PostgresSourceOptions.SCHEMA_NAME.key(), SCHEMA_NAME);
        postgresConfig.put(PostgresSourceOptions.TABLE_NAME.key(), tableName);

        PostgresSyncTableAction action =
                syncTableActionBuilder(postgresConfig).withPrimaryKeys("pk").build();

        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable();

        Map<String, DataField> actual =
                table.schema().fields().stream()
                        .collect(Collectors.toMap(DataField::name, Function.identity()));

        assertThat(actual.get("pk").type()).isEqualTo(DataTypes.INT().notNull());
        assertThat(actual.get("a").type()).isEqualTo(DataTypes.BIGINT());
        assertThat(actual.get("b").type()).isEqualTo(DataTypes.STRING());
        assertThat(actual.get("c").type()).isEqualTo(DataTypes.INT());
    }

    private FileStoreTable getFileStoreTable() throws Exception {
        return getFileStoreTable(tableName);
    }
}
