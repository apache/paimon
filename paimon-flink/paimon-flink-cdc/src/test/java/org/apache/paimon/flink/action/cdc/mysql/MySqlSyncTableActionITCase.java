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
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommonTestUtils;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.SQLException;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link MySqlSyncTableAction}. */
public class MySqlSyncTableActionITCase extends MySqlActionITCaseBase {

    private static final String DATABASE_NAME = "paimon_sync_table";

    @BeforeAll
    public static void startContainers() {
        MYSQL_CONTAINER.withSetupSQL("mysql/sync_table_setup.sql");
        start();
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "schema_evolution_\\d+");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.METASTORE.key(), "test-alter-table"))
                        .withTableConfig(getBasicTableConfig())
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
                        .build();
        runActionWithDefaultEnv(action);

        checkTableSchema(
                "[{\"id\":0,\"name\":\"pt\",\"type\":\"INT NOT NULL\",\"description\":\"primary\"},"
                        + "{\"id\":1,\"name\":\"_id\",\"type\":\"INT NOT NULL\",\"description\":\"_id\"},"
                        + "{\"id\":2,\"name\":\"v1\",\"type\":\"VARCHAR(10)\",\"description\":\"v1\"}]");

        try (Statement statement = getStatement()) {
            testSchemaEvolutionImpl(statement);
        }
    }

    private void checkTableSchema(String excepted) throws Exception {

        FileStoreTable table = getFileStoreTable();

        assertThat(JsonSerdeUtil.toFlatJson(table.schema().fields())).isEqualTo(excepted);
    }

    private void testSchemaEvolutionImpl(Statement statement) throws Exception {
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

        statement.executeUpdate("ALTER TABLE schema_evolution_1 MODIFY COLUMN v2 BIGINT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (2, 7, 'seven', 70000000000)");
        statement.executeUpdate("DELETE FROM schema_evolution_1 WHERE _id = 5");
        statement.executeUpdate("UPDATE schema_evolution_1 SET v2 = 30000000000 WHERE _id = 3");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 MODIFY COLUMN v2 BIGINT");
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

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v3 NUMERIC(8, 3)");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v4 VARBINARY(10)");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v5 FLOAT");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 MODIFY COLUMN v1 VARCHAR(20)");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (1, 9, 'nine', 90000000000, 99999.999, 'nine.bin', 9.9)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v3 NUMERIC(8, 3)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v4 VARBINARY(10)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v5 FLOAT");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 MODIFY COLUMN v1 VARCHAR(20)");
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

        statement.executeUpdate("ALTER TABLE schema_evolution_1 MODIFY COLUMN v4 VARBINARY(20)");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 MODIFY COLUMN v5 DOUBLE");
        statement.executeUpdate(
                "UPDATE schema_evolution_1 SET v4 = 'nine.bin.long', v5 = 9.00000000009 WHERE _id = 9");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 MODIFY COLUMN v4 VARBINARY(20)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 MODIFY COLUMN v5 DOUBLE");
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
    public void testMultipleSchemaEvolutions() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "schema_evolution_multiple");

        MySqlSyncTableAction action = syncTableActionBuilder(mySqlConfig).build();
        runActionWithDefaultEnv(action);

        checkTableSchema(
                "[{\"id\":0,\"name\":\"_id\",\"type\":\"INT NOT NULL\",\"description\":\"primary\"},"
                        + "{\"id\":1,\"name\":\"v1\",\"type\":\"VARCHAR(10)\",\"description\":\"v1\"},"
                        + "{\"id\":2,\"name\":\"v2\",\"type\":\"INT\",\"description\":\"v2\"},"
                        + "{\"id\":3,\"name\":\"v3\",\"type\":\"VARCHAR(10)\",\"description\":\"v3\"}]");

        try (Statement statement = getStatement()) {
            testSchemaEvolutionMultipleImpl(statement);
        }
    }

    private void testSchemaEvolutionMultipleImpl(Statement statement) throws Exception {
        FileStoreTable table = getFileStoreTable();
        statement.executeUpdate("USE " + DATABASE_NAME);

        statement.executeUpdate(
                "INSERT INTO schema_evolution_multiple VALUES (1, 'one', 10, 'string_1')");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10),
                            DataTypes.INT(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"_id", "v1", "v2", "v3"});
        List<String> primaryKeys = Collections.singletonList("_id");
        List<String> expected = Collections.singletonList("+I[1, one, 10, string_1]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate(
                "ALTER TABLE schema_evolution_multiple "
                        + "ADD v4 INT, "
                        + "MODIFY COLUMN v1 VARCHAR(20), "
                        // I'd love to change COMMENT to DEFAULT
                        // however debezium parser seems to have a bug here
                        + "ADD COLUMN (v5 DOUBLE, v6 DECIMAL(5, 3), `$% ^,& *(` VARCHAR(10), v7 INTEGER COMMENT 'Hi, v700 DOUBLE \\', v701 INT a test'), "
                        + "MODIFY v2 BIGINT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_multiple VALUES "
                        + "(2, 'long_string_two', 2000000000000, 'string_2', 20, 20.5, 20.002, 'test_2', 200)");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(20),
                            DataTypes.BIGINT(),
                            DataTypes.VARCHAR(10),
                            DataTypes.INT(),
                            DataTypes.DOUBLE(),
                            DataTypes.DECIMAL(5, 3),
                            DataTypes.VARCHAR(10),
                            DataTypes.INT(),
                        },
                        new String[] {
                            "_id", "v1", "v2", "v3", "v4", "v5", "v6", "$% ^,& *(", "v7"
                        });
        expected =
                Arrays.asList(
                        "+I[1, one, 10, string_1, NULL, NULL, NULL, NULL, NULL]",
                        "+I[2, long_string_two, 2000000000000, string_2, 20, 20.5, 20.002, test_2, 200]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolutionWithComment() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "schema_evolution_comment");
        mySqlConfig.put("debezium.include.schema.comments", "true");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.METASTORE.key(), "test-alter-table"))
                        .withTableConfig(getBasicTableConfig())
                        .withPrimaryKeys("_id")
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            testSchemaEvolutionWithCommentImpl(statement);
        }
    }

    private void testSchemaEvolutionWithCommentImpl(Statement statement) throws Exception {
        FileStoreTable table = getFileStoreTable();
        statement.executeUpdate("USE " + DATABASE_NAME);
        statement.executeUpdate("INSERT INTO schema_evolution_comment VALUES (1, 'one')");

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"_id", "v1"});
        List<String> primaryKeys = Collections.singletonList("_id");
        List<String> expected = Collections.singletonList("+I[1, one]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate(
                "ALTER TABLE schema_evolution_comment MODIFY COLUMN v1 VARCHAR(20) COMMENT 'v1-new'");
        statement.executeUpdate("INSERT INTO schema_evolution_comment VALUES (2, 'two')");

        statement.executeUpdate(
                "ALTER TABLE schema_evolution_comment ADD COLUMN v2 INT COMMENT 'v2'");

        statement.executeUpdate("INSERT INTO schema_evolution_comment VALUES (3, 'three', 30)");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(20), DataTypes.INT()
                        },
                        new String[] {"_id", "v1", "v2"});
        expected = Arrays.asList("+I[1, one, NULL]", "+I[2, two, NULL]", "+I[3, three, 30]");
        waitForResult(expected, table, rowType, primaryKeys);

        checkTableSchema(
                "[{\"id\":0,\"name\":\"_id\",\"type\":\"INT NOT NULL\",\"description\":\"primary\"},"
                        + "{\"id\":1,\"name\":\"v1\",\"type\":\"VARCHAR(20)\",\"description\":\"v1-new\"},"
                        + "{\"id\":2,\"name\":\"v2\",\"type\":\"INT\",\"description\":\"v2\"}]");
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
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "all_types_table");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
                        .build();
        JobClient client = runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            testAllTypesImpl(statement);
        }

        client.cancel().get();
    }

    private void testAllTypesImpl(Statement statement) throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), // _id
                            DataTypes.DECIMAL(2, 1).notNull(), // pt
                            DataTypes.BOOLEAN(), // _bit1
                            DataTypes.BINARY(8), // _bit
                            DataTypes.BOOLEAN(), // _tinyint1
                            DataTypes.BOOLEAN(), // _boolean
                            DataTypes.BOOLEAN(), // _bool
                            DataTypes.TINYINT(), // _tinyint
                            DataTypes.SMALLINT(), // _tinyint_unsigned
                            DataTypes.SMALLINT(), // _tinyint_unsigned_zerofill
                            DataTypes.SMALLINT(), // _smallint
                            DataTypes.INT(), // _smallint_unsigned
                            DataTypes.INT(), // _smallint_unsigned_zerofill
                            DataTypes.INT(), // _mediumint
                            DataTypes.BIGINT(), // _mediumint_unsigned
                            DataTypes.BIGINT(), // _mediumint_unsigned_zerofill
                            DataTypes.INT(), // _int
                            DataTypes.BIGINT(), // _int_unsigned
                            DataTypes.BIGINT(), // _int_unsigned_zerofill
                            DataTypes.BIGINT(), // _bigint
                            DataTypes.DECIMAL(20, 0), // _bigint_unsigned
                            DataTypes.DECIMAL(20, 0), // _bigint_unsigned_zerofill
                            DataTypes.DECIMAL(20, 0).notNull(), // _serial
                            DataTypes.FLOAT(), // _float
                            DataTypes.FLOAT(), // _float_unsigned
                            DataTypes.FLOAT(), // _float_unsigned_zerofill
                            DataTypes.DOUBLE(), // _real
                            DataTypes.DOUBLE(), // _real_unsigned
                            DataTypes.DOUBLE(), // _real_unsigned_zerofill
                            DataTypes.DOUBLE(), // _double
                            DataTypes.DOUBLE(), // _double_unsigned
                            DataTypes.DOUBLE(), // _double_unsigned_zerofill
                            DataTypes.DOUBLE(), // _double_precision
                            DataTypes.DOUBLE(), // _double_precision_unsigned
                            DataTypes.DOUBLE(), // _double_precision_unsigned_zerofill
                            DataTypes.DECIMAL(8, 3), // _numeric
                            DataTypes.DECIMAL(8, 3), // _numeric_unsigned
                            DataTypes.DECIMAL(8, 3), // _numeric_unsigned_zerofill
                            DataTypes.STRING(), // _fixed
                            DataTypes.STRING(), // _fixed_unsigned
                            DataTypes.STRING(), // _fixed_unsigned_zerofill
                            DataTypes.DECIMAL(8, 0), // _decimal
                            DataTypes.DECIMAL(8, 0), // _decimal_unsigned
                            DataTypes.DECIMAL(8, 0), // _decimal_unsigned_zerofill
                            DataTypes.DECIMAL(38, 10), // _big_decimal
                            DataTypes.DATE(), // _date
                            DataTypes.TIMESTAMP(0), // _datetime
                            DataTypes.TIMESTAMP(3), // _datetime3
                            DataTypes.TIMESTAMP(6), // _datetime6
                            DataTypes.TIMESTAMP(0), // _datetime_p
                            DataTypes.TIMESTAMP(2), // _datetime_p2
                            DataTypes.TIMESTAMP(6), // _timestamp
                            DataTypes.TIMESTAMP(0), // _timestamp0
                            DataTypes.CHAR(10), // _char
                            DataTypes.VARCHAR(20), // _varchar
                            DataTypes.STRING(), // _tinytext
                            DataTypes.STRING(), // _text
                            DataTypes.STRING(), // _mediumtext
                            DataTypes.STRING(), // _longtext
                            DataTypes.VARBINARY(10), // _bin
                            DataTypes.VARBINARY(20), // _varbin
                            DataTypes.BYTES(), // _tinyblob
                            DataTypes.BYTES(), // _blob
                            DataTypes.BYTES(), // _mediumblob
                            DataTypes.BYTES(), // _longblob
                            DataTypes.STRING(), // _json
                            DataTypes.STRING(), // _enum
                            DataTypes.INT(), // _year
                            DataTypes.TIME(), // _time
                            DataTypes.STRING(), // _point
                            DataTypes.STRING(), // _geometry
                            DataTypes.STRING(), // _linestring
                            DataTypes.STRING(), // _polygon
                            DataTypes.STRING(), // _multipoint
                            DataTypes.STRING(), // _multiline
                            DataTypes.STRING(), // _multipolygon
                            DataTypes.STRING(), // _geometrycollection
                            DataTypes.ARRAY(DataTypes.STRING()) // _set
                        },
                        new String[] {
                            "_id",
                            "pt",
                            "_bit1",
                            "_bit",
                            "_tinyint1",
                            "_boolean",
                            "_bool",
                            "_tinyint",
                            "_tinyint_unsigned",
                            "_tinyint_unsigned_zerofill",
                            "_smallint",
                            "_smallint_unsigned",
                            "_smallint_unsigned_zerofill",
                            "_mediumint",
                            "_mediumint_unsigned",
                            "_mediumint_unsigned_zerofill",
                            "_int",
                            "_int_unsigned",
                            "_int_unsigned_zerofill",
                            "_bigint",
                            "_bigint_unsigned",
                            "_bigint_unsigned_zerofill",
                            "_serial",
                            "_float",
                            "_float_unsigned",
                            "_float_unsigned_zerofill",
                            "_real",
                            "_real_unsigned",
                            "_real_unsigned_zerofill",
                            "_double",
                            "_double_unsigned",
                            "_double_unsigned_zerofill",
                            "_double_precision",
                            "_double_precision_unsigned",
                            "_double_precision_unsigned_zerofill",
                            "_numeric",
                            "_numeric_unsigned",
                            "_numeric_unsigned_zerofill",
                            "_fixed",
                            "_fixed_unsigned",
                            "_fixed_unsigned_zerofill",
                            "_decimal",
                            "_decimal_unsigned",
                            "_decimal_unsigned_zerofill",
                            "_big_decimal",
                            "_date",
                            "_datetime",
                            "_datetime3",
                            "_datetime6",
                            "_datetime_p",
                            "_datetime_p2",
                            "_timestamp",
                            "_timestamp0",
                            "_char",
                            "_varchar",
                            "_tinytext",
                            "_text",
                            "_mediumtext",
                            "_longtext",
                            "_bin",
                            "_varbin",
                            "_tinyblob",
                            "_blob",
                            "_mediumblob",
                            "_longblob",
                            "_json",
                            "_enum",
                            "_year",
                            "_time",
                            "_point",
                            "_geometry",
                            "_linestring",
                            "_polygon",
                            "_multipoint",
                            "_multiline",
                            "_multipolygon",
                            "_geometrycollection",
                            "_set",
                        });
        FileStoreTable table = getFileStoreTable();
        // BIT(64) data: 0B11111000111 -> 0B00000111_11000111
        String bits =
                Arrays.toString(
                        new byte[] {0, 0, 0, 0, 0, 0, (byte) 0B00000111, (byte) 0B11000111});
        List<String> expected =
                Arrays.asList(
                        "+I["
                                + "1, 1.1, "
                                + String.format("true, %s, ", bits)
                                + "true, true, false, 1, 2, 3, "
                                + "1000, 2000, 3000, "
                                + "100000, 200000, 300000, "
                                + "1000000, 2000000, 3000000, "
                                + "10000000000, 20000000000, 30000000000, 40000000000, "
                                + "1.5, 2.5, 3.5, "
                                + "1.000001, 2.000002, 3.000003, "
                                + "1.000011, 2.000022, 3.000033, "
                                + "1.000111, 2.000222, 3.000333, "
                                + "12345.110, 12345.220, 12345.330, "
                                + "123456789876543212345678987654321.11, 123456789876543212345678987654321.22, 123456789876543212345678987654321.33, "
                                + "11111, 22222, 33333, 2222222222222222300000001111.1234567890, "
                                + "19439, "
                                // display value of datetime is not affected by timezone
                                + "2023-03-23T14:30:05, 2023-03-23T14:30:05.123, 2023-03-23T14:30:05.123456, "
                                + "2023-03-24T14:30, 2023-03-24T14:30:05.120, "
                                // display value of timestamp is affected by timezone
                                // we store 2023-03-23T15:00:10.123456 in UTC-8 system timezone
                                // and query this timestamp in UTC-5 MySQL server timezone
                                // so the display value should increase by 3 hour
                                + "2023-03-23T18:00:10.123456, 2023-03-23T03:10, "
                                + "Paimon, Apache Paimon, Apache Paimon MySQL TINYTEXT Test Data, Apache Paimon MySQL Test Data, Apache Paimon MySQL MEDIUMTEXT Test Data, Apache Paimon MySQL Long Test Data, "
                                + "[98, 121, 116, 101, 115, 0, 0, 0, 0, 0], "
                                + "[109, 111, 114, 101, 32, 98, 121, 116, 101, 115], "
                                + "[84, 73, 78, 89, 66, 76, 79, 66, 32, 116, 121, 112, 101, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "[66, 76, 79, 66, 32, 116, 121, 112, 101, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "[77, 69, 68, 73, 85, 77, 66, 76, 79, 66, 32, 116, 121, 112, 101, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "[76, 79, 78, 71, 66, 76, 79, 66, 32, 32, 98, 121, 116, 101, 115, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "{\"a\": \"b\"}, "
                                + "value1, "
                                + "2023, "
                                + "36803000, "
                                + "{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}, "
                                + "{\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}, "
                                + "{\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0}, "
                                + "{\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0}, "
                                + "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0}, "
                                + "[a, b]"
                                + "]",
                        "+I["
                                + "2, 2.2, "
                                + "NULL, NULL, "
                                + "NULL, NULL, NULL, NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, 50000000000, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, NULL, "
                                + "NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, "
                                + "NULL, NULL, "
                                + "NULL, NULL, NULL, NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, NULL, NULL, NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL"
                                + "]");
        waitForResult(expected, table, rowType, Arrays.asList("pt", "_id"));

        // test all types during schema evolution
        try {
            statement.executeUpdate("USE " + DATABASE_NAME);
            statement.executeUpdate("ALTER TABLE all_types_table ADD COLUMN v INT");
            List<DataField> newFields = new ArrayList<>(rowType.getFields());
            newFields.add(new DataField(rowType.getFieldCount(), "v", DataTypes.INT()));
            RowType newRowType = new RowType(newFields);
            List<String> newExpected =
                    expected.stream()
                            .map(s -> s.substring(0, s.length() - 1) + ", NULL]")
                            .collect(Collectors.toList());
            waitForResult(newExpected, table, newRowType, Arrays.asList("pt", "_id"));
        } finally {
            statement.executeUpdate("ALTER TABLE all_types_table DROP COLUMN v");
            SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
            schemaManager.commitChanges(SchemaChange.dropColumn("v"));
        }
    }

    @Test
    public void testIncompatibleMySqlTable() {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "incompatible_field_\\d+");

        MySqlSyncTableAction action = syncTableActionBuilder(mySqlConfig).build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Column v1 have different types when merging schemas.\n"
                                        + "Current table '{paimon_sync_table.incompatible_field_1}' field: `v1` TIMESTAMP(0) ''\n"
                                        + "To be merged table 'paimon_sync_table.incompatible_field_2' field: `v1` INT ''"));
    }

    @Test
    public void testIncompatiblePaimonTable() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "incompatible_pk_\\d+");

        createFileStoreTable(
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"a", "b", "c"}),
                Collections.emptyList(),
                Collections.singletonList("a"),
                Collections.emptyList(),
                new HashMap<>());

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig).withPrimaryKeys("a").build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Paimon schema and source table schema are not compatible."));
    }

    @Test
    public void testInvalidPrimaryKey() {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "schema_evolution_\\d+");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig).withPrimaryKeys("pk").build();

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
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "incompatible_pk_\\d+");

        MySqlSyncTableAction action = syncTableActionBuilder(mySqlConfig).build();

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

    private void innerTestComputedColumn(boolean executeMysql) throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "test_computed_column");

        List<String> computedColumnDefs =
                Arrays.asList(
                        "_year_date=year(_date)",
                        "_year_datetime=year(_datetime)",
                        "_year_timestamp=year(_timestamp)",
                        "_month_date=month(_date)",
                        "_month_datetime=month(_datetime)",
                        "_month_timestamp=month(_timestamp)",
                        "_day_date=day(_date)",
                        "_day_datetime=day(_datetime)",
                        "_day_timestamp=day(_timestamp)",
                        "_hour_date=hour(_date)",
                        "_hour_datetime=hour(_datetime)",
                        "_hour_timestamp=hour(_timestamp)",
                        "_minute_date=minute(_date)",
                        "_minute_datetime=minute(_datetime)",
                        "_minute_timestamp=minute(_timestamp)",
                        "_second_date=second(_date)",
                        "_second_datetime=second(_datetime)",
                        "_second_timestamp=second(_timestamp)",
                        "_date_format_date=date_format(_date,yyyy)",
                        "_date_format_datetime=date_format(_datetime,yyyy-MM-dd)",
                        "_date_format_timestamp=date_format(_timestamp,yyyyMMdd)",
                        "_substring_date1=substring(_date,2)",
                        "_substring_date2=substring(_timestamp,5,10)",
                        "_truncate_date=trUNcate(pk,2)", // test case-insensitive too
                        "_constant=cast(11,INT)");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withPartitionKeys("_year_date")
                        .withPrimaryKeys("pk", "_year_date")
                        .withComputedColumnArgs(computedColumnDefs)
                        .build();
        runActionWithDefaultEnv(action);

        if (executeMysql) {
            try (Statement statement = getStatement()) {
                statement.execute("USE " + DATABASE_NAME);
                statement.executeUpdate(
                        "INSERT INTO test_computed_column VALUES (1, '2023-03-23', '2022-01-01 14:30', '2021-09-15 15:00:10')");
                statement.executeUpdate(
                        "INSERT INTO test_computed_column VALUES (2, '2023-03-23', null, null)");
            }
        }

        FileStoreTable table = getFileStoreTable();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.DATE(),
                            DataTypes.TIMESTAMP(0),
                            DataTypes.TIMESTAMP(0),
                            DataTypes.INT().notNull(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
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
                            DataTypes.STRING(),
                            DataTypes.INT().notNull(),
                            DataTypes.INT()
                        },
                        new String[] {
                            "pk",
                            "_date",
                            "_datetime",
                            "_timestamp",
                            "_year_date",
                            "_year_datetime",
                            "_year_timestamp",
                            "_month_date",
                            "_month_datetime",
                            "_month_timestamp",
                            "_day_date",
                            "_day_datetime",
                            "_day_timestamp",
                            "_hour_date",
                            "_hour_datetime",
                            "_hour_timestamp",
                            "_minute_date",
                            "_minute_datetime",
                            "_minute_timestamp",
                            "_second_date",
                            "_second_datetime",
                            "_second_timestamp",
                            "_date_format_date",
                            "_date_format_datetime",
                            "_date_format_timestamp",
                            "_substring_date1",
                            "_substring_date2",
                            "_truncate_date",
                            "_constant"
                        });
        List<String> expected =
                Arrays.asList(
                        "+I[1, 19439, 2022-01-01T14:30, 2021-09-15T15:00:10, 2023, 2022, 2021, 3, 1, 9, 23, 1, 15, 0, 14, 15, 0, 30, 0, 0, 0, 10, 2023, 2022-01-01, 20210915, 23-03-23, 09-15, 0, 11]",
                        "+I[2, 19439, NULL, NULL, 2023, NULL, NULL, 3, NULL, NULL, 23, NULL, NULL, 0, NULL, NULL, 0, NULL, NULL, 0, NULL, NULL, 2023, NULL, NULL, 23-03-23, NULL, 2, 11]");
        waitForResult(expected, table, rowType, Arrays.asList("pk", "_year_date"));
    }

    @Test
    @Timeout(60)
    public void testTemporalToIntWithEpochTime() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "test_time_to_int_epoch");

        ThreadLocalRandom random = ThreadLocalRandom.current();
        // pick field reference & time unit
        int fieldRefIndex = random.nextInt(5);
        String fieldReference =
                Arrays.asList(
                                "_second_val0",
                                "_second_val1",
                                "_millis_val",
                                "_micros_val",
                                "_nanos_val")
                        .get(fieldRefIndex);
        String precision = Arrays.asList("", ",0", ",3", ",6", ",9").get(fieldRefIndex);

        // pick test expression
        int expIndex = random.nextInt(6);
        String expression =
                Arrays.asList("year", "month", "day", "hour", "minute", "second").get(expIndex);

        String computedColumnDef =
                String.format("_time_to_int=%s(%s%s)", expression, fieldReference, precision);

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withComputedColumnArgs(computedColumnDef)
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.execute("USE " + DATABASE_NAME);
            insertEpochTime(
                    "test_time_to_int_epoch", 1, "2024-01-01T00:01:02.123456789Z", statement);
            insertEpochTime(
                    "test_time_to_int_epoch", 2, "2024-12-31T12:59:59.123456789Z", statement);
        }

        FileStoreTable table = getFileStoreTable();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.INT()
                        },
                        new String[] {
                            "pk",
                            "_second_val0",
                            "_second_val1",
                            "_millis_val",
                            "_micros_val",
                            "_nanos_val",
                            "_time_to_int"
                        });

        int result1 = Arrays.asList(2024, 1, 1, 0, 1, 2).get(expIndex);
        int result2 = Arrays.asList(2024, 12, 31, 12, 59, 59).get(expIndex);
        List<String> expected =
                Arrays.asList(
                        "+I[1, 1704067262, 1704067262, 1704067262123, 1704067262123456, 1704067262123456789, "
                                + result1
                                + "]",
                        "+I[2, 1735649999, 1735649999, 1735649999123, 1735649999123456, 1735649999123456789, "
                                + result2
                                + "]");
        waitForResult(expected, table, rowType, Collections.singletonList("pk"));
    }

    @Test
    @Timeout(60)
    public void testDateFormatWithEpochTime() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "test_date_format_epoch");

        List<String> computedColumnDefs =
                Arrays.asList(
                        "_from_second0_default=date_format(_second_val0, yyyy-MM-dd HH:mm:ss)",
                        "_from_second0=date_format(_second_val0, yyyy-MM-dd HH:mm:ss, 0)",
                        "_from_second1=date_format(_second_val1, yyyy-MM-dd HH:mm:ss, 0)",
                        // test week format
                        "_from_second1_week=date_format(_second_val1, yyyy-ww, 0)",
                        "_from_millisecond=date_format(_millis_val, yyyy-MM-dd HH:mm:ss.SSS, 3)",
                        "_from_microsecond=date_format(_micros_val, yyyy-MM-dd HH:mm:ss.SSSSSS, 6)",
                        "_from_nanoseconds=date_format(_nanos_val, yyyy-MM-dd HH:mm:ss.SSSSSSSSS, 9)");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withComputedColumnArgs(computedColumnDefs)
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.execute("USE " + DATABASE_NAME);
            insertEpochTime(
                    "test_date_format_epoch", 1, "2024-01-07T00:01:02.123456789Z", statement);
        }

        FileStoreTable table = getFileStoreTable();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {
                            "pk",
                            "_second_val0",
                            "_second_val1",
                            "_millis_val",
                            "_micros_val",
                            "_nanos_val",
                            "_from_second0_default",
                            "_from_second0",
                            "_from_second1",
                            "_from_second1_week",
                            "_from_millisecond",
                            "_from_microsecond",
                            "_from_nanoseconds"
                        });

        // depends on the Locale setting
        WeekFields weekFields = WeekFields.of(Locale.getDefault());
        int week = weekFields.getFirstDayOfWeek() == DayOfWeek.MONDAY ? 1 : 2;

        List<String> expected =
                Collections.singletonList(
                        "+I[1, 1704585662, 1704585662, 1704585662123, 1704585662123456, 1704585662123456789, "
                                + "2024-01-07 00:01:02, 2024-01-07 00:01:02, 2024-01-07 00:01:02, "
                                + String.format("2024-0%s, ", week)
                                + "2024-01-07 00:01:02.123, 2024-01-07 00:01:02.123456, 2024-01-07 00:01:02.123456789]");
        waitForResult(expected, table, rowType, Collections.singletonList("pk"));
    }

    private void insertEpochTime(String table, int pk, String dateStr, Statement statement)
            throws SQLException {
        Instant instant = Instant.parse(dateStr);
        long epochSecond = instant.getEpochSecond();
        int nano = instant.getNano();

        statement.executeUpdate(
                String.format(
                        "INSERT INTO %s VALUES (%d, %d, %d, %d, %d, %d)",
                        table,
                        pk,
                        epochSecond,
                        epochSecond,
                        epochSecond * 1000 + nano / 1_000_000,
                        epochSecond * 1000_000 + nano / 1_000,
                        epochSecond * 1_000_000_000 + nano));
    }

    @Test
    @Timeout(60)
    public void testSyncShards() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();

        // test table list
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String dbPattern = random.nextBoolean() ? "shard_.+" : "shard_1|shard_2";
        String tblPattern = random.nextBoolean() ? "t.+" : "t1|t2";
        mySqlConfig.put("database-name", dbPattern);
        mySqlConfig.put("table-name", tblPattern);

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pk", "pt")
                        .withComputedColumnArgs("pt=substring(_date,5)")
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.execute("USE shard_1");
            statement.executeUpdate("INSERT INTO t1 VALUES (1, '2023-07-30')");
            statement.executeUpdate("INSERT INTO t2 VALUES (2, '2023-07-30')");
            statement.execute("USE shard_2");
            statement.executeUpdate("INSERT INTO t1 VALUES (3, '2023-07-31')");
            statement.executeUpdate("INSERT INTO t1 VALUES (4, '2023-07-31')");
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
        Map<String, String> mySqlConfig = getBasicMySqlConfig();

        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "test_options_change");
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", "1");
        tableConfig.put("sink.parallelism", "1");

        MySqlSyncTableAction action1 =
                syncTableActionBuilder(mySqlConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pk", "pt")
                        .withComputedColumnArgs("pt=substring(_date,5)")
                        .withTableConfig(tableConfig)
                        .build();
        JobClient jobClient = runActionWithDefaultEnv(action1);
        try (Statement statement = getStatement()) {
            statement.execute("USE " + DATABASE_NAME);
            statement.executeUpdate(
                    "INSERT INTO test_options_change VALUES (1, '2023-03-23', '2022-01-01 14:30', '2021-09-15 15:00:10')");
            statement.executeUpdate(
                    "INSERT INTO test_options_change VALUES (2, '2023-03-23', null, null)");
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

        MySqlSyncTableAction action2 =
                syncTableActionBuilder(mySqlConfig)
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
    public void testOptionsChangeInExistingTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");
        options.put("sink.parallelism", "1");
        options.put("sequence.field", "_timestamp");

        createFileStoreTable(
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.DATE(), DataTypes.TIMESTAMP(0)
                        },
                        new String[] {"pk", "_date", "_timestamp"}),
                Collections.emptyList(),
                Collections.singletonList("pk"),
                Collections.emptyList(),
                options);

        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "test_exist_options_change");
        Map<String, String> tableConfig = new HashMap<>();
        // update immutable options
        tableConfig.put("sequence.field", "_date");
        // update existing options
        tableConfig.put("sink.parallelism", "2");
        // add new options
        tableConfig.put("snapshot.expire.limit", "1000");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withPrimaryKeys("pk")
                        .withTableConfig(tableConfig)
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable();

        assertThat(table.options().get("bucket")).isEqualTo("1");
        assertThat(table.options().get("sequence.field")).isEqualTo("_timestamp");
        assertThat(table.options().get("sink.parallelism")).isEqualTo("2");
        assertThat(table.options().get("snapshot.expire.limit")).isEqualTo("1000");
    }

    @Test
    @Timeout(60)
    public void testMetadataColumns() throws Exception {
        try (Statement statement = getStatement()) {
            statement.execute("USE metadata");
            statement.executeUpdate("INSERT INTO test_metadata_columns VALUES (1, '2023-07-30')");
            statement.executeUpdate("INSERT INTO test_metadata_columns VALUES (2, '2023-07-30')");
        }
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "metadata");
        mySqlConfig.put("table-name", "test_metadata_columns");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withPrimaryKeys("pk")
                        .withMetadataColumns("table_name", "database_name", "op_ts")
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
                            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull()
                        },
                        new String[] {"pk", "_date", "table_name", "database_name", "op_ts"});
        // The record is read from the snapshot of the table and the op_ts value is 0. When querying
        // the table using the TIMESTAMP_LTZ(3) type returns 1970-01-01T00:00.
        waitForResult(
                Arrays.asList(
                        "+I[1, 2023-07-30, test_metadata_columns, metadata, 1970-01-01T00:00]",
                        "+I[2, 2023-07-30, test_metadata_columns, metadata, 1970-01-01T00:00]"),
                table,
                rowType,
                Collections.singletonList("pk"));

        // The record is read from the binlog of the table and the op_ts value is the time the
        // change was made in the database.
        // MySQL execute: statement.executeUpdate("INSERT INTO test_metadata_columns VALUES (1,
        // '2023-07-30')");
        // Paimon Table result: +I[3, 2023-11-15, test_metadata_columns, metadata,
        // 2023-11-15T04:44:12]
    }

    @Test
    public void testCatalogAndTableConfig() {
        MySqlSyncTableAction action =
                syncTableActionBuilder(getBasicMySqlConfig())
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

    @Test
    @Timeout(60)
    public void testDefaultCheckpointInterval() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "default_checkpoint");
        mySqlConfig.put("table-name", "t");

        // Using `none` to avoid compatibility issues with Flink 1.18-.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        MySqlSyncTableAction action = syncTableActionBuilder(mySqlConfig).build();
        action.withStreamExecutionEnvironment(env);

        Thread thread =
                new Thread(
                        () -> {
                            try {
                                action.run();
                            } catch (Exception ignore) {
                            }
                        });
        thread.start();

        CommonTestUtils.waitUtil(
                () -> env.getCheckpointConfig().isCheckpointingEnabled(),
                Duration.ofSeconds(5),
                Duration.ofMillis(100));

        assertThat(env.getCheckpointInterval()).isEqualTo(3 * 60 * 1000);

        thread.interrupt();
        env.close();
    }

    @Test
    @Timeout(120)
    public void testComputedColumnWithCaseInsensitive() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "computed_column_with_case_insensitive");
        mySqlConfig.put("table-name", "t");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.CASE_SENSITIVE.key(), "false"))
                        .withComputedColumnArgs("SUBSTRING=substring(UPPERCASE_STRING,2)")
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.execute("USE computed_column_with_case_insensitive");
            statement.executeUpdate("INSERT INTO t VALUES (1, 'apache')");
            statement.executeUpdate("INSERT INTO t VALUES (2, null)");
        }

        FileStoreTable table = getFileStoreTable();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.STRING(),
                        },
                        new String[] {"pk", "uppercase_string", "substring"});
        waitForResult(
                Arrays.asList("+I[1, apache, ache]", "+I[2, NULL, NULL]"),
                table,
                rowType,
                Collections.singletonList("pk"));
    }

    @Test
    @Timeout(60)
    public void testSpecifyKeysWithCaseInsensitive() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "specify_key_with_case_insensitive");
        mySqlConfig.put("table-name", "t");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.CASE_SENSITIVE.key(), "false"))
                        .withPrimaryKeys("ID1", "PART")
                        .withPartitionKeys("PART")
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable();
        assertThat(table.primaryKeys()).containsExactly("id1", "part");
        assertThat(table.partitionKeys()).containsExactly("part");
    }

    @Test
    public void testInvalidAlterBucket() throws Exception {
        // create table with bucket first
        createFileStoreTable(
                RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"k"}),
                Collections.emptyList(),
                Collections.singletonList("k"),
                Collections.emptyList(),
                Collections.singletonMap(BUCKET.key(), "1"));

        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "invalid_alter_bucket");
        mySqlConfig.put("table-name", "t");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withTableConfig(Collections.singletonMap(BUCKET.key(), "2"))
                        .build();

        assertThatCode(action::build).doesNotThrowAnyException();

        FileStoreTable table = getFileStoreTable();
        assertThat(table.options().get(BUCKET.key())).isEqualTo("1");
    }

    @Test
    @Timeout(60)
    public void testColumnCommentChangeInExistingTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");
        options.put("sink.parallelism", "1");

        RowType rowType =
                RowType.builder()
                        .field("pk", DataTypes.INT().notNull(), "pk comment")
                        .field("c1", DataTypes.DATE(), "c1 comment")
                        .field("c2", DataTypes.VARCHAR(10).notNull(), "c2 comment")
                        .build();

        createFileStoreTable(
                rowType,
                Collections.emptyList(),
                Collections.singletonList("pk"),
                Collections.emptyList(),
                options);

        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "test_exist_column_comment_change");

        // Flink cdc 2.3 does not support collecting field comments, and existing paimon table field
        // comments will not be changed.
        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withPrimaryKeys("pk")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable();
        Map<String, DataField> actual =
                table.schema().fields().stream()
                        .collect(Collectors.toMap(DataField::name, Function.identity()));
        assertThat(actual.get("pk").description()).isEqualTo("pk comment");
        assertThat(actual.get("c1").description()).isEqualTo("c1 comment");
        assertThat(actual.get("c2").description()).isEqualTo("c2 comment");
    }

    @Test
    @Timeout(60)
    public void testWriteOnlyAndSchemaEvolution() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "write_only_and_schema_evolution");
        mySqlConfig.put("table-name", "t");

        Map<String, String> tableConfig = getBasicTableConfig();
        tableConfig.put(CoreOptions.WRITE_ONLY.key(), "true");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig).withTableConfig(tableConfig).build();

        runActionWithDefaultEnv(action);
        FileStoreTable table = getFileStoreTable();

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE write_only_and_schema_evolution");
            statement.executeUpdate("INSERT INTO t VALUES (1, 'one'), (2, 'two')");
            RowType rowType =
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                            new String[] {"k", "v1"});
            List<String> primaryKeys = Collections.singletonList("k");
            List<String> expected = Arrays.asList("+I[1, one]", "+I[2, two]");
            waitForResult(expected, table, rowType, primaryKeys);

            statement.executeUpdate("ALTER TABLE t ADD COLUMN v2 INT");
            statement.executeUpdate("UPDATE t SET v2 = 1 WHERE k = 1");

            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                            },
                            new String[] {"k", "v1", "v2"});
            expected = Arrays.asList("+I[1, one, 1]", "+I[2, two, NULL]");
            waitForResult(expected, table, rowType, primaryKeys);
        }
    }

    @Test
    @Timeout(60)
    public void testUnknowMysqlScanStartupMode() {
        String scanStartupMode = "abc";
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "schema_evolution_multiple");
        mySqlConfig.put("scan.startup.mode", scanStartupMode);

        MySqlSyncTableAction action = syncTableActionBuilder(mySqlConfig).build();
        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Unknown scan.startup.mode='"
                                        + scanStartupMode
                                        + "'. Valid scan.startup.mode for MySQL CDC are [initial, earliest-offset, latest-offset, specific-offset, timestamp, snapshot]"));
    }

    @Test
    @Timeout(1000)
    public void testRuntimeExecutionModeCheckForCdcSync() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "check_cdc_sync_runtime_execution_mode");
        mySqlConfig.put("table-name", "t");

        Map<String, String> tableConfig = getBasicTableConfig();
        tableConfig.put(CoreOptions.WRITE_ONLY.key(), "true");

        MySqlSyncTableAction action = syncTableActionBuilder(mySqlConfig).build();

        assertThatThrownBy(() -> runActionWithBatchEnv(action))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "It's only support STREAMING mode for flink-cdc sync table action"));

        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable();

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE check_cdc_sync_runtime_execution_mode");
            statement.executeUpdate("INSERT INTO t VALUES (1, 'one'), (2, 'two')");
            RowType rowType =
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                            new String[] {"k", "v1"});
            List<String> primaryKeys = Collections.singletonList("k");
            List<String> expected = Arrays.asList("+I[1, one]", "+I[2, two]");
            waitForResult(expected, table, rowType, primaryKeys);
        }
    }
}
