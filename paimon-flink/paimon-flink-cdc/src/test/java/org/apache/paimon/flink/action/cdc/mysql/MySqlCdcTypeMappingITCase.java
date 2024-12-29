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

import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.cdc.debezium.utils.JdbcUrlUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.BIGINT_UNSIGNED_TO_BIGINT;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.CHAR_TO_STRING;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.DECIMAL_NO_CHANGE;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.LONGTEXT_TO_BYTES;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.NO_CHANGE;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TINYINT1_NOT_BOOL;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_NULLABLE;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT test for {@link TypeMapping} in MySQL CDC. */
public class MySqlCdcTypeMappingITCase extends MySqlActionITCaseBase {

    @BeforeAll
    public static void startContainers() {
        MYSQL_CONTAINER.withSetupSQL("mysql/type_mapping_test_setup.sql");
        start();
    }

    // ------------------------------------- tinyint1-not-bool -------------------------------------

    @Test
    @Timeout(60)
    public void testTinyInt1NotBool() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "tinyint1_not_bool_test");

        // test tinyInt1isBit compatibility and url building
        mySqlConfig.put(JdbcUrlUtils.PROPERTIES_PREFIX + "tinyInt1isBit", "false");
        mySqlConfig.put(JdbcUrlUtils.PROPERTIES_PREFIX + "useSSL", "false");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withMode(COMBINED.configString())
                        .withTypeMappingModes(TINYINT1_NOT_BOOL.configString())
                        .build();
        runActionWithDefaultEnv(action);

        // read old data
        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.TINYINT()},
                        new String[] {"pk", "_tinyint1"});
        waitForResult(
                Collections.singletonList("+I[1, 1]"),
                getFileStoreTable("t1"),
                rowType1,
                Collections.singletonList("pk"));

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE tinyint1_not_bool_test");

            // test schema evolution
            statement.executeUpdate("ALTER TABLE t1 ADD COLUMN _new_tinyint1 TINYINT(1)");
            statement.executeUpdate("INSERT INTO t1 VALUES (2, -128, 127)");

            RowType rowType2 =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(), DataTypes.TINYINT(), DataTypes.TINYINT()
                            },
                            new String[] {"pk", "_tinyint1", "_new_tinyint1"});
            waitForResult(
                    Arrays.asList("+I[1, 1, NULL]", "+I[2, -128, 127]"),
                    getFileStoreTable("t1"),
                    rowType2,
                    Collections.singletonList("pk"));

            // test newly created table
            statement.executeUpdate(
                    "CREATE TABLE t2 (pk INT, _tinyint1 TINYINT(1), PRIMARY KEY (pk))");
            statement.executeUpdate("INSERT INTO t2 VALUES (1, 1), (2, 127), (3, -128)");

            waitingTables("t2");
            waitForResult(
                    Arrays.asList("+I[1, 1]", "+I[2, 127]", "+I[3, -128]"),
                    getFileStoreTable("t2"),
                    rowType1,
                    Collections.singletonList("pk"));
        }
    }

    @Test
    public void testConflictTinyInt1NotBool() {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "tinyint1_not_bool_test");
        mySqlConfig.put(JdbcUrlUtils.PROPERTIES_PREFIX + "tinyInt1isBit", "true");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withMode(COMBINED.configString())
                        .withTypeMappingModes(TINYINT1_NOT_BOOL.configString())
                        .build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Type mapping option 'tinyint1-not-bool' conflicts with "
                                        + "jdbc properties 'jdbc.properties.tinyInt1isBit=true'. "
                                        + "Option 'tinyint1-not-bool' is equal to 'jdbc.properties.tinyInt1isBit=false'."));
    }

    // --------------------------------------- all-to-string ---------------------------------------

    @Test
    @Timeout(60)
    public void testReadAllTypes() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "all_to_string_test");
        mySqlConfig.put("table-name", "all_types_table");

        MySqlSyncTableAction action =
                syncTableActionBuilder(mySqlConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
                        .withTypeMappingModes(TO_STRING.configString())
                        .build();
        runActionWithDefaultEnv(action);

        int allTypeNums = 78;
        DataType[] types =
                IntStream.range(0, allTypeNums)
                        .mapToObj(i -> DataTypes.STRING())
                        .toArray(DataType[]::new);
        types[0] = types[0].notNull(); // id
        types[1] = types[1].notNull(); // pt
        types[22] = types[22].notNull(); // _serial SERIAL

        RowType rowType =
                RowType.of(
                        types,
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

        List<String> expected =
                Arrays.asList(
                        "+I["
                                + "1, 1.1, "
                                + "true, 0000000000000000000000000000000000000000000000000000011111000111, "
                                + "1, 1, 0, 1, 2, 3, "
                                + "1000, 2000, 3000, "
                                + "100000, 200000, 300000, "
                                + "1000000, 2000000, 3000000, "
                                + "10000000000, 20000000000, 30000000000, 40000000000, "
                                + "1.5, 2.5, 3.5, "
                                + "1.000001, 2.000002, 3.000003, "
                                + "1.000011, 2.000022, 3.000033, "
                                + "1.000111, 2.000222, 3.000333, "
                                + "12345.11, 12345.22, 12345.33, "
                                + "123456789876543212345678987654321.11, 123456789876543212345678987654321.22, 123456789876543212345678987654321.33, "
                                + "11111, 22222, 33333, 2222222222222222300000001111.123456789, "
                                + "2023-03-23, "
                                // display value of datetime is not affected by timezone
                                + "2023-03-23 14:30:05.000, 2023-03-23 14:30:05.123, 2023-03-23 14:30:05.123456, "
                                + "2023-03-24 14:30:00.000, 2023-03-24 14:30:05.120, "
                                // display value of timestamp is affected by timezone
                                // we store 2023-03-23T15:00:10.123456 in UTC-8 system timezone
                                // and query this timestamp in UTC-5 MySQL server timezone
                                // so the display value should increase by 3 hour
                                + "2023-03-23 18:00:10.123456, 2023-03-23 03:10:00.000000, "
                                + "Paimon, Apache Paimon, Apache Paimon MySQL TINYTEXT Test Data, Apache Paimon MySQL Test Data, Apache Paimon MySQL MEDIUMTEXT Test Data, Apache Paimon MySQL Long Test Data, "
                                + "bytes\u0000\u0000\u0000\u0000\u0000, more bytes, TINYBLOB type test data, BLOB type test data, MEDIUMBLOB type test data, LONGBLOB  bytes test data, "
                                + "{\"a\": \"b\"}, "
                                + "value1, "
                                + "2023, "
                                + "10:13:23, "
                                + "{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}, "
                                + "{\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}, "
                                + "{\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0}, "
                                + "{\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0}, "
                                + "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0}, "
                                + "a,b"
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
        waitForResult(expected, getFileStoreTable(tableName), rowType, Arrays.asList("pt", "_id"));
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolutionAndNewlyCreatedTable() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "all_to_string_test");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .excludingTables("all_types_table")
                        .withMode(COMBINED.configString())
                        .withTypeMappingModes(TO_STRING.configString())
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE all_to_string_test");

            // test schema evolution
            statement.executeUpdate("INSERT INTO schema_evolution_test VALUES (1, 1)");
            FileStoreTable table = getFileStoreTable("schema_evolution_test");
            RowType rowType =
                    RowType.of(
                            new DataType[] {DataTypes.STRING().notNull(), DataTypes.STRING()},
                            new String[] {"pk", "v1"});
            waitForResult(
                    Collections.singletonList("+I[1, 1]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            statement.executeUpdate("ALTER TABLE schema_evolution_test MODIFY COLUMN v1 BIGINT");
            statement.executeUpdate("INSERT INTO schema_evolution_test VALUES (2, 20000000000)");

            waitForResult(
                    Arrays.asList("+I[1, 1]", "+I[2, 20000000000]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            statement.executeUpdate(
                    "ALTER TABLE schema_evolution_test ADD COLUMN v2 VARBINARY(10)");
            statement.executeUpdate(
                    "INSERT INTO schema_evolution_test VALUES (3, 3, '0123456789'), (4, 4, 'A')");

            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.STRING().notNull(), DataTypes.STRING(), DataTypes.STRING()
                            },
                            new String[] {"pk", "v1", "v2"});
            waitForResult(
                    Arrays.asList(
                            "+I[1, 1, NULL]",
                            "+I[2, 20000000000, NULL]",
                            "+I[3, 3, 0123456789]",
                            "+I[4, 4, A]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            // test newly created table
            statement.executeUpdate(
                    "CREATE TABLE _new_table (pk INT, v VARBINARY(10), PRIMARY KEY (pk))");
            statement.executeUpdate("INSERT INTO _new_table VALUES (1, 'Paimon')");

            waitingTables("_new_table");
            waitForResult(
                    Collections.singletonList("+I[1, Paimon]"),
                    getFileStoreTable("_new_table"),
                    RowType.of(
                            new DataType[] {DataTypes.STRING().notNull(), DataTypes.STRING()},
                            new String[] {"pk", "v"}),
                    Collections.singletonList("pk"));
        }
    }

    // ------------------------------------- ignore-not-null -------------------------------------
    @Test
    @Timeout(60)
    public void testIgnoreNotNull() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "ignore_not_null_test");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withMode(COMBINED.configString())
                        .withTypeMappingModes(TO_NULLABLE.configString())
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable("t1");
        assertThat(table.rowType().getTypeAt(1).isNullable()).isTrue();

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE ignore_not_null_test");

            RowType rowType =
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                            new String[] {"pk", "v1"});
            waitForResult(
                    Collections.singletonList("+I[1, A]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            // test schema evolution
            statement.executeUpdate("ALTER TABLE t1 ADD COLUMN v2 INT NOT NULL DEFAULT 100");
            statement.executeUpdate("INSERT INTO t1 VALUES (2, 'B', 10)");

            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                            },
                            new String[] {"pk", "v1", "v2"});
            // Currently we haven't handle default value
            waitForResult(
                    Arrays.asList("+I[1, A, NULL]", "+I[2, B, 10]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            // test newly created table
            statement.executeUpdate(
                    "CREATE TABLE _new_table (pk INT, v INT NOT NULL, PRIMARY KEY (pk))");

            waitingTables("_new_table");
            assertThat(getFileStoreTable("_new_table").rowType().getTypeAt(1).isNullable())
                    .isTrue();
        }
    }

    // -------------------------------------- char-to-string --------------------------------------

    @Test
    @Timeout(60)
    public void testCharToString() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "char_to_string_test");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withMode(COMBINED.configString())
                        .withTypeMappingModes(CHAR_TO_STRING.configString())
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable("t1");

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE char_to_string_test");

            // test schema evolution
            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(), DataTypes.STRING().notNull()
                            },
                            new String[] {"pk", "v1"});
            waitForResult(
                    Collections.singletonList("+I[1, 1]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            statement.executeUpdate("ALTER TABLE t1 ADD COLUMN v2 CHAR(1)");
            statement.executeUpdate("INSERT INTO t1 VALUES (2, '2', 'A'), (3, '3', 'B')");

            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING()
                            },
                            new String[] {"pk", "v1", "v2"});
            waitForResult(
                    Arrays.asList("+I[1, 1, NULL]", "+I[2, 2, A]", "+I[3, 3, B]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            // test newly created table
            statement.executeUpdate(
                    "CREATE TABLE _new_table (pk INT, v VARCHAR(10), PRIMARY KEY (pk))");
            statement.executeUpdate("INSERT INTO _new_table VALUES (1, 'Paimon')");

            waitingTables("_new_table");
            waitForResult(
                    Collections.singletonList("+I[1, Paimon]"),
                    getFileStoreTable("_new_table"),
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.STRING()},
                            new String[] {"pk", "v"}),
                    Collections.singletonList("pk"));
        }
    }

    // ------------------------------------- longtext-to-bytes -------------------------------------

    @Test
    @Timeout(60)
    public void testLongtextToBytes() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "longtext_to_bytes_test");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withMode(COMBINED.configString())
                        .withTypeMappingModes(LONGTEXT_TO_BYTES.configString())
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable("t1");

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE longtext_to_bytes_test");

            // test schema evolution
            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(), DataTypes.VARCHAR(10).notNull()
                            },
                            new String[] {"pk", "v1"});
            waitForResult(
                    Collections.singletonList("+I[1, 1]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            statement.executeUpdate("ALTER TABLE t1 ADD COLUMN v2 LONGTEXT");
            statement.executeUpdate(
                    "INSERT INTO t1 VALUES (2, '2', 'This is an example of a long text string, meant to demonstrate the usage of the LONGTEXT data type in SQL databases.')");

            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(10).notNull(),
                                DataTypes.BYTES()
                            },
                            new String[] {"pk", "v1", "v2"});
            waitForResult(
                    Arrays.asList(
                            "+I[1, 1, NULL]",
                            "+I[2, 2, [84, 104, 105, 115, 32, 105, 115, 32, 97, 110, 32, 101, 120, 97, 109, 112, 108, 101, 32, 111, 102, 32, 97, 32, 108, 111, 110, 103, 32, 116, 101, 120, 116, 32, 115, 116, 114, 105, 110, 103, 44, 32, 109, 101, 97, 110, 116, 32, 116, 111, 32, 100, 101, 109, 111, 110, 115, 116, 114, 97, 116, 101, 32, 116, 104, 101, 32, 117, 115, 97, 103, 101, 32, 111, 102, 32, 116, 104, 101, 32, 76, 79, 78, 71, 84, 69, 88, 84, 32, 100, 97, 116, 97, 32, 116, 121, 112, 101, 32, 105, 110, 32, 83, 81, 76, 32, 100, 97, 116, 97, 98, 97, 115, 101, 115, 46]]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            // test newly created table
            statement.executeUpdate(
                    "CREATE TABLE _new_table (pk INT, v LONGTEXT, PRIMARY KEY (pk))");
            statement.executeUpdate("INSERT INTO _new_table VALUES (1, 'Paimon')");

            waitingTables("_new_table");
            waitForResult(
                    Collections.singletonList("+I[1, [80, 97, 105, 109, 111, 110]]"),
                    getFileStoreTable("_new_table"),
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.BYTES()},
                            new String[] {"pk", "v"}),
                    Collections.singletonList("pk"));
        }
    }

    // --------------------------------- bigint-unsigned-to-bigint ---------------------------------

    @Test
    @Timeout(60)
    public void testBigintUnsignedToBigint() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "bigint_unsigned_to_bigint_test");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withMode(COMBINED.configString())
                        .withTypeMappingModes(BIGINT_UNSIGNED_TO_BIGINT.configString())
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable("t1");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT().notNull()
                        },
                        new String[] {"pk", "v1", "v2", "v3"});
        waitForResult(
                Collections.singletonList("+I[1, 12345, 56789, 123456789]"),
                table,
                rowType,
                Collections.singletonList("pk"));

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE bigint_unsigned_to_bigint_test");

            // test schema evolution
            statement.executeUpdate("ALTER TABLE t1 ADD COLUMN v4 BIGINT UNSIGNED");
            statement.executeUpdate(
                    "INSERT INTO t1 VALUES (2, 23456, 67890, 234567890, 1234567890)");

            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT().notNull(),
                                DataTypes.BIGINT(),
                            },
                            new String[] {"pk", "v1", "v2", "v3", "v4"});
            waitForResult(
                    Arrays.asList(
                            "+I[1, 12345, 56789, 123456789, NULL]",
                            "+I[2, 23456, 67890, 234567890, 1234567890]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            // test newly created table
            statement.executeUpdate(
                    "CREATE TABLE _new_table (pk INT, v BIGINT UNSIGNED, PRIMARY KEY (pk))");
            statement.executeUpdate("INSERT INTO _new_table VALUES (1, 1234567890)");

            waitingTables("_new_table");
            waitForResult(
                    Collections.singletonList("+I[1, 1234567890]"),
                    getFileStoreTable("_new_table"),
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.BIGINT()},
                            new String[] {"pk", "v"}),
                    Collections.singletonList("pk"));
        }
    }

    // --------------------------------- bigint-unsigned-to-bigint ---------------------------------

    @Test
    @Timeout(60)
    public void testDecimalNoChange() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "decimal_no_change_test");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withMode(COMBINED.configString())
                        .withTypeMappingModes(DECIMAL_NO_CHANGE.configString())
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable("t1");
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.DECIMAL(10, 2)},
                        new String[] {"pk", "v1"});
        waitForResult(
                Collections.singletonList("+I[1, 1.23]"),
                table,
                rowType,
                Collections.singletonList("pk"));

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE decimal_no_change_test");

            // test schema evolution
            statement.executeUpdate("ALTER TABLE t1 ADD COLUMN v2 DECIMAL(10,2)");
            statement.executeUpdate("INSERT INTO t1 VALUES (2, 2.34, 2.56)");

            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.DECIMAL(10, 2)
                            },
                            new String[] {"pk", "v1", "v2"});
            waitForResult(
                    Arrays.asList("+I[1, 1.23, NULL]", "+I[2, 2.34, 2.56]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            // test decimal precision change
            statement.executeUpdate("ALTER TABLE t1 MODIFY COLUMN v2 DECIMAL(15,4)");
            statement.executeUpdate("INSERT INTO t1 VALUES (3, 3.45, 3.67)");

            waitForResult(
                    Arrays.asList("+I[1, 1.23, NULL]", "+I[2, 2.34, 2.56]", "+I[3, 3.45, 3.67]"),
                    table,
                    rowType, // should not change
                    Collections.singletonList("pk"));

            // test newly created table
            statement.executeUpdate(
                    "CREATE TABLE _new_table (pk INT, v Decimal(10,2), PRIMARY KEY (pk))");
            statement.executeUpdate("INSERT INTO _new_table VALUES (1, 1.23)");

            waitingTables("_new_table");
            waitForResult(
                    Collections.singletonList("+I[1, 1.23]"),
                    getFileStoreTable("_new_table"),
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.DECIMAL(10, 2)},
                            new String[] {"pk", "v"}),
                    Collections.singletonList("pk"));
        }
    }

    // -------------------------------------- no-change --------------------------------------

    @Test
    @Timeout(60)
    public void testNoChange() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", "char_no_change_test");

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withMode(COMBINED.configString())
                        .withTypeMappingModes(
                                CHAR_TO_STRING.configString(), NO_CHANGE.configString())
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable("t1");

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE char_no_change_test");

            // test schema evolution
            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(), DataTypes.VARCHAR(10).notNull()
                            },
                            new String[] {"pk", "v1"});
            waitForResult(
                    Collections.singletonList("+I[1, 1]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            statement.executeUpdate("ALTER TABLE t1 ADD COLUMN v2 CHAR(1)");
            statement.executeUpdate("INSERT INTO t1 VALUES (2, '2', 'A'), (3, '3', 'B')");

            rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(10).notNull(),
                                DataTypes.CHAR(1)
                            },
                            new String[] {"pk", "v1", "v2"});
            waitForResult(
                    Arrays.asList("+I[1, 1, NULL]", "+I[2, 2, A]", "+I[3, 3, B]"),
                    table,
                    rowType,
                    Collections.singletonList("pk"));

            // test newly created table
            statement.executeUpdate(
                    "CREATE TABLE _new_table (pk INT, v VARCHAR(10), PRIMARY KEY (pk))");
            statement.executeUpdate("INSERT INTO _new_table VALUES (1, 'Paimon')");

            waitingTables("_new_table");
            waitForResult(
                    Collections.singletonList("+I[1, Paimon]"),
                    getFileStoreTable("_new_table"),
                    RowType.of(
                            new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                            new String[] {"pk", "v"}),
                    Collections.singletonList("pk"));
        }
    }
}
