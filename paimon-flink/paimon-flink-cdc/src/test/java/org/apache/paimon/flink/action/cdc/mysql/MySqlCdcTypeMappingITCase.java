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
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TINYINT1_NOT_BOOL;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_NULLABLE;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;
import static org.assertj.core.api.Assertions.assertThat;

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
}
