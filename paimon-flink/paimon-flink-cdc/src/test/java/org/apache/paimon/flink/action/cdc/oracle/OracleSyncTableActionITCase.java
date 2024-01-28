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

package org.apache.paimon.flink.action.cdc.oracle;

import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.paimon.flink.action.cdc.postgres.PostgresSyncTableAction;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.cdc.oracle.OracleContainer.createAndInitialize;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link OracleSyncTableAction}. */
public class OracleSyncTableActionITCase extends OracleActionITCaseBase {
    private static final String DATABASE_NAME = "ORCLCDB";

    @BeforeAll
    public static void startContainers() throws Exception {
        //ORACLE_CONTAINER.withSetupSQL("oracle/sync_table_setup.sql");

        start();
        createAndInitialize("oracle/sync_table_setup.sql");
    }

    @Test
    // @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        Map<String, String> oracleConfig = getBasicOracleConfig();
        oracleConfig.put(OracleSourceOptions.SCHEMA_NAME.key(), ORACLE_SCHEMA);
        oracleConfig.put(OracleSourceOptions.TABLE_NAME.key(), "COMPOSITE\\d+");

        OracleSyncTableAction action =
                syncTableActionBuilder(oracleConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.METASTORE.key(), "test-alter-table"))
                        .withTableConfig(getBasicTableConfig())
                        .withPartitionKeys("ID")
                        .withPrimaryKeys("ID", "NAME")
                        .build();
        runActionWithDefaultEnv(action);

                checkTableSchema(
                        "[{\"id\":0,\"name\":\"ID\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"NAME\",\"type\":\"STRING NOT NULL\"}," +
                                "{\"id\":2,\"name\":\"WEIGHT\",\"type\":\"STRING\"}]");


        try (Statement statement = getStatementDBA()) {
            FileStoreTable table = getFileStoreTable();

            statement.executeUpdate("INSERT INTO DEBEZIUM.composite1 (id,name,weight) VALUES (101,'Jack',3.25)");
            statement.executeUpdate("INSERT INTO DEBEZIUM.composite2 (id,name,weight) VALUES (102,'test66',7.14)");

            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.STRING().notNull(),
                                    DataTypes.STRING()
                            },
                            new String[] {"ID", "NAME","WEIGHT"});
            List<String> primaryKeys = Arrays.asList("ID", "NAME");
            List<String> expected =
                    Arrays.asList("+I[102, test66, 7.14]", "+I[101, Jack, 3.25]");
            waitForResult(expected, table, rowType, primaryKeys);

            boolean execute = statement.execute("ALTER TABLE DEBEZIUM.composite1 ADD v1 FLOAT");
            boolean execute1 = statement.execute(
                    "INSERT INTO DEBEZIUM.composite1 VALUES (103,'three',3.25,5.12)");

            statement.execute("ALTER TABLE DEBEZIUM.composite2 ADD v1 FLOAT");
            statement.execute(
                    "UPDATE DEBEZIUM.composite2 SET id = 1020, name = 'test88' where id = 102");
            rowType =
                    RowType.of(
                            new DataType[] {
                                    DataTypes.INT().notNull(),
                                    DataTypes.STRING().notNull(),
                                    DataTypes.STRING(),
                                    DataTypes.STRING()
                            },
                            new String[] {"ID", "NAME", "WEIGHT", "V1"});
            expected =
                    Arrays.asList(
                            "+I[101, Jack, 3.25, NULL]",
                            "+I[1020, test88, 7.14, NULL]",
                            "+I[103, three, 3.25, 5.12]");
            waitForResult(expected, table, rowType, primaryKeys);
            }
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
        Map<String, String> oracleConfig = getBasicOracleConfig();
        oracleConfig.put(OracleSourceOptions.SCHEMA_NAME.key(), ORACLE_SCHEMA);
        oracleConfig.put(OracleSourceOptions.TABLE_NAME.key(), "COMPOSITE\\d+");

        OracleSyncTableAction action =
                syncTableActionBuilder(oracleConfig)
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.METASTORE.key(), "test-alter-table"))
                        .withTableConfig(getBasicTableConfig())
                        .withPartitionKeys("ID")
                        .withPrimaryKeys("ID", "NAME")
                        .build();
        runActionWithDefaultEnv(action);
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

        try (Statement statement = getStatement(DATABASE_NAME)) {
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
                                + "Paimon, Apache Paimon, Apache Paimon PostgreSQL Test Data, "
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

    private void checkTableSchema(String excepted) throws Exception {

        FileStoreTable table = getFileStoreTable();

        assertThat(JsonSerdeUtil.toFlatJson(table.schema().fields())).isEqualTo(excepted);
    }

    private FileStoreTable getFileStoreTable() throws Exception {
        return getFileStoreTable(tableName);
    }
}
