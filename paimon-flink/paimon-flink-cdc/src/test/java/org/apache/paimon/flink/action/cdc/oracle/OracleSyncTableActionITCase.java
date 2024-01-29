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

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions;
import org.apache.flink.core.execution.JobClient;
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

    @BeforeAll
    public static void startContainers() throws Exception {
        start();
        createAndInitialize("oracle/sync_table_setup.sql");
    }

    @Test
    @Timeout(240)
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
                        + "{\"id\":1,\"name\":\"NAME\",\"type\":\"STRING NOT NULL\"},"
                        + "{\"id\":2,\"name\":\"WEIGHT\",\"type\":\"STRING\"}]");

        try (Statement statement = getStatement()) {
            FileStoreTable table = getFileStoreTable();

            statement.executeUpdate(
                    "INSERT INTO DEBEZIUM.composite1 (id,name,weight) VALUES (101,'Jack',3.25)");
            statement.executeUpdate(
                    "INSERT INTO DEBEZIUM.composite2 (id,name,weight) VALUES (102,'test66',7.14)");

            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.INT().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING()
                            },
                            new String[] {"ID", "NAME", "WEIGHT"});
            List<String> primaryKeys = Arrays.asList("ID", "NAME");
            List<String> expected = Arrays.asList("+I[102, test66, 7.14]", "+I[101, Jack, 3.25]");
            waitForResult(expected, table, rowType, primaryKeys);

            statement.execute("ALTER TABLE DEBEZIUM.composite1 ADD v1 FLOAT");
            statement.execute("INSERT INTO DEBEZIUM.composite1 VALUES (103,'three',3.25,5.12)");

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
    @Timeout(240)
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
        oracleConfig.put(OracleSourceOptions.TABLE_NAME.key(), "FULL_TYPES");

        OracleSyncTableAction action =
                syncTableActionBuilder(oracleConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withPartitionKeys("ID")
                        .withPrimaryKeys("ID", "VAL_VARCHAR")
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
                            DataTypes.INT().notNull(),
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.FLOAT(),
                            DataTypes.DOUBLE(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.TINYINT(),
                            DataTypes.TINYINT(),
                            DataTypes.SMALLINT(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.TIMESTAMP(6),
                            DataTypes.TIMESTAMP(6),
                            DataTypes.TIMESTAMP(6),
                            DataTypes.TIMESTAMP(6),
                            DataTypes.TIMESTAMP(6),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                        },
                        new String[] {
                            "ID",
                            "VAL_VARCHAR",
                            "VAL_VARCHAR2",
                            "VAL_NVARCHAR2",
                            "VAL_CHAR",
                            "VAL_NCHAR",
                            "VAL_BF",
                            "VAL_BD",
                            "VAL_F",
                            "VAL_F_10",
                            "VAL_NUM",
                            "VAL_DP",
                            "VAL_R",
                            "VAL_DECIMAL",
                            "VAL_NUMERIC",
                            "VAL_NUM_VS",
                            "VAL_INT",
                            "VAL_INTEGER",
                            "VAL_SMALLINT",
                            "VAL_NUMBER_38_NO_SCALE",
                            "VAL_NUMBER_38_SCALE_0",
                            "VAL_NUMBER_1",
                            "VAL_NUMBER_2",
                            "VAL_NUMBER_4",
                            "VAL_NUMBER_9",
                            "VAL_NUMBER_18",
                            "VAL_DATE",
                            "VAL_TS",
                            "VAL_TS_PRECISION2",
                            "VAL_TS_PRECISION4",
                            "VAL_TS_PRECISION9",
                            "VAL_TSLTZ",
                            "T15VARCHAR",
                        });
        FileStoreTable table = getFileStoreTable();

        List<String> expected =
                Arrays.asList(
                        "+I[1, vc2, vc2, nvc2, c  , nc , 1.1, 2.22, 3.33, 8.888, 4.444400, 5.555, 6.66, 1234.567891, 1234.567891, 77.323, 1, 22, 333, 4444, 5555, 1, 9, 999, 99999999, 99999999999999999, 2022-10-30T00:00, 2022-10-30T12:34:56.007890, 2022-10-30T12:34:56.130, 2022-10-30T12:34:56.125500, 2022-10-30T12:34:56.125457, 2022-10-29 17:34:56.007890, <name>\n"
                                + "  <a id=\"1\" value=\"some values\">test xmlType</a>\n"
                                + "</name>\n"
                                + "]");
        waitForResult(expected, table, rowType, Arrays.asList("ID", "VAL_VARCHAR"));
    }

    private void checkTableSchema(String excepted) throws Exception {

        FileStoreTable table = getFileStoreTable();

        assertThat(JsonSerdeUtil.toFlatJson(table.schema().fields())).isEqualTo(excepted);
    }

    private FileStoreTable getFileStoreTable() throws Exception {
        return getFileStoreTable(tableName);
    }
}
