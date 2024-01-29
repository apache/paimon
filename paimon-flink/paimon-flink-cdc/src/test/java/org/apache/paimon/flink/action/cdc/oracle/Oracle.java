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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.cdc.oracle.OracleContainer.createAndInitialize;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link OracleSyncTableAction}. */
public class Oracle extends OracleActionITCaseBase {
    private static final String DATABASE_NAME = "ORCLCDB";

    @BeforeAll
    public static void startContainers() throws Exception {
        // ORACLE_CONTAINER.withSetupSQL("oracle/sync_table_setup.sql");

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
                        + "{\"id\":1,\"name\":\"NAME\",\"type\":\"STRING NOT NULL\"},"
                        + "{\"id\":2,\"name\":\"WEIGHT\",\"type\":\"STRING\"}]");

        try (Statement statement = getStatementDBA()) {
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

            boolean execute = statement.execute("ALTER TABLE DEBEZIUM.composite1 ADD v1 FLOAT");
            boolean execute1 =
                    statement.execute(
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

    private void checkTableSchema(String excepted) throws Exception {

        FileStoreTable table = getFileStoreTable();

        assertThat(JsonSerdeUtil.toFlatJson(table.schema().fields())).isEqualTo(excepted);
    }

    private FileStoreTable getFileStoreTable() throws Exception {
        return getFileStoreTable(tableName);
    }
}
