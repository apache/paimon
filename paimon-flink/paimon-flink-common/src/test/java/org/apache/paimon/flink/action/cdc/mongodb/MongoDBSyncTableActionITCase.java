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

package org.apache.paimon.flink.action.cdc.mongodb;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** IT cases for {@link MongoDBSyncTableActionITCase}. */
public class MongoDBSyncTableActionITCase extends MongoDBActionITCaseBase {

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        runSingleTableSchemaEvolution("inventory-1");
    }

    private void runSingleTableSchemaEvolution(String sourceDir) throws Exception {
        // ---------- Write the Document into MongoDB -------------------
        String inventory = createRecordsToMongoDB(sourceDir, "table");
        Map<String, String> mongodbConfig = getBasicMongoDBConfig();
        mongodbConfig.put("database", inventory);
        mongodbConfig.put("collection", "products");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        Map<String, String> tableConfig = getBasicTableConfig();
        MongoDBSyncTableAction action =
                new MongoDBSyncTableAction(
                        mongodbConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

        testSchemaEvolutionImpl(inventory);
    }

    private void testSchemaEvolutionImpl(String dbName) throws Exception {
        waitTablesCreated(tableName);
        FileStoreTable table = getFileStoreTable(tableName);
        List<String> primaryKeys = Collections.singletonList("_id");

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "name", "description", "weight"});
        List<String> expected =
                Arrays.asList(
                        "+I[100000000000000000000101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[100000000000000000000102, car battery, 12V car battery, 8.1]",
                        "+I[100000000000000000000103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]");
        waitForResult(expected, table, rowType, primaryKeys);

        writeRecordsToMongoDB("inventory-2", dbName, "table");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "name", "description", "weight"});
        expected =
                Arrays.asList(
                        "+U[100000000000000000000101, scooter, Small 2-wheel scooter, 350]",
                        "+U[100000000000000000000102, car battery, High-performance car battery, 8.1]",
                        "+U[100000000000000000000103, 12-pack drill bits, Set of 12 professional-grade drill bits, 0.8]");
        waitForResult(expected, table, rowType, primaryKeys);

        writeRecordsToMongoDB("inventory-3", dbName, "table");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {
                            "_id", "name", "description", "weight", "hobby", "age", "address"
                        });
        expected =
                Arrays.asList(
                        "+U[100000000000000000000102, car battery, High-performance car battery, 8.1, NULL, 18, NULL]",
                        "+U[100000000000000000000103, 12-pack drill bits, Set of 12 professional-grade drill bits, 0.8, NULL, NULL, I live in Sanlitun]",
                        "+U[100000000000000000000101, scooter, Small 2-wheel scooter, 350, playing computer games, NULL, NULL]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testSpecifiedMode() throws Exception {
        String inventory = createRecordsToMongoDB("inventory-1", "table");
        Map<String, String> mongodbConfig = getBasicMongoDBConfig();
        mongodbConfig.put("database", inventory);
        mongodbConfig.put("collection", "products");
        mongodbConfig.put("field.name", "_id,name,description");
        mongodbConfig.put("parser.path", "_id,name,description");
        mongodbConfig.put("schema.start.mode", "specified");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        Map<String, String> tableConfig = getBasicTableConfig();
        MongoDBSyncTableAction action =
                new MongoDBSyncTableAction(
                        mongodbConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);
        waitTablesCreated(tableName);
        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(), DataTypes.STRING(), DataTypes.STRING()
                        },
                        new String[] {"_id", "name", "description"});
        List<String> primaryKeys = Collections.singletonList("_id");
        List<String> expected =
                Arrays.asList(
                        "+I[100000000000000000000101, scooter, Small 2-wheel scooter]",
                        "+I[100000000000000000000102, car battery, 12V car battery]",
                        "+I[100000000000000000000103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3]");
        waitForResult(expected, table, rowType, primaryKeys);
    }
}
