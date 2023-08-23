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

/** IT cases for {@link MongoDBSyncDatabaseAction}. */
public class MongoDBSyncDatabaseActionITCase extends MongoDBActionITCaseBase {

    @Test
    @Timeout(120)
    public void testSchemaEvolution() throws Exception {
        writeRecordsToMongoDB("test-data-1", database, "database");
        writeRecordsToMongoDB("test-data-2", database, "database");

        Map<String, String> mongodbConfig = getBasicMongoDBConfig();
        mongodbConfig.put("database", database);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        Map<String, String> tableConfig = getBasicTableConfig();
        MongoDBSyncDatabaseAction action =
                new MongoDBSyncDatabaseAction(
                        mongodbConfig, warehouse, database, Collections.emptyMap(), tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        testSchemaEvolutionImpl();
    }

    private void testSchemaEvolutionImpl() throws Exception {
        waitTablesCreated("t1", "t2");

        FileStoreTable table1 = getFileStoreTable("t1");
        FileStoreTable table2 = getFileStoreTable("t2");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "name", "description", "weight"});
        List<String> primaryKeys1 = Collections.singletonList("_id");
        List<String> expected =
                Arrays.asList(
                        "+I[100000000000000000000101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[100000000000000000000102, car battery, 12V car battery, 8.1]",
                        "+I[100000000000000000000103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "name", "address", "phone_number"});
        List<String> primaryKeys2 = Collections.singletonList("_id");
        expected =
                Arrays.asList(
                        "+I[100000000000000000000101, user_1, Shanghai, 123563291234]",
                        "+I[100000000000000000000102, user_2, Beijing, 1234347891234]",
                        "+I[100000000000000000000103, user_3, Hangzhou, 1235567891234]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        writeRecordsToMongoDB("test-data-3", database, "database");

        expected =
                Arrays.asList(
                        "+U[100000000000000000000101, scooter, Small 2-wheel scooter, 350]",
                        "+U[100000000000000000000102, car battery, High-performance car battery, 8.1]",
                        "+U[100000000000000000000103, 12-pack drill bits, Set of 12 professional-grade drill bits, 0.8]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        writeRecordsToMongoDB("test-data-4", database, "database");

        expected =
                Arrays.asList(
                        "+U[100000000000000000000101, user_1, Guangzhou, 123563291234]",
                        "+U[100000000000000000000102, user_2, Beijing, 1234546591234]",
                        "+U[100000000000000000000103, user_3, Nanjing, 1235567891234]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }
}
