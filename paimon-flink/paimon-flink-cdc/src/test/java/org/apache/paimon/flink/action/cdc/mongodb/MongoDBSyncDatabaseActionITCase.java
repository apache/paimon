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

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link MongoDBSyncDatabaseAction}. */
public class MongoDBSyncDatabaseActionITCase extends MongoDBActionITCaseBase {

    @Test
    @Timeout(90)
    public void testSchemaEvolution() throws Exception {
        writeRecordsToMongoDB("test-data-1", database, "database");
        writeRecordsToMongoDB("test-data-2", database, "database");

        Map<String, String> mongodbConfig = getBasicMongoDBConfig();
        mongodbConfig.put("database", database);
        MongoDBSyncDatabaseAction action =
                syncDatabaseActionBuilder(mongodbConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl("t1", "t2", database);
    }

    private void testSchemaEvolutionImpl(String table1Name, String table2Name, String dbName)
            throws Exception {
        waitingTables(table1Name, table2Name);

        FileStoreTable table1 = getFileStoreTable(table1Name);
        FileStoreTable table2 = getFileStoreTable(table2Name);

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

        writeRecordsToMongoDB("test-data-3", dbName, "database");

        expected =
                Arrays.asList(
                        "+I[100000000000000000000101, scooter, Small 2-wheel scooter, 350]",
                        "+I[100000000000000000000102, car battery, High-performance car battery, 8.1]",
                        "+I[100000000000000000000103, 12-pack drill bits, Set of 12 professional-grade drill bits, 0.8]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        writeRecordsToMongoDB("test-data-4", dbName, "database");

        expected =
                Arrays.asList(
                        "+I[100000000000000000000101, user_1, Guangzhou, 123563291234]",
                        "+I[100000000000000000000102, user_2, Beijing, 1234546591234]",
                        "+I[100000000000000000000103, user_3, Nanjing, 1235567891234]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }

    @Test
    public void testCatalogAndTableConfig() {
        MongoDBSyncDatabaseAction action =
                syncDatabaseActionBuilder(getBasicMongoDBConfig())
                        .withCatalogConfig(Collections.singletonMap("catalog-key", "catalog-value"))
                        .withTableConfig(Collections.singletonMap("table-key", "table-value"))
                        .build();

        assertThat(action.catalogConfig()).containsEntry("catalog-key", "catalog-value");
        assertThat(action.tableConfig())
                .containsExactlyEntriesOf(Collections.singletonMap("table-key", "table-value"));
    }

    @Test
    @Timeout(90)
    public void testMongoDBNestedDataSynchronizationAndVerification() throws Exception {
        writeRecordsToMongoDB("test-data-5", database, "database");
        writeRecordsToMongoDB("test-data-6", database, "database");
        Map<String, String> mongodbConfig = getBasicMongoDBConfig();
        mongodbConfig.put("database", database);
        MongoDBSyncDatabaseAction action =
                syncDatabaseActionBuilder(mongodbConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        waitingTables("t3", "t4");
        FileStoreTable table1 = getFileStoreTable("t3");
        FileStoreTable table2 = getFileStoreTable("t4");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "country", "languages", "religions"});
        List<String> primaryKeys1 = Collections.singletonList("_id");
        List<String> expected1 =
                Arrays.asList(
                        "+I[610000000000000000000101, Switzerland, Italian, {\"f\":\"v\",\"n\":null}]",
                        "+I[610000000000000000000102, Switzerland, Italian, ]",
                        "+I[610000000000000000000103, Switzerland, [\"Italian\"], ]");
        waitForResult(expected1, table1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "kind", "etag", "pageInfo", "items"});
        List<String> primaryKeys2 = Collections.singletonList("_id");
        List<String> expected2 =
                Arrays.asList(
                        "+I[610000000000000000000101, youtube#videoListResponse, \\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\", {\"totalResults\":1,\"resultsPerPage\":1}, [{\"kind\":\"youtube#video\",\"etag\":\"\\\\\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\\\\\"\",\"id\":\"wHkPb68dxEw\",\"statistics\":{\"viewCount\":\"9211\",\"likeCount\":\"79\",\"dislikeCount\":\"11\",\"favoriteCount\":\"0\",\"commentCount\":\"29\"},\"topicDetails\":{\"topicIds\":[\"/m/02mjmr\"],\"relevantTopicIds\":[\"/m/0cnfvd\",\"/m/01jdpf\"]}}]]",
                        "+I[610000000000000000000102, youtube#videoListResponse, \\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\", page, [{\"kind\":\"youtube#video\",\"etag\":\"\\\\\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\\\\\"\",\"id\":\"wHkPb68dxEw\",\"statistics\":{\"viewCount\":\"9211\",\"likeCount\":\"79\",\"dislikeCount\":\"11\",\"favoriteCount\":\"0\",\"commentCount\":\"29\"},\"topicDetails\":{\"topicIds\":[\"/m/02mjmr\"],\"relevantTopicIds\":[\"/m/0cnfvd\",\"/m/01jdpf\"]}}]]",
                        "+I[610000000000000000000103, youtube#videoListResponse, \\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\", {\"pagehit\":{\"kind\":\"youtube#video\"},\"totalResults\":1,\"resultsPerPage\":1}, [{\"kind\":\"youtube#video\",\"etag\":\"\\\\\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\\\\\"\",\"id\":\"wHkPb68dxEw\",\"statistics\":{\"viewCount\":\"9211\",\"likeCount\":\"79\",\"dislikeCount\":\"11\",\"favoriteCount\":\"0\",\"commentCount\":\"29\"},\"topicDetails\":{\"topicIds\":[\"/m/02mjmr\"],\"relevantTopicIds\":[\"/m/0cnfvd\",\"/m/01jdpf\"]}}]]");
        waitForResult(expected2, table2, rowType2, primaryKeys2);
    }

    @Test
    @Timeout(90)
    public void testDynamicTableCreationInMongoDB() throws Exception {
        String dbName = database + UUID.randomUUID();
        writeRecordsToMongoDB("test-data-5", dbName, "database");
        Map<String, String> mongodbConfig = getBasicMongoDBConfig();
        mongodbConfig.put("database", dbName);
        MongoDBSyncDatabaseAction action =
                syncDatabaseActionBuilder(mongodbConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        CatalogOptions.CASE_SENSITIVE.key(), "false"))
                        .build();
        runActionWithDefaultEnv(action);

        waitingTables("t3");
        FileStoreTable table1 = getFileStoreTable("t3");
        writeRecordsToMongoDB("test-data-6", dbName, "database");
        waitingTables("t4");
        FileStoreTable table2 = getFileStoreTable("t4");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "country", "languages", "religions"});
        List<String> primaryKeys1 = Collections.singletonList("_id");
        List<String> expected1 =
                Arrays.asList(
                        "+I[610000000000000000000101, Switzerland, Italian, {\"f\":\"v\",\"n\":null}]",
                        "+I[610000000000000000000102, Switzerland, Italian, ]",
                        "+I[610000000000000000000103, Switzerland, [\"Italian\"], ]");
        waitForResult(expected1, table1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "kind", "etag", "pageinfo", "items"});
        List<String> primaryKeys2 = Collections.singletonList("_id");
        List<String> expected2 =
                Arrays.asList(
                        "+I[610000000000000000000101, youtube#videoListResponse, \\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\", {\"totalResults\":1,\"resultsPerPage\":1}, [{\"kind\":\"youtube#video\",\"etag\":\"\\\\\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\\\\\"\",\"id\":\"wHkPb68dxEw\",\"statistics\":{\"viewCount\":\"9211\",\"likeCount\":\"79\",\"dislikeCount\":\"11\",\"favoriteCount\":\"0\",\"commentCount\":\"29\"},\"topicDetails\":{\"topicIds\":[\"/m/02mjmr\"],\"relevantTopicIds\":[\"/m/0cnfvd\",\"/m/01jdpf\"]}}]]",
                        "+I[610000000000000000000102, youtube#videoListResponse, \\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\", page, [{\"kind\":\"youtube#video\",\"etag\":\"\\\\\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\\\\\"\",\"id\":\"wHkPb68dxEw\",\"statistics\":{\"viewCount\":\"9211\",\"likeCount\":\"79\",\"dislikeCount\":\"11\",\"favoriteCount\":\"0\",\"commentCount\":\"29\"},\"topicDetails\":{\"topicIds\":[\"/m/02mjmr\"],\"relevantTopicIds\":[\"/m/0cnfvd\",\"/m/01jdpf\"]}}]]",
                        "+I[610000000000000000000103, youtube#videoListResponse, \\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\", {\"pagehit\":{\"kind\":\"youtube#video\"},\"totalResults\":1,\"resultsPerPage\":1}, [{\"kind\":\"youtube#video\",\"etag\":\"\\\\\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\\\\\"\",\"id\":\"wHkPb68dxEw\",\"statistics\":{\"viewCount\":\"9211\",\"likeCount\":\"79\",\"dislikeCount\":\"11\",\"favoriteCount\":\"0\",\"commentCount\":\"29\"},\"topicDetails\":{\"topicIds\":[\"/m/02mjmr\"],\"relevantTopicIds\":[\"/m/0cnfvd\",\"/m/01jdpf\"]}}]]");
        waitForResult(expected2, table2, rowType2, primaryKeys2);
    }

    @Test
    @Timeout(90)
    public void testTableAffix() throws Exception {
        // create table t1
        createFileStoreTable(
                "test_prefix_t1_test_suffix",
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"_id", "name", "description", "weight"}),
                Collections.emptyList(),
                Collections.singletonList("_id"),
                Collections.emptyList(),
                Collections.emptyMap());

        // try synchronization
        String dbName = database + UUID.randomUUID();
        writeRecordsToMongoDB("test-data-1", dbName, "database");
        writeRecordsToMongoDB("test-data-2", dbName, "database");

        Map<String, String> mongodbConfig = getBasicMongoDBConfig();
        mongodbConfig.put("database", dbName);
        MongoDBSyncDatabaseAction action =
                syncDatabaseActionBuilder(mongodbConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withTablePrefix("test_prefix_")
                        .withTableSuffix("_test_suffix")
                        // test including check with affix
                        .includingTables(ThreadLocalRandom.current().nextBoolean() ? "t1|t2" : ".*")
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl("test_prefix_t1_test_suffix", "test_prefix_t2_test_suffix", dbName);
    }

    @Test
    @Timeout(120)
    public void testNewlyAddedTablesOptionsChange() throws Exception {
        String dbName = database + UUID.randomUUID();
        writeRecordsToMongoDB("test-data-5", dbName, "database");
        Map<String, String> mongodbConfig = getBasicMongoDBConfig();
        mongodbConfig.put("database", dbName);
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", "1");
        tableConfig.put("sink.parallelism", "1");

        MongoDBSyncDatabaseAction action1 =
                syncDatabaseActionBuilder(mongodbConfig).withTableConfig(tableConfig).build();

        JobClient jobClient = runActionWithDefaultEnv(action1);

        waitingTables("t3");
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

        writeRecordsToMongoDB("test-data-6", dbName, "database");

        MongoDBSyncDatabaseAction action2 =
                syncDatabaseActionBuilder(mongodbConfig).withTableConfig(tableConfig).build();
        runActionWithDefaultEnv(action2);
        waitingTables("t4");

        FileStoreTable table = getFileStoreTable("t4");
        Map<String, String> tableOptions = table.options();
        assertThat(tableOptions).containsAllEntriesOf(tableConfig);
    }
}
