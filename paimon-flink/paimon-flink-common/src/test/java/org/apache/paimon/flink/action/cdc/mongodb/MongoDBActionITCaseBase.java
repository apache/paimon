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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommonTestUtils;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for {@link org.apache.paimon.flink.action.Action}s related to MongoDB. */
public abstract class MongoDBActionITCaseBase extends ActionITCaseBase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBActionITCaseBase.class);
    protected static MongoClient client;
    public static final MongoDBContainer MONGODB_CONTAINER =
            new MongoDBContainer("mongo:6.0.6")
                    .withSharding()
                    .withLogConsumer(new Slf4jLogConsumer(LOG));
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    @BeforeEach
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, false);
    }

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        // MONGODB_CONTAINER.setPortBindings(Collections.singletonList("27017:27017"));
        Startables.deepStart(Stream.of(MONGODB_CONTAINER)).join();
        LOG.info("Containers are started.");
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(MONGODB_CONTAINER.getConnectionString()))
                        .build();
        client = MongoClients.create(settings);
    }

    protected Map<String, String> getBasicMongoDBConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("hosts", MONGODB_CONTAINER.getHostAndPort());
        return config;
    }

    protected String createRecordsToMongoDB(String fileName, String content) {
        return MONGODB_CONTAINER.executeCommandFileInSeparateDatabase(fileName, content);
    }

    protected static String writeRecordsToMongoDB(String fileName, String dbName, String content) {
        return MONGODB_CONTAINER.executeCommandFileInSeparateDatabase(fileName, dbName, content);
    }

    protected void waitJobRunning(JobClient client) throws Exception {
        while (true) {
            JobStatus status = client.getJobStatus().get();
            if (status == JobStatus.RUNNING) {
                break;
            }
            Thread.sleep(1000);
        }
    }

    protected void waitForResult(
            List<String> expected, FileStoreTable table, RowType rowType, List<String> primaryKeys)
            throws Exception {
        assertThat(table.schema().primaryKeys()).isEqualTo(primaryKeys);

        // wait for table schema to become our expected schema
        while (true) {
            if (rowType.getFieldCount() == table.schema().fields().size()) {
                int cnt = 0;
                for (int i = 0; i < table.schema().fields().size(); i++) {
                    DataField field = table.schema().fields().get(i);
                    boolean sameName = field.name().equals(rowType.getFieldNames().get(i));
                    boolean sameType = field.type().equals(rowType.getFieldTypes().get(i));
                    if (sameName && sameType) {
                        cnt++;
                    }
                }
                if (cnt == rowType.getFieldCount()) {
                    break;
                }
            }
            table = table.copyWithLatestSchema();
            Thread.sleep(1000);
        }

        // wait for data to become expected
        List<String> sortedExpected = new ArrayList<>(expected);
        Collections.sort(sortedExpected);
        while (true) {
            ReadBuilder readBuilder = table.newReadBuilder();
            TableScan.Plan plan = readBuilder.newScan().plan();
            List<String> result =
                    getResult(
                            readBuilder.newRead(),
                            plan == null ? Collections.emptyList() : plan.splits(),
                            rowType);
            List<String> sortedActual = new ArrayList<>(result);
            Collections.sort(sortedActual);
            LOG.info("Expected sortedExpected is:{}", sortedExpected);
            LOG.info("Actual sortedActual is:{}", sortedExpected);
            if (sortedExpected.equals(sortedActual)) {
                break;
            }
            Thread.sleep(1000);
        }
    }

    protected FileStoreTable getFileStoreTable(String tableName) throws Exception {
        Identifier identifier = Identifier.create(database, tableName);
        return (FileStoreTable) catalog().getTable(identifier);
    }

    protected Map<String, String> getBasicTableConfig() {
        Map<String, String> config = new HashMap<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        config.put("bucket", String.valueOf(random.nextInt(3) + 1));
        config.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        return config;
    }

    protected void waitTablesCreated(String... tables) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        List<String> existed = catalog().listTables(database);
                        return existed.containsAll(Arrays.asList(tables));
                    } catch (Catalog.DatabaseNotExistException e) {
                        throw new RuntimeException(e);
                    }
                },
                Duration.ofMinutes(5),
                Duration.ofMillis(100),
                "Failed to wait tables to be created in 5 seconds.");
    }
}
