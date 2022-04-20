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

package org.apache.flink.table.store.tests;

import org.apache.flink.util.DockerImageVersions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.UUID;

/** Tests for reading and writing log store in stream jobs. */
public class LogStoreE2eTest extends E2eTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(LogStoreE2eTest.class);

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";

    private KafkaContainer kafka;

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        kafka =
                new KafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA))
                        .withEmbeddedZookeeper()
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS)
                        .withEnv(
                                "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                                String.valueOf(Duration.ofHours(2).toMillis()))
                        // Disable log deletion to prevent records from being deleted during test
                        // run
                        .withEnv("KAFKA_LOG_RETENTION_MS", "-1")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        kafka.start();
        LOG.info("Kafka started.");
    }

    @AfterEach
    @Override
    public void after() throws Exception {
        super.after();
        kafka.stop();
        LOG.info("Kafka stopped.");
    }

    @Test
    public void testWithPk() throws Exception {
        String tableStoreBatchDdl =
                "CREATE TABLE IF NOT EXISTS table_store (\n"
                        + "    k VARCHAR,\n"
                        + "    v INT,\n"
                        + "    PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "    'bucket' = '3',\n"
                        + "    'path' = '%s'\n"
                        + ");";
        String tableStoreDir = UUID.randomUUID().toString() + ".store";
        tableStoreBatchDdl = String.format(tableStoreBatchDdl, TEST_DATA_DIR + "/" + tableStoreDir);

        // prepare data only in file store
        runSql(
                "SET 'execution.runtime-mode' = 'batch';\n"
                        + "SET 'table.dml-sync' = 'true';\n"
                        + "INSERT INTO table_store VALUES ('A', 1), ('B', 2), ('C', 3)",
                tableStoreBatchDdl);

        String testDataSourceDdl =
                "CREATE TABLE test_source (\n"
                        + "    k VARCHAR,\n"
                        + "    v INT\n"
                        + ") WITH (\n"
                        + "    'connector' = 'filesystem',\n"
                        + "    'format' = 'csv',\n"
                        + "    'path' = '%s'\n,"
                        + "    'source.monitor-interval' = '3s'\n"
                        + ");";
        String testDataSourceDir = UUID.randomUUID().toString() + ".data";
        testDataSourceDdl =
                String.format(testDataSourceDdl, TEST_DATA_DIR + "/" + testDataSourceDir);

        String tableStoreStreamDdl =
                "CREATE TABLE IF NOT EXISTS table_store (\n"
                        + "    k VARCHAR,\n"
                        + "    v INT,\n"
                        + "    PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "    'bucket' = '3',\n"
                        + "    'path' = '%s',\n"
                        + "    'log.consistency' = 'eventual',\n"
                        + "    'log.system' = 'kafka',\n"
                        + "    'log.kafka.bootstrap.servers' = '%s'\n"
                        + ");";
        tableStoreStreamDdl =
                String.format(
                        tableStoreStreamDdl,
                        TEST_DATA_DIR + "/" + tableStoreDir,
                        getBootstrapServers());

        // prepare first part of test data
        writeTestData(testDataSourceDir + "/1.csv", "A,10\nC,30\nD,40\n");

        // insert data into table store
        runSql(
                // long checkpoint interval ensures that new data are only visible from log store
                "SET 'execution.checkpointing.interval' = '9999s';\n"
                        + "INSERT INTO table_store SELECT * FROM test_source;",
                testDataSourceDdl,
                tableStoreStreamDdl);

        // read all data from table store
        runSql(
                "INSERT INTO result1 SELECT * FROM table_store;",
                tableStoreStreamDdl,
                createResultSink("result1", "k VARCHAR, v INT"));

        // check that we can read data both from file store and log store
        checkResult(s -> s.split(",")[0], "A, 10", "B, 2", "C, 30", "D, 40");

        // prepare second part of test data
        writeTestData(testDataSourceDir + "/2.csv", "A,100\nD,400\n");

        // check that we can receive data from log store quickly
        checkResult(s -> s.split(",")[0], "A, 100", "B, 2", "C, 30", "D, 400");
    }

    private String getBootstrapServers() {
        return INTER_CONTAINER_KAFKA_ALIAS + ":9092";
    }

    private void runSql(String sql, String... ddls) throws Exception {
        runSql(String.join("\n", ddls) + "\n" + sql);
    }
}
