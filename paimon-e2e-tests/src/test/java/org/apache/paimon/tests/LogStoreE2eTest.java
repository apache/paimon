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

package org.apache.paimon.tests;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

/** Tests for reading and writing log store in stream jobs. */
public class LogStoreE2eTest extends E2eTestBase {

    private String topicName;

    private int bucketNum;

    public LogStoreE2eTest() {
        super(true, false);
    }

    @Override
    @BeforeEach
    public void before() throws Exception {
        super.before();
        topicName = "ts-topic-" + UUID.randomUUID();
        bucketNum = 2;
        createKafkaTopic(topicName, bucketNum);
    }

    @Test
    public void testWithPk() throws Exception {
        String catalogDdl =
                String.format(
                        "CREATE CATALOG ts_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'warehouse' = '%s'\n"
                                + ");",
                        TEST_DATA_DIR + "/" + UUID.randomUUID() + ".store");

        String useCatalogCmd = "USE CATALOG ts_catalog;";

        String streamTableDdl =
                String.format(
                        "CREATE TABLE IF NOT EXISTS ts_table (\n"
                                + "    k VARCHAR,\n"
                                + "    v INT,\n"
                                + "    PRIMARY KEY (k) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "    'bucket' = '%d',\n"
                                + "    'log.consistency' = 'eventual',\n"
                                + "    'log.system' = 'kafka',\n"
                                + "    'kafka.bootstrap.servers' = 'kafka:9092',\n"
                                + "    'kafka.topic' = '%s'\n"
                                + ");",
                        bucketNum, topicName);

        // prepare data only in file store
        runBatchSql(
                "INSERT INTO ts_table VALUES ('A', 1), ('B', 2), ('C', 3)",
                catalogDdl,
                useCatalogCmd,
                streamTableDdl);

        // prepare first part of test data
        String testTopicName = "ts-topic-" + UUID.randomUUID();
        createKafkaTopic(testTopicName, 1);
        sendKafkaMessage("1.csv", "A,10\nC,30\nD,40", testTopicName);

        String testDataSourceDdl =
                String.format(
                        "CREATE TEMPORARY TABLE test_source (\n"
                                + "    k VARCHAR,\n"
                                + "    v INT\n"
                                + ") WITH (\n"
                                + "    'connector' = 'kafka',\n"
                                + "    'properties.bootstrap.servers' = 'kafka:9092',\n"
                                + "    'properties.group.id' = 'testGroup',\n"
                                + "    'scan.startup.mode' = 'earliest-offset',\n"
                                + "    'topic' = '%s',\n"
                                + "    'format' = 'csv'\n"
                                + ");",
                        testTopicName);

        // insert data into paimon
        runStreamingSql(
                // long checkpoint interval ensures that new data are only visible from log store
                "SET 'execution.checkpointing.interval' = '9999s';\n"
                        + "INSERT INTO ts_table SELECT * FROM test_source;",
                catalogDdl,
                useCatalogCmd,
                streamTableDdl,
                testDataSourceDdl);

        // read all data from paimon
        runBatchSql(
                "INSERT INTO result1 SELECT * FROM ts_table;",
                catalogDdl,
                useCatalogCmd,
                streamTableDdl,
                createResultSink("result1", "k VARCHAR, v INT"));

        // check that we can read data both from file store and log store
        checkResult(s -> s.split(",")[0], "A, 10", "B, 2", "C, 30", "D, 40");

        // prepare second part of test data
        sendKafkaMessage("2.csv", "A,100\nD,400", testTopicName);

        // check that we can receive data from log store quickly
        checkResult(s -> s.split(",")[0], "A, 100", "B, 2", "C, 30", "D, 400");
    }

    private void runBatchSql(String sql, String... ddls) throws Exception {
        runBatchSql(String.join("\n", ddls) + "\n" + sql);
    }

    private void runStreamingSql(String sql, String... ddls) throws Exception {
        runStreamingSql(String.join("\n", ddls) + "\n" + sql);
    }
}
