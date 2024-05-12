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

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.util.UUID;

/** Tests for {@code FlinkActions}. */
public class FlinkActionsWithKafkaE2eTest extends FlinkActionsE2eTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkActionsWithKafkaE2eTest.class);

    public FlinkActionsWithKafkaE2eTest() {
        super(true, false);
    }

    @Test
    public void testCompact() throws Exception {
        String topicName = "ts-topic-" + UUID.randomUUID();
        createKafkaTopic(topicName, 1);
        // prepare first part of test data
        sendKafkaMessage("1.csv", "20221205,1,100\n20221206,1,100\n20221207,1,100", topicName);

        String testDataSourceDdl =
                String.format(
                        "CREATE TEMPORARY TABLE test_source (\n"
                                + "    dt STRING,\n"
                                + "    k INT,\n"
                                + "    v INT"
                                + ") WITH (\n"
                                + "    'connector' = 'kafka',\n"
                                + "    'properties.bootstrap.servers' = 'kafka:9092',\n"
                                + "    'properties.group.id' = 'testGroup',\n"
                                + "    'scan.startup.mode' = 'earliest-offset',\n"
                                + "    'topic' = '%s',\n"
                                + "    'format' = 'csv'\n"
                                + ");",
                        topicName);

        String tableDdl =
                "CREATE TABLE IF NOT EXISTS ts_table (\n"
                        + "    dt STRING,\n"
                        + "    k INT,\n"
                        + "    v INT,\n"
                        + "    PRIMARY KEY (dt, k) NOT ENFORCED\n"
                        + ") PARTITIONED BY (dt) WITH (\n"
                        + "    'changelog-producer' = 'full-compaction',\n"
                        + "    'changelog-producer.compaction-interval' = '1s',\n"
                        + "    'write-only' = 'true'\n"
                        + ");";

        // insert data into paimon
        runStreamingSql(
                "INSERT INTO ts_table SELECT * FROM test_source;",
                catalogDdl,
                useCatalogCmd,
                tableDdl,
                testDataSourceDdl);

        // run dedicated compact job
        Container.ExecResult execResult =
                jobManager.execInContainer(
                        "bin/flink",
                        "run",
                        "-D",
                        "execution.checkpointing.interval=1s",
                        "--detached",
                        "lib/paimon-flink-action.jar",
                        "compact",
                        "--warehouse",
                        warehousePath,
                        "--database",
                        "default",
                        "--table",
                        "ts_table",
                        "--partition",
                        "dt=20221205",
                        "--partition",
                        "dt=20221206");
        LOG.info(execResult.getStdout());
        LOG.info(execResult.getStderr());

        // read all data from paimon
        runBatchSql(
                "INSERT INTO result1 SELECT * FROM ts_table;",
                catalogDdl,
                useCatalogCmd,
                tableDdl,
                createResultSink("result1", "dt STRING, k INT, v INT"));

        // check that first part of test data are compacted
        checkResult("20221205, 1, 100", "20221206, 1, 100");

        // prepare second part of test data
        sendKafkaMessage("2.csv", "20221205,1,101\n20221206,1,101\n20221207,1,101", topicName);

        // check that second part of test data are compacted
        checkResult("20221205, 1, 101", "20221206, 1, 101");
    }
}
