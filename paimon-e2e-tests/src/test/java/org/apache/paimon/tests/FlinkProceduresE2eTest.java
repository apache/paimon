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
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.UUID;

/** Tests for flink {@code Procedure}s. */
@EnabledIf("runTest")
public class FlinkProceduresE2eTest extends E2eTestBase {

    private static boolean runTest() {
        return System.getProperty("test.flink.main.version").compareTo("1.18") >= 0;
    }

    public FlinkProceduresE2eTest() {
        super(true, true);
    }

    @Test
    public void testCompact() throws Exception {
        String topicName = "ts-topic-" + UUID.randomUUID();
        createKafkaTopic(topicName, 1);
        // prepare first part of test data
        sendKafkaMessage("1.csv", "20221205,1,100\n20221206,1,100\n20221207,1,100", topicName);

        // create hive catalog to test catalog loading
        String warehouse = HDFS_ROOT + "/" + UUID.randomUUID() + ".warehouse";
        String catalogDdl =
                String.format(
                        "CREATE CATALOG ts_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'warehouse' = '%s',\n"
                                + "    'metastore' = 'hive',\n"
                                + "    'uri' = 'thrift://hive-metastore:9083'\n"
                                + ");",
                        warehouse);
        String useCatalogCmd = "USE CATALOG ts_catalog;";

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

        // execute compact procedure
        String callStatement;
        if (System.getProperty("test.flink.main.version").compareTo("1.18") == 0) {
            callStatement = "CALL sys.compact('default.ts_table', 'dt=20221205;dt=20221206');";
        } else {
            callStatement =
                    "CALL sys.compact(\\`table\\` => 'default.ts_table', partitions => 'dt=20221205;dt=20221206');";
        }

        runStreamingSql(callStatement, catalogDdl, useCatalogCmd);

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
