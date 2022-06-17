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

import org.junit.jupiter.api.Test;

import java.util.UUID;

/** Tests for reading and writing file store in stream jobs. */
public class FileStoreStreamE2eTest extends E2eTestBase {

    @Test
    public void testWithoutPk() throws Exception {
        String testDataSourceDdl =
                "CREATE TABLE test_source (\n"
                        + "    a VARCHAR,\n"
                        + "    b INT\n"
                        + ") WITH (\n"
                        + "    'connector' = 'filesystem',\n"
                        + "    'format' = 'csv',\n"
                        + "    'path' = '%s'\n,"
                        + "    'source.monitor-interval' = '3s'\n"
                        + ");";
        String testDataSourceDir = UUID.randomUUID().toString() + ".data";
        testDataSourceDdl =
                String.format(testDataSourceDdl, TEST_DATA_DIR + "/" + testDataSourceDir);

        String tableStoreDdl =
                "CREATE TABLE IF NOT EXISTS table_store (\n"
                        + "    a VARCHAR,\n"
                        + "    b INT,\n"
                        + "    rn BIGINT\n"
                        + ") WITH (\n"
                        + "    'bucket' = '3',\n"
                        + "    'root-path' = '%s'\n"
                        + ");";
        tableStoreDdl =
                String.format(
                        tableStoreDdl,
                        TEST_DATA_DIR + "/" + UUID.randomUUID().toString() + ".store");

        // prepare first part of test data
        writeSharedFile(testDataSourceDir + "/1.csv", "A,5\nB,10\nA,4\nB,9\n");

        // insert data into table store
        runSql(
                "SET 'execution.checkpointing.interval' = '5s';\n"
                        + "INSERT INTO table_store SELECT a, b, rn FROM (\n"
                        + "  SELECT a, b, row_number() over (PARTITION BY a ORDER BY b) AS rn FROM test_source\n"
                        + ") WHERE rn <= 3;",
                testDataSourceDdl,
                tableStoreDdl);

        // read all data from table store
        runSql(
                "INSERT INTO result1 SELECT * FROM table_store;",
                tableStoreDdl,
                createResultSink("result1", "a VARCHAR, b INT, rn BIGINT"));

        // check that we can read the first part of test data
        checkResult("A, 4, 1", "A, 5, 2", "B, 9, 1", "B, 10, 2");

        // prepare second part of test data
        writeSharedFile(testDataSourceDir + "/2.csv", "A,3\nB,8\nA,2\nB,11\n");

        // check that we can read the second part of test data
        checkResult("A, 2, 1", "A, 3, 2", "A, 4, 3", "B, 8, 1", "B, 9, 2", "B, 10, 3");

        // check that we can read all test data with a batch job
        clearCurrentResults();
        runSql(
                "SET 'execution.runtime-mode' = 'batch';\n"
                        + "RESET 'execution.checkpointing.interval';\n"
                        + "INSERT INTO result2 SELECT * FROM table_store;",
                tableStoreDdl,
                createResultSink("result2", "a VARCHAR, b INT, rn BIGINT"));
        checkResult("A, 2, 1", "A, 3, 2", "A, 4, 3", "B, 8, 1", "B, 9, 2", "B, 10, 3");
    }

    private void runSql(String sql, String... ddls) throws Exception {
        runSql(String.join("\n", ddls) + "\n" + sql);
    }
}
