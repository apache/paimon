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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/** Tests for reading and writing file store in batch jobs. */
public class FileStoreBatchE2eTest extends E2eTestBase {

    @Test
    public void testWithoutPk() throws Exception {
        List<String> data =
                Arrays.asList(
                        "20211110,08,Alice,Food,10",
                        "20211110,08,Alice,Drink,20",
                        "20211110,08,Bob,Food,30",
                        "20211110,08,Bob,Drink,40",
                        "20211110,09,Alice,Food,50",
                        "20211110,09,Alice,Drink,60",
                        "20211110,09,Bob,Food,70",
                        "20211110,09,Bob,Drink,80",
                        "20211111,08,Alice,Food,90",
                        "20211111,08,Alice,Drink,100",
                        "20211111,08,Bob,Food,110",
                        "20211111,08,Bob,Drink,120",
                        "20211111,09,Alice,Food,130",
                        "20211111,09,Alice,Drink,140",
                        "20211111,09,Bob,Food,150",
                        "20211111,09,Bob,Drink,160");
        data = new ArrayList<>(data);
        Collections.shuffle(data);

        String testDataSourceDdl =
                "CREATE TABLE test_source (\n"
                        + "    dt VARCHAR,\n"
                        + "    hr VARCHAR,\n"
                        + "    person VARCHAR,\n"
                        + "    category VARCHAR,\n"
                        + "    price INT\n"
                        + ") WITH (\n"
                        + "    'connector' = 'filesystem',\n"
                        + "    'format' = 'csv',\n"
                        + "    'path' = '%s'\n"
                        + ");";
        String testDataSourceFile = UUID.randomUUID().toString() + ".csv";
        testDataSourceDdl =
                String.format(testDataSourceDdl, TEST_DATA_DIR + "/" + testDataSourceFile);

        String tableStoreDdl =
                "CREATE TABLE IF NOT EXISTS table_store (\n"
                        + "    dt VARCHAR,\n"
                        + "    hr VARCHAR,\n"
                        + "    person VARCHAR,\n"
                        + "    category VARCHAR,\n"
                        + "    price INT\n"
                        + ") PARTITIONED BY (dt, hr) WITH (\n"
                        + "    'bucket' = '3',\n"
                        + "    'root-path' = '%s'\n"
                        + ");";
        tableStoreDdl =
                String.format(
                        tableStoreDdl,
                        TEST_DATA_DIR + "/" + UUID.randomUUID().toString() + ".store");

        // prepare test data
        writeSharedFile(testDataSourceFile, String.join("\n", data));

        // insert data into table store
        runSql(
                "INSERT INTO table_store SELECT * FROM test_source;",
                testDataSourceDdl,
                tableStoreDdl);

        // test #1: read all data from table store
        runSql(
                "INSERT INTO result1 SELECT * FROM table_store;",
                tableStoreDdl,
                createResultSink(
                        "result1",
                        "dt VARCHAR, hr VARCHAR, person VARCHAR, category VARCHAR, price INT"));
        checkResult(
                "20211110, 08, Alice, Drink, 20",
                "20211110, 08, Alice, Food, 10",
                "20211110, 08, Bob, Drink, 40",
                "20211110, 08, Bob, Food, 30",
                "20211110, 09, Alice, Drink, 60",
                "20211110, 09, Alice, Food, 50",
                "20211110, 09, Bob, Drink, 80",
                "20211110, 09, Bob, Food, 70",
                "20211111, 08, Alice, Drink, 100",
                "20211111, 08, Alice, Food, 90",
                "20211111, 08, Bob, Drink, 120",
                "20211111, 08, Bob, Food, 110",
                "20211111, 09, Alice, Drink, 140",
                "20211111, 09, Alice, Food, 130",
                "20211111, 09, Bob, Drink, 160",
                "20211111, 09, Bob, Food, 150");
        clearCurrentResults();

        // test #2: partition filter
        runSql(
                "INSERT INTO result2 SELECT * FROM table_store WHERE dt > '20211110' AND hr < '09';",
                tableStoreDdl,
                createResultSink(
                        "result2",
                        "dt VARCHAR, hr VARCHAR, person VARCHAR, category VARCHAR, price INT"));
        checkResult(
                "20211111, 08, Alice, Drink, 100",
                "20211111, 08, Alice, Food, 90",
                "20211111, 08, Bob, Drink, 120",
                "20211111, 08, Bob, Food, 110");
        clearCurrentResults();

        // test #3: value filter
        runSql(
                "INSERT INTO result3 SELECT * FROM table_store WHERE person = 'Alice' AND category = 'Food';",
                tableStoreDdl,
                createResultSink(
                        "result3",
                        "dt VARCHAR, hr VARCHAR, person VARCHAR, category VARCHAR, price INT"));
        checkResult(
                "20211110, 08, Alice, Food, 10",
                "20211110, 09, Alice, Food, 50",
                "20211111, 08, Alice, Food, 90",
                "20211111, 09, Alice, Food, 130");
        clearCurrentResults();

        // test #4: aggregation
        runSql(
                "INSERT INTO result4 SELECT dt, category, sum(price) AS total FROM table_store GROUP BY dt, category;",
                tableStoreDdl,
                createResultSink("result4", "dt VARCHAR, hr VARCHAR, total INT"));
        checkResult(
                "20211110, Drink, 200",
                "20211110, Food, 160",
                "20211111, Drink, 520",
                "20211111, Food, 480");
        clearCurrentResults();
    }

    private void runSql(String sql, String... ddls) throws Exception {
        runSql(
                "SET 'execution.runtime-mode' = 'batch';\n"
                        + "SET 'table.dml-sync' = 'true';\n"
                        + String.join("\n", ddls)
                        + "\n"
                        + sql);
    }
}
