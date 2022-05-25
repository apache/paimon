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

/**
 * Test that file store supports format not included in flink-table-store-format but provided by
 * Flink itself.
 */
public class FileStoreFlinkFormatE2eTest extends E2eTestBase {

    @Test
    public void testCsv() throws Exception {
        String tableStoreDdl =
                "CREATE TABLE IF NOT EXISTS table_store (\n"
                        + "    a INT,\n"
                        + "    b VARCHAR\n"
                        + ") WITH (\n"
                        + "    'bucket' = '3',\n"
                        + "    'root-path' = '%s',\n"
                        + "    'file.format' = 'csv'\n"
                        + ");";
        tableStoreDdl =
                String.format(
                        tableStoreDdl,
                        TEST_DATA_DIR + "/" + UUID.randomUUID().toString() + ".store");

        runSql("INSERT INTO table_store VALUES (1, 'Hi'), (2, 'Hello');", tableStoreDdl);
        runSql(
                "INSERT INTO result1 SELECT * FROM table_store;",
                tableStoreDdl,
                createResultSink("result1", "a INT, b VARCHAR"));
        checkResult("1, Hi", "2, Hello");
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
