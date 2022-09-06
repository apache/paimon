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

/** Test that file store supports format included in flink-table-store-format. */
public class FileStoreBuiltInFormatE2eTest extends E2eTestBase {

    @Test
    public void testParquet() throws Exception {
        String schema =
                "id INT,\n"
                        + "isMan BOOLEAN,\n"
                        + "houseNum TINYINT,\n"
                        + "debugNum SMALLINT,\n"
                        + "age INT,\n"
                        + "cash BIGINT,\n"
                        + "money FLOAT,\n"
                        + "f1 DOUBLE,\n"
                        + "f2 DECIMAL(5, 3),\n"
                        + "f3 DECIMAL(26, 8),\n"
                        + "f4 CHAR(10),\n"
                        + "f5 VARCHAR(10),\n"
                        + "f6 STRING,\n"
                        + "f7 DATE\n";
        String tableStoreDdl =
                "CREATE TABLE IF NOT EXISTS table_store (\n"
                        + schema
                        + ") WITH (\n"
                        + "    'bucket' = '3',\n"
                        + "    'root-path' = '%s',\n"
                        + "    'file.format' = 'parquet'\n"
                        + ");";
        tableStoreDdl =
                String.format(
                        tableStoreDdl,
                        TEST_DATA_DIR + "/" + UUID.randomUUID().toString() + ".store");
        String insertDml =
                "INSERT INTO table_store VALUES ("
                        + "1,"
                        + "true,"
                        + "cast(1 as tinyint),"
                        + "cast(10 as smallint),"
                        + "cast(100 as int),"
                        + "cast(999999 as bigint),"
                        + "cast(1.1 as float),"
                        + "1.11,"
                        + "12.456,"
                        + "cast('123456789123456789.12345678' as decimal(26, 8)),"
                        + "cast('hi' as char(10)),"
                        + "'Parquet',"
                        + "'这是一个parquet format',"
                        + "DATE '2022-05-21'"
                        + "),("
                        + "2,"
                        + "false,"
                        + "cast(2 as tinyint),"
                        + "cast(29 as smallint),"
                        + "cast(200 as int),"
                        + "cast(9999999 as bigint),"
                        + "cast(2.2 as float),"
                        + "2.22,"
                        + "22.557,"
                        + "cast('222222789123456789.12345678' as decimal(26, 8)),"
                        + "cast('hello' as char(10)),"
                        + "'Hi Yu bin',"
                        + "'这是一个 built in parquet format',"
                        + "DATE '2022-05-23'"
                        + ")";
        String resultDdl = createResultSink("result1", schema);
        runSql(insertDml, tableStoreDdl);
        runSql(
                "INSERT INTO result1 SELECT * FROM table_store where id > 1;",
                tableStoreDdl,
                resultDdl);
        checkResult(
                "2, "
                        + "false, "
                        + "2, "
                        + "29, "
                        + "200, "
                        + "9999999, "
                        + "2.2, "
                        + "2.22, "
                        + "22.557, "
                        + "222222789123456789.12345678, "
                        + "hello     , "
                        + "Hi Yu bin, "
                        + "这是一个 built in parquet format, "
                        + "2022-05-23");
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
