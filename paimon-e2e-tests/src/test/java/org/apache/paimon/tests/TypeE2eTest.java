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

import java.util.Arrays;
import java.util.UUID;

/** Test for currently supported data types. */
public class TypeE2eTest extends E2eTestBase {

    @Test
    public void testAllTypesAsKey() throws Exception {
        String schema =
                String.join(
                        "\n",
                        Arrays.asList(
                                "f0 BOOLEAN,",
                                "f1 TINYINT,",
                                "f2 SMALLINT,",
                                "f3 INT,",
                                "f4 BIGINT,",
                                "f5 FLOAT,",
                                "f6 DOUBLE,",
                                "f7 DECIMAL(5, 3),",
                                "f8 DECIMAL(26, 8),",
                                "f9 string,",
                                "f10 string,",
                                "f11 STRING,",
                                "f12 BYTES,",
                                "f13 DATE,",
                                "f14 TIMESTAMP(3),",
                                "f15 ARRAY<STRING>,",
                                "f16 ROW<a INT, b BIGINT, c STRING>"));
        String catalogDdl =
                String.format(
                        "CREATE CATALOG ts_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'warehouse' = '%s'\n"
                                + ");",
                        TEST_DATA_DIR + "/" + UUID.randomUUID() + ".store");

        String useCatalogCmd = "USE CATALOG ts_catalog;";

        String tableDdl =
                String.join(
                        "\n", Arrays.asList("CREATE TABLE IF NOT EXISTS ts_table(", schema, ");"));

        runBatchSql(
                "INSERT INTO ts_table VALUES ("
                        + "true, cast(1 as tinyint), cast(10 as smallint), "
                        + "100, 1000, cast(1.1 as float), 1.11, 12.456, "
                        + "cast('123456789123456789.12345678' as decimal(26, 8)), "
                        + "cast('hi' as string), 'hello', 'table桌子store商店', "
                        + "ENCODE('table桌子store商店', 'UTF-8'), "
                        + "DATE '2022-04-28', TIMESTAMP '2022-04-28 15:35:45.123', "
                        + "ARRAY['hi', 'hello', cast(null as string), 'test'], (1, 10, '测试')"
                        + "), ("
                        + "cast(null as boolean), cast(null as tinyint), cast(null as smallint), "
                        + "cast(null as int), cast(null as bigint), cast(null as float), "
                        + "cast(null as double), cast(null as decimal(5, 3)), cast(null as decimal(26, 8)), "
                        + "cast(null as string), cast(null as string), cast(null as string), "
                        + "cast(null as bytes), cast(null as date), cast(null as timestamp(3)), "
                        + "cast(null as array<string>), cast(null as row<a int, b bigint, c string>)"
                        + ");",
                catalogDdl,
                useCatalogCmd,
                tableDdl);
        runBatchSql(
                "INSERT INTO result1 SELECT f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, "
                        + "f10, f11, f12, f13, f14, f15, f16 FROM ts_table;",
                catalogDdl,
                useCatalogCmd,
                tableDdl,
                createResultSink("result1", schema));

        String expected =
                "true, 1, 10, 100, 1000, 1.1, 1.11, 12.456, "
                        + "123456789123456789.12345678, hi, hello, table桌子store商店, [116], "
                        + "2022-04-28, 2022-04-28T15:35:45.123, [hi, hello, null, test], +I[1, 10, 测试]";
        checkResult(
                expected,
                "null, null, null, null, null, null, null, null, null, "
                        + "null, null, null, null, null, null, null, null");
    }

    @Test
    public void testAllTypesAsValue() throws Exception {
        String schema =
                String.join(
                        "\n",
                        Arrays.asList(
                                "pk INT,",
                                "f0 BOOLEAN,",
                                "f1 TINYINT,",
                                "f2 SMALLINT,",
                                "f3 INT,",
                                "f4 BIGINT,",
                                "f5 FLOAT,",
                                "f6 DOUBLE,",
                                "f7 DECIMAL(5, 3),",
                                "f8 DECIMAL(26, 8),",
                                "f9 STRING,",
                                "f10 STRING,",
                                "f11 STRING,",
                                "f12 BYTES,",
                                "f13 DATE,",
                                "f14 TIMESTAMP(3),",
                                "f15 ARRAY<STRING>,",
                                "f16 ROW<a INT, b BIGINT, c STRING>,",
                                "f17 MAP<STRING, BIGINT>"));

        String catalogDdl =
                String.format(
                        "CREATE CATALOG ts_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'warehouse' = '%s'\n"
                                + ");",
                        TEST_DATA_DIR + "/" + UUID.randomUUID() + ".store");

        String useCatalogCmd = "USE CATALOG ts_catalog;";

        String tableDdl =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE IF NOT EXISTS ts_table(",
                                schema + ",",
                                "PRIMARY KEY (pk) NOT ENFORCED",
                                ") WITH (",
                                "  'bucket' = '1'",
                                ");"));

        runBatchSql(
                "INSERT INTO ts_table VALUES (1,"
                        + "true, cast(1 as tinyint), cast(10 as smallint), "
                        + "100, 1000, cast(1.1 as float), 1.11, 12.456, "
                        + "cast('123456789123456789.12345678' as decimal(26, 8)), "
                        + "cast('hi' as string), 'hello', 'table桌子store商店', "
                        + "ENCODE('table桌子store商店', 'UTF-8'), "
                        + "DATE '2022-04-28', TIMESTAMP '2022-04-28 15:35:45.123', "
                        + "ARRAY['hi', 'hello', cast(null as string), 'test'], (1, 10, '测试'), "
                        + "MAP['hi', 1, 'hello', cast(null as bigint), 'test', 3]"
                        + "), (2,"
                        + "cast(null as boolean), cast(null as tinyint), cast(null as smallint), "
                        + "cast(null as int), cast(null as bigint), cast(null as float), "
                        + "cast(null as double), cast(null as decimal(5, 3)), cast(null as decimal(26, 8)), "
                        + "cast(null as string), cast(null as string), cast(null as string), "
                        + "cast(null as bytes), cast(null as date), cast(null as timestamp(3)), "
                        + "cast(null as array<string>), cast(null as row<a int, b bigint, c string>), "
                        + "cast(null as map<string, bigint>)"
                        + ");",
                catalogDdl,
                useCatalogCmd,
                tableDdl);
        runBatchSql(
                "INSERT INTO result1 SELECT * FROM ts_table;",
                catalogDdl,
                useCatalogCmd,
                tableDdl,
                createResultSink("result1", schema));

        String expected =
                "1, true, 1, 10, 100, 1000, 1.1, 1.11, 12.456, "
                        + "123456789123456789.12345678, hi, hello, table桌子store商店, [116], "
                        + "2022-04-28, 2022-04-28T15:35:45.123, [hi, hello, null, test], +I[1, 10, 测试], "
                        + "{hi=1, test=3, hello=null}";
        checkResult(
                expected,
                "2, null, null, null, null, null, null, null, null, null, "
                        + "null, null, null, null, null, null, null, null, null");
    }
}
