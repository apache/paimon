/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink;

import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * IT cases to test predicate push down. We test weather all data type and predicate can push down
 * correctly. test flink filter convert to paimon predicate please see {@link
 * PredicateConverterTest}
 */
public class FormatFilterPushdownITCase extends CatalogITCaseBase {

    private final String tableName = "testPushDown";

    private static Stream<String> fileFormat() {
        return Stream.of("orc", "parquet");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testNull(String fileFormat) {
        sql(
                "CREATE TABLE  "
                        + tableName
                        + "(id INT PRIMARY KEY NOT ENFORCED,name STRING) with ('file.format' = '"
                        + fileFormat
                        + "')");
        sql("INSERT INTO " + tableName + " VALUES(1,'aaa'),(2, CAST(NULL AS STRING))");
        checkFilters("name IS NULL", "IS NULL(name)", "[+I[2, null]]");
        checkFilters("name IS NOT NULL ", "IS NOT NULL(name)", "[+I[1, aaa]]");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testBetween(String fileFormat) {
        sql(
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "id INT PRIMARY KEY NOT ENFORCED,"
                        + "a FLOAT,"
                        + "b DOUBLE,"
                        + "c DECIMAL(10,2),"
                        + "d DATE,"
                        + "e BIGINT,"
                        + "f TIMESTAMP,"
                        + "g TIMESTAMP(0),"
                        + "h TIMESTAMP(9),"
                        + "i TIME,"
                        + "j TINYINT,"
                        + "k SMALLINT"
                        + ") with ('file.format' = '"
                        + fileFormat
                        + "') ");
        sql(
                "INSERT INTO "
                        + tableName
                        + " VALUES(1, 1.2, 3.4, 4.5,DATE '2023-06-06',100, TIMESTAMP '2021-06-30 01:02:03.123456', TIMESTAMP '2021-06-30 01:02:03', "
                        + "TIMESTAMP '2021-06-30 01:00:00.123456789', TIME '12:13:14', CAST(-1 AS TINYINT), CAST(-2 AS SMALLINT))");
        checkFilters(
                "id between 1 and 3 ",
                "and(>=(id, 1), <=(id, 3))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "a between 1.0 and 3.0 ",
                "and(>=(a, 1.0:DECIMAL(2, 1)), <=(a, 3.0:DECIMAL(2, 1)))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "b between 1.0 and 4.0 ",
                "and(>=(b, 1.0:DECIMAL(2, 1)), <=(b, 4.0:DECIMAL(2, 1)))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "c between 1.2 and 4.5 ",
                "and(>=(c, 1.2:DECIMAL(2, 1)), <=(c, 4.5:DECIMAL(2, 1)))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "d between DATE '2023-06-01' and DATE '2023-06-08' ",
                "and(>=(d, 2023-06-01), <=(d, 2023-06-08))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "e between 100 AND 200 ",
                "and(>=(e, 100), <=(e, 200))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "f between TIMESTAMP '2021-06-30 00:00:00.123' and TIMESTAMP '2021-06-30 02:00:00.234567' ",
                "and(>=(f, 2021-06-30 00:00:00.123:TIMESTAMP(3)), <=(f, 2021-06-30 02:00:00.234:TIMESTAMP(3)))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "g between TIMESTAMP '2021-06-30 00:00:00' and TIMESTAMP '2021-06-30 02:00:00' ",
                "and(>=(g, 2021-06-30 00:00:00), <=(g, 2021-06-30 02:00:00))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "h between TIMESTAMP '2021-05-30 01:00:00.123456789' and TIMESTAMP '2021-07-30 01:00:00.123456789' ",
                "and(>=(h, 2021-05-30 01:00:00.123456789:TIMESTAMP(9)), <=(h, 2021-07-30 01:00:00.123456789:TIMESTAMP(9)))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "i between TIME '01:02:03' and TIME '13:14:15' ",
                "and(>=(i, 01:02:03), <=(i, 13:14:15))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "j between -3 and 0 ",
                "and(>=(j, -3), <=(j, 0))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "k between -3 and 0 ",
                "and(>=(k, -3), <=(k, 0))",
                "[+I[1, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testIN(String fileFormat) {
        sql(
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "id INT PRIMARY KEY NOT ENFORCED,"
                        + "name STRING,"
                        + "a FLOAT,"
                        + "b DOUBLE,"
                        + "c DECIMAL(10,2),"
                        + "d DATE,"
                        + "e BIGINT,"
                        + "f TIMESTAMP,"
                        + "g TIMESTAMP(0),"
                        + "h TIMESTAMP(9),"
                        + "i TIME,"
                        + "j TINYINT,"
                        + "k SMALLINT,"
                        + "m CHAR"
                        + ") with ('file.format' = '"
                        + fileFormat
                        + "')");
        sql(
                "INSERT INTO "
                        + tableName
                        + " VALUES(1, 'paimon', 1.2, 3.4, 4.5,DATE '2023-06-06',100, TIMESTAMP '2021-06-30 01:02:03.123456', TIMESTAMP '2021-06-30 01:02:03', "
                        + "TIMESTAMP '2021-06-30 01:00:00.123456789', TIME '12:13:14', CAST(-1 AS TINYINT), CAST(-2 AS SMALLINT), 'a')");

        checkFilters(
                "id IN (1) ",
                "=(id, 1)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");

        checkFilters(
                "name IN ( 'paimon' ) ",
                "=(name, _UTF-16LE'paimon')",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");
        checkFilters(
                "a IN ( CAST(1.2 AS FLOAT) ) ",
                "=(a, 1.2E0:FLOAT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");

        checkFilters(
                "b IN ( CAST(3.4 AS DOUBLE) )  ",
                "=(b, 3.4E0:DOUBLE)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");

        checkFilters(
                "c IN ( 4.5 )",
                "=(c, 4.5:DECIMAL(2, 1))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");
        checkFilters(
                "d IN ( DATE '2023-06-06' ) ",
                "=(d, 2023-06-06)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");
        checkFilters(
                "e IN ( 100 ) ",
                "=(e, 100:BIGINT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");
        checkFilters(
                "f IN ( TIMESTAMP '2021-06-30 01:02:03.123456' ) ",
                "=(f, 2021-06-30 01:02:03.123456:TIMESTAMP(6))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");
        checkFilters(
                "g IN ( TIMESTAMP '2021-06-30 01:02:03' ) ",
                "=(g, 2021-06-30 01:02:03)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");
        checkFilters(
                "h IN ( TIMESTAMP '2021-06-30 01:00:00.123456789' ) ",
                "=(h, 2021-06-30 01:00:00.123456789:TIMESTAMP(9))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");
        checkFilters(
                "i IN ( TIME '12:13:14' ) ",
                "=(i, 12:13:14)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");
        checkFilters(
                "j IN ( -1 )",
                "=(j, -1:TINYINT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");
        checkFilters(
                "k IN ( -2 )",
                "=(k, -2:SMALLINT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");

        checkFilters(
                "m IN ( 'a' )",
                "=(m, _UTF-16LE'a')",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, a]]");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testNotIN(String fileFormat) {
        sql(
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "id INT PRIMARY KEY NOT ENFORCED,"
                        + "name STRING,"
                        + "a FLOAT,"
                        + "b DOUBLE,"
                        + "c DECIMAL(10,2),"
                        + "d DATE,"
                        + "e BIGINT,"
                        + "f TIMESTAMP,"
                        + "g TIMESTAMP(0),"
                        + "h TIMESTAMP(9),"
                        + "i TIME,"
                        + "j TINYINT,"
                        + "k SMALLINT,"
                        + "l CHAR"
                        + ") with ('file.format' = '"
                        + fileFormat
                        + "') ");
        sql(
                "INSERT INTO "
                        + tableName
                        + " VALUES(1, 'paimon', 1.2, 3.4, 4.5,DATE '2023-06-06',100, TIMESTAMP '2021-06-30 01:02:03.123456', TIMESTAMP '2021-06-30 01:02:03', "
                        + "TIMESTAMP '2021-06-30 01:00:00.123456789', TIME '12:13:14', CAST(-1 AS TINYINT), CAST(-2 AS SMALLINT), 'b')");
        checkFilters(
                "id NOT IN (2) ",
                "<>(id, 2)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
        checkFilters(
                "name NOT IN ( 'flink' ) ",
                "<>(name, _UTF-16LE'flink')",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
        checkFilters(
                "a NOT IN ( CAST(1.3 AS FLOAT) ) ",
                "<>(a, 1.3E0:FLOAT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");

        checkFilters(
                "b NOT IN ( CAST(3.5 AS DOUBLE) )  ",
                "<>(b, 3.5E0:DOUBLE)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");

        checkFilters(
                "c NOT IN ( 4.6 )",
                "<>(c, 4.6:DECIMAL(2, 1))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
        checkFilters(
                "d NOT IN ( DATE '2023-06-01' ) ",
                "<>(d, 2023-06-01)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
        checkFilters(
                "e NOT IN ( 101 ) ",
                "<>(e, 101:BIGINT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
        checkFilters(
                "f NOT IN ( TIMESTAMP '2021-06-01 01:02:03.123456' ) ",
                "<>(f, 2021-06-01 01:02:03.123456:TIMESTAMP(6))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
        checkFilters(
                "g NOT IN ( TIMESTAMP '2021-06-10 01:02:03' ) ",
                "<>(g, 2021-06-10 01:02:03)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
        checkFilters(
                "h NOT IN ( TIMESTAMP '2021-06-10 01:00:00.123456789' ) ",
                "<>(h, 2021-06-10 01:00:00.123456789:TIMESTAMP(9))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
        checkFilters(
                "i NOT IN ( TIME '22:13:14' ) ",
                "<>(i, 22:13:14)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
        checkFilters(
                "j NOT IN ( -10 )",
                "<>(j, -10:TINYINT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
        checkFilters(
                "k NOT IN ( -20 )",
                "<>(k, -20:SMALLINT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, b]]");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testEqual(String fileFormat) {
        sql(
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "id INT PRIMARY KEY NOT ENFORCED,"
                        + "name STRING,"
                        + "a FLOAT,"
                        + "b DOUBLE,"
                        + "c DECIMAL(10,2),"
                        + "d DATE,"
                        + "e BIGINT,"
                        + "f TIMESTAMP,"
                        + "g TIMESTAMP(0),"
                        + "h TIMESTAMP(9),"
                        + "i TIME,"
                        + "j TINYINT,"
                        + "k SMALLINT,"
                        + "l BOOLEAN"
                        + ") with ('file.format' = '"
                        + fileFormat
                        + "') ");
        sql(
                "INSERT INTO "
                        + tableName
                        + " VALUES(1, 'paimon', 1.2, 3.4, 4.5,DATE '2023-06-06',100, TIMESTAMP '2021-06-30 01:02:03.123456', TIMESTAMP '2021-06-30 01:02:03', "
                        + "TIMESTAMP '2021-06-30 01:00:00.123456789', TIME '12:13:14', CAST(-1 AS TINYINT), CAST(-2 AS SMALLINT), false)");
        checkFilters(
                "id = 1 ",
                "=(id, 1)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "name = 'paimon' ",
                "=(name, _UTF-16LE'paimon':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\")",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "a = CAST(1.2 AS FLOAT) ",
                "=(a, 1.2E0:FLOAT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");

        checkFilters(
                "b = CAST(3.4 AS DOUBLE)  ",
                "=(b, 3.4E0:DOUBLE)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");

        checkFilters(
                "c = 4.5",
                "=(c, 4.5:DECIMAL(10, 2))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "d = DATE '2023-06-06' ",
                "=(d, 2023-06-06)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "e = 100 ",
                "=(e, 100:BIGINT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "f = TIMESTAMP '2021-06-30 01:02:03.123456' ",
                "=(f, 2021-06-30 01:02:03.123456:TIMESTAMP(6))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "g = TIMESTAMP '2021-06-30 01:02:03' ",
                "=(g, 2021-06-30 01:02:03)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "h = TIMESTAMP '2021-06-30 01:00:00.123456789' ",
                "=(h, 2021-06-30 01:00:00.123456789:TIMESTAMP(9))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "i = TIME '12:13:14' ",
                "=(i, 12:13:14)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "j = -1 ",
                "=(CAST(j AS INTEGER), -1)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "k = -2",
                "=(CAST(k AS INTEGER), -2)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");

        checkFilters(
                "l = false",
                "NOT(l)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testNotEqual(String fileFormat) {
        sql(
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "id INT PRIMARY KEY NOT ENFORCED,"
                        + "name STRING,"
                        + "a FLOAT,"
                        + "b DOUBLE,"
                        + "c DECIMAL(10,2),"
                        + "d DATE,"
                        + "e BIGINT,"
                        + "f TIMESTAMP,"
                        + "g TIMESTAMP(0),"
                        + "h TIMESTAMP(9),"
                        + "i TIME,"
                        + "j TINYINT,"
                        + "k SMALLINT,"
                        + "l BOOLEAN"
                        + ") with ('file.format' = '"
                        + fileFormat
                        + "') ");
        sql(
                "INSERT INTO "
                        + tableName
                        + " VALUES(1, 'paimon', 1.2, 3.4, 4.5,DATE '2023-06-06',100, TIMESTAMP '2021-06-30 01:02:03.123456', TIMESTAMP '2021-06-30 01:02:03', "
                        + "TIMESTAMP '2021-06-30 01:00:00.123456789', TIME '12:13:14', CAST(-1 AS TINYINT), CAST(-2 AS SMALLINT), false)");
        checkFilters(
                "id <> 2 ",
                "<>(id, 2)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "name <> 'flink' ",
                "<>(name, _UTF-16LE'flink':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\")",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "a <> CAST(1.3 AS FLOAT) ",
                "<>(a, 1.3E0:FLOAT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");

        checkFilters(
                "b <> CAST(3.3 AS DOUBLE)  ",
                "<>(b, 3.3E0:DOUBLE)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");

        checkFilters(
                "c <> 4.4",
                "<>(c, 4.4:DECIMAL(10, 2))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "d <> DATE '2023-06-01' ",
                "<>(d, 2023-06-01)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "e <> 101 ",
                "<>(e, 101:BIGINT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "f <> TIMESTAMP '2021-06-10 01:02:03.123456' ",
                "<>(f, 2021-06-10 01:02:03.123456:TIMESTAMP(6))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "g <> TIMESTAMP '2021-06-20 01:02:03' ",
                "<>(g, 2021-06-20 01:02:03)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "h <> TIMESTAMP '2021-06-10 01:00:00.123456789' ",
                "<>(h, 2021-06-10 01:00:00.123456789:TIMESTAMP(9))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "i <> TIME '22:13:14' ",
                "<>(i, 22:13:14)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "j <> -11 ",
                "<>(CAST(j AS INTEGER), -11)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "k <> -22",
                "<>(CAST(k AS INTEGER), -22)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
        checkFilters(
                "l <> true",
                "NOT(l)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2, false]]");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testLess(String fileFormat) {
        sql(
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "id INT PRIMARY KEY NOT ENFORCED,"
                        + "name STRING,"
                        + "a FLOAT,"
                        + "b DOUBLE,"
                        + "c DECIMAL(10,2),"
                        + "d DATE,"
                        + "e BIGINT,"
                        + "f TIMESTAMP,"
                        + "g TIMESTAMP(0),"
                        + "h TIMESTAMP(9),"
                        + "i TIME,"
                        + "j TINYINT,"
                        + "k SMALLINT"
                        + ") with ('file.format' = '"
                        + fileFormat
                        + "') ");
        sql(
                "INSERT INTO "
                        + tableName
                        + " VALUES(1, 'paimon', 1.2, 3.4, 4.5,DATE '2023-06-06',100, TIMESTAMP '2021-06-30 01:02:03.123456', TIMESTAMP '2021-06-30 01:02:03', "
                        + "TIMESTAMP '2021-06-30 01:00:00.123456789', TIME '12:13:14', CAST(-1 AS TINYINT), CAST(-2 AS SMALLINT))");
        checkFilters(
                "id < 2 ",
                "<(id, 2)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "a < CAST(1.3 AS FLOAT) ",
                "<(a, 1.3E0:FLOAT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");

        checkFilters(
                "b < CAST(3.5 AS DOUBLE)  ",
                "<(b, 3.5E0:DOUBLE)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");

        checkFilters(
                "c < 4.6",
                "<(c, 4.6:DECIMAL(2, 1))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "d < DATE '2023-06-07' ",
                "<(d, 2023-06-07)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "e < 101 ",
                "<(e, 101)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "f < TIMESTAMP '2021-07-30 01:02:03.123456' ",
                "<(f, 2021-07-30 01:02:03.123456:TIMESTAMP(6))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "g < TIMESTAMP '2022-06-30 01:02:03' ",
                "<(g, 2022-06-30 01:02:03)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "h < TIMESTAMP '2022-06-30 01:00:00.123456789' ",
                "<(h, 2022-06-30 01:00:00.123456789:TIMESTAMP(9))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "i < TIME '15:13:14' ",
                "<(i, 15:13:14)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "j < 0 ",
                "<(j, 0)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "k < -1",
                "<(k, -1)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testLessEqual(String fileFormat) {
        sql(
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "id INT PRIMARY KEY NOT ENFORCED,"
                        + "name STRING,"
                        + "a FLOAT,"
                        + "b DOUBLE,"
                        + "c DECIMAL(10,2),"
                        + "d DATE,"
                        + "e BIGINT,"
                        + "f TIMESTAMP,"
                        + "g TIMESTAMP(0),"
                        + "h TIMESTAMP(9),"
                        + "i TIME,"
                        + "j TINYINT,"
                        + "k SMALLINT"
                        + ") with ('file.format' = '"
                        + fileFormat
                        + "') ");
        sql(
                "INSERT INTO "
                        + tableName
                        + " VALUES(1, 'paimon', 1.2, 3.4, 4.5,DATE '2023-06-06',100, TIMESTAMP '2021-06-30 01:02:03.123456', TIMESTAMP '2021-06-30 01:02:03', "
                        + "TIMESTAMP '2021-06-30 01:00:00.123456789', TIME '12:13:14', CAST(-1 AS TINYINT), CAST(-2 AS SMALLINT))");
        checkFilters(
                "id <= 1 ",
                "<=(id, 1)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "a <= CAST(1.2 AS FLOAT) ",
                "<=(a, 1.2E0:FLOAT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");

        checkFilters(
                "b <= CAST(3.4 AS DOUBLE)  ",
                "<=(b, 3.4E0:DOUBLE)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");

        checkFilters(
                "c <= 4.5",
                "<=(c, 4.5:DECIMAL(2, 1))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "d <= DATE '2023-06-06' ",
                "<=(d, 2023-06-06)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "e <= 100 ",
                "<=(e, 100)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "f <= TIMESTAMP '2021-06-30 01:02:03.123456' ",
                "<=(f, 2021-06-30 01:02:03.123456:TIMESTAMP(6))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "g <= TIMESTAMP '2021-06-30 01:02:03' ",
                "<=(g, 2021-06-30 01:02:03)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "h <= TIMESTAMP '2021-06-30 01:00:00.123456789' ",
                "<=(h, 2021-06-30 01:00:00.123456789:TIMESTAMP(9))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "i <= TIME '12:13:14' ",
                "<=(i, 12:13:14)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "j <= -1 ",
                "<=(j, -1)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "k <= -2",
                "<=(k, -2)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testGreater(String fileFormat) {
        sql(
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "id INT PRIMARY KEY NOT ENFORCED,"
                        + "name STRING,"
                        + "a FLOAT,"
                        + "b DOUBLE,"
                        + "c DECIMAL(10,2),"
                        + "d DATE,"
                        + "e BIGINT,"
                        + "f TIMESTAMP,"
                        + "g TIMESTAMP(0),"
                        + "h TIMESTAMP(9),"
                        + "i TIME,"
                        + "j TINYINT,"
                        + "k SMALLINT"
                        + ") with ('file.format' = '"
                        + fileFormat
                        + "') ");
        sql(
                "INSERT INTO "
                        + tableName
                        + " VALUES(1, 'paimon', 1.2, 3.4, 4.5,DATE '2023-06-06',100, TIMESTAMP '2021-06-30 01:02:03.123456', TIMESTAMP '2021-06-30 01:02:03', "
                        + "TIMESTAMP '2021-06-30 01:00:00.123456789', TIME '12:13:14', CAST(-1 AS TINYINT), CAST(-2 AS SMALLINT))");
        checkFilters(
                "id > 0 ",
                ">(id, 0)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "a > CAST(1.1 AS FLOAT) ",
                ">(a, 1.1E0:FLOAT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");

        checkFilters(
                "b > CAST(3.2 AS DOUBLE)  ",
                ">(b, 3.2E0:DOUBLE)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");

        checkFilters(
                "c > 4.1",
                ">(c, 4.1:DECIMAL(2, 1))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "d > DATE '2023-06-01' ",
                ">(d, 2023-06-01)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "e > 10 ",
                ">(e, 10)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "f > TIMESTAMP '2021-05-30 01:02:03.123456' ",
                ">(f, 2021-05-30 01:02:03.123456:TIMESTAMP(6))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "g > TIMESTAMP '2021-05-30 01:02:03' ",
                ">(g, 2021-05-30 01:02:03)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "h > TIMESTAMP '2021-05-30 01:00:00.123456789' ",
                ">(h, 2021-05-30 01:00:00.123456789:TIMESTAMP(9))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "i > TIME '12:13:10' ",
                ">(i, 12:13:10)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "j > -2 ",
                ">(j, -2)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "k > -3",
                ">(k, -3)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testGreaterEqual(String fileFormat) {
        sql(
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "id INT PRIMARY KEY NOT ENFORCED,"
                        + "name STRING,"
                        + "a FLOAT,"
                        + "b DOUBLE,"
                        + "c DECIMAL(10,2),"
                        + "d DATE,"
                        + "e BIGINT,"
                        + "f TIMESTAMP,"
                        + "g TIMESTAMP(0),"
                        + "h TIMESTAMP(9),"
                        + "i TIME,"
                        + "j TINYINT,"
                        + "k SMALLINT"
                        + ") with ('file.format' = '"
                        + fileFormat
                        + "') ");
        sql(
                "INSERT INTO "
                        + tableName
                        + " VALUES(1, 'paimon', 1.2, 3.4, 4.5,DATE '2023-06-06',100, TIMESTAMP '2021-06-30 01:02:03.123456', TIMESTAMP '2021-06-30 01:02:03', "
                        + "TIMESTAMP '2021-06-30 01:00:00.123456789', TIME '12:13:14', CAST(-1 AS TINYINT), CAST(-2 AS SMALLINT))");
        checkFilters(
                "id >= 1 ",
                ">=(id, 1)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "a >= CAST(1.2 AS FLOAT) ",
                ">=(a, 1.2E0:FLOAT)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");

        checkFilters(
                "b >= CAST(3.4 AS DOUBLE)  ",
                ">=(b, 3.4E0:DOUBLE)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");

        checkFilters(
                "c >= 4.5",
                ">=(c, 4.5:DECIMAL(2, 1))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "d >= DATE '2023-06-06' ",
                ">=(d, 2023-06-06)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "e >= 100 ",
                ">=(e, 100)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "f >= TIMESTAMP '2021-06-30 01:02:03.123456' ",
                ">=(f, 2021-06-30 01:02:03.123456:TIMESTAMP(6))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "g >= TIMESTAMP '2021-06-30 01:02:03' ",
                ">=(g, 2021-06-30 01:02:03)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "h >= TIMESTAMP '2021-06-30 01:00:00.123456789' ",
                ">=(h, 2021-06-30 01:00:00.123456789:TIMESTAMP(9))",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "i >= TIME '12:13:14' ",
                ">=(i, 12:13:14)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "j >= -1 ",
                ">=(j, -1)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
        checkFilters(
                "k >= -2",
                ">=(k, -2)",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
    }

    @ParameterizedTest
    @MethodSource("fileFormat")
    public void testStartWith(String fileFormat) {
        sql(
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "id INT PRIMARY KEY NOT ENFORCED,"
                        + "name STRING,"
                        + "a FLOAT,"
                        + "b DOUBLE,"
                        + "c DECIMAL(10,2),"
                        + "d DATE,"
                        + "e BIGINT,"
                        + "f TIMESTAMP,"
                        + "g TIMESTAMP(0),"
                        + "h TIMESTAMP(9),"
                        + "i TIME,"
                        + "j TINYINT,"
                        + "k SMALLINT"
                        + ") with ('file.format' = '"
                        + fileFormat
                        + "') ");
        sql(
                "INSERT INTO "
                        + tableName
                        + " VALUES(1, 'paimon', 1.2, 3.4, 4.5,DATE '2023-06-06',100, TIMESTAMP '2021-06-30 01:02:03.123456', TIMESTAMP '2021-06-30 01:02:03', "
                        + "TIMESTAMP '2021-06-30 01:00:00.123456789', TIME '12:13:14', CAST(-1 AS TINYINT), CAST(-2 AS SMALLINT))");
        checkFilters(
                "name like 'pai%' ",
                "LIKE(name, _UTF-16LE'pai%')",
                "[+I[1, paimon, 1.2, 3.4, 4.50, 2023-06-06, 100, 2021-06-30T01:02:03.123456, 2021-06-30T01:02:03, 2021-06-30T01:00:00.123456789, 12:13:14, -1, -2]]");
    }

    private void checkFilters(String predicate, String flinkFilter, String expectedRows) {

        String sql = "SELECT * FROM " + tableName + " WHERE " + predicate;
        List<Row> list = sql(sql);
        assertEquals(expectedRows, list.toString());

        String planAsString = sql("EXPLAIN " + sql).toString();

        if (flinkFilter != null) {
            assertThat(planAsString).contains("filter=[" + flinkFilter + "]");
        } else {
            assertThat(planAsString).doesNotContain("filter");
        }
    }
}
