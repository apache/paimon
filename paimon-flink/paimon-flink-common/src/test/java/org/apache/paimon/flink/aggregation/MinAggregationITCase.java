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

package org.apache.paimon.flink.aggregation;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for min aggregate function. */
public class MinAggregationITCase extends CatalogITCaseBase {
    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS T3 ("
                        + "j INT, k INT, "
                        + "a INT, "
                        + "b Decimal(4,2), "
                        + "c TINYINT,"
                        + "d SMALLINT,"
                        + "e BIGINT,"
                        + "f FLOAT,"
                        + "h DOUBLE,"
                        + "i DATE,"
                        + "l TIMESTAMP,"
                        + "m CHAR(1),"
                        + "n VARCHAR,"
                        + "PRIMARY KEY (j,k) NOT ENFORCED)"
                        + " WITH ('merge-engine'='aggregation', "
                        + "'fields.a.aggregate-function'='min', "
                        + "'fields.b.aggregate-function'='min', "
                        + "'fields.c.aggregate-function'='min', "
                        + "'fields.d.aggregate-function'='min', "
                        + "'fields.e.aggregate-function'='min', "
                        + "'fields.f.aggregate-function'='min',"
                        + "'fields.h.aggregate-function'='min',"
                        + "'fields.i.aggregate-function'='min',"
                        + "'fields.l.aggregate-function'='min',"
                        + "'fields.m.aggregate-function'='min',"
                        + "'fields.n.aggregate-function'='min'"
                        + ");");
    }

    @Test
    public void testMergeInMemory() {
        batchSql(
                "INSERT INTO T3 VALUES "
                        + "(1, 2, CAST(NULL AS INT), 1.01, CAST(-1 AS TINYINT), CAST(-1 AS SMALLINT), "
                        + "CAST(1000 AS BIGINT), 1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), "
                        + "CAST('2021-01-01 01:01:01' AS TIMESTAMP), 'a', 'aaa'),"
                        + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), "
                        + "CAST(100000 AS BIGINT), -1.11, CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), "
                        + "CAST('2022-01-01 01:01:01' AS TIMESTAMP), 'b', 'bbb'), "
                        + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), "
                        + "CAST(10000000 AS BIGINT), 0, CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), "
                        + "CAST('2022-01-01 02:00:00' AS TIMESTAMP), 'c', 'ccc')");
        List<Row> result = batchSql("SELECT * FROM T3");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                2,
                                2,
                                new BigDecimal("1.01"),
                                (byte) -1,
                                (short) -1,
                                (long) 1000,
                                (float) -1.11,
                                -1.11,
                                LocalDate.of(2020, 1, 1),
                                LocalDateTime.of(2021, 1, 1, 1, 1, 1),
                                "a",
                                "aaa"));
    }

    @Test
    public void testMergeRead() {
        batchSql(
                "INSERT INTO T3 VALUES "
                        + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                        + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), CAST('2021-01-01 01:01:01' AS TIMESTAMP), 'a', 'aaa')");
        batchSql(
                "INSERT INTO T3 VALUES "
                        + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                        + "-1.11, CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), CAST('2022-01-01 01:01:01' AS TIMESTAMP), 'b', 'bbb')");
        batchSql(
                "INSERT INTO T3 VALUES "
                        + "(1, 2, 3, 10.00, CAST(-1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                        + "0, CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), CAST('2022-01-01 02:00:00' AS TIMESTAMP), 'c', 'ccc')");

        List<Row> result = batchSql("SELECT * FROM T3");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                2,
                                2,
                                new BigDecimal("1.01"),
                                (byte) -1,
                                (short) -1,
                                (long) 1000,
                                (float) -1.11,
                                -1.11,
                                LocalDate.of(2020, 1, 1),
                                LocalDateTime.of(2021, 1, 1, 1, 1, 1),
                                "a",
                                "aaa"));
    }

    @Test
    public void testMergeCompaction() {
        // Wait compaction
        batchSql("ALTER TABLE T3 SET ('commit.force-compact'='true')");

        // key 1 2
        batchSql(
                "INSERT INTO T3 VALUES "
                        + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                        + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), CAST('2021-01-01 01:01:01' AS TIMESTAMP), 'a', 'aaa')");
        batchSql(
                "INSERT INTO T3 VALUES "
                        + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                        + "-1.11, CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), CAST('2022-01-01 01:01:01' AS TIMESTAMP), 'b', 'bbb')");
        batchSql(
                "INSERT INTO T3 VALUES "
                        + "(1, 2, 3, 10.00, CAST(-1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                        + "0, CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), CAST('2022-01-01 02:00:00' AS TIMESTAMP), 'c', 'ccc')");

        // key 1 3
        batchSql(
                "INSERT INTO T3 VALUES "
                        + "(1, 3, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                        + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), CAST('2021-01-01 01:01:01' AS TIMESTAMP), 'a', 'aaa')");
        batchSql(
                "INSERT INTO T3 VALUES "
                        + "(1, 3, 6, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                        + "-1.11, CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), CAST('2022-01-01 01:01:01' AS TIMESTAMP), 'b', 'bbb')");
        batchSql(
                "INSERT INTO T3 VALUES "
                        + "(1, 3, 3, 10.00, CAST(-1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                        + "0, CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), CAST('2022-01-01 02:00:00' AS TIMESTAMP), 'c', 'ccc')");

        assertThat(batchSql("SELECT * FROM T3"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                2,
                                2,
                                new BigDecimal("1.01"),
                                (byte) -1,
                                (short) -1,
                                (long) 1000,
                                (float) -1.11,
                                -1.11,
                                LocalDate.of(2020, 1, 1),
                                LocalDateTime.of(2021, 1, 1, 1, 1, 1),
                                "a",
                                "aaa"),
                        Row.of(
                                1,
                                3,
                                3,
                                new BigDecimal("1.01"),
                                (byte) -1,
                                (short) -1,
                                (long) 1000,
                                (float) -1.11,
                                -1.11,
                                LocalDate.of(2020, 1, 1),
                                LocalDateTime.of(2021, 1, 1, 1, 1, 1),
                                "a",
                                "aaa"));
    }

    @Test
    public void testStreamingRead() {
        assertThatThrownBy(
                () -> sEnv.from("T3").execute().print(),
                "Pre-aggregate continuous reading is not supported");
    }
}
