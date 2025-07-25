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

/** ITCase for max aggregate function. */
public class MaxAggregationITCase extends CatalogITCaseBase {
    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS T2 ("
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
                        + "m CHAR,"
                        + "n VARCHAR,"
                        + "PRIMARY KEY (j,k) NOT ENFORCED)"
                        + " WITH ('merge-engine'='aggregation', "
                        + "'fields.a.aggregate-function'='max', "
                        + "'fields.b.aggregate-function'='max', "
                        + "'fields.c.aggregate-function'='max', "
                        + "'fields.d.aggregate-function'='max', "
                        + "'fields.e.aggregate-function'='max', "
                        + "'fields.f.aggregate-function'='max',"
                        + "'fields.h.aggregate-function'='max',"
                        + "'fields.i.aggregate-function'='max',"
                        + "'fields.l.aggregate-function'='max',"
                        + "'fields.m.aggregate-function'='max',"
                        + "'fields.n.aggregate-function'='max'"
                        + ");");
    }

    @Test
    public void testMergeInMemory() {
        batchSql(
                "INSERT INTO T2 VALUES "
                        + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), "
                        + "CAST(1000 AS BIGINT), 1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), "
                        + "CAST('2021-01-01 01:01:01' AS TIMESTAMP), 'a', 'aaa'),"
                        + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                        + "-1.11, CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), "
                        + "CAST('2022-01-01 01:01:01' AS TIMESTAMP), 'b', 'bbb'), "
                        + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                        + "0, CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), "
                        + "CAST('2022-01-01 02:00:00' AS TIMESTAMP), 'c', 'ccc')");
        List<Row> result = batchSql("SELECT * FROM T2");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                2,
                                3,
                                new BigDecimal("10.00"),
                                (byte) 2,
                                (short) 2,
                                (long) 10000000,
                                (float) 1.11,
                                1.21,
                                LocalDate.of(2022, 1, 2),
                                LocalDateTime.of(2022, 1, 1, 2, 0, 0),
                                "c",
                                "ccc"));
    }

    @Test
    public void testMergeRead() {
        batchSql(
                "INSERT INTO T2 VALUES "
                        + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                        + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), "
                        + "CAST('2021-01-01 01:01:01' AS TIMESTAMP), 'a', 'aaa')");
        batchSql(
                "INSERT INTO T2 VALUES "
                        + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), -1.11, "
                        + "CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), "
                        + "CAST('2022-01-01 01:01:01' AS TIMESTAMP), 'b', 'bbb')");
        batchSql(
                "INSERT INTO T2 VALUES "
                        + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), 0, "
                        + "CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), "
                        + "CAST('2022-01-01 02:00:00' AS TIMESTAMP), 'c', 'ccc')");

        List<Row> result = batchSql("SELECT * FROM T2");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                2,
                                3,
                                new BigDecimal("10.00"),
                                (byte) 2,
                                (short) 2,
                                (long) 10000000,
                                (float) 1.11,
                                1.21,
                                LocalDate.of(2022, 1, 2),
                                LocalDateTime.of(2022, 1, 1, 2, 0, 0),
                                "c",
                                "ccc"));
    }

    @Test
    public void testMergeCompaction() {
        // Wait compaction
        batchSql("ALTER TABLE T2 SET ('commit.force-compact'='true')");

        // key 1 2
        batchSql(
                "INSERT INTO T2 VALUES "
                        + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                        + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), CAST('2021-01-01 01:01:01' AS TIMESTAMP), 'a', 'aaa')");
        batchSql(
                "INSERT INTO T2 VALUES "
                        + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), -1.11, "
                        + "CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), CAST('2022-01-01 01:01:01' AS TIMESTAMP), 'c', 'ccc')");
        batchSql(
                "INSERT INTO T2 VALUES "
                        + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), 0, "
                        + "CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), CAST('2022-01-01 02:00:00' AS TIMESTAMP), 'b', 'bbb')");

        // key 1 3
        batchSql(
                "INSERT INTO T2 VALUES "
                        + "(1, 3, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                        + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), CAST('2021-01-01 01:01:01' AS TIMESTAMP), 'a', 'aaa')");
        batchSql(
                "INSERT INTO T2 VALUES "
                        + "(1, 3, 6, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), -1.11, "
                        + "CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), CAST('2022-01-01 01:01:01' AS TIMESTAMP), 'c', 'ccc')");
        batchSql(
                "INSERT INTO T2 VALUES "
                        + "(1, 3, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), 0, "
                        + "CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), CAST('2022-01-01 02:00:00' AS TIMESTAMP), 'b', 'bbb')");

        assertThat(batchSql("SELECT * FROM T2"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                2,
                                3,
                                new BigDecimal("10.00"),
                                (byte) 2,
                                (short) 2,
                                (long) 10000000,
                                (float) 1.11,
                                1.21,
                                LocalDate.of(2022, 1, 2),
                                LocalDateTime.of(2022, 1, 1, 2, 0, 0),
                                "c",
                                "ccc"),
                        Row.of(
                                1,
                                3,
                                6,
                                new BigDecimal("10.00"),
                                (byte) 2,
                                (short) 2,
                                (long) 10000000,
                                (float) 1.11,
                                1.21,
                                LocalDate.of(2022, 1, 2),
                                LocalDateTime.of(2022, 1, 1, 2, 0, 0),
                                "c",
                                "ccc" + ""));
    }

    @Test
    public void testStreamingRead() {
        assertThatThrownBy(
                () -> sEnv.from("T2").execute().print(),
                "Pre-aggregate continuous reading is not supported");
    }
}
