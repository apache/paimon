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
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for sum aggregate function. */
public class SumAggregationITCase extends CatalogITCaseBase {
    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS T1 ("
                        + "j INT, k INT, "
                        + "a INT, "
                        + "b Decimal(4,2), "
                        + "c TINYINT,"
                        + "d SMALLINT,"
                        + "e BIGINT,"
                        + "f FLOAT,"
                        + "h DOUBLE,"
                        + "PRIMARY KEY (j,k) NOT ENFORCED)"
                        + " WITH ('merge-engine'='aggregation', "
                        + "'fields.a.aggregate-function'='sum', "
                        + "'fields.b.aggregate-function'='sum', "
                        + "'fields.c.aggregate-function'='sum', "
                        + "'fields.d.aggregate-function'='sum', "
                        + "'fields.e.aggregate-function'='sum', "
                        + "'fields.f.aggregate-function'='sum',"
                        + "'fields.h.aggregate-function'='sum'"
                        + ");");
    }

    @Test
    public void testMergeInMemory() {
        batchSql(
                "INSERT INTO T1 VALUES "
                        + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), "
                        + "CAST(1000 AS BIGINT), 1.11, CAST(1.11 AS DOUBLE)),"
                        + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), "
                        + "CAST(100000 AS BIGINT), -1.11, CAST(1.11 AS DOUBLE)), "
                        + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), "
                        + "CAST(10000000 AS BIGINT), 0, CAST(-1.11 AS DOUBLE))");
        assertThat(batchSql("SELECT * FROM T1"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                2,
                                5,
                                new BigDecimal("12.11"),
                                (byte) 4,
                                (short) 2,
                                (long) 10101000,
                                (float) 0,
                                1.11));

        // projection
        assertThat(batchSql("SELECT f,e FROM T1"))
                .containsExactlyInAnyOrder(Row.of((float) 0, (long) 10101000));
    }

    @Test
    public void testMergeRead() {
        batchSql(
                "INSERT INTO T1 VALUES "
                        + "(1, 2, 1, 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                        + "1.11, CAST(1.11 AS DOUBLE))");
        batchSql(
                "INSERT INTO T1 VALUES "
                        + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                        + "CAST(NULL AS FLOAT), CAST(1.11 AS DOUBLE))");
        batchSql(
                "INSERT INTO T1 VALUES "
                        + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                        + "-1.11, CAST(-1.11 AS DOUBLE))");

        List<Row> result = batchSql("SELECT * FROM T1");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                2,
                                6,
                                new BigDecimal("12.11"),
                                (byte) 4,
                                (short) 2,
                                (long) 10101000,
                                (float) 0,
                                1.11));
    }

    @Test
    public void testMergeCompaction() {
        // Wait compaction
        batchSql("ALTER TABLE T1 SET ('commit.force-compact'='true')");

        // key 1 2
        batchSql(
                "INSERT INTO T1 VALUES "
                        + "(1, 2, 1, 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                        + "1.11, CAST(1.11 AS DOUBLE))");
        batchSql(
                "INSERT INTO T1 VALUES "
                        + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                        + "CAST(NULL AS FLOAT), CAST(1.11 AS DOUBLE))");
        batchSql(
                "INSERT INTO T1 VALUES "
                        + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                        + "-1.11, CAST(-1.11 AS DOUBLE))");

        // key 1 3
        batchSql(
                "INSERT INTO T1 VALUES "
                        + "(1, 3, 2, 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                        + "1.11, CAST(1.11 AS DOUBLE))");
        batchSql(
                "INSERT INTO T1 VALUES "
                        + "(1, 3, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                        + "CAST(NULL AS FLOAT), CAST(1.11 AS DOUBLE))");
        batchSql(
                "INSERT INTO T1 VALUES "
                        + "(1, 3, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                        + "-1.11, CAST(-1.11 AS DOUBLE))");

        assertThat(batchSql("SELECT * FROM T1"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                2,
                                6,
                                new BigDecimal("12.11"),
                                (byte) 4,
                                (short) 2,
                                (long) 10101000,
                                (float) 0,
                                1.11),
                        Row.of(
                                1,
                                3,
                                7,
                                new BigDecimal("12.11"),
                                (byte) 4,
                                (short) 2,
                                (long) 10101000,
                                (float) 0,
                                1.11));
    }

    @Test
    public void testStreamingRead() {
        assertThatThrownBy(
                () -> sEnv.from("T1").execute().print(),
                "Pre-aggregate continuous reading is not supported");
    }
}
