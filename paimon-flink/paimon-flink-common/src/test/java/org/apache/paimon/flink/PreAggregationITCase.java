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

package org.apache.paimon.flink;

import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for pre-aggregation. */
public class PreAggregationITCase {
    /** ITCase for bool_or/bool_and aggregate function. */
    public static class BoolOrAndAggregation extends CatalogITCaseBase {
        @Override
        protected List<String> ddl() {
            return Collections.singletonList(
                    "CREATE TABLE IF NOT EXISTS T7 ("
                            + "j INT, k INT, "
                            + "a BOOLEAN, "
                            + "b BOOLEAN,"
                            + "PRIMARY KEY (j,k) NOT ENFORCED)"
                            + " WITH ('merge-engine'='aggregation', "
                            + "'fields.a.aggregate-function'='bool_or',"
                            + "'fields.b.aggregate-function'='bool_and'"
                            + ");");
        }

        @Test
        public void testMergeInMemory() {
            batchSql(
                    "INSERT INTO T7 VALUES "
                            + "(1, 2, CAST('TRUE' AS  BOOLEAN), CAST('TRUE' AS BOOLEAN)),"
                            + "(1, 2, CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN)), "
                            + "(1, 2, CAST('FALSE' AS BOOLEAN), CAST('FALSE' AS BOOLEAN))");
            List<Row> result = batchSql("SELECT * FROM T7");
            assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2, true, false));
        }

        @Test
        public void testMergeRead() {
            batchSql(
                    "INSERT INTO T7 VALUES (1, 2, CAST('TRUE' AS  BOOLEAN), CAST('TRUE' AS BOOLEAN))");
            batchSql("INSERT INTO T7 VALUES (1, 2, CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN))");
            batchSql(
                    "INSERT INTO T7 VALUES (1, 2, CAST('FALSE' AS BOOLEAN), CAST('FALSE' AS BOOLEAN))");

            List<Row> result = batchSql("SELECT * FROM T7");
            assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2, true, false));
        }

        @Test
        public void testMergeCompaction() {
            // Wait compaction
            batchSql("ALTER TABLE T7 SET ('commit.force-compact'='true')");

            // key 1 2
            batchSql(
                    "INSERT INTO T7 VALUES (1, 2, CAST('TRUE' AS  BOOLEAN), CAST('TRUE' AS BOOLEAN))");
            batchSql("INSERT INTO T7 VALUES (1, 2, CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN))");
            batchSql(
                    "INSERT INTO T7 VALUES (1, 2, CAST('FALSE' AS BOOLEAN), CAST('FALSE' AS BOOLEAN))");

            // key 1 3
            batchSql(
                    "INSERT INTO T7 VALUES (1, 3, CAST('FALSE' AS  BOOLEAN), CAST('TRUE' AS BOOLEAN))");
            batchSql("INSERT INTO T7 VALUES (1, 3, CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN))");
            batchSql(
                    "INSERT INTO T7 VALUES (1, 3, CAST('FALSE' AS BOOLEAN), CAST('TRUE' AS BOOLEAN))");

            assertThat(batchSql("SELECT * FROM T7"))
                    .containsExactlyInAnyOrder(
                            Row.of(1, 2, true, false), Row.of(1, 3, false, true));
        }

        @Test
        public void testStreamingRead() {
            assertThatThrownBy(
                    () -> sEnv.from("T7").execute().print(),
                    "Pre-aggregate continuous reading is not supported");
        }
    }

    /** ITCase for listagg aggregate function. */
    public static class ListAggAggregation extends CatalogITCaseBase {
        @Override
        protected int defaultParallelism() {
            // set parallelism to 1 so that the order of input data is determined
            return 1;
        }

        @Override
        protected List<String> ddl() {
            return Collections.singletonList(
                    "CREATE TABLE IF NOT EXISTS T6 ("
                            + "j INT, k INT, "
                            + "a STRING, "
                            + "PRIMARY KEY (j,k) NOT ENFORCED)"
                            + " WITH ('merge-engine'='aggregation', "
                            + "'fields.a.aggregate-function'='listagg'"
                            + ");");
        }

        @Test
        public void testMergeInMemory() {
            // VALUES does not guarantee order, but order is important for list aggregations.
            // So we need to sort the input data.
            batchSql(
                    "CREATE TABLE myTable AS "
                            + "SELECT b, c, d FROM "
                            + "(VALUES "
                            + "  (1, 1, 2, 'first line'),"
                            + "  (2, 1, 2, CAST(NULL AS STRING)),"
                            + "  (3, 1, 2, 'second line')"
                            + ") AS V(a, b, c, d) "
                            + "ORDER BY a");
            batchSql("INSERT INTO T6 SELECT * FROM myTable");
            List<Row> result = batchSql("SELECT * FROM T6");
            assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2, "first line,second line"));
        }

        @Test
        public void testMergeRead() {
            batchSql("INSERT INTO T6 VALUES (1, 2, 'first line')");
            batchSql("INSERT INTO T6 VALUES (1, 2, CAST(NULL AS STRING))");
            batchSql("INSERT INTO T6 VALUES (1, 2, 'second line')");

            List<Row> result = batchSql("SELECT * FROM T6");
            assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2, "first line,second line"));
        }

        @Test
        public void testMergeCompaction() {
            // Wait compaction
            batchSql("ALTER TABLE T6 SET ('commit.force-compact'='true')");

            // key 1 2
            batchSql("INSERT INTO T6 VALUES (1, 2, 'first line')");
            batchSql("INSERT INTO T6 VALUES (1, 2, CAST(NULL AS STRING))");
            batchSql("INSERT INTO T6 VALUES (1, 2, 'second line')");

            // key 1 3
            batchSql("INSERT INTO T6 VALUES (1, 3, CAST(NULL AS STRING))");
            batchSql("INSERT INTO T6 VALUES (1, 3, CAST(NULL AS STRING))");
            batchSql("INSERT INTO T6 VALUES (1, 3, CAST(NULL AS STRING))");

            assertThat(batchSql("SELECT * FROM T6"))
                    .containsExactlyInAnyOrder(
                            Row.of(1, 2, "first line,second line"), Row.of(1, 3, null));
        }

        @Test
        public void testStreamingRead() {
            assertThatThrownBy(
                    () -> sEnv.from("T6").execute().print(),
                    "Pre-aggregate continuous reading is not supported");
        }
    }

    /** ITCase for last value aggregate function. */
    public static class LastValueAggregation extends CatalogITCaseBase {
        @Override
        protected int defaultParallelism() {
            // set parallelism to 1 so that the order of input data is determined
            return 1;
        }

        @Override
        protected List<String> ddl() {
            return Collections.singletonList(
                    "CREATE TABLE IF NOT EXISTS T5 ("
                            + "j INT, k INT, "
                            + "a INT, "
                            + "i DATE,"
                            + "PRIMARY KEY (j,k) NOT ENFORCED)"
                            + " WITH ('merge-engine'='aggregation', "
                            + "'fields.a.aggregate-function'='last_value', "
                            + "'fields.i.aggregate-function'='last_value'"
                            + ");");
        }

        @Test
        public void testMergeInMemory() {
            // VALUES does not guarantee order, but order is important for last value aggregations.
            // So we need to sort the input data.
            batchSql(
                    "CREATE TABLE myTable AS "
                            + "SELECT b, c, d, e FROM "
                            + "(VALUES "
                            + "  (1, 1, 2, CAST(NULL AS INT), CAST('2020-01-01' AS DATE)),"
                            + "  (2, 1, 2, 2, CAST('2020-01-02' AS DATE)),"
                            + "  (3, 1, 2, 3, CAST(NULL AS DATE))"
                            + ") AS V(a, b, c, d, e) "
                            + "ORDER BY a");
            batchSql("INSERT INTO T5 SELECT * FROM myTable");
            List<Row> result = batchSql("SELECT * FROM T5");
            assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2, 3, null));
        }

        @Test
        public void testMergeRead() {
            batchSql("INSERT INTO T5 VALUES (1, 2, CAST(NULL AS INT), CAST('2020-01-01' AS DATE))");
            batchSql("INSERT INTO T5 VALUES (1, 2, 2, CAST('2020-01-02' AS DATE))");
            batchSql("INSERT INTO T5 VALUES (1, 2, 3, CAST(NULL AS DATE))");

            List<Row> result = batchSql("SELECT * FROM T5");
            assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2, 3, null));
        }

        @Test
        public void testMergeCompaction() {
            // Wait compaction
            batchSql("ALTER TABLE T5 SET ('commit.force-compact'='true')");

            // key 1 2
            batchSql("INSERT INTO T5 VALUES (1, 2, CAST(NULL AS INT), CAST('2020-01-01' AS DATE))");
            batchSql("INSERT INTO T5 VALUES (1, 2, 2, CAST('2020-01-02' AS DATE))");
            batchSql("INSERT INTO T5 VALUES (1, 2, 3, CAST(NULL AS DATE))");

            // key 1 3
            batchSql("INSERT INTO T5 VALUES (1, 3, 3, CAST('2020-01-01' AS DATE))");
            batchSql("INSERT INTO T5 VALUES (1, 3, 2, CAST(NULL AS DATE))");
            batchSql("INSERT INTO T5 VALUES (1, 3, CAST(NULL AS INT), CAST('2022-01-02' AS DATE))");

            assertThat(batchSql("SELECT * FROM T5"))
                    .containsExactlyInAnyOrder(
                            Row.of(1, 2, 3, null), Row.of(1, 3, null, LocalDate.of(2022, 1, 2)));
        }

        @Test
        public void testStreamingRead() {
            assertThatThrownBy(
                    () -> sEnv.from("T5").execute().print(),
                    "Pre-aggregate continuous reading is not supported");
        }
    }

    /** ITCase for last non-null value aggregate function. */
    public static class LastNonNullValueAggregation extends CatalogITCaseBase {
        @Override
        protected int defaultParallelism() {
            // set parallelism to 1 so that the order of input data is determined
            return 1;
        }

        @Override
        protected List<String> ddl() {
            return Collections.singletonList(
                    "CREATE TABLE IF NOT EXISTS T4 ("
                            + "j INT, k INT, "
                            + "a INT, "
                            + "b INT, "
                            + "i DATE,"
                            + "PRIMARY KEY (j,k) NOT ENFORCED)"
                            + " WITH ('merge-engine'='aggregation', "
                            + "'fields.a.aggregate-function'='last_non_null_value', "
                            + "'fields.i.aggregate-function'='last_non_null_value'"
                            + ");");
        }

        @Test
        public void testMergeInMemory() {
            // VALUES does not guarantee order, but order is important for last value aggregations.
            // So we need to sort the input data.
            batchSql(
                    "CREATE TABLE myTable AS "
                            + "SELECT b, c, d, e, f FROM "
                            + "(VALUES "
                            + "  (1, 1, 2, CAST(NULL AS INT), 4, CAST('2020-01-01' AS DATE)),"
                            + "  (2, 1, 2, 2, CAST(NULL as INT), CAST('2020-01-02' AS DATE)),"
                            + "  (3, 1, 2, 3, 5, CAST(NULL AS DATE))"
                            + ") AS V(a, b, c, d, e, f) "
                            + "ORDER BY a");
            batchSql("INSERT INTO T4 SELECT * FROM myTable");
            List<Row> result = batchSql("SELECT * FROM T4");
            assertThat(result)
                    .containsExactlyInAnyOrder(Row.of(1, 2, 3, 5, LocalDate.of(2020, 1, 2)));
        }

        @Test
        public void testMergeRead() {
            batchSql(
                    "INSERT INTO T4 VALUES (1, 2, CAST(NULL AS INT), 3, CAST('2020-01-01' AS DATE))");
            batchSql(
                    "INSERT INTO T4 VALUES (1, 2, 2, CAST(NULL AS INT), CAST('2020-01-02' AS DATE))");
            batchSql("INSERT INTO T4 VALUES (1, 2, 3, 5, CAST(NULL AS DATE))");

            List<Row> result = batchSql("SELECT * FROM T4");
            assertThat(result)
                    .containsExactlyInAnyOrder(Row.of(1, 2, 3, 5, LocalDate.of(2020, 1, 2)));
        }

        @Test
        public void testMergeCompaction() {
            // Wait compaction
            batchSql("ALTER TABLE T4 SET ('commit.force-compact'='true')");

            // key 1 2
            batchSql(
                    "INSERT INTO T4 VALUES (1, 2, CAST(NULL AS INT), 3, CAST('2020-01-01' AS DATE))");
            batchSql(
                    "INSERT INTO T4 VALUES (1, 2, 2, CAST(NULL AS INT), CAST('2020-01-02' AS DATE))");
            batchSql("INSERT INTO T4 VALUES (1, 2, 3, 5, CAST(NULL AS DATE))");

            // key 1 3
            batchSql("INSERT INTO T4 VALUES (1, 3, 3, 4, CAST('2020-01-01' AS DATE))");
            batchSql("INSERT INTO T4 VALUES (1, 3, 2, 6, CAST(NULL AS DATE))");
            batchSql(
                    "INSERT INTO T4 VALUES (1, 3, CAST(NULL AS INT), CAST(NULL AS INT), CAST('2022-01-02' AS DATE))");

            assertThat(batchSql("SELECT * FROM T4"))
                    .containsExactlyInAnyOrder(
                            Row.of(1, 2, 3, 5, LocalDate.of(2020, 1, 2)),
                            Row.of(1, 3, 2, 6, LocalDate.of(2022, 1, 2)));
        }

        @Test
        public void testStreamingRead() {
            assertThatThrownBy(
                    () -> sEnv.from("T4").execute().print(),
                    "Pre-aggregate continuous reading is not supported");
        }
    }

    /** ITCase for min aggregate function. */
    public static class MinAggregation extends CatalogITCaseBase {
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
                            + "'fields.l.aggregate-function'='min'"
                            + ");");
        }

        @Test
        public void testMergeInMemory() {
            batchSql(
                    "INSERT INTO T3 VALUES "
                            + "(1, 2, CAST(NULL AS INT), 1.01, CAST(-1 AS TINYINT), CAST(-1 AS SMALLINT), "
                            + "CAST(1000 AS BIGINT), 1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), "
                            + "CAST('0001-01-01 01:01:01' AS TIMESTAMP)),"
                            + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), "
                            + "CAST(100000 AS BIGINT), -1.11, CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), "
                            + "CAST('0002-01-01 01:01:01' AS TIMESTAMP)), "
                            + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), "
                            + "CAST(10000000 AS BIGINT), 0, CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), "
                            + "CAST('0002-01-01 02:00:00' AS TIMESTAMP))");
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
                                    LocalDateTime.of(1, 1, 1, 1, 1, 1)));
        }

        @Test
        public void testMergeRead() {
            batchSql(
                    "INSERT INTO T3 VALUES "
                            + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                            + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), CAST('0001-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T3 VALUES "
                            + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                            + "-1.11, CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), CAST('0002-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T3 VALUES "
                            + "(1, 2, 3, 10.00, CAST(-1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                            + "0, CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), CAST('0002-01-01 02:00:00' AS TIMESTAMP))");

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
                                    LocalDateTime.of(1, 1, 1, 1, 1, 1)));
        }

        @Test
        public void testMergeCompaction() {
            // Wait compaction
            batchSql("ALTER TABLE T3 SET ('commit.force-compact'='true')");

            // key 1 2
            batchSql(
                    "INSERT INTO T3 VALUES "
                            + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                            + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), CAST('0001-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T3 VALUES "
                            + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                            + "-1.11, CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), CAST('0002-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T3 VALUES "
                            + "(1, 2, 3, 10.00, CAST(-1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                            + "0, CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), CAST('0002-01-01 02:00:00' AS TIMESTAMP))");

            // key 1 3
            batchSql(
                    "INSERT INTO T3 VALUES "
                            + "(1, 3, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                            + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), CAST('0001-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T3 VALUES "
                            + "(1, 3, 6, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                            + "-1.11, CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), CAST('0002-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T3 VALUES "
                            + "(1, 3, 3, 10.00, CAST(-1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                            + "0, CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), CAST('0002-01-01 02:00:00' AS TIMESTAMP))");

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
                                    LocalDateTime.of(1, 1, 1, 1, 1, 1)),
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
                                    LocalDateTime.of(1, 1, 1, 1, 1, 1)));
        }

        @Test
        public void testStreamingRead() {
            assertThatThrownBy(
                    () -> sEnv.from("T3").execute().print(),
                    "Pre-aggregate continuous reading is not supported");
        }
    }

    /** ITCase for max aggregate function. */
    public static class MaxAggregation extends CatalogITCaseBase {
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
                            + "'fields.l.aggregate-function'='max'"
                            + ");");
        }

        @Test
        public void testMergeInMemory() {
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), "
                            + "CAST(1000 AS BIGINT), 1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), "
                            + "CAST('0001-01-01 01:01:01' AS TIMESTAMP)),"
                            + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), "
                            + "-1.11, CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), "
                            + "CAST('0002-01-01 01:01:01' AS TIMESTAMP)), "
                            + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), "
                            + "0, CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), "
                            + "CAST('0002-01-01 02:00:00' AS TIMESTAMP))");
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
                                    LocalDateTime.of(2, 1, 1, 2, 0, 0)));
        }

        @Test
        public void testMergeRead() {
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                            + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), "
                            + "CAST('0001-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), -1.11, "
                            + "CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), "
                            + "CAST('0002-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), 0, "
                            + "CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), "
                            + "CAST('0002-01-01 02:00:00' AS TIMESTAMP))");

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
                                    LocalDateTime.of(2, 1, 1, 2, 0, 0)));
        }

        @Test
        public void testMergeCompaction() {
            // Wait compaction
            batchSql("ALTER TABLE T2 SET ('commit.force-compact'='true')");

            // key 1 2
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                            + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), CAST('0001-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), -1.11, "
                            + "CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), CAST('0002-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), 0, "
                            + "CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), CAST('0002-01-01 02:00:00' AS TIMESTAMP))");

            // key 1 3
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + "(1, 3, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(1000 AS BIGINT), "
                            + "1.11, CAST(1.11 AS DOUBLE), CAST('2020-01-01' AS DATE), CAST('0001-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + "(1, 3, 6, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(100000 AS BIGINT), -1.11, "
                            + "CAST(1.21 AS DOUBLE), CAST('2020-01-02' AS DATE), CAST('0002-01-01 01:01:01' AS TIMESTAMP))");
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + "(1, 3, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(10000000 AS BIGINT), 0, "
                            + "CAST(-1.11 AS DOUBLE), CAST('2022-01-02' AS DATE), CAST('0002-01-01 02:00:00' AS TIMESTAMP))");

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
                                    LocalDateTime.of(2, 1, 1, 2, 0, 0)),
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
                                    LocalDateTime.of(2, 1, 1, 2, 0, 0)));
        }

        @Test
        public void testStreamingRead() {
            assertThatThrownBy(
                    () -> sEnv.from("T2").execute().print(),
                    "Pre-aggregate continuous reading is not supported");
        }
    }

    /** ITCase for sum aggregate function. */
    public static class SumAggregation extends CatalogITCaseBase {
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

    /** ITCase for sum aggregate function retraction. */
    public static class SumRetractionAggregation extends CatalogITCaseBase {
        @Override
        protected List<String> ddl() {
            return Collections.singletonList(
                    "CREATE TABLE T ("
                            + "k INT,"
                            + "b Decimal(12, 2),"
                            + "PRIMARY KEY (k) NOT ENFORCED)"
                            + " WITH ('merge-engine'='aggregation', "
                            + "'changelog-producer' = 'full-compaction',"
                            + "'fields.b.aggregate-function'='sum'"
                            + ");");
        }

        @Test
        public void testRetraction() throws Exception {
            sql("CREATE TABLE INPUT (" + "k INT," + "b INT," + "PRIMARY KEY (k) NOT ENFORCED);");
            CloseableIterator<Row> insert =
                    streamSqlIter("INSERT INTO T SELECT k, SUM(b) FROM INPUT GROUP BY k;");
            BlockingIterator<Row, Row> select = streamSqlBlockIter("SELECT * FROM T");

            sql("INSERT INTO INPUT VALUES (1, 1), (2, 2)");
            assertThat(select.collect(2))
                    .containsExactlyInAnyOrder(
                            Row.of(1, BigDecimal.valueOf(100, 2)),
                            Row.of(2, BigDecimal.valueOf(200, 2)));

            sql("INSERT INTO INPUT VALUES (1, 3), (2, 4)");
            assertThat(select.collect(4))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, BigDecimal.valueOf(100, 2)),
                            Row.ofKind(RowKind.UPDATE_AFTER, 1, BigDecimal.valueOf(300, 2)),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 2, BigDecimal.valueOf(200, 2)),
                            Row.ofKind(RowKind.UPDATE_AFTER, 2, BigDecimal.valueOf(400, 2)));

            select.close();
            insert.close();
        }
    }

    /** ITCase for first_value aggregate function. */
    public static class FirstValueAggregation extends CatalogITCaseBase {
        @Override
        protected List<String> ddl() {
            return Collections.singletonList(
                    "CREATE TABLE T ("
                            + "k INT,"
                            + "a INT,"
                            + "b VARCHAR,"
                            + "PRIMARY KEY (k) NOT ENFORCED)"
                            + " WITH ('merge-engine'='aggregation', "
                            + "'changelog-producer' = 'full-compaction',"
                            + "'fields.b.aggregate-function'='first_value',"
                            + "'sequence.field'='a'"
                            + ");");
        }

        @Test
        public void tesInMemoryMerge() {
            batchSql(
                    "INSERT INTO T VALUES "
                            + "(1, 0, CAST(NULL AS VARCHAR)),"
                            + "(1, 1, '1'), "
                            + "(2, 2, '2'),"
                            + "(2, 3, '22')");
            List<Row> result = batchSql("SELECT * FROM T");
            assertThat(result).containsExactlyInAnyOrder(Row.of(1, 1, null), Row.of(2, 3, "2"));
        }

        @Test
        public void tesUnOrderInput() {
            batchSql(
                    "INSERT INTO T VALUES "
                            + "(1, 0, CAST(NULL AS VARCHAR)),"
                            + "(1, 1, '1'), "
                            + "(2, 3, '2'),"
                            + "(2, 2, '22')");
            List<Row> result = batchSql("SELECT * FROM T");
            assertThat(result).containsExactlyInAnyOrder(Row.of(1, 1, null), Row.of(2, 3, "22"));
            batchSql("INSERT INTO T VALUES (2, 1, '1')");
            result = batchSql("SELECT * FROM T");
            assertThat(result).containsExactlyInAnyOrder(Row.of(1, 1, null), Row.of(2, 3, "1"));
        }

        @Test
        public void testMergeRead() {
            batchSql("INSERT INTO T VALUES (1, 1, CAST(NULL AS VARCHAR))");
            batchSql("INSERT INTO T VALUES (1, 2, '1')");
            batchSql("INSERT INTO T VALUES (2, 1, '2')");
            batchSql("INSERT INTO T VALUES (2, 2, '22')");
            List<Row> result = batchSql("SELECT * FROM T");
            assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2, null), Row.of(2, 2, "2"));
        }
    }

    /** IT Test for aggregation merge engine. */
    public static class BasicAggregateITCase extends CatalogITCaseBase {

        @Test
        public void testLocalMerge() {
            sql(
                    "CREATE TABLE T ("
                            + "k INT,"
                            + "v INT,"
                            + "d INT,"
                            + "PRIMARY KEY (k, d) NOT ENFORCED) PARTITIONED BY (d) "
                            + " WITH ('merge-engine'='aggregation', "
                            + "'fields.v.aggregate-function'='sum',"
                            + "'local-merge-buffer-size'='1m'"
                            + ");");

            sql("INSERT INTO T VALUES(1, 1, 1), (2, 1, 1), (1, 2, 1)");
            assertThat(batchSql("SELECT * FROM T"))
                    .containsExactlyInAnyOrder(Row.of(1, 3, 1), Row.of(2, 1, 1));
        }
    }
}
