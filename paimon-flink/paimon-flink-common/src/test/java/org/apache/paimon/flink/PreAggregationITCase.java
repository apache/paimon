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

import org.apache.paimon.mergetree.compact.aggregate.FieldNestedUpdateAgg;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

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
                            + "c VARCHAR,"
                            + "PRIMARY KEY (k) NOT ENFORCED)"
                            + " WITH ('merge-engine'='aggregation', "
                            + "'changelog-producer' = 'full-compaction',"
                            + "'fields.b.aggregate-function'='first_value',"
                            + "'fields.c.aggregate-function'='first_not_null_value',"
                            + "'sequence.field'='a'"
                            + ");");
        }

        @Test
        public void tesInMemoryMerge() {
            batchSql(
                    "INSERT INTO T VALUES "
                            + "(1, 0, CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)),"
                            + "(1, 1, '1', '1'), "
                            + "(2, 2, '2', '2'),"
                            + "(2, 3, '22', '22')");
            List<Row> result = batchSql("SELECT * FROM T");
            assertThat(result)
                    .containsExactlyInAnyOrder(Row.of(1, 1, null, "1"), Row.of(2, 3, "2", "2"));
        }

        @Test
        public void tesUnOrderInput() {
            batchSql(
                    "INSERT INTO T VALUES "
                            + "(1, 0, CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)),"
                            + "(1, 1, '1', '1'), "
                            + "(2, 3, '2', '2'),"
                            + "(2, 2, '22', '22')");
            List<Row> result = batchSql("SELECT * FROM T");
            assertThat(result)
                    .containsExactlyInAnyOrder(Row.of(1, 1, null, "1"), Row.of(2, 3, "22", "22"));
            batchSql("INSERT INTO T VALUES (2, 1, '1', '1')");
            result = batchSql("SELECT * FROM T");
            assertThat(result)
                    .containsExactlyInAnyOrder(Row.of(1, 1, null, "1"), Row.of(2, 3, "1", "1"));
        }

        @Test
        public void testMergeRead() {
            batchSql("INSERT INTO T VALUES (1, 1, CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR))");
            batchSql("INSERT INTO T VALUES (1, 2, '1', '1')");
            batchSql("INSERT INTO T VALUES (2, 1, '2', '2')");
            batchSql("INSERT INTO T VALUES (2, 2, '22', '22')");
            List<Row> result = batchSql("SELECT * FROM T");
            assertThat(result)
                    .containsExactlyInAnyOrder(Row.of(1, 2, null, "1"), Row.of(2, 2, "2", "2"));
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

    /** ITCase for {@link FieldNestedUpdateAgg}. */
    public static class NestedUpdateAggregationITCase extends CatalogITCaseBase {

        @Override
        protected long checkpointInterval() {
            return 2_000;
        }

        @Override
        protected List<String> ddl() {
            String ordersTable =
                    "CREATE TABLE orders (\n"
                            + "  order_id INT PRIMARY KEY NOT ENFORCED,\n"
                            + "  user_name STRING,\n"
                            + "  address STRING\n"
                            + ");";

            String subordersTable =
                    "CREATE TABLE sub_orders (\n"
                            + "  order_id INT,\n"
                            + "  daily_id INT,\n"
                            + "  today STRING,\n"
                            + "  product_name STRING,\n"
                            + "  price BIGINT,\n"
                            + "  PRIMARY KEY (order_id, daily_id, today) NOT ENFORCED\n"
                            + ");";

            String wideTable =
                    "CREATE TABLE order_wide (\n"
                            + "  order_id INT PRIMARY KEY NOT ENFORCED,\n"
                            + "  user_name STRING,\n"
                            + "  address STRING,\n"
                            + "  sub_orders ARRAY<ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>>\n"
                            + ") WITH (\n"
                            + "  'merge-engine' = 'aggregation',\n"
                            + "  'fields.sub_orders.aggregate-function' = 'nested-update',\n"
                            + "  'fields.sub_orders.nested-keys' = 'daily_id;today'\n"
                            + ")";

            return Arrays.asList(ordersTable, subordersTable, wideTable);
        }

        @Test
        @Timeout(60)
        public void testUseCase() throws Exception {
            sEnv.getConfig()
                    .set(
                            ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
                            ExecutionConfigOptions.UpsertMaterialize.NONE);

            // widen
            sEnv.executeSql(
                    "INSERT INTO order_wide\n"
                            + "SELECT order_id, user_name, address, "
                            + "CAST (NULL AS ARRAY<ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>>) FROM orders\n"
                            + "UNION ALL\n"
                            + "SELECT order_id, CAST (NULL AS STRING), CAST (NULL AS STRING), "
                            + "ARRAY[ROW(daily_id, today, product_name, price)] FROM sub_orders");

            sEnv.executeSql(
                    "INSERT INTO orders VALUES "
                            + "(1, 'Wang', 'HangZhou'),"
                            + "(2, 'Zhao', 'ChengDu'),"
                            + "(3, 'Liu', 'NanJing')");

            sEnv.executeSql(
                    "INSERT INTO sub_orders VALUES "
                            + "(1, 1, '12-20', 'Apple', 8000),"
                            + "(1, 2, '12-20', 'Tesla', 400000),"
                            + "(1, 1, '12-21', 'Sangsung', 5000),"
                            + "(2, 1, '12-20', 'Tea', 40),"
                            + "(2, 2, '12-20', 'Pot', 60),"
                            + "(3, 1, '12-25', 'Bat', 15),"
                            + "(3, 1, '12-26', 'Cup', 30)");

            while (true) {
                List<Row> result = waitExpectedSizeRecordsSorted(3);
                boolean allChecked =
                        checkOneRecord(
                                        result.get(0),
                                        1,
                                        "Wang",
                                        "HangZhou",
                                        Row.of(1, "12-20", "Apple", 8000L),
                                        Row.of(1, "12-21", "Sangsung", 5000L),
                                        Row.of(2, "12-20", "Tesla", 400000L))
                                && checkOneRecord(
                                        result.get(1),
                                        2,
                                        "Zhao",
                                        "ChengDu",
                                        Row.of(1, "12-20", "Tea", 40L),
                                        Row.of(2, "12-20", "Pot", 60L))
                                && checkOneRecord(
                                        result.get(2),
                                        3,
                                        "Liu",
                                        "NanJing",
                                        Row.of(1, "12-25", "Bat", 15L),
                                        Row.of(1, "12-26", "Cup", 30L));
                if (allChecked) {
                    break;
                }
            }

            // query using UNNEST
            List<Row> unnested =
                    sql(
                            "SELECT order_id, user_name, address, daily_id, today, product_name, price "
                                    + "FROM order_wide, UNNEST(sub_orders) AS so(daily_id, today, product_name, price)");

            assertThat(unnested)
                    .containsExactlyInAnyOrder(
                            Row.of(1, "Wang", "HangZhou", 1, "12-20", "Apple", 8000L),
                            Row.of(1, "Wang", "HangZhou", 2, "12-20", "Tesla", 400000L),
                            Row.of(1, "Wang", "HangZhou", 1, "12-21", "Sangsung", 5000L),
                            Row.of(2, "Zhao", "ChengDu", 1, "12-20", "Tea", 40L),
                            Row.of(2, "Zhao", "ChengDu", 2, "12-20", "Pot", 60L),
                            Row.of(3, "Liu", "NanJing", 1, "12-25", "Bat", 15L),
                            Row.of(3, "Liu", "NanJing", 1, "12-26", "Cup", 30L));
        }

        // TODO: test DELETE statement after solving
        // https://github.com/apache/incubator-paimon/issues/2561

        private List<Row> waitExpectedSizeRecordsSorted(int size) throws InterruptedException {
            List<Row> result;
            do {
                Thread.sleep(500);
                result = sql("SELECT * FROM order_wide");
            } while (result.size() != size);

            return result.stream()
                    .sorted(Comparator.comparingInt(r -> r.getFieldAs(0)))
                    .collect(Collectors.toList());
        }

        private boolean checkOneRecord(
                Row record, int orderId, String userName, String address, Row... subOrders) {
            if ((int) record.getField(0) != orderId) {
                return false;
            }
            if (!record.getFieldAs(1).equals(userName)) {
                return false;
            }
            if (!record.getFieldAs(2).equals(address)) {
                return false;
            }

            return checkNestedTable(record.getFieldAs(3), subOrders);
        }

        private boolean checkNestedTable(Row[] nestedTable, Row... subOrders) {
            if (nestedTable.length != subOrders.length) {
                return false;
            }

            Comparator<Row> comparator =
                    (Comparator)
                            Comparator.comparingInt(r -> ((Row) r).getFieldAs(0))
                                    .thenComparing(r -> (String) ((Row) r).getField(1));

            List<Row> sortedActual =
                    Arrays.stream(nestedTable).sorted(comparator).collect(Collectors.toList());
            List<Row> sortedExpected =
                    Arrays.stream(subOrders).sorted(comparator).collect(Collectors.toList());

            for (int i = 0; i < sortedActual.size(); i++) {
                if (!sortedActual.get(i).equals(sortedExpected.get(i))) {
                    return false;
                }
            }

            return true;
        }
    }
}
