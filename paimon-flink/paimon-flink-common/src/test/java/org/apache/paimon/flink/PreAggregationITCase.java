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

import org.apache.paimon.mergetree.compact.aggregate.FieldCollectAgg;
import org.apache.paimon.mergetree.compact.aggregate.FieldMergeMapAgg;
import org.apache.paimon.mergetree.compact.aggregate.FieldNestedUpdateAgg;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.RoaringBitmap32;
import org.apache.paimon.utils.RoaringBitmap64;

import org.apache.commons.codec.binary.Hex;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.utils.ThetaSketch.sketchOf;
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

    /** ITCase for product aggregate function. */
    public static class ProductAggregation extends CatalogITCaseBase {
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
                            + "'fields.a.aggregate-function'='product', "
                            + "'fields.b.aggregate-function'='product', "
                            + "'fields.c.aggregate-function'='product', "
                            + "'fields.d.aggregate-function'='product', "
                            + "'fields.e.aggregate-function'='product', "
                            + "'fields.f.aggregate-function'='product',"
                            + "'fields.h.aggregate-function'='product'"
                            + ");");
        }

        @Test
        public void testMergeInMemory() {
            batchSql("ALTER TABLE T1 MODIFY b DECIMAL(5, 3)");

            batchSql(
                    "INSERT INTO T1 VALUES "
                            + "(1, 2, CAST(NULL AS INT), 1.01, CAST(1 AS TINYINT), CAST(-1 AS SMALLINT), "
                            + "CAST(1000 AS BIGINT), 1.11, CAST(1.11 AS DOUBLE)),"
                            + "(1, 2, 2, 1.10, CAST(2 AS TINYINT), CAST(2 AS SMALLINT), "
                            + "CAST(100000 AS BIGINT), -1.11, CAST(1.11 AS DOUBLE)), "
                            + "(1, 2, 3, 10.00, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), "
                            + "CAST(10000000 AS BIGINT), 0, CAST(-1.11 AS DOUBLE))");
            List<Row> result = batchSql("SELECT * FROM T1");
            assertThat(result)
                    .containsExactlyInAnyOrder(
                            Row.of(
                                    1,
                                    2,
                                    6,
                                    new BigDecimal("11.110"),
                                    (byte) 2,
                                    (short) -2,
                                    1000000000000000L,
                                    (float) -0.0,
                                    -1.3676310000000003));

            // projection
            assertThat(batchSql("SELECT f,e FROM T1"))
                    .containsExactlyInAnyOrder(Row.of((float) -0.0, 1000000000000000L));
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
                                    new BigDecimal("11.10"),
                                    (byte) 2,
                                    (short) -2,
                                    1000000000000000L,
                                    (float) -1.2321,
                                    -1.3676310000000003));
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

            List<Row> result = batchSql("SELECT * FROM T1");
            assertThat(result)
                    .containsExactlyInAnyOrder(
                            Row.of(
                                    1,
                                    2,
                                    6,
                                    new BigDecimal("11.10"),
                                    (byte) 2,
                                    (short) -2,
                                    1000000000000000L,
                                    (float) -1.2321,
                                    -1.3676310000000003),
                            Row.of(
                                    1,
                                    3,
                                    12,
                                    new BigDecimal("11.10"),
                                    (byte) 2,
                                    (short) -2,
                                    1000000000000000L,
                                    (float) -1.2321,
                                    -1.3676310000000003));
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
            return Arrays.asList(
                    "CREATE TABLE T ("
                            + "k INT,"
                            + "a INT,"
                            + "b VARCHAR,"
                            + "c VARCHAR,"
                            + "d VARCHAR,"
                            + "PRIMARY KEY (k) NOT ENFORCED)"
                            + " WITH ('merge-engine'='aggregation', "
                            + "'changelog-producer' = 'full-compaction',"
                            + "'fields.b.aggregate-function'='first_value',"
                            + "'fields.c.aggregate-function'='first_non_null_value',"
                            + "'fields.d.aggregate-function'='first_not_null_value',"
                            + "'sequence.field'='a'"
                            + ");",
                    "CREATE TABLE T2 ("
                            + "k INT,"
                            + "v STRING,"
                            + "PRIMARY KEY (k) NOT ENFORCED)"
                            + "WITH ("
                            + "'merge-engine' = 'aggregation',"
                            + "'fields.v.aggregate-function' = 'first_value',"
                            + "'fields.v.ignore-retract' = 'true'"
                            + ");");
        }

        @Test
        public void tesInMemoryMerge() {
            batchSql(
                    "INSERT INTO T VALUES "
                            + "(1, 0, CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)),"
                            + "(1, 1, '1', '1', '1'), "
                            + "(2, 2, '2', '2', '2'),"
                            + "(2, 3, '22', '22', '22')");
            List<Row> result = batchSql("SELECT * FROM T");
            assertThat(result)
                    .containsExactlyInAnyOrder(
                            Row.of(1, 1, null, "1", "1"), Row.of(2, 3, "2", "2", "2"));
        }

        @Test
        public void tesUnOrderInput() {
            batchSql(
                    "INSERT INTO T VALUES "
                            + "(1, 0, CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)),"
                            + "(1, 1, '1', '1', '1'), "
                            + "(2, 3, '2', '2', '2'),"
                            + "(2, 2, '22', '22', '22')");
            List<Row> result = batchSql("SELECT * FROM T");
            assertThat(result)
                    .containsExactlyInAnyOrder(
                            Row.of(1, 1, null, "1", "1"), Row.of(2, 3, "22", "22", "22"));
            batchSql("INSERT INTO T VALUES (2, 1, '1', '1', '1')");
            result = batchSql("SELECT * FROM T");
            assertThat(result)
                    .containsExactlyInAnyOrder(
                            Row.of(1, 1, null, "1", "1"), Row.of(2, 3, "1", "1", "1"));
        }

        @Test
        public void testMergeRead() {
            batchSql(
                    "INSERT INTO T VALUES (1, 1, CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR))");
            batchSql("INSERT INTO T VALUES (1, 2, '1', '1', '1')");
            batchSql("INSERT INTO T VALUES (2, 1, '2', '2', '2')");
            batchSql("INSERT INTO T VALUES (2, 2, '22', '22', '22')");
            List<Row> result = batchSql("SELECT * FROM T");
            assertThat(result)
                    .containsExactlyInAnyOrder(
                            Row.of(1, 2, null, "1", "1"), Row.of(2, 2, "2", "2", "2"));
        }

        @Test
        public void testAggregatorResetWhenIgnoringRetract() {
            int numRows = 100;
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + IntStream.range(0, numRows)
                                    .mapToObj(i -> String.format("(%d, '%d')", i, i))
                                    .collect(Collectors.joining(", ")));
            batchSql(
                    "INSERT INTO T2 VALUES "
                            + IntStream.range(numRows / 2, numRows)
                                    .mapToObj(i -> String.format("(%d, '%d')", i, i + numRows))
                                    .collect(Collectors.joining(", ")));
            List<Row> result = batchSql("SELECT * FROM T2");
            assertThat(result)
                    .containsExactlyInAnyOrder(
                            IntStream.range(0, numRows)
                                    .mapToObj(i -> Row.of(i, String.valueOf(i)))
                                    .toArray(Row[]::new));
        }
    }

    /** IT Test for aggregation merge engine. */
    public static class BasicAggregateITCase extends CatalogITCaseBase {

        @Override
        protected List<String> ddl() {
            return Collections.singletonList(
                    "CREATE TABLE T ("
                            + "k INT,"
                            + "v INT,"
                            + "d INT,"
                            + "PRIMARY KEY (k, d) NOT ENFORCED) PARTITIONED BY (d) "
                            + " WITH ('merge-engine'='aggregation', "
                            + "'fields.v.aggregate-function'='sum',"
                            + "'local-merge-buffer-size'='5m'"
                            + ");");
        }

        @Test
        public void testLocalMerge() {
            sql("INSERT INTO T VALUES(1, 1, 1), (2, 1, 1), (1, 2, 1)");
            assertThat(batchSql("SELECT * FROM T"))
                    .containsExactlyInAnyOrder(Row.of(1, 3, 1), Row.of(2, 1, 1));
        }

        @Test
        public void testMergeRead() {
            sql("INSERT INTO T VALUES(1, 1, 1), (2, 1, 1)");
            sql("INSERT INTO T VALUES(1, 2, 1)");
            assertThat(batchSql("SELECT * FROM T"))
                    .containsExactlyInAnyOrder(Row.of(1, 3, 1), Row.of(2, 1, 1));
            // filter
            assertThat(batchSql("SELECT * FROM T where v = 3"))
                    .containsExactlyInAnyOrder(Row.of(1, 3, 1));
            assertThat(batchSql("SELECT * FROM T where v = 1"))
                    .containsExactlyInAnyOrder(Row.of(2, 1, 1));
        }
    }

    /** ITCase for {@link FieldNestedUpdateAgg}. */
    public static class NestedUpdateAggregationITCase extends CatalogITCaseBase {

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
                            + "  'fields.sub_orders.aggregate-function' = 'nested_update',\n"
                            + "  'fields.sub_orders.nested-key' = 'daily_id,today',\n"
                            + "  'fields.sub_orders.ignore-retract' = 'true',"
                            + "  'fields.user_name.ignore-retract' = 'true',"
                            + "  'fields.address.ignore-retract' = 'true'"
                            + ")";

            String wideAppendTable =
                    "CREATE TABLE order_append_wide (\n"
                            + "  order_id INT PRIMARY KEY NOT ENFORCED,\n"
                            + "  user_name STRING,\n"
                            + "  address STRING,\n"
                            + "  sub_orders ARRAY<ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>>\n"
                            + ") WITH (\n"
                            + "  'merge-engine' = 'aggregation',\n"
                            + "  'fields.sub_orders.aggregate-function' = 'nested_update',\n"
                            + "  'fields.sub_orders.ignore-retract' = 'true',"
                            + "  'fields.user_name.ignore-retract' = 'true',"
                            + "  'fields.address.ignore-retract' = 'true'"
                            + ")";

            return Arrays.asList(ordersTable, subordersTable, wideTable, wideAppendTable);
        }

        @Test
        public void testUseCase() {
            sql(
                    "INSERT INTO orders VALUES "
                            + "(1, 'Wang', 'HangZhou'),"
                            + "(2, 'Zhao', 'ChengDu'),"
                            + "(3, 'Liu', 'NanJing')");

            sql(
                    "INSERT INTO sub_orders VALUES "
                            + "(1, 1, '12-20', 'Apple', 8000),"
                            + "(1, 2, '12-20', 'Tesla', 400000),"
                            + "(1, 1, '12-21', 'Sangsung', 5000),"
                            + "(2, 1, '12-20', 'Tea', 40),"
                            + "(2, 2, '12-20', 'Pot', 60),"
                            + "(3, 1, '12-25', 'Bat', 15),"
                            + "(3, 1, '12-26', 'Cup', 30)");

            sql(widenSql());

            List<Row> result =
                    sql("SELECT * FROM order_wide").stream()
                            .sorted(Comparator.comparingInt(r -> r.getFieldAs(0)))
                            .collect(Collectors.toList());

            assertThat(
                            checkOneRecord(
                                    result.get(0),
                                    1,
                                    "Wang",
                                    "HangZhou",
                                    Row.of(1, "12-20", "Apple", 8000L),
                                    Row.of(1, "12-21", "Sangsung", 5000L),
                                    Row.of(2, "12-20", "Tesla", 400000L)))
                    .isTrue();
            assertThat(
                            checkOneRecord(
                                    result.get(1),
                                    2,
                                    "Zhao",
                                    "ChengDu",
                                    Row.of(1, "12-20", "Tea", 40L),
                                    Row.of(2, "12-20", "Pot", 60L)))
                    .isTrue();
            assertThat(
                            checkOneRecord(
                                    result.get(2),
                                    3,
                                    "Liu",
                                    "NanJing",
                                    Row.of(1, "12-25", "Bat", 15L),
                                    Row.of(1, "12-26", "Cup", 30L)))
                    .isTrue();

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

        @Test
        public void testUseCaseWithNullValue() {
            sql(
                    "INSERT INTO order_wide\n"
                            + "SELECT 6, CAST (NULL AS STRING), CAST (NULL AS STRING), "
                            + "ARRAY[cast(null as ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>)]");

            List<Row> result =
                    sql("SELECT * FROM order_wide").stream()
                            .sorted(Comparator.comparingInt(r -> r.getFieldAs(0)))
                            .collect(Collectors.toList());

            assertThat(checkOneRecord(result.get(0), 6, null, null, (Row) null)).isTrue();

            sql(
                    "INSERT INTO order_wide\n"
                            + "SELECT 6, 'Sun', CAST (NULL AS STRING), "
                            + "ARRAY[ROW(1, '01-01','Apple', 6999)]");

            result =
                    sql("SELECT * FROM order_wide").stream()
                            .sorted(Comparator.comparingInt(r -> r.getFieldAs(0)))
                            .collect(Collectors.toList());
            assertThat(
                            checkOneRecord(
                                    result.get(0),
                                    6,
                                    "Sun",
                                    null,
                                    Row.of(1, "01-01", "Apple", 6999L)))
                    .isTrue();
        }

        @Test
        public void testUseCaseAppend() {
            sql(
                    "INSERT INTO orders VALUES "
                            + "(1, 'Wang', 'HangZhou'),"
                            + "(2, 'Zhao', 'ChengDu'),"
                            + "(3, 'Liu', 'NanJing')");

            sql(
                    "INSERT INTO sub_orders VALUES "
                            + "(1, 1, '12-20', 'Apple', 8000),"
                            + "(2, 1, '12-20', 'Tesla', 400000),"
                            + "(3, 1, '12-25', 'Bat', 15),"
                            + "(3, 1, '12-26', 'Cup', 30)");

            sql(widenAppendSql());

            // query using UNNEST
            List<Row> unnested =
                    sql(
                            "SELECT order_id, user_name, address, daily_id, today, product_name, price "
                                    + "FROM order_append_wide, UNNEST(sub_orders) AS so(daily_id, today, product_name, price)");

            assertThat(unnested)
                    .containsExactlyInAnyOrder(
                            Row.of(1, "Wang", "HangZhou", 1, "12-20", "Apple", 8000L),
                            Row.of(2, "Zhao", "ChengDu", 1, "12-20", "Tesla", 400000L),
                            Row.of(3, "Liu", "NanJing", 1, "12-25", "Bat", 15L),
                            Row.of(3, "Liu", "NanJing", 1, "12-26", "Cup", 30L));
        }

        @Test
        @Timeout(90)
        public void testUpdateWithIgnoreRetract() throws Exception {
            sEnv.getConfig()
                    .set(
                            ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
                            ExecutionConfigOptions.UpsertMaterialize.NONE);

            sql("INSERT INTO orders VALUES (1, 'Wang', 'HangZhou')");

            sql(
                    "INSERT INTO sub_orders VALUES "
                            + "(1, 1, '12-20', 'Apple', 8000),"
                            + "(1, 2, '12-20', 'Tesla', 400000),"
                            + "(1, 1, '12-21', 'Sangsung', 5000)");

            sEnv.executeSql(widenSql());

            boolean checkResult;
            List<Row> result;

            do {
                Thread.sleep(500);
                result = sql("SELECT * FROM order_wide");
                checkResult =
                        !result.isEmpty()
                                && checkOneRecord(
                                        result.get(0),
                                        1,
                                        "Wang",
                                        "HangZhou",
                                        Row.of(1, "12-20", "Apple", 8000L),
                                        Row.of(1, "12-21", "Sangsung", 5000L),
                                        Row.of(2, "12-20", "Tesla", 400000L));
            } while (!checkResult);

            sql("INSERT INTO sub_orders VALUES (1, 2, '12-20', 'Benz', 380000)");

            do {
                Thread.sleep(500);
                result = sql("SELECT * FROM order_wide");
                checkResult =
                        !result.isEmpty()
                                && checkOneRecord(
                                        result.get(0),
                                        1,
                                        "Wang",
                                        "HangZhou",
                                        Row.of(1, "12-20", "Apple", 8000L),
                                        Row.of(1, "12-21", "Sangsung", 5000L),
                                        Row.of(2, "12-20", "Benz", 380000L));
            } while (!checkResult);
        }

        private String widenSql() {
            return "INSERT INTO order_wide\n"
                    + "SELECT order_id, user_name, address, "
                    + "CAST (NULL AS ARRAY<ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>>) FROM orders\n"
                    + "UNION ALL\n"
                    + "SELECT order_id, CAST (NULL AS STRING), CAST (NULL AS STRING), "
                    + "ARRAY[ROW(daily_id, today, product_name, price)] FROM sub_orders";
        }

        private String widenAppendSql() {
            return "INSERT INTO order_append_wide\n"
                    + "SELECT order_id, user_name, address, "
                    + "CAST (NULL AS ARRAY<ROW<daily_id INT, today STRING, product_name STRING, price BIGINT>>) FROM orders\n"
                    + "UNION ALL\n"
                    + "SELECT order_id, CAST (NULL AS STRING), CAST (NULL AS STRING), "
                    + "ARRAY[ROW(daily_id, today, product_name, price)] FROM sub_orders";
        }

        private boolean checkOneRecord(
                Row record, int orderId, String userName, String address, Row... subOrders) {
            if ((int) record.getField(0) != orderId) {
                return false;
            }
            if (!Objects.equals(record.getFieldAs(1), userName)) {
                return false;
            }
            if (!Objects.equals(record.getFieldAs(2), address)) {
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
                if (!Objects.equals(sortedActual.get(i), sortedExpected.get(i))) {
                    return false;
                }
            }

            return true;
        }
    }

    /** ITCase for {@link FieldCollectAgg}. */
    public static class CollectAggregationITCase extends CatalogITCaseBase {

        @Override
        protected int defaultParallelism() {
            // set parallelism to 1 so that the order of input data is determined
            return 1;
        }

        @Test
        public void testAggWithDistinct() {
            sql(
                    "CREATE TABLE test_collect("
                            + "  id INT PRIMARY KEY NOT ENFORCED,"
                            + "  f0 ARRAY<STRING>"
                            + ") WITH ("
                            + "  'merge-engine' = 'aggregation',"
                            + "  'fields.f0.aggregate-function' = 'collect',"
                            + "  'fields.f0.distinct' = 'true'"
                            + ")");

            sql(
                    "INSERT INTO test_collect VALUES "
                            + "(1, CAST (NULL AS ARRAY<STRING>)), "
                            + "(2, ARRAY['A', 'B']), "
                            + "(3, ARRAY['car', 'watch'])");

            List<Row> result = queryAndSort("SELECT * FROM test_collect");
            checkOneRecord(result.get(0), 1);
            checkOneRecord(result.get(1), 2, "A", "B");
            checkOneRecord(result.get(2), 3, "car", "watch");

            sql(
                    "INSERT INTO test_collect VALUES "
                            + "(1, ARRAY['paimon', 'paimon']), "
                            + "(2, ARRAY['A', 'B', 'C']), "
                            + "(3, CAST (NULL AS ARRAY<STRING>))");

            result = queryAndSort("SELECT * FROM test_collect");
            checkOneRecord(result.get(0), 1, "paimon");
            checkOneRecord(result.get(1), 2, "A", "B", "C");
            checkOneRecord(result.get(2), 3, "car", "watch");
        }

        @Test
        public void testAggWithoutDistinct() {
            sql(
                    "CREATE TABLE test_collect("
                            + "  id INT PRIMARY KEY NOT ENFORCED,"
                            + "  f0 ARRAY<STRING>"
                            + ") WITH ("
                            + "  'merge-engine' = 'aggregation',"
                            + "  'fields.f0.aggregate-function' = 'collect'"
                            + ")");

            sql(
                    "INSERT INTO test_collect VALUES "
                            + "(1, CAST (NULL AS ARRAY<STRING>)), "
                            + "(2, ARRAY['A', 'B', 'B']), "
                            + "(3, ARRAY['car', 'watch'])");

            List<Row> result = queryAndSort("SELECT * FROM test_collect");
            checkOneRecord(result.get(0), 1);
            checkOneRecord(result.get(1), 2, "A", "B", "B");
            checkOneRecord(result.get(2), 3, "car", "watch");

            sql(
                    "INSERT INTO test_collect VALUES "
                            + "(1, ARRAY['paimon', 'paimon']), "
                            + "(2, ARRAY['A', 'B', 'C']), "
                            + "(3, CAST (NULL AS ARRAY<STRING>))");

            result = queryAndSort("SELECT * FROM test_collect");
            checkOneRecord(result.get(0), 1, "paimon", "paimon");
            checkOneRecord(result.get(1), 2, "A", "A", "B", "B", "B", "C");
            checkOneRecord(result.get(2), 3, "car", "watch");
        }

        private static List<Arguments> retractArguments() {
            return Arrays.asList(
                    Arguments.arguments("lookup", "aggregation"),
                    Arguments.arguments("lookup", "partial-update"),
                    Arguments.arguments("full-compaction", "aggregation"),
                    Arguments.arguments("full-compaction", "partial-update"));
        }

        @ParameterizedTest(name = "changelog-producer = {0}, merge-engine = {1}")
        @MethodSource("retractArguments")
        public void testRetract(String changelogProducer, String mergeEngine) throws Exception {
            String sequenceGroup = "";
            if (mergeEngine.equals("partial-update")) {
                sequenceGroup = ", 'fields.f1.sequence-group' = 'f0'";
            }
            sql(
                    "CREATE TABLE test_collect("
                            + "  id INT PRIMARY KEY NOT ENFORCED,"
                            + "  f0 ARRAY<STRING>,"
                            + "  f1 INT"
                            + ") WITH ("
                            + "  'changelog-producer' = '%s',"
                            + "  'merge-engine' = '%s',"
                            + "  'fields.f0.aggregate-function' = 'collect'"
                            + "  %s"
                            + ")",
                    changelogProducer, mergeEngine, sequenceGroup);

            BlockingIterator<Row, Row> select = streamSqlBlockIter("SELECT * FROM test_collect");

            String temporaryTableTemplate =
                    "CREATE TEMPORARY TABLE %s ("
                            + "  id INT PRIMARY KEY NOT ENFORCED,"
                            + "  f0 ARRAY<STRING>,"
                            + "  f1 INT"
                            + ") WITH ("
                            + "  'connector' = 'values',"
                            + "  'data-id' = '%s',"
                            + "  'bounded' = 'true',"
                            + "  'changelog-mode' = '%s'"
                            + ")";

            // 1 only exists single key record
            sql("INSERT INTO test_collect VALUES (1, ARRAY['A', 'B'], 1)");
            List<Row> result = select.collect(1);
            checkOneRecord(result.get(0), 1, "A", "B");

            // 1.1 UB + UA (CANNOT handle)
            List<Row> inputRecords =
                    Arrays.asList(
                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, new String[] {"A", "B"}, 2),
                            Row.ofKind(RowKind.UPDATE_AFTER, 1, new String[] {"C", "D"}, 3));
            sEnv.executeSql(
                            String.format(
                                    temporaryTableTemplate,
                                    "INPUT11",
                                    TestValuesTableFactory.registerData(inputRecords),
                                    "UB,UA"))
                    .await();
            sEnv.executeSql("INSERT INTO test_collect SELECT * FROM INPUT11").await();

            result = select.collect(2);
            assertThat(result.get(0).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
            checkOneRecord(result.get(0), 1, "A", "B");
            assertThat(result.get(1).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
            checkOneRecord(result.get(1), 1, "A", "B", "C", "D");

            // 1.2 -D
            inputRecords =
                    Collections.singletonList(
                            Row.ofKind(RowKind.DELETE, 1, new String[] {"C", "D"}, 4));
            sEnv.executeSql(
                            String.format(
                                    temporaryTableTemplate,
                                    "INPUT12",
                                    TestValuesTableFactory.registerData(inputRecords),
                                    "D"))
                    .await();
            sEnv.executeSql("INSERT INTO test_collect SELECT * FROM INPUT12").await();

            result = select.collect(2);
            assertThat(result.get(0).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
            checkOneRecord(result.get(0), 1, "A", "B", "C", "D");
            assertThat(result.get(1).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
            checkOneRecord(result.get(1), 1, "A", "B");

            // 2 exists multiple key records
            sql("INSERT INTO test_collect VALUES (2, ARRAY['A', 'B'], 5), (3, ARRAY['A', 'B'], 6)");
            result = select.collect(2);
            checkOneRecord(result.get(0), 2, "A", "B");
            checkOneRecord(result.get(1), 3, "A", "B");

            // 2.1 UB + UA (CANNOT handle)
            inputRecords =
                    Arrays.asList(
                            Row.ofKind(RowKind.UPDATE_BEFORE, 2, new String[] {"A", "B"}, 7),
                            Row.ofKind(RowKind.UPDATE_AFTER, 2, new String[] {"C", "D"}, 8));
            sEnv.executeSql(
                            String.format(
                                    temporaryTableTemplate,
                                    "INPUT21",
                                    TestValuesTableFactory.registerData(inputRecords),
                                    "UB,UA"))
                    .await();
            sEnv.executeSql("INSERT INTO test_collect SELECT * FROM INPUT21").await();

            result = select.collect(2);
            assertThat(result.get(0).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
            checkOneRecord(result.get(0), 2, "A", "B");
            assertThat(result.get(1).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
            checkOneRecord(result.get(1), 2, "A", "B", "C", "D");

            // 2.2 -D
            inputRecords =
                    Collections.singletonList(Row.ofKind(RowKind.DELETE, 3, new String[] {"A"}, 9));
            sEnv.executeSql(
                            String.format(
                                    temporaryTableTemplate,
                                    "INPUT22",
                                    TestValuesTableFactory.registerData(inputRecords),
                                    "D"))
                    .await();
            sEnv.executeSql("INSERT INTO test_collect SELECT * FROM INPUT22").await();

            result = select.collect(2);
            assertThat(result.get(0).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
            checkOneRecord(result.get(0), 3, "A", "B");
            assertThat(result.get(1).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
            checkOneRecord(result.get(1), 3, "B");

            select.close();
        }

        private void checkOneRecord(Row row, int id, String... elements) {
            assertThat(row.getField(0)).isEqualTo(id);
            if (elements == null || elements.length == 0) {
                assertThat(row.getField(1)).isNull();
            } else {
                assertThat((String[]) row.getField(1)).containsExactlyInAnyOrder(elements);
            }
        }
    }

    /** ITCase for {@link FieldMergeMapAgg}. */
    public static class MergeMapAggregationITCase extends CatalogITCaseBase {

        @Override
        protected List<String> ddl() {
            return Collections.singletonList(
                    "CREATE TABLE test_merge_map("
                            + "  id INT PRIMARY KEY NOT ENFORCED,"
                            + "  f0 MAP<INT, STRING>"
                            + ") WITH ("
                            + "  'merge-engine' = 'aggregation',"
                            + "  'fields.f0.aggregate-function' = 'merge_map'"
                            + ")");
        }

        @Test
        public void testMergeMap() {
            sql(
                    "INSERT INTO test_merge_map VALUES "
                            + "(1, CAST (NULL AS MAP<INT, STRING>)), "
                            + "(2, MAP[1, 'A']), "
                            + "(3, MAP[1, 'A', 2, 'B'])");

            List<Row> result = queryAndSort("SELECT * FROM test_merge_map");
            checkOneRecord(result.get(0), 1, null);
            checkOneRecord(result.get(1), 2, toMap(1, "A"));
            checkOneRecord(result.get(2), 3, toMap(1, "A", 2, "B"));

            sql(
                    "INSERT INTO test_merge_map VALUES "
                            + "(1, MAP[1, 'A']), "
                            + "(2, MAP[1, 'B']), "
                            + "(3, MAP[1, 'a', 2, 'b', 3, 'c'])");

            result = queryAndSort("SELECT * FROM test_merge_map");
            checkOneRecord(result.get(0), 1, toMap(1, "A"));
            checkOneRecord(result.get(1), 2, toMap(1, "B"));
            checkOneRecord(result.get(2), 3, toMap(1, "a", 2, "b", 3, "c"));
        }

        private Map<Object, Object> toMap(Object... kvs) {
            Map<Object, Object> result = new HashMap<>();
            for (int i = 0; i < kvs.length; i += 2) {
                result.put(kvs[i], kvs[i + 1]);
            }
            return result;
        }

        private void checkOneRecord(Row row, int id, Map<Object, Object> map) {
            assertThat(row.getField(0)).isEqualTo(id);
            if (map == null || map.isEmpty()) {
                assertThat(row.getField(1)).isNull();
            } else {
                assertThat((Map<Object, Object>) row.getField(1))
                        .containsExactlyInAnyOrderEntriesOf(map);
            }
        }
    }

    /** ITCase for last non-null value aggregate function. */
    public static class FieldsDefaultAggregationITCase extends CatalogITCaseBase {
        @Override
        protected int defaultParallelism() {
            // set parallelism to 1 so that the order of input data is determined
            return 1;
        }

        @Override
        protected List<String> ddl() {
            return Collections.singletonList(
                    "CREATE TABLE IF NOT EXISTS test_default_agg_func ("
                            + "j INT, k INT, "
                            + "a INT, "
                            + "b INT, "
                            + "i DATE,"
                            + "PRIMARY KEY (j,k) NOT ENFORCED)"
                            + " WITH ('merge-engine'='aggregation', "
                            + "'fields.default-aggregate-function'='first_non_null_value', "
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
            batchSql("INSERT INTO test_default_agg_func SELECT * FROM myTable");
            List<Row> result = batchSql("SELECT * FROM test_default_agg_func");
            assertThat(result)
                    .containsExactlyInAnyOrder(Row.of(1, 2, 2, 4, LocalDate.of(2020, 1, 2)));
        }

        @Test
        public void testMergeRead() {
            batchSql(
                    "INSERT INTO test_default_agg_func VALUES (1, 2, CAST(NULL AS INT), 3, CAST('2020-01-01' AS DATE))");
            batchSql(
                    "INSERT INTO test_default_agg_func VALUES (1, 2, 2, CAST(NULL AS INT), CAST('2020-01-02' AS DATE))");
            batchSql("INSERT INTO test_default_agg_func VALUES (1, 2, 3, 5, CAST(NULL AS DATE))");

            List<Row> result = batchSql("SELECT * FROM test_default_agg_func");
            assertThat(result)
                    .containsExactlyInAnyOrder(Row.of(1, 2, 2, 3, LocalDate.of(2020, 1, 2)));
        }

        @Test
        public void testMergeCompaction() {
            // Wait compaction
            batchSql("ALTER TABLE test_default_agg_func SET ('commit.force-compact'='true')");

            // key 1 2
            batchSql(
                    "INSERT INTO test_default_agg_func VALUES (1, 2, CAST(NULL AS INT), 3, CAST('2020-01-01' AS DATE))");
            batchSql(
                    "INSERT INTO test_default_agg_func VALUES (1, 2, 2, CAST(NULL AS INT), CAST('2020-01-02' AS DATE))");
            batchSql("INSERT INTO test_default_agg_func VALUES (1, 2, 3, 5, CAST(NULL AS DATE))");

            // key 1 3
            batchSql(
                    "INSERT INTO test_default_agg_func VALUES (1, 3, 3, 4, CAST('2020-01-01' AS DATE))");
            batchSql("INSERT INTO test_default_agg_func VALUES (1, 3, 2, 6, CAST(NULL AS DATE))");
            batchSql(
                    "INSERT INTO test_default_agg_func VALUES (1, 3, CAST(NULL AS INT), CAST(NULL AS INT), CAST('2022-01-02' AS DATE))");

            assertThat(batchSql("SELECT * FROM test_default_agg_func"))
                    .containsExactlyInAnyOrder(
                            Row.of(1, 2, 2, 3, LocalDate.of(2020, 1, 2)),
                            Row.of(1, 3, 3, 4, LocalDate.of(2022, 1, 2)));
        }

        @Test
        public void testStreamingRead() {
            assertThatThrownBy(
                    () -> sEnv.from("test_default_agg_func").execute().print(),
                    "Pre-aggregate continuous reading is not supported");
        }
    }

    /** ITCase for {@link org.apache.paimon.mergetree.compact.aggregate.FieldThetaSketchAgg}. */
    public static class ThetaSketchAggAggregationITCase extends CatalogITCaseBase {

        @Test
        public void testThetaSketchAgg() {
            sql(
                    "CREATE TABLE test_collect("
                            + "  id INT PRIMARY KEY NOT ENFORCED,"
                            + "  f0 VARBINARY"
                            + ") WITH ("
                            + "  'merge-engine' = 'aggregation',"
                            + "  'fields.f0.aggregate-function' = 'theta_sketch'"
                            + ")");

            String str1 = Hex.encodeHexString(sketchOf(1)).toUpperCase();
            String str2 = Hex.encodeHexString(sketchOf(2)).toUpperCase();
            String str3 = Hex.encodeHexString(sketchOf(3)).toUpperCase();

            sql(
                    String.format(
                            "INSERT INTO test_collect VALUES (1, CAST (NULL AS VARBINARY)),(2, CAST(x'%s' AS VARBINARY)), (3, CAST(x'%s' AS VARBINARY))",
                            str1, str2));

            List<Row> result = queryAndSort("SELECT * FROM test_collect");
            checkOneRecord(result.get(0), 1, null);
            checkOneRecord(result.get(1), 2, sketchOf(1));
            checkOneRecord(result.get(2), 3, sketchOf(2));

            sql(
                    String.format(
                            "INSERT INTO test_collect VALUES (1, CAST (x'%s' AS VARBINARY)),(2, CAST(x'%s' AS VARBINARY)), (2, CAST(x'%s' AS VARBINARY)), (3, CAST(x'%s' AS VARBINARY))",
                            str1, str2, str2, str3));

            result = queryAndSort("SELECT * FROM test_collect");
            checkOneRecord(result.get(0), 1, sketchOf(1));
            checkOneRecord(result.get(1), 2, sketchOf(1, 2));
            checkOneRecord(result.get(2), 3, sketchOf(2, 3));
        }

        private void checkOneRecord(Row row, int id, byte[] expected) {
            assertThat(row.getField(0)).isEqualTo(id);
            assertThat(row.getField(1)).isEqualTo(expected);
        }
    }

    /**
     * ITCase for {@link org.apache.paimon.mergetree.compact.aggregate.FieldRoaringBitmap32Agg} &
     * {@link org.apache.paimon.mergetree.compact.aggregate.FieldRoaringBitmap64Agg}.
     */
    public static class RoaringBitmapAggAggregationITCase extends CatalogITCaseBase {

        @Test
        public void testRoaring32BitmapAgg() throws IOException {
            sql(
                    "CREATE TABLE test_rbm64("
                            + "  id INT PRIMARY KEY NOT ENFORCED,"
                            + "  f0 VARBINARY"
                            + ") WITH ("
                            + "  'merge-engine' = 'aggregation',"
                            + "  'fields.f0.aggregate-function' = 'rbm32'"
                            + ")");

            byte[] v1Bytes = RoaringBitmap32.bitmapOf(1).serialize();
            byte[] v2Bytes = RoaringBitmap32.bitmapOf(2).serialize();
            byte[] v3Bytes = RoaringBitmap32.bitmapOf(3).serialize();
            byte[] v4Bytes = RoaringBitmap32.bitmapOf(1, 2).serialize();
            byte[] v5Bytes = RoaringBitmap32.bitmapOf(2, 3).serialize();
            String v1 = Hex.encodeHexString(v1Bytes).toUpperCase();
            String v2 = Hex.encodeHexString(v2Bytes).toUpperCase();
            String v3 = Hex.encodeHexString(v3Bytes).toUpperCase();

            sql(
                    "INSERT INTO test_rbm64 VALUES "
                            + "(1, CAST (NULL AS VARBINARY)), "
                            + "(2, CAST (x'"
                            + v1
                            + "' AS VARBINARY)), "
                            + "(3, CAST (x'"
                            + v2
                            + "' AS VARBINARY))");

            List<Row> result = queryAndSort("SELECT * FROM test_rbm64");
            checkOneRecord(result.get(0), 1, null);
            checkOneRecord(result.get(1), 2, v1Bytes);
            checkOneRecord(result.get(2), 3, v2Bytes);

            sql(
                    "INSERT INTO test_rbm64 VALUES "
                            + "(1, CAST (x'"
                            + v1
                            + "' AS VARBINARY)), "
                            + "(2, CAST (x'"
                            + v2
                            + "' AS VARBINARY)), "
                            + "(2, CAST (x'"
                            + v2
                            + "' AS VARBINARY)), "
                            + "(3, CAST (x'"
                            + v3
                            + "' AS VARBINARY))");

            result = queryAndSort("SELECT * FROM test_rbm64");
            checkOneRecord(result.get(0), 1, v1Bytes);
            checkOneRecord(result.get(1), 2, v4Bytes);
            checkOneRecord(result.get(2), 3, v5Bytes);
        }

        @Test
        public void testRoaring64BitmapAgg() throws IOException {
            sql(
                    "CREATE TABLE test_rbm64("
                            + "  id INT PRIMARY KEY NOT ENFORCED,"
                            + "  f0 VARBINARY"
                            + ") WITH ("
                            + "  'merge-engine' = 'aggregation',"
                            + "  'fields.f0.aggregate-function' = 'rbm64'"
                            + ")");

            byte[] v1Bytes = RoaringBitmap64.bitmapOf(1L).serialize();
            byte[] v2Bytes = RoaringBitmap64.bitmapOf(2L).serialize();
            byte[] v3Bytes = RoaringBitmap64.bitmapOf(3L).serialize();
            byte[] v4Bytes = RoaringBitmap64.bitmapOf(1L, 2L).serialize();
            byte[] v5Bytes = RoaringBitmap64.bitmapOf(2L, 3L).serialize();
            String v1 = Hex.encodeHexString(v1Bytes).toUpperCase();
            String v2 = Hex.encodeHexString(v2Bytes).toUpperCase();
            String v3 = Hex.encodeHexString(v3Bytes).toUpperCase();

            sql(
                    "INSERT INTO test_rbm64 VALUES "
                            + "(1, CAST (NULL AS VARBINARY)), "
                            + "(2, CAST (x'"
                            + v1
                            + "' AS VARBINARY)), "
                            + "(3, CAST (x'"
                            + v2
                            + "' AS VARBINARY))");

            List<Row> result = queryAndSort("SELECT * FROM test_rbm64");
            checkOneRecord(result.get(0), 1, null);
            checkOneRecord(result.get(1), 2, v1Bytes);
            checkOneRecord(result.get(2), 3, v2Bytes);

            sql(
                    "INSERT INTO test_rbm64 VALUES "
                            + "(1, CAST (x'"
                            + v1
                            + "' AS VARBINARY)), "
                            + "(2, CAST (x'"
                            + v2
                            + "' AS VARBINARY)), "
                            + "(2, CAST (x'"
                            + v2
                            + "' AS VARBINARY)), "
                            + "(3, CAST (x'"
                            + v3
                            + "' AS VARBINARY))");

            result = queryAndSort("SELECT * FROM test_rbm64");
            checkOneRecord(result.get(0), 1, v1Bytes);
            checkOneRecord(result.get(1), 2, v4Bytes);
            checkOneRecord(result.get(2), 3, v5Bytes);
        }

        private void checkOneRecord(Row row, int id, byte[] expected) {
            assertThat(row.getField(0)).isEqualTo(id);
            assertThat(row.getField(1)).isEqualTo(expected);
        }
    }
}
