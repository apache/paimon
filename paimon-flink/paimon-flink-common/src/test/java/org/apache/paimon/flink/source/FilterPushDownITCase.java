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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for filter push down. */
public class FilterPushDownITCase extends CatalogITCaseBase {

    @Override
    public List<String> ddl() {
        return ImmutableList.of("CREATE TABLE T (" + "a INT, b INT, c STRING) PARTITIONED BY (a);");
    }

    @BeforeEach
    @Override
    public void before() throws IOException {
        super.before();
        batchSql("INSERT INTO T VALUES (1, 1, '1'), (1, 2, '2'), (2, 3, '3'), (3, 3, '3')");
    }

    @Test
    public void testPartitionConditionConsuming_OnePartitionCondition() {
        String sql = "SELECT * FROM T where a = 1 limit 1";
        assertPlanAndResult(
                sql,
                "+- Limit(offset=[0], fetch=[1], global=[false])\n"
                        + "+- TableSourceScan(table=[[PAIMON, default, T, filter=[=(a, 1)], project=[b, c], limit=[1]]], fields=[b, c])",
                Row.ofKind(RowKind.INSERT, 1, 1, "1"));
    }

    @Test
    public void testPartitionConditionConsuming_PartitionConditionAndOther() {
        String sql = "SELECT * FROM T where (a = 1 or a = 2) and c = '1' limit 1";
        // c = '1' is not consumed and limit 1 not push to source
        assertPlanAndResult(
                sql,
                "+- Calc(select=[a, b, CAST('1' AS VARCHAR(2147483647)) AS c], where=[(c = '1')])\n"
                        + "+- TableSourceScan(table=[[PAIMON, default, T, filter=[and(OR(=(a, 1), =(a, 2)), =(c, _UTF-16LE'1':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"))]]], fields=[a, b, c])",
                Row.ofKind(RowKind.INSERT, 1, 1, "1"));
    }

    /**
     * Flink does not compare FLOAT/DOUBLE the same way in every plan: {@code d < 0.0} and {@code d
     * >= 0.0} order -0.0 below 0.0, while {@code d = 0.0} and {@code d < 0} treat the two zeros as
     * equal. Whatever Flink decides, a pushed-down filter must not drop a row Flink keeps, and one
     * consumed on a partition field must not keep a row Flink drops. Flink's own evaluation of each
     * condition, as a projected column of the whole table, is the oracle.
     */
    @ParameterizedTest(name = "{0}, partitioned: {1}")
    @CsvSource({"parquet, false", "orc, false", "avro, false", "parquet, true"})
    public void testSignedZeroFiltersAgreeWithFlink(String format, boolean partitioned) {
        // constants are folded through DECIMAL, which has no -0.0, so negate at runtime
        sql("CREATE TABLE ZERO_SRC (d DOUBLE, f FLOAT)");
        batchSql("INSERT INTO ZERO_SRC VALUES (CAST(0 AS DOUBLE), CAST(0 AS FLOAT))");
        sql(
                "CREATE TABLE Z (id INT, d DOUBLE, f FLOAT) %s WITH ('file.format' = '%s')",
                partitioned ? "PARTITIONED BY (d, f)" : "", format);
        // one file, or partition, per value
        batchSql("INSERT INTO Z SELECT 1, -d, -f FROM ZERO_SRC");
        batchSql("INSERT INTO Z SELECT 2, d, f FROM ZERO_SRC");
        batchSql("INSERT INTO Z VALUES (3, -1.0, -1.0)");
        batchSql("INSERT INTO Z VALUES (4, 1.0, 1.0)");
        batchSql("INSERT INTO Z SELECT 5, d / d, f / f FROM ZERO_SRC");
        assertThat(batchSql("SELECT id, CAST(d AS STRING), CAST(f AS STRING) FROM Z"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "-0.0", "-0.0"),
                        Row.of(2, "0.0", "0.0"),
                        Row.of(3, "-1.0", "-1.0"),
                        Row.of(4, "1.0", "1.0"),
                        Row.of(5, "NaN", "NaN"));

        // %1$s is the column, %2$s its type
        List<String> templates =
                Arrays.asList(
                        "%1$s = 0.0",
                        "%1$s <> 0.0",
                        "%1$s < 0.0",
                        "%1$s <= 0.0",
                        "%1$s > 0.0",
                        "%1$s >= 0.0",
                        "%1$s < 0",
                        "%1$s >= 0",
                        "%1$s > CAST('-0.0' AS %2$s)",
                        "%1$s <= CAST('-0.0' AS %2$s)",
                        "0.0 > %1$s",
                        "0.0 <= %1$s",
                        "%1$s IN (0.0, 5.0)",
                        "%1$s NOT IN (0.0, 5.0)",
                        "%1$s BETWEEN 0.0 AND 0.5",
                        "%1$s NOT BETWEEN 0.0 AND 0.5",
                        "%1$s IS NOT DISTINCT FROM 0.0",
                        "(%1$s <> 0.0 AND %1$s <> 5.0)",
                        "(%1$s < 0.0 OR %1$s > 0.5)",
                        "NOT (%1$s >= 0.0 AND id > 0)",
                        "%1$s > 0.5",
                        "%1$s <> 1.0");
        for (String column : Arrays.asList("d", "f")) {
            String type = column.equals("d") ? "DOUBLE" : "FLOAT";
            List<String> conditions =
                    templates.stream()
                            .map(template -> String.format(template, column, type))
                            .collect(Collectors.toList());
            List<Row> evaluated = batchSql("SELECT id, %s FROM Z", String.join(", ", conditions));
            for (int i = 0; i < conditions.size(); i++) {
                int field = i + 1;
                List<Row> expected =
                        evaluated.stream()
                                .filter(row -> Boolean.TRUE.equals(row.getField(field)))
                                .map(row -> Row.of(row.getField(0)))
                                .collect(Collectors.toList());
                assertThat(batchSql("SELECT id FROM Z WHERE %s", conditions.get(i)))
                        .as("%s", conditions.get(i))
                        .containsExactlyInAnyOrderElementsOf(expected);
            }
        }
    }

    /**
     * A DELETE on partition keys only drops whole partitions. Spelled from the literals, a
     * FLOAT/DOUBLE partition would be matched by its exact value and {@code d = 0.0} would keep the
     * -0.0 partition. Flink's own evaluation of each condition is the oracle.
     */
    @ParameterizedTest(name = "partitioned by: {0}")
    @CsvSource(
            value = {"d", "d, f"},
            delimiter = ';')
    public void testSignedZeroDeleteOnPartitionAgreesWithFlink(String partitionKeys) {
        // constants are folded through DECIMAL, which has no -0.0, so negate at runtime
        sql("CREATE TABLE ZERO_SRC (d DOUBLE, f FLOAT)");
        batchSql("INSERT INTO ZERO_SRC VALUES (CAST(0 AS DOUBLE), CAST(0 AS FLOAT))");

        // only conditions Paimon converts and that are on partition keys only: otherwise Flink
        // deletes row by row and writes d back as the literal it was compared with, so it cannot
        // find -0.0 by key. f = 0.0 is not converted, Flink compares CAST(f AS DOUBLE).
        List<String> conditions =
                new ArrayList<>(
                        Arrays.asList(
                                "d = 0.0",
                                "d = 0",
                                "d = CAST('-0.0' AS DOUBLE)",
                                "d = CAST('NaN' AS DOUBLE)"));
        if (partitionKeys.contains("f")) {
            conditions.addAll(
                    Arrays.asList(
                            "f = 0",
                            "d = 0.0 AND f = 0",
                            // the planner fails on CAST('NaN' AS FLOAT) next to a comparison
                            "d = CAST('NaN' AS DOUBLE) AND f = 0"));
        }
        for (int i = 0; i < conditions.size(); i++) {
            String table = "DZ" + i;
            sql(
                    "CREATE TABLE %s (id INT, d DOUBLE, f FLOAT, PRIMARY KEY (id, %s) NOT ENFORCED)"
                            + " PARTITIONED BY (%s)",
                    table, partitionKeys, partitionKeys);
            batchSql(
                    "INSERT INTO %s SELECT 1, -d, -f FROM ZERO_SRC"
                            + " UNION ALL SELECT 2, d, f FROM ZERO_SRC"
                            + " UNION ALL SELECT 3, d + 1, f + 1 FROM ZERO_SRC"
                            + " UNION ALL SELECT 4, d / d, f / f FROM ZERO_SRC",
                    table);
            assertThat(batchSql("SELECT id, CAST(d AS STRING), CAST(f AS STRING) FROM %s", table))
                    .containsExactlyInAnyOrder(
                            Row.of(1, "-0.0", "-0.0"),
                            Row.of(2, "0.0", "0.0"),
                            Row.of(3, "1.0", "1.0"),
                            Row.of(4, "NaN", "NaN"));

            String condition = conditions.get(i);
            List<Row> expected =
                    batchSql("SELECT id, %s FROM %s", condition, table).stream()
                            .filter(row -> !Boolean.TRUE.equals(row.getField(1)))
                            .map(row -> Row.of(row.getField(0)))
                            .collect(Collectors.toList());
            batchSql("DELETE FROM %s WHERE %s", table, condition);
            assertThat(batchSql("SELECT id FROM %s", table))
                    .as("%s", condition)
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @Test
    public void testPartitionConditionNotConsuming1() {
        // a = 1 not consumed
        String sql = "SELECT * FROM T where a + 1 = 2 limit 1";
        assertPlanAndResult(
                sql,
                "+- Calc(select=[a, b, c], where=[((a + 1) = 2)])\n"
                        + "+- TableSourceScan(table=[[PAIMON, default, T, filter=[=(+(a, 1), 2)]]], fields=[a, b, c])",
                Row.ofKind(RowKind.INSERT, 1, 1, "1"));
    }

    @Test
    public void testPartitionConditionNotConsuming2() {
        // UNIX_TIMESTAMP() > 0 not consumed
        String sql = "SELECT * FROM T where UNIX_TIMESTAMP() > 0";
        assertPlanAndResult(
                sql,
                "Calc(select=[a, b, c], where=[(UNIX_TIMESTAMP() > 0)])\n"
                        + "+- TableSourceScan(table=[[PAIMON, default, T, filter=[>(UNIX_TIMESTAMP(), 0)]]], fields=[a, b, c])",
                Row.ofKind(RowKind.INSERT, 1, 1, "1"),
                Row.ofKind(RowKind.INSERT, 1, 2, "2"),
                Row.ofKind(RowKind.INSERT, 2, 3, "3"),
                Row.ofKind(RowKind.INSERT, 3, 3, "3"));
    }

    @Test
    public void testPartitionConditionNotConsuming3() {
        // all not consumed
        String sql = "SELECT * FROM T where b = 3 and ( a = 2 or c = '3')";
        assertPlanAndResult(
                sql,
                "Calc(select=[a, CAST(3 AS INTEGER) AS b, c], where=[((b = 3) AND ((a = 2) OR (c = '3')))])\n"
                        + "+- TableSourceScan(table=[[PAIMON, default, T, filter=[and(=(b, 3), OR(=(a, 2), =(c, _UTF-16LE'3':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\")))]]], fields=[a, b, c])",
                Row.ofKind(RowKind.INSERT, 2, 3, "3"),
                Row.ofKind(RowKind.INSERT, 3, 3, "3"));
    }

    @Test
    public void testStreamingReadingNotConsumePartitionCondition() throws TimeoutException {
        String sql = "SELECT * FROM T WHERE a = 5";
        String plan = sEnv.explainSql(sql, ExplainFormat.TEXT);
        Assertions.assertThat(plan)
                .contains(
                        "Calc(select=[CAST(5 AS INTEGER) AS a, b, c], where=[(a = 5)])\n"
                                + "+- TableSourceScan(table=[[PAIMON, default, T, filter=[=(a, 5)]]], fields=[a, b, c])");

        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(sql).collect());
        sql("INSERT INTO T VALUES (5, 5, '5'), (6, 6, '6'), (5, 5, '5_1')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of(5, 5, "5"), Row.of(5, 5, "5_1"));
    }

    @Test
    public void testPartitionCondition_ProjectionPushDown() {
        String sql = "SELECT b, a FROM T where a = 1 limit 1";
        assertPlanAndResult(
                sql,
                "+- Limit(offset=[0], fetch=[1], global=[false])\n"
                        + "+- TableSourceScan(table=[[PAIMON, default, T, filter=[=(a, 1)], project=[b], limit=[1]]], fields=[b])",
                Row.ofKind(RowKind.INSERT, 1, 1));
    }

    private void assertPlanAndResult(String sql, String planIdentifier, Row... expectedRows) {
        String plan = tEnv.explainSql(sql, ExplainFormat.TEXT);
        String[] lines = plan.split("\n");
        String trimmed = Arrays.stream(lines).map(String::trim).collect(Collectors.joining("\n"));
        Assertions.assertThat(trimmed).contains(planIdentifier);
        List<Row> result = batchSql(sql);
        Assertions.assertThat(result).containsExactlyInAnyOrder(expectedRows);
    }
}
