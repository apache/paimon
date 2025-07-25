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

import java.io.IOException;
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
