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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Tests for {@link SupportsProjectionPushDown}. */
public class ProjectionPushDownITCase extends CatalogITCaseBase {

    @Override
    public List<String> ddl() {
        return ImmutableList.of(
                "CREATE TABLE T ("
                        + "a INT, b ROW<b0 INT, b1 STRING>, c STRING, d ROW<d0 ROW<d00 STRING, d01 INT>, d1 BOOLEAN, d2 INT>) PARTITIONED BY (a);");
    }

    @BeforeEach
    @Override
    public void before() throws IOException {
        super.before();
        batchSql(
                "INSERT INTO T VALUES "
                        + "(1, ROW(10, 'value1'), '1', ROW(ROW('valued1', 1), true, 10)), "
                        + "(1, ROW(20, 'value2'), '2', ROW(ROW('valued2', 2), false, 20)), "
                        + "(2, ROW(30, 'value3'), '3', ROW(ROW('valued3', 3), true, 30)), "
                        + "(3, ROW(30, 'value3'), '3', ROW(ROW('valued3', 3), false, 30))");
    }

    @Test
    public void testProjectionPushDown() {
        String sql = "SELECT a, c FROM T";
        assertPlanAndResult(
                sql,
                "TableSourceScan(table=[[PAIMON, default, T, project=[a, c]]], fields=[a, c])",
                Row.ofKind(RowKind.INSERT, 1, "1"),
                Row.ofKind(RowKind.INSERT, 1, "2"),
                Row.ofKind(RowKind.INSERT, 2, "3"),
                Row.ofKind(RowKind.INSERT, 3, "3"));
    }

    @Test
    public void testProjectionPushDownWithUnorderedColumns() {
        String sql = "SELECT c, a FROM T";
        assertPlanAndResult(
                sql,
                "TableSourceScan(table=[[PAIMON, default, T, project=[c, a]]], fields=[c, a])",
                Row.ofKind(RowKind.INSERT, "1", 1),
                Row.ofKind(RowKind.INSERT, "2", 1),
                Row.ofKind(RowKind.INSERT, "3", 2),
                Row.ofKind(RowKind.INSERT, "3", 3));
    }

    @Test
    public void testNestedProjectionPushDown() {
        String sql = "SELECT a, b.b1 FROM T";
        assertPlanAndResult(
                sql,
                "TableSourceScan(table=[[PAIMON, default, T, project=[a, b_b1]]], fields=[a, b_b1])",
                Row.ofKind(RowKind.INSERT, 1, "value1"),
                Row.ofKind(RowKind.INSERT, 1, "value2"),
                Row.ofKind(RowKind.INSERT, 2, "value3"),
                Row.ofKind(RowKind.INSERT, 3, "value3"));
    }

    @Test
    public void testNestedProjectionPushDownTripleLevel() {
        String sql = "SELECT a, d.d0.d00 FROM T";
        assertPlanAndResult(
                sql,
                "TableSourceScan(table=[[PAIMON, default, T, project=[a, d_d0_d00]]], fields=[a, d_d0_d00])",
                Row.ofKind(RowKind.INSERT, 1, "valued1"),
                Row.ofKind(RowKind.INSERT, 1, "valued2"),
                Row.ofKind(RowKind.INSERT, 2, "valued3"),
                Row.ofKind(RowKind.INSERT, 3, "valued3"));
    }

    @Test
    public void testNestedProjectionPushDownMultipleFields() {
        String sql = "SELECT a, b.b1, d.d2 FROM T";
        assertPlanAndResult(
                sql,
                "TableSourceScan(table=[[PAIMON, default, T, project=[a, b_b1, d_d2]]], fields=[a, b_b1, d_d2])",
                Row.ofKind(RowKind.INSERT, 1, "value1", 10),
                Row.ofKind(RowKind.INSERT, 1, "value2", 20),
                Row.ofKind(RowKind.INSERT, 2, "value3", 30),
                Row.ofKind(RowKind.INSERT, 3, "value3", 30));
    }

    @Test
    public void testMultipleNestedProjectionPushDownWithUnorderedColumns() {
        String sql = "SELECT c, d.d1, b.b1, a FROM T";
        assertPlanAndResult(
                sql,
                "TableSourceScan(table=[[PAIMON, default, T, project=[c, d_d1, b_b1, a]]], fields=[c, d_d1, b_b1, a])",
                Row.ofKind(RowKind.INSERT, "1", true, "value1", 1),
                Row.ofKind(RowKind.INSERT, "2", false, "value2", 1),
                Row.ofKind(RowKind.INSERT, "3", true, "value3", 2),
                Row.ofKind(RowKind.INSERT, "3", false, "value3", 3));
    }

    @Test
    public void testSystemTableProjectionPushDown() {
        String sql = "SELECT schema_id, primary_keys FROM T$schemas";
        assertPlanAndResult(
                sql,
                "TableSourceScan(table=[[PAIMON, default, T$schemas, project=[schema_id, primary_keys]]], fields=[schema_id, primary_keys])",
                Row.ofKind(RowKind.INSERT, 0L, "[]"));
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
