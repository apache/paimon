/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for partition push down. */
public class PartitionPushDownITCase extends CatalogITCaseBase {

    @Test
    public void testPartitionPushDown() {
        sql(
                "CREATE TABLE IF NOT EXISTS T ("
                        + "a INT, b INT, c STRING, PRIMARY KEY (a, b) NOT ENFORCED) PARTITIONED BY (a) "
                        + " WITH ('merge-engine'='deduplicate', 'format' = 'orc');");
        batchSql("INSERT INTO T VALUES (1, 1, '1'), (1, 2, '2'), (1, 3, '3')");
        String sql = "SELECT * FROM T where a = 1 limit 1";
        String plan = tEnv.explainSql(sql, ExplainFormat.TEXT);
        Assertions.assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[PAIMON, default, T, partitions=[{a=1}], project=[b, c], limit=[1]]], fields=[b, c])");
        List<Row> result = batchSql(sql);
        assertThat(result).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, 1, 1, "1"));
    }

    @Test
    public void testDateTypePartitionNotPushedDown() {
        sql(
                "CREATE TABLE IF NOT EXISTS T ("
                        + "a INT, b DATE, c STRING, d STRING,  PRIMARY KEY (a, b, c) NOT ENFORCED) PARTITIONED BY (a, b) "
                        + " WITH ('merge-engine'='deduplicate', 'format' = 'parquet');");
        batchSql(
                "INSERT INTO T VALUES (1, DATE '2023-10-13', '1', '1'), "
                        + "(1, DATE '2023-10-14', '2', '2'), (2, DATE '2023-10-14', '3', '3'),"
                        + " (3, DATE '2023-10-14', '4', '4')");
        String sql = "SELECT * FROM T where b = DATE '2023-10-14' limit 2";
        List<Row> result = batchSql(sql);
        String plan = tEnv.explainSql(sql, ExplainFormat.TEXT);
        Assertions.assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[PAIMON, default, T, filter=[=(b, 2023-10-14)]]], fields=[a, b, c, d])");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 2, LocalDate.of(2023, 10, 14), "3", "3"),
                        Row.ofKind(RowKind.INSERT, 1, LocalDate.of(2023, 10, 14), "2", "2"));
    }

    @Test
    public void testPartialPartitionConditionAndMultiMatchPushDown() {
        sql(
                "CREATE TABLE IF NOT EXISTS T ("
                        + "a INT, b INT, c STRING, PRIMARY KEY (a, b, c) NOT ENFORCED) PARTITIONED BY (a, c) "
                        + " WITH ('merge-engine'='deduplicate', 'format' = 'parquet');");
        batchSql("INSERT INTO T VALUES (1, 1, '1'), (1, 2, '2'), (1, 2, '22'), (1, 3, '2')");
        String sql = "SELECT * FROM T where c like '%%2%%' and b = 2";
        List<Row> result = batchSql(sql);
        String plan = tEnv.explainSql(sql, ExplainFormat.TEXT);
        Assertions.assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[PAIMON, default, T, partitions=[{a=1, c=22}, {a=1, c=2}], filter=[=(b, 2)]]], fields=[a, b, c])");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, 2, "22"),
                        Row.ofKind(RowKind.INSERT, 1, 2, "2"));
    }
}
