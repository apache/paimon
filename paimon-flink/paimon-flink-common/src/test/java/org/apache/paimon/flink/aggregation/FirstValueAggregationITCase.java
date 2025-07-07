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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for first_value aggregate function. */
public class FirstValueAggregationITCase extends CatalogITCaseBase {
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
