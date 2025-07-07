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

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for last value aggregate function. */
public class LastValueAggregationITCase extends CatalogITCaseBase {
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
