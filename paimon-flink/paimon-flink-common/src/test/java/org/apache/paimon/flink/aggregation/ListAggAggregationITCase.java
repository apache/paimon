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

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for listagg aggregate function. */
public class ListAggAggregationITCase extends CatalogITCaseBase {
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
