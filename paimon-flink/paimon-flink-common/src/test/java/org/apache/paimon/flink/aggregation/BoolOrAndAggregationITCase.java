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

/** ITCase for bool_or/bool_and aggregate function. */
public class BoolOrAndAggregationITCase extends CatalogITCaseBase {
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
        batchSql("INSERT INTO T7 VALUES (1, 2, CAST('TRUE' AS  BOOLEAN), CAST('TRUE' AS BOOLEAN))");
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
        batchSql("INSERT INTO T7 VALUES (1, 2, CAST('TRUE' AS  BOOLEAN), CAST('TRUE' AS BOOLEAN))");
        batchSql("INSERT INTO T7 VALUES (1, 2, CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN))");
        batchSql(
                "INSERT INTO T7 VALUES (1, 2, CAST('FALSE' AS BOOLEAN), CAST('FALSE' AS BOOLEAN))");

        // key 1 3
        batchSql(
                "INSERT INTO T7 VALUES (1, 3, CAST('FALSE' AS  BOOLEAN), CAST('TRUE' AS BOOLEAN))");
        batchSql("INSERT INTO T7 VALUES (1, 3, CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN))");
        batchSql("INSERT INTO T7 VALUES (1, 3, CAST('FALSE' AS BOOLEAN), CAST('TRUE' AS BOOLEAN))");

        assertThat(batchSql("SELECT * FROM T7"))
                .containsExactlyInAnyOrder(Row.of(1, 2, true, false), Row.of(1, 3, false, true));
    }

    @Test
    public void testStreamingRead() {
        assertThatThrownBy(
                () -> sEnv.from("T7").execute().print(),
                "Pre-aggregate continuous reading is not supported");
    }
}
