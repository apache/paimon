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
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT Test for aggregation merge engine. */
public class BasicAggregateITCase extends CatalogITCaseBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMergeTopLevel(boolean partialUpdate) {
        sql(
                "CREATE TABLE json_merge (id INT PRIMARY KEY NOT ENFORCED, `json` STRING, seq BIGINT) WITH ("
                        + "'merge-engine'='"
                        + (partialUpdate ? "partial-update" : "aggregation")
                        + "', 'fields.json.aggregate-function'='merge_top_level', "
                        + (partialUpdate
                                ? "'fields.seq.sequence-group'='json'"
                                : "'sequence.field'='seq'")
                        + ")");
        sql(
                "INSERT INTO json_merge VALUES "
                        + "(1, '{\"object\":{\"new\":2},\"array\":[3],\"nil\":null}', 2), "
                        + "(1, '{\"keep\":1,\"object\":{\"old\":1},\"array\":[1,2],\"nil\":1}', 1)");
        assertThat(batchSql("SELECT `json` FROM json_merge"))
                .extracting(row -> JsonSerdeUtil.fromJson(row.getField(0).toString(), Map.class))
                .containsExactly(
                        JsonSerdeUtil.fromJson(
                                "{\"keep\":1,\"object\":{\"new\":2},\"array\":[3],\"nil\":null}",
                                Map.class));
        sql(
                "INSERT INTO json_merge VALUES "
                        + "(1, '{\"object\":\"stale\",\"nil\":\"stale\",\"missing\":7}', 0)");
        assertThat(batchSql("SELECT `json` FROM json_merge"))
                .extracting(row -> JsonSerdeUtil.fromJson(row.getField(0).toString(), Map.class))
                .containsExactly(
                        JsonSerdeUtil.fromJson(
                                "{\"keep\":1,\"object\":{\"new\":2},\"array\":[3],\"nil\":null,\"missing\":7}",
                                Map.class));
        assertThatThrownBy(() -> sql("INSERT INTO json_merge VALUES (2, '[]', 4)"))
                .hasStackTraceContaining("merge_top_level requires valid JSON Object strings");
    }

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

    @Test
    public void testSequenceFieldWithDefaultAgg() {
        sql(
                "CREATE TABLE seq_default_agg ("
                        + " pk INT PRIMARY KEY NOT ENFORCED,"
                        + " seq INT,"
                        + " v INT) WITH ("
                        + " 'merge-engine'='aggregation',"
                        + " 'sequence.field'='seq',"
                        + " 'fields.default-aggregate-function'='sum'"
                        + ")");

        sql("INSERT INTO seq_default_agg VALUES (0, 1, 1)");
        sql("INSERT INTO seq_default_agg VALUES (0, 2, 2)");

        assertThat(sql("SELECT * FROM seq_default_agg")).containsExactly(Row.of(0, 2, 3));
    }
}
