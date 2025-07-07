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
import org.apache.paimon.mergetree.compact.aggregate.FieldMergeMapAgg;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link FieldMergeMapAgg}. */
public class MergeMapAggregationITCase extends CatalogITCaseBase {

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

    @Test
    public void testRetractInputNull() throws Exception {
        sql(
                "CREATE TABLE test_merge_map1 ("
                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                        + "  f0 MAP<INT, STRING>,"
                        + "  f1 INT"
                        + ") WITH ("
                        + "  'changelog-producer' = 'lookup',"
                        + "  'merge-engine' = 'partial-update',"
                        + "  'fields.f0.aggregate-function' = 'merge_map',"
                        + "  'fields.f1.sequence-group' = 'f0'"
                        + ")");

        List<Row> input =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, null, 1),
                        Row.ofKind(RowKind.INSERT, 1, Collections.singletonMap(1, "A"), 2),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, null, 1),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, Collections.singletonMap(2, "B"), 3));
        sEnv.executeSql(
                        String.format(
                                "CREATE TEMPORARY TABLE input ("
                                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                                        + "  f0 MAP<INT, STRING>,"
                                        + "  f1 INT"
                                        + ") WITH ("
                                        + "  'connector' = 'values',"
                                        + "  'data-id' = '%s',"
                                        + "  'bounded' = 'true',"
                                        + "  'changelog-mode' = 'UB,UA'"
                                        + ")",
                                TestValuesTableFactory.registerData(input)))
                .await();
        sEnv.executeSql("INSERT INTO test_merge_map1 SELECT * FROM input").await();

        assertThat(sql("SELECT * FROM test_merge_map1"))
                .containsExactly(Row.of(1, toMap(1, "A", 2, "B"), 3));
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
