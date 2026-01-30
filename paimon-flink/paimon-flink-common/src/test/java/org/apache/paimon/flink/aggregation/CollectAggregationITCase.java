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
import org.apache.paimon.mergetree.compact.aggregate.FieldCollectAgg;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link FieldCollectAgg}. */
public class CollectAggregationITCase extends CatalogITCaseBase {

    @Override
    protected int defaultParallelism() {
        // set parallelism to 1 so that the order of input data is determined
        return 1;
    }

    @Test
    public void testAggWithDistinct() {
        sql(
                "CREATE TABLE test_collect("
                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                        + "  f0 ARRAY<STRING>"
                        + ") WITH ("
                        + "  'merge-engine' = 'aggregation',"
                        + "  'fields.f0.aggregate-function' = 'collect',"
                        + "  'fields.f0.distinct' = 'true'"
                        + ")");

        sql(
                "INSERT INTO test_collect VALUES "
                        + "(1, CAST (NULL AS ARRAY<STRING>)), "
                        + "(2, ARRAY['A', 'B']), "
                        + "(3, ARRAY['car', 'watch']), "
                        + "(4, ARRAY['A', 'B', 'A'])");

        List<Row> result = queryAndSort("SELECT * FROM test_collect");
        checkOneRecord(result.get(0), 1);
        checkOneRecord(result.get(1), 2, "A", "B");
        checkOneRecord(result.get(2), 3, "car", "watch");
        checkOneRecord(result.get(3), 4, "A", "B");

        sql(
                "INSERT INTO test_collect VALUES "
                        + "(1, ARRAY['paimon', 'paimon']), "
                        + "(2, ARRAY['A', 'B', 'C']), "
                        + "(3, CAST (NULL AS ARRAY<STRING>)), "
                        + "(4, ARRAY['C', 'D', 'C'])");

        result = queryAndSort("SELECT * FROM test_collect");
        checkOneRecord(result.get(0), 1, "paimon");
        checkOneRecord(result.get(1), 2, "A", "B", "C");
        checkOneRecord(result.get(2), 3, "car", "watch");
        checkOneRecord(result.get(3), 4, "A", "B", "C", "D");
    }

    @Test
    public void testAggWithoutDistinct() {
        sql(
                "CREATE TABLE test_collect("
                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                        + "  f0 ARRAY<STRING>"
                        + ") WITH ("
                        + "  'merge-engine' = 'aggregation',"
                        + "  'fields.f0.aggregate-function' = 'collect'"
                        + ")");

        sql(
                "INSERT INTO test_collect VALUES "
                        + "(1, CAST (NULL AS ARRAY<STRING>)), "
                        + "(2, ARRAY['A', 'B', 'B']), "
                        + "(3, ARRAY['car', 'watch'])");

        List<Row> result = queryAndSort("SELECT * FROM test_collect");
        checkOneRecord(result.get(0), 1);
        checkOneRecord(result.get(1), 2, "A", "B", "B");
        checkOneRecord(result.get(2), 3, "car", "watch");

        sql(
                "INSERT INTO test_collect VALUES "
                        + "(1, ARRAY['paimon', 'paimon']), "
                        + "(2, ARRAY['A', 'B', 'C']), "
                        + "(3, CAST (NULL AS ARRAY<STRING>))");

        result = queryAndSort("SELECT * FROM test_collect");
        checkOneRecord(result.get(0), 1, "paimon", "paimon");
        checkOneRecord(result.get(1), 2, "A", "A", "B", "B", "B", "C");
        checkOneRecord(result.get(2), 3, "car", "watch");
    }

    private static List<Arguments> retractArguments() {
        return Arrays.asList(
                Arguments.arguments("lookup", "aggregation"),
                Arguments.arguments("lookup", "partial-update"),
                Arguments.arguments("full-compaction", "aggregation"),
                Arguments.arguments("full-compaction", "partial-update"));
    }

    @ParameterizedTest(name = "changelog-producer = {0}, merge-engine = {1}")
    @MethodSource("retractArguments")
    public void testRetract(String changelogProducer, String mergeEngine) throws Exception {
        String sequenceGroup = "";
        if (mergeEngine.equals("partial-update")) {
            sequenceGroup = ", 'fields.f1.sequence-group' = 'f0'";
        }
        sql(
                "CREATE TABLE test_collect("
                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                        + "  f0 ARRAY<STRING>,"
                        + "  f1 INT"
                        + ") WITH ("
                        + "  'changelog-producer' = '%s',"
                        + "  'merge-engine' = '%s',"
                        + "  'fields.f0.aggregate-function' = 'collect'"
                        + "  %s"
                        + ")",
                changelogProducer, mergeEngine, sequenceGroup);

        BlockingIterator<Row, Row> select = streamSqlBlockIter("SELECT * FROM test_collect");

        String temporaryTableTemplate =
                "CREATE TEMPORARY TABLE %s ("
                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                        + "  f0 ARRAY<STRING>,"
                        + "  f1 INT"
                        + ") WITH ("
                        + "  'connector' = 'values',"
                        + "  'data-id' = '%s',"
                        + "  'bounded' = 'true',"
                        + "  'changelog-mode' = '%s'"
                        + ")";

        // 1 only exists single key record
        sql("INSERT INTO test_collect VALUES (1, ARRAY['A', 'B'], 1)");
        List<Row> result = select.collect(1);
        checkOneRecord(result.get(0), 1, "A", "B");

        // 1.1 UB + UA (CANNOT handle)
        List<Row> inputRecords =
                Arrays.asList(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, new String[] {"A", "B"}, 2),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, new String[] {"C", "D"}, 3));
        sEnv.executeSql(
                        String.format(
                                temporaryTableTemplate,
                                "INPUT11",
                                TestValuesTableFactory.registerData(inputRecords),
                                "UB,UA"))
                .await();
        sEnv.executeSql("INSERT INTO test_collect SELECT * FROM INPUT11").await();

        result = select.collect(2);
        assertThat(result.get(0).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
        checkOneRecord(result.get(0), 1, "A", "B");
        assertThat(result.get(1).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
        checkOneRecord(result.get(1), 1, "A", "B", "C", "D");

        // 1.2 -D
        inputRecords =
                Collections.singletonList(
                        Row.ofKind(RowKind.DELETE, 1, new String[] {"C", "D"}, 4));
        sEnv.executeSql(
                        String.format(
                                temporaryTableTemplate,
                                "INPUT12",
                                TestValuesTableFactory.registerData(inputRecords),
                                "D"))
                .await();
        sEnv.executeSql("INSERT INTO test_collect SELECT * FROM INPUT12").await();

        result = select.collect(2);
        assertThat(result.get(0).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
        checkOneRecord(result.get(0), 1, "A", "B", "C", "D");
        assertThat(result.get(1).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
        checkOneRecord(result.get(1), 1, "A", "B");

        // 2 exists multiple key records
        sql("INSERT INTO test_collect VALUES (2, ARRAY['A', 'B'], 5), (3, ARRAY['A', 'B'], 6)");
        result = select.collect(2);
        checkOneRecord(result.get(0), 2, "A", "B");
        checkOneRecord(result.get(1), 3, "A", "B");

        // 2.1 UB + UA (CANNOT handle)
        inputRecords =
                Arrays.asList(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 2, new String[] {"A", "B"}, 7),
                        Row.ofKind(RowKind.UPDATE_AFTER, 2, new String[] {"C", "D"}, 8));
        sEnv.executeSql(
                        String.format(
                                temporaryTableTemplate,
                                "INPUT21",
                                TestValuesTableFactory.registerData(inputRecords),
                                "UB,UA"))
                .await();
        sEnv.executeSql("INSERT INTO test_collect SELECT * FROM INPUT21").await();

        result = select.collect(2);
        assertThat(result.get(0).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
        checkOneRecord(result.get(0), 2, "A", "B");
        assertThat(result.get(1).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
        checkOneRecord(result.get(1), 2, "A", "B", "C", "D");

        // 2.2 -D
        inputRecords =
                Collections.singletonList(Row.ofKind(RowKind.DELETE, 3, new String[] {"A"}, 9));
        sEnv.executeSql(
                        String.format(
                                temporaryTableTemplate,
                                "INPUT22",
                                TestValuesTableFactory.registerData(inputRecords),
                                "D"))
                .await();
        sEnv.executeSql("INSERT INTO test_collect SELECT * FROM INPUT22").await();

        result = select.collect(2);
        assertThat(result.get(0).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
        checkOneRecord(result.get(0), 3, "A", "B");
        assertThat(result.get(1).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
        checkOneRecord(result.get(1), 3, "B");

        select.close();
    }

    @Test
    public void testRetractInputNull() throws Exception {
        sql(
                "CREATE TABLE test_collect ("
                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                        + "  f0 ARRAY<STRING>,"
                        + "  f1 INT"
                        + ") WITH ("
                        + "  'changelog-producer' = 'lookup',"
                        + "  'merge-engine' = 'partial-update',"
                        + "  'fields.f0.aggregate-function' = 'collect',"
                        + "  'fields.f1.sequence-group' = 'f0'"
                        + ")");

        List<Row> input =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, null, 1),
                        Row.ofKind(RowKind.INSERT, 1, new String[] {"A"}, 2),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, null, 1),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, new String[] {"B"}, 3));
        sEnv.executeSql(
                        String.format(
                                "CREATE TEMPORARY TABLE input ("
                                        + "  id INT PRIMARY KEY NOT ENFORCED,"
                                        + "  f0 ARRAY<STRING>,"
                                        + "  f1 INT"
                                        + ") WITH ("
                                        + "  'connector' = 'values',"
                                        + "  'data-id' = '%s',"
                                        + "  'bounded' = 'true',"
                                        + "  'changelog-mode' = 'UB,UA'"
                                        + ")",
                                TestValuesTableFactory.registerData(input)))
                .await();
        sEnv.executeSql("INSERT INTO test_collect SELECT * FROM input").await();

        assertThat(sql("SELECT * FROM test_collect"))
                .containsExactly(Row.of(1, new String[] {"A", "B"}, 3));
    }

    private void checkOneRecord(Row row, int id, String... elements) {
        assertThat(row.getField(0)).isEqualTo(id);
        if (elements == null || elements.length == 0) {
            assertThat(row.getField(1)).isNull();
        } else {
            assertThat((String[]) row.getField(1)).containsExactlyInAnyOrder(elements);
        }
    }
}
