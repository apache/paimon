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

package org.apache.paimon.flink;

import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.createTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.sEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.validateStreamingReadResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for updating with partial columns in batch mode. */
public class BatchUpdateWithPartialColumnsITCase extends AbstractTestBase {

    @BeforeEach
    public void setUp() {
        init(getTempDirPath());
    }

    private static String[] getMergeEngines() {
        return new String[] {"partial-update", "aggregation"};
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testBoolOrAndAggregation(String mergeEngine) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a BOOLEAN",
                                "b BOOLEAN",
                                "ut1 TIMESTAMP",
                                "ut2 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "a",
                                "fields.a.aggregate-function",
                                "bool_or",
                                "fields.ut2.sequence-group",
                                "b",
                                "fields.b.aggregate-function",
                                "bool_and"));
        insertInto(
                table,
                "(1, CAST('TRUE' AS BOOLEAN), CAST('TRUE' AS BOOLEAN), CAST('2022-01-01 10:10:10' AS TIMESTAMP), CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, CAST('FALSE' AS BOOLEAN), CAST('FALSE' AS BOOLEAN), CAST('2022-01-02 20:20:20' AS TIMESTAMP), CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");

        String updateStatement1 =
                String.format(
                        "UPDATE %s SET b = CAST('FALSE' AS BOOLEAN) WHERE id = 1 and dt = '2022-01-01'",
                        table);
        String updateStatement2 =
                String.format(
                        "UPDATE %s SET a = CAST('TRUE' AS BOOLEAN) WHERE id = 2 and dt = '2022-01-02'",
                        table);
        testExplainSqlMatchPartialColumns(
                updateStatement1,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "b", "ut2", "dt")
                        : Arrays.asList("id", "b", "dt"));
        testExplainSqlMatchPartialColumns(
                updateStatement2,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "a", "ut1", "dt")
                        : Arrays.asList("id", "a", "dt"));

        executeUpdates(updateStatement1, updateStatement2);
        testBatchRead(
                String.format("SELECT * FROM %s", table),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow(
                                "+I",
                                1L,
                                true,
                                false,
                                LocalDateTime.parse("2022-01-01T10:10:10"),
                                LocalDateTime.parse("2022-01-01T10:10:10"),
                                "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow(
                                "+I",
                                2L,
                                true,
                                false,
                                LocalDateTime.parse("2022-01-02T20:20:20"),
                                LocalDateTime.parse("2022-01-02T20:20:20"),
                                "2022-01-02")));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testListAggAggregation(String mergeEngine) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a STRING",
                                "b INT",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "a",
                                "fields.a.aggregate-function",
                                "listagg"));
        insertInto(
                table,
                "(1, 'first line', 1, CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, 'first line', 2, CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");
        String updateStatement =
                String.format(
                        "UPDATE %s SET a = 'second line' WHERE id = 1 and dt = '2022-01-01'",
                        table);
        testExplainSqlMatchPartialColumns(
                updateStatement,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "a", "ut1", "dt")
                        : Arrays.asList("id", "a", "dt"));
        updateAndSelect(
                updateStatement,
                String.format("SELECT * FROM %s", table),
                // part = 2022-01-01
                changelogRow(
                        "+I",
                        1L,
                        "first line,second line",
                        1,
                        LocalDateTime.parse("2022-01-01T10:10:10"),
                        "2022-01-01"),
                // part = 2022-01-02
                changelogRow(
                        "+I",
                        2L,
                        "first line",
                        2,
                        LocalDateTime.parse("2022-01-02T20:20:20"),
                        "2022-01-02"));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testLastValueAndNonValueAggregation(String mergeEngine) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a STRING",
                                "b INT",
                                "c INT",
                                "aa STRING",
                                "bb INT",
                                "cc INT",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "a,aa",
                                "fields.a.aggregate-function",
                                "last_value",
                                "fields.c.aggregate-function",
                                "last_value",
                                "fields.aa.aggregate-function",
                                "last_non_null_value",
                                "fields.cc.aggregate-function",
                                "last_non_null_value"));
        insertInto(
                table,
                "(1, 'a', 1, 11, 'aa', 111, 1111, CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, 'b', 2, 22, 'bb', 222, 2222, CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");

        String updateStatement1 =
                String.format("UPDATE %s SET a = NULL WHERE id = 1 and dt = '2022-01-01'", table);
        String updateStatement2 =
                String.format("UPDATE %s SET c = 222 WHERE id = 2 and dt = '2022-01-02'", table);
        String updateStatement3 =
                String.format("UPDATE %s SET aa = NULL WHERE id = 1 and dt = '2022-01-01'", table);
        String updateStatement4 =
                String.format("UPDATE %s SET cc = 22222 WHERE id = 2 and dt = '2022-01-02'", table);
        String updateStatement5 =
                String.format(
                        "UPDATE %s SET cc = CAST(NULL AS INT) WHERE id = 2 and dt = '2022-01-02'",
                        table);
        testExplainSqlMatchPartialColumns(
                updateStatement1,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "a", "c", "ut1", "dt")
                        : Arrays.asList("id", "a", "c", "dt"));
        testExplainSqlMatchPartialColumns(
                updateStatement2, table, Arrays.asList("id", "a", "c", "dt"));
        testExplainSqlMatchPartialColumns(
                updateStatement3,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "a", "c", "aa", "ut1", "dt")
                        : Arrays.asList("id", "a", "c", "aa", "dt"));
        testExplainSqlMatchPartialColumns(
                updateStatement4, table, Arrays.asList("id", "a", "c", "cc", "dt"));
        testExplainSqlMatchPartialColumns(
                updateStatement5, table, Arrays.asList("id", "a", "c", "cc", "dt"));

        executeUpdates(
                updateStatement1,
                updateStatement2,
                updateStatement3,
                updateStatement4,
                updateStatement5);
        testBatchRead(
                String.format("SELECT * FROM %s", table),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow(
                                "+I",
                                1L,
                                null,
                                1,
                                11,
                                "aa",
                                111,
                                1111,
                                LocalDateTime.parse("2022-01-01T10:10:10"),
                                "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow(
                                "+I",
                                2L,
                                "b",
                                2,
                                222,
                                "bb",
                                222,
                                22222,
                                LocalDateTime.parse("2022-01-02T20:20:20"),
                                "2022-01-02")));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testMinAggregation(String mergeEngine) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a INT",
                                "b BIGINT",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "a",
                                "fields.a.aggregate-function",
                                "min"));
        insertInto(
                table,
                "(1, 1, 1, CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, 2, 2, CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");
        String updateStatement =
                String.format("UPDATE %s SET a = -1 WHERE id = 1 and dt = '2022-01-01'", table);
        testExplainSqlMatchPartialColumns(
                updateStatement,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "a", "ut1", "dt")
                        : Arrays.asList("id", "a", "dt"));
        updateAndSelect(
                updateStatement,
                String.format("SELECT * FROM %s", table),
                // part = 2022-01-01
                changelogRow(
                        "+I", 1L, -1, 1L, LocalDateTime.parse("2022-01-01T10:10:10"), "2022-01-01"),
                // part = 2022-01-02
                changelogRow(
                        "+I", 2L, 2, 2L, LocalDateTime.parse("2022-01-02T20:20:20"), "2022-01-02"));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testMaxAggregation(String mergeEngine) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a INT",
                                "b BIGINT",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "b",
                                "fields.a.aggregate-function",
                                "max"));
        insertInto(
                table,
                "(1, 1, 1, CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, 2, 2, CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");
        String updateStatement =
                String.format(
                        "UPDATE %s SET b = CAST(22 AS BIGINT) WHERE id = 2 and dt = '2022-01-02'",
                        table);
        testExplainSqlMatchPartialColumns(
                updateStatement,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "b", "ut1", "dt")
                        : Arrays.asList("id", "b", "dt"));
        updateAndSelect(
                updateStatement,
                String.format("SELECT * FROM %s", table),
                // part = 2022-01-01
                changelogRow(
                        "+I", 1L, 1, 1L, LocalDateTime.parse("2022-01-01T10:10:10"), "2022-01-01"),
                // part = 2022-01-02
                changelogRow(
                        "+I",
                        2L,
                        2,
                        22L,
                        LocalDateTime.parse("2022-01-02T20:20:20"),
                        "2022-01-02"));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testSumAggregation(String mergeEngine) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a INT",
                                "b BIGINT",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "a",
                                "fields.a.aggregate-function",
                                "sum"));
        insertInto(
                table,
                "(1, 1, 1, CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, 2, 2, CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");
        String updateStatement =
                String.format("UPDATE %s SET a = 1 WHERE id = 1 and dt = '2022-01-01'", table);
        testExplainSqlMatchPartialColumns(
                updateStatement,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "a", "ut1", "dt")
                        : Arrays.asList("id", "a", "dt"));
        updateAndSelect(
                updateStatement,
                String.format("SELECT * FROM %s", table),
                // part = 2022-01-01
                changelogRow(
                        "+I", 1L, 2, 1L, LocalDateTime.parse("2022-01-01T10:10:10"), "2022-01-01"),
                // part = 2022-01-02
                changelogRow(
                        "+I", 2L, 2, 2L, LocalDateTime.parse("2022-01-02T20:20:20"), "2022-01-02"));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testProductAggregation(String mergeEngine) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a INT",
                                "b BIGINT",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "b",
                                "fields.b.aggregate-function",
                                "product"));
        insertInto(
                table,
                "(1, 1, 1, CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, 2, 2, CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");
        String updateStatement =
                String.format(
                        "UPDATE %s SET b = CAST(2 AS BIGINT) WHERE id = 2 and dt = '2022-01-02'",
                        table);
        testExplainSqlMatchPartialColumns(
                updateStatement,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "b", "ut1", "dt")
                        : Arrays.asList("id", "b", "dt"));
        updateAndSelect(
                updateStatement,
                String.format("SELECT * FROM %s", table),
                // part = 2022-01-01
                changelogRow(
                        "+I", 1L, 1, 1L, LocalDateTime.parse("2022-01-01T10:10:10"), "2022-01-01"),
                // part = 2022-01-02
                changelogRow(
                        "+I", 2L, 2, 4L, LocalDateTime.parse("2022-01-02T20:20:20"), "2022-01-02"));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testFirstValueAndNonValueAggregation(String mergeEngine) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a STRING",
                                "b INT",
                                "c INT",
                                "aa STRING",
                                "bb INT",
                                "cc INT",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "a,aa",
                                "fields.a.aggregate-function",
                                "first_value",
                                "fields.c.aggregate-function",
                                "first_value",
                                "fields.aa.aggregate-function",
                                "first_non_null_value",
                                "fields.cc.aggregate-function",
                                "first_non_null_value"));
        insertInto(
                table,
                "(1, 'a', CAST(NULL AS INT), CAST(NULL AS INT), CAST(NULL AS STRING), CAST(NULL AS INT), 1111, CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, CAST(NULL AS STRING), CAST(NULL AS INT), 22, 'bb', CAST(NULL AS INT), CAST(NULL AS INT), CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");

        String updateStatement1 =
                String.format(
                        "UPDATE %s SET a = NULL, b = 1 WHERE id = 1 and dt = '2022-01-01'", table);
        String updateStatement2 =
                String.format("UPDATE %s SET c = 222 WHERE id = 2 and dt = '2022-01-02'", table);
        String updateStatement3 =
                String.format(
                        "UPDATE %s SET aa = NULL, bb = 111 WHERE id = 1 and dt = '2022-01-01'",
                        table);
        String updateStatement4 =
                String.format("UPDATE %s SET aa = 'aaa' WHERE id = 1 and dt = '2022-01-01'", table);
        String updateStatement5 =
                String.format("UPDATE %s SET cc = 2222 WHERE id = 2 and dt = '2022-01-02'", table);
        String updateStatement6 =
                String.format(
                        "UPDATE %s SET cc = CAST(NULL AS INT), bb = 222 WHERE id = 2 and dt = '2022-01-02'",
                        table);
        testExplainSqlMatchPartialColumns(
                updateStatement1,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "a", "b", "ut1", "dt")
                        : Arrays.asList("id", "a", "b", "dt"));
        testExplainSqlMatchPartialColumns(updateStatement2, table, Arrays.asList("id", "c", "dt"));
        testExplainSqlMatchPartialColumns(
                updateStatement3,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "aa", "bb", "ut1", "dt")
                        : Arrays.asList("id", "aa", "bb", "dt"));
        testExplainSqlMatchPartialColumns(
                updateStatement4,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "aa", "ut1", "dt")
                        : Arrays.asList("id", "aa", "dt"));
        testExplainSqlMatchPartialColumns(updateStatement5, table, Arrays.asList("id", "cc", "dt"));
        testExplainSqlMatchPartialColumns(
                updateStatement6, table, Arrays.asList("id", "bb", "cc", "dt"));

        executeUpdates(
                updateStatement1,
                updateStatement2,
                updateStatement3,
                updateStatement4,
                updateStatement5,
                updateStatement6);
        testBatchRead(
                String.format("SELECT * FROM %s", table),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow(
                                "+I",
                                1L,
                                "a",
                                1,
                                null,
                                "aaa",
                                111,
                                1111,
                                LocalDateTime.parse("2022-01-01T10:10:10"),
                                "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow(
                                "+I",
                                2L,
                                null,
                                null,
                                22,
                                "bb",
                                222,
                                2222,
                                LocalDateTime.parse("2022-01-02T20:20:20"),
                                "2022-01-02")));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testNestedUpdateAggregation(String mergeEngine) {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a STRING",
                                "b INT",
                                "sub_row ARRAY<ROW<d INT NOT NULL, e STRING, f DOUBLE>>",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "sub_row",
                                "fields.sub_row.aggregate-function",
                                "nested_update",
                                "fields.sub_row.nested-key",
                                "d,e"));
        String updateStatement1 =
                String.format(
                        "UPDATE %s SET sub_row = ARRAY[ROW(40, CAST('row40' AS STRING), CAST(40.4 AS DOUBLE))] WHERE id = 2 AND dt = '2022-01-02'",
                        table);
        String updateStatement2 =
                String.format(
                        "UPDATE %s SET sub_row = CAST(ARRAY[ROW(40, CAST('row40' AS STRING), CAST(40.4 AS DOUBLE))] AS ARRAY<ROW<d INT NOT NULL, e STRING, f DOUBLE>>) WHERE id = 2 AND dt = '2022-01-02'",
                        table);
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> executeUpdates(updateStatement1));
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> executeUpdates(updateStatement2));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testCollectAggregation(String mergeEngine) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a STRING",
                                "b INT",
                                "c ARRAY<STRING>",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "c",
                                "fields.c.aggregate-function",
                                "collect"));
        insertInto(
                table,
                "(1, 'a', 1, ARRAY['A', 'B'], CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, 'b', 2, ARRAY['X', 'Y'], CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");

        String updateStatement =
                String.format(
                        "UPDATE %s SET c = ARRAY['A', 'D'] WHERE id = 1 and dt = '2022-01-01'",
                        table);
        testExplainSqlMatchPartialColumns(
                updateStatement,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "c", "ut1", "dt")
                        : Arrays.asList("id", "c", "dt"));
        executeUpdates(
                updateStatement,
                String.format(
                        "UPDATE %s SET c = CAST(NULL AS ARRAY<STRING>) WHERE id = 2 and dt = '2022-01-02'",
                        table));

        testBatchRead(
                String.format("SELECT * FROM %s", table),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow(
                                "+I",
                                1L,
                                "a",
                                1,
                                new String[] {"A", "B", "A", "D"},
                                LocalDateTime.parse("2022-01-01T10:10:10"),
                                "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow(
                                "+I",
                                2L,
                                "b",
                                2,
                                new String[] {"X", "Y"},
                                LocalDateTime.parse("2022-01-02T20:20:20"),
                                "2022-01-02")));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testMergeMapAggregation(String mergeEngine) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a STRING",
                                "b INT",
                                "c MAP<INT, STRING>",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "c",
                                "fields.c.aggregate-function",
                                "merge_map"));
        insertInto(
                table,
                "(1, 'a', 1, MAP[1, 'A'], CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, 'b', 2, MAP[2, 'B'], CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");
        String updateStatement1 =
                String.format(
                        "UPDATE %s SET c = MAP[2, CAST('BB' AS STRING)] WHERE id = 2 and dt = '2022-01-02'",
                        table);
        String updateStatement2 =
                String.format(
                        "UPDATE %s SET c = CAST(MAP[2, CAST('BB' AS STRING)] AS MAP<INT, STRING>) WHERE id = 2 and dt = '2022-01-02'",
                        table);
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> executeUpdates(updateStatement1));
        testExplainSqlMatchPartialColumns(
                updateStatement2,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "c", "ut1", "dt")
                        : Arrays.asList("id", "c", "dt"));
        executeUpdates(updateStatement2);
        testBatchRead(
                String.format("SELECT * FROM %s", table),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow(
                                "+I",
                                1L,
                                "a",
                                1,
                                new HashMap<Integer, String>() {
                                    {
                                        put(1, "A");
                                    }
                                },
                                LocalDateTime.parse("2022-01-01T10:10:10"),
                                "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow(
                                "+I",
                                2L,
                                "b",
                                2,
                                new HashMap<Integer, String>() {
                                    {
                                        put(2, "BB");
                                    }
                                },
                                LocalDateTime.parse("2022-01-02T20:20:20"),
                                "2022-01-02")));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngines")
    public void testUpdateWithPrimaryKeyPartialColumnsTypeMismatch(String mergeEngine)
            throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a STRING",
                                "b INT",
                                "c DOUBLE",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                "fields.ut1.sequence-group",
                                "c",
                                "fields.c.aggregate-function",
                                "sum"));
        String updateStatement =
                String.format("UPDATE %s SET c = 6.6 WHERE id = 1 AND dt = '2022-01-01'", table);
        assertThatThrownBy(() -> executeUpdates(updateStatement))
                .hasMessageStartingWith("The data types of the source and sink are inconsistent.");
    }

    private static Stream<Arguments> getMergeEngineWithChangelogProducers() {
        List<String> mergeEngines = Arrays.asList("partial-update", "aggregation");
        List<String> changelogProducers = Arrays.asList("lookup", "input");

        return mergeEngines.stream()
                .flatMap(
                        mergeEngine ->
                                changelogProducers.stream()
                                        .map(
                                                changelogProducer ->
                                                        Arguments.of(
                                                                mergeEngine, changelogProducer)));
    }

    @ParameterizedTest
    @MethodSource("getMergeEngineWithChangelogProducers")
    public void testUpdateWithChangelog(String mergeEngine, String producer) throws Exception {
        String table =
                createTable(
                        Arrays.asList(
                                "id BIGINT NOT NULL",
                                "a STRING",
                                "b INT",
                                "ut1 TIMESTAMP",
                                "dt STRING"),
                        Arrays.asList("id", "dt"),
                        Collections.singletonList("dt"),
                        setupOptions(
                                MERGE_ENGINE.key(),
                                mergeEngine,
                                CHANGELOG_PRODUCER.key(),
                                producer,
                                "fields.ut1.sequence-group",
                                "b",
                                "fields.b.aggregate-function",
                                "sum"));
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        sEnv.executeSql(String.format("SELECT * FROM %s", table)).collect());

        insertInto(
                table,
                "(1, 'a', 1, CAST('2022-01-01 10:10:10' AS TIMESTAMP), '2022-01-01')",
                "(2, 'b', 2, CAST('2022-01-02 20:20:20' AS TIMESTAMP), '2022-01-02')");
        validateStreamingReadResult(
                iterator,
                Arrays.asList(
                        changelogRow(
                                "+I",
                                1L,
                                "a",
                                1,
                                LocalDateTime.parse("2022-01-01T10:10:10"),
                                "2022-01-01"),
                        changelogRow(
                                "+I",
                                2L,
                                "b",
                                2,
                                LocalDateTime.parse("2022-01-02T20:20:20"),
                                "2022-01-02")));

        String updateStatement =
                String.format("UPDATE %s SET b = 10 WHERE id = 1 and dt = '2022-01-01'", table);
        testExplainSqlMatchPartialColumns(
                updateStatement,
                table,
                "partial-update".equals(mergeEngine)
                        ? Arrays.asList("id", "b", "ut1", "dt")
                        : Arrays.asList("id", "b", "dt"));
        executeUpdates(updateStatement);

        if ("input".equals(producer)) {
            validateStreamingReadResult(
                    iterator,
                    Collections.singletonList(
                            changelogRow(
                                    "+U",
                                    1L,
                                    null,
                                    10,
                                    "partial-update".equals(mergeEngine)
                                            ? LocalDateTime.parse("2022-01-01T10:10:10")
                                            : null,
                                    "2022-01-01")));
        } else {
            validateStreamingReadResult(
                    iterator,
                    Arrays.asList(
                            changelogRow(
                                    "-U",
                                    1L,
                                    "a",
                                    1,
                                    LocalDateTime.parse("2022-01-01T10:10:10"),
                                    "2022-01-01"),
                            changelogRow(
                                    "+U",
                                    1L,
                                    "a",
                                    11,
                                    LocalDateTime.parse("2022-01-01T10:10:10"),
                                    "2022-01-01")));
        }

        iterator.close();
    }

    private void updateAndSelect(String updateStatement, String querySql, Row... rows)
            throws Exception {
        bEnv.executeSql(updateStatement).await();
        testBatchRead(querySql, Arrays.asList(rows));
    }

    private void executeUpdates(String... sqls) throws Exception {
        for (String sql : sqls) {
            bEnv.executeSql(sql).await();
        }
    }

    private Map<String, String> setupOptions(String... options) {
        assertThat(options.length).isEven();
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < options.length; i += 2) {
            map.put(options[i], options[i + 1]);
        }

        return map;
    }

    /** Compare the expected required columns with the sink fields in the explain output. */
    private void testExplainSqlMatchPartialColumns(
            String updateStatement, String table, List<String> columnNames) {
        Pattern pattern =
                Pattern.compile(
                        String.format("Sink\\(table=\\[PAIMON\\.default\\.%s.*?\\]\\)", table));
        Matcher matcher =
                pattern.matcher(
                        bEnv.explainSql(updateStatement, ExplainDetail.JSON_EXECUTION_PLAN));
        assertThat(matcher.find()).isTrue();
        String explainSelect = matcher.group();
        assertThat(explainSelect).isNotEmpty();

        String[] fields = explainSelect.split("fields=")[1].split(", ");
        assertThat(fields).hasSize(columnNames.size());
        IntStream.range(0, fields.length)
                .forEach(i -> assertThat(fields[i]).contains(columnNames.get(i)));
    }
}
