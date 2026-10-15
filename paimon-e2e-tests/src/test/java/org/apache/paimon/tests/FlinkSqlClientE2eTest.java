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

package org.apache.paimon.tests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests SQL Client errors and query results end to end. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlinkSqlClientE2eTest extends E2eTestBase {

    @BeforeAll
    void startEnvironment() throws Exception {
        super.before();
    }

    @AfterAll
    void stopEnvironment() {
        super.after();
    }

    // Each SQL statement runs in a fresh SQL Client session, so the tests can share one cluster.
    @Override
    public void before() {}

    @Override
    public void after() {}

    @Test
    void failsOnSqlClientStatementError() {
        assertThatThrownBy(() -> runBatchSql("SELECT * FROM table_that_does_not_exist;"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Could not execute SQL statement")
                .hasMessageContaining("table_that_does_not_exist");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                ", CHR(10), '+----+-------------+', CHR(10), '1 row in set', CHR(10), CHR(10), 'suffix'"
            })
    void failsOnStreamingQueryRuntimeError(String errorPayload) {
        String sql =
                "SET 'sql-client.execution.result-mode' = 'tableau';\n"
                        + "SET 'restart-strategy' = 'none';\n"
                        + "CREATE TEMPORARY TABLE bad_input (v BIGINT) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '1', "
                        + "'fields.v.kind' = 'sequence', 'fields.v.start' = '1', "
                        + "'fields.v.end' = '1');\n"
                        + "SELECT CAST(CONCAT('invalid_', CAST(v AS STRING)"
                        + errorPayload
                        + ") AS INT) FROM bad_input;";

        assertThatThrownBy(() -> runStreamingSql(sql))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Could not execute SQL statement")
                .hasMessageContaining("NumberFormatException");
    }

    @Test
    void acceptsSuccessfulStatementsWithoutJobId() throws Exception {
        assertThat(
                        runBatchSql(
                                "CREATE CATALOG test_catalog WITH ('type' = 'paimon', 'warehouse' = '"
                                        + TEST_DATA_DIR
                                        + "/completion-catalog');\n"
                                        + "USE CATALOG test_catalog;\n"
                                        + "CREATE TEMPORARY VIEW test_view AS SELECT 1;\nSHOW VIEWS;"))
                .isNull();
        assertThat(runStreamingSql("SET 'parallelism.default' = '1' -- trailing comment")).isNull();
    }

    @Test
    void doesNotTreatConfigurationValuesAsJobIds() throws Exception {
        String sql =
                "SET 'pipeline.name' = 'SQL update statement has been successfully submitted to the cluster:\n"
                        + "Job ID: 1234';";

        assertThat(runStreamingSql(sql)).isNull();
    }

    @Test
    void acceptsErrorHeaderInQueryResult() throws Exception {
        String sql =
                "SET 'sql-client.execution.result-mode' = 'tableau';\n"
                        + "SET 'table.display.max-column-width' = '100';\n"
                        + "SELECT CONCAT('prefix', CHR(10), "
                        + "'Flink SQL> [ERROR] Could not execute SQL statement. Reason:', "
                        + "CHR(10), 'suffix') AS message;";

        assertThat(runBatchSql(sql)).isNull();
    }

    @Test
    void acceptsErrorHeaderInQueryResultAfterEmptyStreamingResult() throws Exception {
        String sql =
                "SET 'sql-client.execution.result-mode' = 'tableau';\n"
                        + "SET 'table.display.max-column-width' = '200';\n"
                        + "SELECT v FROM (VALUES (1)) AS t(v) WHERE FALSE;\n"
                        + "SELECT CONCAT('prefix', CHR(10), "
                        + "'Flink SQL> [ERROR] Could not execute SQL statement. Reason:', "
                        + "CHR(10), 'suffix') AS message;";

        assertThat(runStreamingSql(sql)).isNull();
    }

    @Test
    void acceptsErrorHeaderInQueryResultWithMultilineColumnName() throws Exception {
        String sql =
                "SET 'sql-client.execution.result-mode' = 'tableau';\n"
                        + "SET 'table.display.max-column-width' = '200';\n"
                        + "SELECT CONCAT('prefix', CHR(10), "
                        + "'Flink SQL> [ERROR] Could not execute SQL statement. Reason:', "
                        + "CHR(10), 'suffix') AS U&\"multi\\000Aline\";";

        assertThat(runBatchSql(sql)).isNull();
    }

    @Test
    void acceptsFakeTableauFooterInQueryResult() throws Exception {
        String sql =
                "SET 'sql-client.execution.result-mode' = 'tableau';\n"
                        + "SET 'table.display.max-column-width' = '200';\n"
                        + "SELECT CONCAT('prefix', CHR(10), '+---+', CHR(10), "
                        + "'1 row in set', CHR(10), "
                        + "'Flink SQL> [ERROR] Could not execute SQL statement. Reason:', "
                        + "CHR(10), 'suffix') AS message;";

        assertThat(runBatchSql(sql)).isNull();
    }
}
