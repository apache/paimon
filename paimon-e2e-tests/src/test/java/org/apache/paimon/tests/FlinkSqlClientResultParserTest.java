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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests SQL Client script completion and Job ID extraction. */
class FlinkSqlClientResultParserTest {

    private static final String COMPLETION_TOKEN = "f592da88-42f5-4842-8b1f-1787855c3cd2";
    private static final String COMPLETION_COLUMN =
            "paimon_e2e_completion_f592da8842f548428b1f1787855c3cd2";
    private static final String COMPLETION_ROW =
            "| " + COMPLETION_COLUMN + " | INT | FALSE | | | |\n";
    private static final String SQL_EXECUTION_ERROR =
            "[ERROR] Could not execute SQL statement. Reason:";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejectsStatementErrorRegardlessOfOutputStream(boolean stderrError) {
        String error = SQL_EXECUTION_ERROR + "\nObject 'missing_table' not found";
        String stdout = stderrError ? "" : error;
        String stderr = stderrError ? error : "";

        assertThatThrownBy(
                        () ->
                                FlinkSqlClientResultParser.parseJobIdOrThrowOnError(
                                        0, stdout, stderr, COMPLETION_TOKEN))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("missing_table");
    }

    @Test
    void rejectsRuntimeErrorContainingTableauFooter() {
        String stdout =
                "+----+-------------+\n"
                        + "| op |      EXPR$0 |\n"
                        + "+----+-------------+\n"
                        + SQL_EXECUTION_ERROR
                        + "\njava.lang.NumberFormatException: For input string: 'invalid_1\n"
                        + "+----+-------------+\n"
                        + "1 row in set\n\n"
                        + "suffix'. Invalid character found.\n";

        assertThatThrownBy(
                        () ->
                                FlinkSqlClientResultParser.parseJobIdOrThrowOnError(
                                        0, stdout, "", COMPLETION_TOKEN))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("NumberFormatException");
    }

    @Test
    void rejectsMissingCompletionWithoutAnErrorMessage() {
        assertThatThrownBy(
                        () ->
                                FlinkSqlClientResultParser.parseJobIdOrThrowOnError(
                                        0, "Flink SQL> ", "", COMPLETION_TOKEN))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("did not complete the SQL script");
    }

    @Test
    void rejectsEchoedCompletionStatement() {
        String stdout =
                "Flink SQL> CREATE TEMPORARY VIEW "
                        + COMPLETION_COLUMN
                        + " AS SELECT 1 AS "
                        + COMPLETION_COLUMN
                        + ";\nFlink SQL> DESCRIBE "
                        + COMPLETION_COLUMN
                        + ";\n";

        assertThatThrownBy(
                        () ->
                                FlinkSqlClientResultParser.parseJobIdOrThrowOnError(
                                        0, stdout, "", COMPLETION_TOKEN))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void rejectsCompletionFromAnotherScript() {
        assertThatThrownBy(
                        () ->
                                FlinkSqlClientResultParser.parseJobIdOrThrowOnError(
                                        0, COMPLETION_ROW, "", "another-token"))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void rejectsTokenOutsideCompletionRow() {
        assertThatThrownBy(
                        () ->
                                FlinkSqlClientResultParser.parseJobIdOrThrowOnError(
                                        0,
                                        "| message | " + COMPLETION_TOKEN + " |\n",
                                        "",
                                        COMPLETION_TOKEN))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void rejectsNonzeroExitCodeEvenWithCompletion() {
        assertThatThrownBy(
                        () ->
                                FlinkSqlClientResultParser.parseJobIdOrThrowOnError(
                                        1, COMPLETION_ROW, "stderr", COMPLETION_TOKEN))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("exit code 1")
                .hasMessageContaining("stdout")
                .hasMessageContaining("stderr");
    }

    @Test
    void acceptsCompletedScriptWithoutJobId() {
        assertThat(
                        FlinkSqlClientResultParser.parseJobIdOrThrowOnError(
                                0, COMPLETION_ROW, "", COMPLETION_TOKEN))
                .isNull();
    }

    @Test
    void acceptsErrorLikeQueryDataInCompletedScript() {
        String stdout = SQL_EXECUTION_ERROR + "\n+---+\n1 row in set\n\n" + COMPLETION_ROW;

        assertThat(
                        FlinkSqlClientResultParser.parseJobIdOrThrowOnError(
                                0, stdout, "", COMPLETION_TOKEN))
                .isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "\033[34;1m"})
    void extractsJobIdFromCompletedScript(String color) {
        String stdout =
                color
                        + "SQL update statement has been successfully submitted to the cluster:"
                        + (color.isEmpty() ? "" : "\033[0m")
                        + "\nJob ID: 1234\n"
                        + COMPLETION_ROW;

        assertThat(
                        FlinkSqlClientResultParser.parseJobIdOrThrowOnError(
                                0, stdout, "", COMPLETION_TOKEN))
                .isEqualTo("1234");
    }
}
