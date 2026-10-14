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

import javax.annotation.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the output of the Flink SQL Client used by end-to-end tests.
 *
 * <p>This helper supports scripts using Flink's default SQL dialect. The appended completion check
 * uses CREATE TEMPORARY VIEW and DESCRIBE in that dialect; Hive dialect scripts are not supported.
 *
 * <p>The E2E framework manages the SQL Client lifecycle. Scripts must run to completion without
 * QUIT/EXIT commands, which skip the appended completion check and are reported as incomplete
 * scripts.
 */
final class FlinkSqlClientResultParser {

    // Extracts the Job ID from a successful asynchronous SQL submission.
    private static final Pattern JOB_ID_PATTERN =
            Pattern.compile(
                    "SQL update statement has been successfully submitted to the cluster:\\s+Job ID: (\\S+)");

    // Removes terminal color and style escapes before inspecting completion and submission output.
    private static final Pattern ANSI_SGR_PATTERN = Pattern.compile("\\x1B\\[[0-9;]*m");

    private FlinkSqlClientResultParser() {}

    static String appendCompletionCheck(String sql, String completionToken) {
        // Like the SQL Client, allow a final statement without a semicolon. Put the terminator
        // on a new line so a trailing line comment cannot consume it.
        String terminatedSql = sql.trim().endsWith(";") ? sql : sql + "\n;";
        // File execution stops on the first statement error, even when the process exits with 0.
        // Describe a unique temporary view to prove completion without printing user configuration,
        // whose values could mimic job submission messages. Both statements only access metadata
        // and do not submit an additional Flink job.
        String viewName = completionViewName(completionToken);
        return terminatedSql
                + "\nCREATE TEMPORARY VIEW "
                + viewName
                + " AS SELECT 1 AS "
                + viewName
                + ";\nDESCRIBE "
                + viewName
                + ";\n";
    }

    @Nullable
    static String parseJobIdOrThrowOnError(
            int exitCode, String stdout, String stderr, String completionToken) {
        // DESCRIBE prints the view's unique column name as a metadata table row in all supported
        // Flink versions (1.16+). Echoed SQL cannot produce this row, and a fresh token keeps it
        // independent of query data and exception messages.
        Pattern completionPattern =
                Pattern.compile(
                        "^\\|[ \\t]*"
                                + Pattern.quote(completionViewName(completionToken))
                                + "[ \\t]*\\|",
                        Pattern.MULTILINE);
        String stdoutWithoutAnsi = ANSI_SGR_PATTERN.matcher(stdout).replaceAll("");
        if (exitCode != 0 || !completionPattern.matcher(stdoutWithoutAnsi).find()) {
            throw new AssertionError(
                    String.format(
                            "Flink SQL Client did not complete the SQL script (exit code %d).\nstdout:\n%s\nstderr:\n%s",
                            exitCode, stdout, stderr));
        }

        Matcher matcher = JOB_ID_PATTERN.matcher(stdoutWithoutAnsi);
        return matcher.find() ? matcher.group(1) : null;
    }

    private static String completionViewName(String completionToken) {
        return "paimon_e2e_completion_" + completionToken.replace("-", "");
    }
}
