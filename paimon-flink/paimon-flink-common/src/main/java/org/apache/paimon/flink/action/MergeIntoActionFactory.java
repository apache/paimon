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

package org.apache.paimon.flink.action;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.Action.checkRequiredArgument;

/** Factory to create {@link MergeIntoAction}. */
public class MergeIntoActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "merge-into";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        Tuple3<String, String, String> tablePath = Action.getTablePath(params);

        Map<String, String> catalogConfig = Action.optionalConfigMap(params, "catalog-conf");

        MergeIntoAction action =
                new MergeIntoAction(tablePath.f0, tablePath.f1, tablePath.f2, catalogConfig);

        if (params.has("target-as")) {
            action.withTargetAlias(params.get("target-as"));
        }

        if (params.has("source-sql")) {
            Collection<String> sourceSqls = params.getMultiParameter("source-sql");
            action.withSourceSqls(sourceSqls.toArray(new String[0]));
        }

        checkRequiredArgument(params, "source-table");
        action.withSourceTable(params.get("source-table"));

        checkRequiredArgument(params, "on");
        action.withMergeCondition(params.get("on"));

        List<String> actions =
                Arrays.stream(params.get("merge-actions").split(","))
                        .map(String::trim)
                        .collect(Collectors.toList());
        if (actions.contains("matched-upsert")) {
            checkRequiredArgument(params, "matched-upsert-set");
            action.withMatchedUpsert(
                    params.get("matched-upsert-condition"), params.get("matched-upsert-set"));
        }
        if (actions.contains("not-matched-by-source-upsert")) {
            checkRequiredArgument(params, "not-matched-by-source-upsert-set");
            action.withNotMatchedBySourceUpsert(
                    params.get("not-matched-by-source-upsert-condition"),
                    params.get("not-matched-by-source-upsert-set"));
        }
        if (actions.contains("matched-delete")) {
            action.withMatchedDelete(params.get("matched-delete-condition"));
        }
        if (actions.contains("not-matched-by-source-delete")) {
            action.withNotMatchedBySourceDelete(
                    params.get("not-matched-by-source-delete-condition"));
        }
        if (actions.contains("not-matched-insert")) {
            checkRequiredArgument(params, "not-matched-insert-values");
            action.withNotMatchedInsert(
                    params.get("not-matched-insert-condition"),
                    params.get("not-matched-insert-values"));
        }

        validate(action);

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"merge-into\" simulates the \"MERGE INTO\" syntax.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  merge-into --warehouse <warehouse-path>\n"
                        + "             --database <database-name>\n"
                        + "             --table <target-table-name>\n"
                        + "             [--target-as <target-table-alias>]\n"
                        + "             [--source-sql <sql> ...]\n"
                        + "             --source-table <source-table-name>\n"
                        + "             --on <merge-condition>\n"
                        + "             --merge-actions <matched-upsert,matched-delete,not-matched-insert,not-matched-by-source-upsert,not-matched-by-source-delete>\n"
                        + "             --matched-upsert-condition <matched-condition>\n"
                        + "             --matched-upsert-set <upsert-changes>\n"
                        + "             --matched-delete-condition <matched-condition>\n"
                        + "             --not-matched-insert-condition <not-matched-condition>\n"
                        + "             --not-matched-insert-values <insert-values>\n"
                        + "             --not-matched-by-source-upsert-condition <not-matched-by-source-condition>\n"
                        + "             --not-matched-by-source-upsert-set <not-matched-upsert-changes>\n"
                        + "             --not-matched-by-source-delete-condition <not-matched-by-source-condition>");

        System.out.println("  matched-upsert-changes format:");
        System.out.println(
                "    col=<source-table>.col | expression [, ...] (do not add '<target-table>.' before 'col')");
        System.out.println(
                "    * (upsert with all source cols; require target table's schema is equal to source's)");

        System.out.println("  not-matched-upsert-changes format:");
        System.out.println("    col=expression (cannot use source table's col)");

        System.out.println("  insert-values format:");
        System.out.println(
                "    col1,col2,...,col_end (must specify values of all columns; can use <source-table>.col or expression)");
        System.out.println(
                "    * (insert with all source cols; require target table's schema is equal to source's)");

        System.out.println(
                "  not-matched-condition: cannot use target table's columns to construct condition expression.");
        System.out.println(
                "  not-matched-by-source-condition: cannot use source table's columns to construct condition expression.");

        System.out.println("  alternative arguments:");
        System.out.println("    --path <table-path> to represent the table path.");
        System.out.println();

        System.out.println("Note: ");
        System.out.println("  1. Target table must has primary keys.");
        System.out.println(
                "  2. All conditions, set changes and values should use Flink SQL syntax. Please quote them with \" to escape special characters.");
        System.out.println(
                "  3. You can pass sqls by --source-sql to config environment and create source table at runtime");
        System.out.println("  4. Target alias cannot be duplicated with existed table name.");
        System.out.println(
                "  5. If the source table is not in the current catalog and current database, "
                        + "the source-table-name must be qualified (database.table or catalog.database.table if in different catalog).");
        System.out.println("  6. At least one merge action must be specified.");
        System.out.println("  7. How to determine the changed rows with different \"matched\":");
        System.out.println(
                "    matched: changed rows are from target table and each can match a source table row "
                        + "based on merge-condition and optional matched-condition.");
        System.out.println(
                "    not-matched: changed rows are from source table and all rows cannot match any target table row "
                        + "based on merge-condition and optional not-matched-condition.");
        System.out.println(
                "    not-matched-by-source: changed rows are from target table and all row cannot match any source table row "
                        + "based on merge-condition and optional not-matched-by-source-condition.");
        System.out.println(
                "  8. If both matched-upsert and matched-delete actions are present, their conditions must both be present too "
                        + "(same to not-matched-by-source-upsert and not-matched-by-source-delete). Otherwise, all conditions are optional.");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  merge-into --path hdfs:///path/to/T\n"
                        + "             --source-table S\n"
                        + "             --on \"T.k = S.k\"\n"
                        + "             --merge-actions matched-upsert\n"
                        + "             --matched-upsert-condition \"T.v <> S.v\"\n"
                        + "             --matched-upsert-set \"v = S.v\"");
        System.out.println(
                "  It will find matched rows of target table that meet condition (T.k = S.k), then update T.v with S.v where (T.v <> S.v).");
    }

    private void validate(MergeIntoAction action) {
        if (!action.matchedUpsert
                && !action.notMatchedUpsert
                && !action.matchedDelete
                && !action.notMatchedDelete
                && !action.insert) {
            throw new IllegalArgumentException(
                    "Must specify at least one merge action. Run 'merge-into --help' for help.");
        }

        if ((action.matchedUpsert && action.matchedDelete)
                && (action.matchedUpsertCondition == null
                        || action.matchedDeleteCondition == null)) {
            throw new IllegalArgumentException(
                    "If both matched-upsert and matched-delete actions are present, their conditions must both be present too.");
        }

        if ((action.notMatchedUpsert && action.notMatchedDelete)
                && (action.notMatchedBySourceUpsertCondition == null
                        || action.notMatchedBySourceDeleteCondition == null)) {
            throw new IllegalArgumentException(
                    "If both not-matched-by-source-upsert and not-matched-by--source-delete actions are present, "
                            + "their conditions must both be present too.\n");
        }

        if (action.notMatchedBySourceUpsertSet != null
                && action.notMatchedBySourceUpsertSet.equals("*")) {
            throw new IllegalArgumentException(
                    "The '*' cannot be used in not-matched-by-source-upsert-set");
        }
    }
}
