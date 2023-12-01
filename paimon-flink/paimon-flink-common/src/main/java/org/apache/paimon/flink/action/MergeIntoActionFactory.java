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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Factory to create {@link MergeIntoAction}. */
public class MergeIntoActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "merge_into";

    public static final String MATCHED_UPSERT = "matched-upsert";
    public static final String NOT_MATCHED_BY_SOURCE_UPSERT = "not-matched-by-source-upsert";
    public static final String MATCHED_DELETE = "matched-delete";
    public static final String NOT_MATCHED_BY_SOURCE_DELETE = "not-matched-by-source-delete";
    public static final String NOT_MATCHED_INSERT = "not-matched-insert";

    private static final String TARGET_AS = "target_as";
    private static final String SOURCE_SQL = "source_sql";
    private static final String SOURCE_TABLE = "source_table";
    private static final String ON = "on";
    private static final String MERGE_ACTIONS = "merge_actions";
    private static final String MATCHED_UPSERT_SET = "matched_upsert_set";
    private static final String MATCHED_UPSERT_CONDITION = "matched_upsert_condition";
    private static final String NOT_MATCHED_BY_SOURCE_UPSERT_SET =
            "not_matched_by_source_upsert_set";
    private static final String NOT_MATCHED_BY_SOURCE_UPSERT_CONDITION =
            "not_matched_by_source_upsert_condition";
    private static final String MATCHED_DELETE_CONDITION = "matched_delete_condition";
    private static final String NOT_MATCHED_BY_SOURCE_DELETE_CONDITION =
            "not_matched_by_source_delete_condition";
    private static final String NOT_MATCHED_INSERT_VALUES = "not_matched_insert_values";
    private static final String NOT_MATCHED_INSERT_CONDITION = "not_matched_insert_condition";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);

        Map<String, String> catalogConfig = optionalConfigMap(params, CATALOG_CONF);

        MergeIntoAction action =
                new MergeIntoAction(tablePath.f0, tablePath.f1, tablePath.f2, catalogConfig);

        if (params.has(TARGET_AS)) {
            action.withTargetAlias(params.get(TARGET_AS));
        }

        if (params.has(SOURCE_SQL)) {
            Collection<String> sourceSqls = params.getMultiParameter(SOURCE_SQL);
            action.withSourceSqls(sourceSqls.toArray(new String[0]));
        }

        checkRequiredArgument(params, SOURCE_TABLE);
        action.withSourceTable(params.get(SOURCE_TABLE));

        checkRequiredArgument(params, ON);
        action.withMergeCondition(params.get(ON));

        List<String> actions =
                Arrays.stream(params.get(MERGE_ACTIONS).split(","))
                        .map(String::trim)
                        .collect(Collectors.toList());
        if (actions.contains(MATCHED_UPSERT)) {
            checkRequiredArgument(params, MATCHED_UPSERT_SET);
            action.withMatchedUpsert(
                    params.get(MATCHED_UPSERT_CONDITION), params.get(MATCHED_UPSERT_SET));
        }
        if (actions.contains(NOT_MATCHED_BY_SOURCE_UPSERT)) {
            checkRequiredArgument(params, NOT_MATCHED_BY_SOURCE_UPSERT_SET);
            action.withNotMatchedBySourceUpsert(
                    params.get(NOT_MATCHED_BY_SOURCE_UPSERT_CONDITION),
                    params.get(NOT_MATCHED_BY_SOURCE_UPSERT_SET));
        }
        if (actions.contains(MATCHED_DELETE)) {
            action.withMatchedDelete(params.get(MATCHED_DELETE_CONDITION));
        }
        if (actions.contains(NOT_MATCHED_BY_SOURCE_DELETE)) {
            action.withNotMatchedBySourceDelete(params.get(NOT_MATCHED_BY_SOURCE_DELETE_CONDITION));
        }
        if (actions.contains(NOT_MATCHED_INSERT)) {
            checkRequiredArgument(params, NOT_MATCHED_INSERT_VALUES);
            action.withNotMatchedInsert(
                    params.get(NOT_MATCHED_INSERT_CONDITION),
                    params.get(NOT_MATCHED_INSERT_VALUES));
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

    public static void validate(MergeIntoAction action) {
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
