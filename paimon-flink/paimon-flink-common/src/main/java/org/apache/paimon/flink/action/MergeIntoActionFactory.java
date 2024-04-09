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

        action.validate();

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"merge_into\" simulates the \"MERGE INTO\" syntax.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  merge_into --warehouse <warehouse_path>\n"
                        + "             --database <database_name>\n"
                        + "             --table <target_table_name>\n"
                        + "             [--target_as <target_table_alias>]\n"
                        + "             [--source_sql <sql> ...]\n"
                        + "             --source_table <source_table_name>\n"
                        + "             --on <merge_condition>\n"
                        + "             --merge_actions <matched-upsert,matched-delete,not-matched-insert,not-matched-by-source-upsert,not-matched-by-source-delete>\n"
                        + "             --matched_upsert_condition <matched_condition>\n"
                        + "             --matched_upsert_set <upsert_changes>\n"
                        + "             --matched_delete_condition <matched_condition>\n"
                        + "             --not_matched_insert_condition <not_matched_condition>\n"
                        + "             --not_matched_insert_values <insert_values>\n"
                        + "             --not_matched_by_source_upsert_condition <not_matched_by_source_condition>\n"
                        + "             --not_matched_by_source_upsert_set <not_matched_upsert_changes>\n"
                        + "             --not_matched_by_source_delete_condition <not_matched_by_source_condition>");

        System.out.println("  matched_upsert_set format:");
        System.out.println(
                "    col=<source_table>.col | expression [, ...] (do not add '<target_table>.' before 'col')");
        System.out.println(
                "    * (upsert with all source cols; require target table's schema is equal to source's)");

        System.out.println("  not_matched_upsert_set format:");
        System.out.println("    col=expression (cannot use source table's col)");

        System.out.println("  insert_values format:");
        System.out.println(
                "    col1,col2,...,col_end (must specify values of all columns; can use <source_table>.col or expression)");
        System.out.println(
                "    * (insert with all source cols; require target table's schema is equal to source's)");

        System.out.println(
                "  not_matched_condition: cannot use target table's columns to construct condition expression.");
        System.out.println(
                "  not_matched_by_source_condition: cannot use source table's columns to construct condition expression.");

        System.out.println("  alternative arguments:");
        System.out.println("    --path <table_path> to represent the table path.");
        System.out.println();

        System.out.println("Note: ");
        System.out.println("  1. Target table must has primary keys.");
        System.out.println(
                "  2. All conditions, set changes and values should use Flink SQL syntax. Please quote them with \" to escape special characters.");
        System.out.println(
                "  3. You can pass sqls by --source_sql to config environment and create source table at runtime");
        System.out.println("  4. Target alias cannot be duplicated with existed table name.");
        System.out.println(
                "  5. If the source table is not in the current catalog and current database, "
                        + "the source_table_name must be qualified (database.table or catalog.database.table if in different catalog).");
        System.out.println("  6. At least one merge action must be specified.");
        System.out.println("  7. How to determine the changed rows with different \"matched\":");
        System.out.println(
                "    matched: changed rows are from target table and each can match a source table row "
                        + "based on merge_condition and optional matched_condition.");
        System.out.println(
                "    not_matched: changed rows are from source table and all rows cannot match any target table row "
                        + "based on merge_condition and optional not_matched_condition.");
        System.out.println(
                "    not_matched_by_source: changed rows are from target table and all row cannot match any source table row "
                        + "based on merge_condition and optional not_matched_by_source_condition.");
        System.out.println(
                "  8. If both matched_upsert and matched_delete actions are present, their conditions must both be present too "
                        + "(same to not_matched_by_source_upsert and not_matched_by_source_delete). Otherwise, all conditions are optional.");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  merge_into --path hdfs:///path/to/T\n"
                        + "             --source_table S\n"
                        + "             --on \"T.k = S.k\"\n"
                        + "             --merge_actions matched-upsert\n"
                        + "             --matched_upsert_condition \"T.v <> S.v\"\n"
                        + "             --matched_upsert_set \"v = S.v\"");
        System.out.println(
                "  It will find matched rows of target table that meet condition (T.k = S.k), then update T.v with S.v where (T.v <> S.v).");
    }
}
