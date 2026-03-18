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

import java.util.Collection;
import java.util.Optional;

/** The {@link ActionFactory} for {@link DataEvolutionMergeIntoAction}. */
public class DataEvolutionMergeIntoActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "data_evolution_merge_into";

    private static final String TARGET_AS = "target_as";
    private static final String SOURCE_SQL = "source_sql";
    private static final String SOURCE_TABLE = "source_table";
    private static final String ON = "on";
    private static final String MATCHED_UPDATE_SET = "matched_update_set";
    private static final String SINK_PARALLELISM = "sink_parallelism";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        DataEvolutionMergeIntoAction action =
                new DataEvolutionMergeIntoAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params));

        // optional params
        if (params.has(TARGET_AS)) {
            action.withTargetAlias(params.getRequired(TARGET_AS));
        }

        if (params.has(SOURCE_SQL)) {
            Collection<String> sourceSqls = params.getMultiParameter(SOURCE_SQL);
            action.withSourceSqls(sourceSqls.toArray(new String[0]));
        }

        // required params
        action.withSourceTable(params.getRequired(SOURCE_TABLE));
        action.withMergeCondition(params.getRequired(ON));
        action.withMatchedUpdateSet(params.getRequired(MATCHED_UPDATE_SET));

        int sinkParallelism;
        try {
            sinkParallelism = Integer.parseInt(params.getRequired(SINK_PARALLELISM));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid sink parallelism, must be an integer", e);
        }
        action.withSinkParallelism(sinkParallelism);

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"merge_into\" specially implemented for DataEvolutionTables.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  data_evolution_merge_into \\\n"
                        + "--warehouse <warehouse_path> \\\n"
                        + "--database <database_name> \\\n"
                        + "--table <target_table_name> \\\n"
                        + "[--target_as <target_table_alias>] \\\n"
                        + "[--source_sql <sql> ...] \\\n"
                        + "--source_table <source_table_name> \\\n"
                        + "--on <merge_condition> \\\n"
                        + "--matched_update_set <update_changes> \\\n"
                        + "--sink_parallelism <sink_parallelism>");

        System.out.println("  matched_update_set format:");
        System.out.println(
                "    col=<source_table>.col | expression [, ...] (do not add '<target_table>.' before 'col')");
        System.out.println(
                "    * (update with all source cols; require target table's schema is a projection of source's)");

        System.out.println("  alternative arguments:");
        System.out.println("    --path <table_path> to represent the table path.");
        System.out.println();

        System.out.println("Note: ");
        System.out.println("  1. Target table must be a data-evolution table.");
        System.out.println(
                "  2. This is a simplified merge-into action, specially implemented for DataEvolutionTables:\n"
                        + "       (1) Only supports matched update action.\n"
                        + "       (2) Only generates new data files without rewriting existing files.\n"
                        + "       (3) Nulls will also override existing values.");
        System.out.println(
                "  2. All conditions, set changes and values should use Flink SQL syntax. Please quote them with \" to escape special characters.");
        System.out.println(
                "  3. You can pass sqls by --source_sql to config environment and create source table at runtime");
        System.out.println("  4. Target alias cannot be duplicated with existed table name.");
        System.out.println(
                "  5. If the source table is not in the current catalog and current database, "
                        + "the source_table_name must be qualified (database.table or catalog.database.table if in different catalog).");

        System.out.println("Examples:");
        System.out.println(
                "  data_evolution_merge_into \\\n"
                        + "--path hdfs:///path/to/T \\\n"
                        + "--source_table S \\\n"
                        + "--on \"T.id = S.id\" \\\n"
                        + "--matched_upsert_set \"value = S.`value`\"");
        System.out.println(
                "  It will find matched rows of target table that meet condition (T.id = S.id), then write new files which only contain"
                        + " the updated column `value`.");
    }
}
