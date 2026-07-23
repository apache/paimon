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

/** Factory to create {@link DeleteAction}. */
public class DeleteActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "delete";

    private static final String WHERE = "where";
    private static final String SOURCE_SQL = "source_sql";
    private static final String SINK_PARALLELISM = "sink_parallelism";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String filter = params.get(WHERE);
        if (filter == null || filter.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Please specify deletion filter. If you want to delete all records, please use overwrite (see doc).");
        }

        DeleteAction action =
                new DeleteAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        filter,
                        catalogConfigMap(params));

        if (params.has(SOURCE_SQL)) {
            Collection<String> sourceSqls = params.getMultiParameter(SOURCE_SQL);
            action.withSourceSqls(sourceSqls.toArray(new String[0]));
        }

        if (params.has(SINK_PARALLELISM)) {
            int sinkParallelism;
            try {
                sinkParallelism = Integer.parseInt(params.getRequired(SINK_PARALLELISM));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid sink parallelism, must be an integer", e);
            }
            action.withSinkParallelism(sinkParallelism);
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"delete\" deletes data from a primary-key or Data Evolution table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  delete \\\n"
                        + " --warehouse <warehouse_path> \\\n"
                        + "--database <database_name> \\\n"
                        + "--table <table_name> \\\n"
                        + "[--source_sql <sql> ...] \\\n"
                        + "--where <filter_spec> \\\n"
                        + "[--sink_parallelism <sink_parallelism>]");
        System.out.println("  delete --path <table_path> --where <filter_spec>");
        System.out.println();

        System.out.println(
                "The '--where <filter_spec>' part is equal to the 'WHERE' clause in SQL DELETE statement. If you want to delete all records, please use overwrite (see doc).");
        System.out.println(
                "For Data Evolution tables, use repeated --source_sql arguments to register bounded external tables referenced by subqueries in --where.");
        System.out.println(
                "The --sink_parallelism option applies only to Data Evolution deletion-vector writing.");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  delete --path hdfs:///path/to/warehouse/test_db.db/test_table --where 'id > (SELECT count(*) FROM employee)'");
        System.out.println(
                "  delete --warehouse hdfs:///path/to/warehouse --database test_db --table test_table --source_sql \"CREATE TEMPORARY TABLE candidates (url STRING) WITH (...)\" --where \"url IN (SELECT url FROM candidates)\" --sink_parallelism 4");
    }
}
