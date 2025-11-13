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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.fromString;

/** Factory to create {@link RemoveOrphanFilesAction}. */
public class RemoveOrphanFilesActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "remove_orphan_files";
    private static final String OLDER_THAN = "older_than";
    private static final String DRY_RUN = "dry_run";
    private static final String PARALLELISM = "parallelism";
    private static final String TABLES = "tables";
    private static final String MODE = "mode";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        // Check that table and tables parameters are not used together
        if (params.has(TABLE) && params.has(TABLES)) {
            throw new IllegalArgumentException(
                    "Cannot specify both '--table' and '--tables' parameters. "
                            + "Use '--table' for a single table or '--tables' for multiple tables.");
        }

        List<String> tableNames;
        if (params.has(TABLE)) {
            // Single table mode
            tableNames = Collections.singletonList(params.getRequired(TABLE));
        } else if (params.has(TABLES)) {
            // Multiple tables mode
            Collection<String> tablesParams = params.getMultiParameter(TABLES);
            if (tablesParams == null || tablesParams.isEmpty()) {
                tableNames = Collections.emptyList();
            } else {
                tableNames = new ArrayList<>(tablesParams);
            }
        } else {
            // No table specified, process all tables
            tableNames = Collections.emptyList();
        }

        RemoveOrphanFilesAction action =
                new RemoveOrphanFilesAction(
                        params.getRequired(DATABASE),
                        tableNames,
                        params.get(PARALLELISM),
                        catalogConfigMap(params));

        if (params.has(OLDER_THAN)) {
            action.olderThan(params.get(OLDER_THAN));
        }

        if (params.has(DRY_RUN) && Boolean.parseBoolean(params.get(DRY_RUN))) {
            action.dryRun();
        }

        if (params.has(MODE)) {
            action.mode(fromString(params.get(MODE)));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"remove_orphan_files\" remove files which are not used by any snapshots or tags of a Paimon table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  remove_orphan_files \\\n"
                        + "--warehouse <warehouse_path> \\\n"
                        + "--database <database_name> \\\n"
                        + "[--table <table_name>] \\\n"
                        + "[--tables <table1,table2,...>] \\\n"
                        + "[--older_than <timestamp>] \\\n"
                        + "[--dry_run <false/true>] \\\n"
                        + "[--mode <divided|combined>]");

        System.out.println();
        System.out.println(
                "To avoid deleting newly written files, this action only deletes orphan files older than 1 day by default. "
                        + "The interval can be modified by '--older_than'. <timestamp> format: yyyy-MM-dd HH:mm:ss");

        System.out.println();
        System.out.println(
                "When '--dry_run true', view only orphan files, don't actually remove files. Default is false.");
        System.out.println();

        System.out.println(
                "If neither '--table' nor '--tables' is specified, all orphan files in all tables under the db will be cleaned up.");
        System.out.println();

        System.out.println(
                "Use '--table' to specify a single table, or '--tables' to specify multiple tables (comma-separated, e.g., 'table1,table2,table3'). "
                        + "These two parameters cannot be used together.");
        System.out.println();

        System.out.println(
                "When '--mode combined', multiple tables will be processed within a single DataStream "
                        + "during job graph construction, instead of creating one dataStream per table. "
                        + "This significantly reduces job graph construction time, when processing "
                        + "thousands of tables (jobs may fail to start within timeout limits). "
                        + "It also reduces JobGraph complexity and avoids stack over flow issue and resource allocation failures during job running. "
                        + "When '--mode divided', create one DataStream per table during job graph construction. "
                        + "Default is 'divided'.");
        System.out.println();
    }
}
