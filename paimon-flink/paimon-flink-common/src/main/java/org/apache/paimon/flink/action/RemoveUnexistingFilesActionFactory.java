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

import java.util.Optional;

/** Factory to create {@link RemoveUnexistingFilesAction}. */
public class RemoveUnexistingFilesActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "remove_unexisting_files";
    private static final String DRY_RUN = "dry_run";
    private static final String PARALLELISM = "parallelism";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        RemoveUnexistingFilesAction action =
                new RemoveUnexistingFilesAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params));

        if (params.has(DRY_RUN) && Boolean.parseBoolean(params.get(DRY_RUN))) {
            action.dryRun();
        }

        if (params.has(PARALLELISM)) {
            action.withParallelism(Integer.parseInt(params.get(PARALLELISM)));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"remove_unexisting_files\" removes unexisting data files from manifest entries.");
        System.out.println(
                "See Java docs in https://paimon.apache.org/docs/master/api/java/org/apache/paimon/flink/action/RemoveUnexistingFilesAction.html for detailed use cases.");
        System.out.println(
                "Note that user is on his own risk using this procedure, which may cause data loss when used outside from the use cases in Java docs.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  remove_unexisting_files --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> [--partition <partition_name> [--partition <partition_name>]] "
                        + "[--dry_run <false/true>] "
                        + "[--parallelism <parallelism>]");
        System.out.println(
                "If partitions are not specified, this action will remove unexisting files from all partitions.");
        System.out.println(
                "When dry_run is set to true (default false), this action only checks what files will be removed, but not really remove them.");
    }
}
