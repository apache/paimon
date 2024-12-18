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

import java.util.Map;
import java.util.Optional;

/** Action Factory for {@link MigrateFileAction}. */
public class MigrateFileActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "migrate_file";

    private static final String SOURCE_TYPE = "source_type";

    private static final String SOURCE_TABLE = "source_table";

    private static final String TARGET_TABLE = "target_table";

    private static final String DELETE_ORIGIN = "delete_origin";

    private static final String OPTIONS = "options";
    private static final String PARALLELISM = "parallelism";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String connector = params.get(SOURCE_TYPE);
        String sourceHiveTable = params.get(SOURCE_TABLE);
        String targetTable = params.get(TARGET_TABLE);
        boolean deleteOrigin = Boolean.parseBoolean(params.get(DELETE_ORIGIN));
        Map<String, String> catalogConfig = catalogConfigMap(params);
        String tableConf = params.get(OPTIONS);
        Integer parallelism = Integer.parseInt(params.get(PARALLELISM));

        MigrateFileAction migrateFileAction =
                new MigrateFileAction(
                        connector,
                        sourceHiveTable,
                        targetTable,
                        deleteOrigin,
                        catalogConfig,
                        tableConf,
                        parallelism);
        return Optional.of(migrateFileAction);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"migrate_file\" runs a migrating job from hive to paimon.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  migrate_file --warehouse <warehouse_path> --source_type hive "
                        + "--source_table <database.table_name> "
                        + "--target_table <database.table_name> "
                        + "--delete_origin true "
                        + "[--catalog_conf <key>=<value] "
                        + "[--options <key>=<value>,<key>=<value>,...]");
    }
}
