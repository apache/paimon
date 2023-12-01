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

import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Map;
import java.util.Optional;

/** Action Factory for {@link MigrateTableAction}. */
public class MigrateTableActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "migrate_table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        String warehouse = params.get("warehouse");
        String connector = params.get("source_type");
        String sourceHiveTable = params.get("table");
        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");
        String tableConf = params.get("options");

        MigrateTableAction migrateTableAction =
                new MigrateTableAction(
                        connector, warehouse, sourceHiveTable, catalogConfig, tableConf);
        return Optional.of(migrateTableAction);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"migrate_table\" runs a migrating job from hive to paimon.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  migrate_table --warehouse <warehouse_path> --source_type hive "
                        + "--table <database.table_name> "
                        + "[--catalog_conf <key>=<value] "
                        + "[--options <key>=<value>,<key>=<value>,...]");
    }
}
