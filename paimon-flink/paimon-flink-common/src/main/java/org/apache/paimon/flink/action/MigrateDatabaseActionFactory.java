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

/** Action Factory for {@link MigrateDatabaseAction}. */
public class MigrateDatabaseActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "migrate_database";

    private static final String SOURCE_TYPE = "source_type";
    private static final String OPTIONS = "options";
    private static final String PARALLELISM = "parallelism";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String connector = params.get(SOURCE_TYPE);
        String sourceHiveDatabase = params.get(DATABASE);
        Map<String, String> catalogConfig = catalogConfigMap(params);
        String tableConf = params.get(OPTIONS);
        Integer parallelism = Integer.parseInt(params.get(PARALLELISM));

        MigrateDatabaseAction migrateDatabaseAction =
                new MigrateDatabaseAction(
                        connector, sourceHiveDatabase, catalogConfig, tableConf, parallelism);
        return Optional.of(migrateDatabaseAction);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"migrate_database\" migrate all tables in database from hive to paimon.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  migrate_database --warehouse <warehouse_path> --source_type hive "
                        + "--database <database_name> "
                        + "[--catalog_conf <key>=<value] "
                        + "[--options <key>=<value>,<key>=<value>,...]");
    }
}
