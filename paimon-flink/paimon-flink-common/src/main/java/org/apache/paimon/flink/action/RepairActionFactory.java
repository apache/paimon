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

/** Factory to create {@link RepairAction}. */
public class RepairActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "repair";

    private static final String IDENTIFIER_KEY = "identifier";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String warehouse = params.get(WAREHOUSE);
        Map<String, String> catalogConfig = optionalConfigMap(params, CATALOG_CONF);
        String identifier = params.get(IDENTIFIER_KEY);
        RepairAction action = new RepairAction(warehouse, identifier, catalogConfig);
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"repair\" synchronize information from the file system to Metastore.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  repair --warehouse <warehouse_path> [--identifier <database.table>] ");
        System.out.println();

        System.out.println(
                "If --identifier is not provided, all databases and tables in the catalog will be synchronized.");
        System.out.println(
                "If --identifier is a database name, all tables in that database will be synchronized.");
        System.out.println(
                "If --identifier is a databaseName.tableName, only that specific table will be synchronized.");
        System.out.println();

        System.out.println("Examples:");
        System.out.println("  repair --warehouse hdfs:///path/to/warehouse");
        System.out.println("  repair --warehouse hdfs:///path/to/warehouse --identifier test_db");
        System.out.println("  repair --warehouse hdfs:///path/to/warehouse --identifier test_db.T");
    }
}
