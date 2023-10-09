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

import java.util.Optional;

/** Factory to create {@link QueryServiceAction}. */
public class QueryServiceActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "query-service";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        QueryServiceAction action =
                new QueryServiceAction(
                        getRequiredValue(params, "warehouse"),
                        optionalConfigMap(params, "catalog-conf"));

        action.includingDatabases(params.get("including-databases"))
                .includingTables(params.get("including-tables"))
                .excludingTables(params.get("excluding-tables"))
                .withTableOptions(optionalConfigMap(params, "table-conf"));

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"query-service\" runs a dedicated job starting query service for one or multiple database.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  query-service --warehouse <warehouse-path> --database <database-name> "
                        + "[--including-tables <paimon-table-name|name-regular-expr>] "
                        + "[--excluding-tables <paimon-table-name|name-regular-expr>] ");
        System.out.println(
                "  query-service --warehouse s3://path/to/warehouse --database <database-name> "
                        + "[--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]]");
        System.out.println();

        System.out.println(
                "--including-tables is used to specify which source tables are to be queried. "
                        + "You must use '|' to separate multiple tables, the format is `databaseName.tableName`, Regular expression is supported.");
        System.out.println(
                "--excluding-tables is used to specify which source tables are not to be queried. "
                        + "The usage is same as --including-tables.");
        System.out.println(
                "--excluding-tables has higher priority than --including-tables if you specified both.");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  query-service --warehouse hdfs:///path/to/warehouse --database test_db");
        System.out.println(
                "  query-service --warehouse s3:///path/to/warehouse "
                        + "--database test_db "
                        + "--catalog-conf s3.endpoint=https://****.com "
                        + "--catalog-conf s3.access-key=***** "
                        + "--catalog-conf s3.secret-key=***** ");
    }
}
