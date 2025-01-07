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

import org.apache.paimon.flink.service.QueryService;

import java.util.Map;
import java.util.Optional;

/** Factory to create QueryService Action. */
public class QueryServiceActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "query_service";

    public static final String PARALLELISM = "parallelism";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Map<String, String> catalogConfig = catalogConfigMap(params);
        Map<String, String> tableConfig = optionalConfigMap(params, TABLE_CONF);
        String parallStr = params.get(PARALLELISM);
        int parallelism = parallStr == null ? 1 : Integer.parseInt(parallStr);
        Action action =
                new TableActionBase(
                        params.getRequired(DATABASE), params.getRequired(TABLE), catalogConfig) {
                    @Override
                    public void run() throws Exception {
                        QueryService.build(env, table.copy(tableConfig), parallelism);
                        execute("Query Service job");
                    }
                };
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"query-service\" runs a dedicated job starting query service for a table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  query-service --warehouse <warehouse-path> --database <database-name> --table <table-name> --parallelism <parallelism>"
                        + "[--catalog_conf <key>=<value> [--catalog_conf <key>=<value> ...]] "
                        + "[--table_conf <key>=<value> [--table_conf <key>=<value> ...]] ");
    }
}
