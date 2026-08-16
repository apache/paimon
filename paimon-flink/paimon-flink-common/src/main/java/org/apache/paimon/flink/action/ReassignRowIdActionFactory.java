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

import org.apache.paimon.flink.procedure.ReassignRowIdProcedure;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Factory to create {@link ReassignRowIdAction}. */
public class ReassignRowIdActionFactory implements ActionFactory {

    public static final String IDENTIFIER = ReassignRowIdProcedure.IDENTIFIER;

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        return Optional.of(
                new ReassignRowIdAction(
                        catalogConfigMap(params),
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        optionalConfigMap(params, TABLE_CONF),
                        optionalPartitions(params)));
    }

    private List<Map<String, String>> optionalPartitions(MultipleParameterToolAdapter params) {
        return params.has(PARTITION) ? getPartitions(params) : Collections.emptyList();
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \""
                        + IDENTIFIER
                        + "\" reassigns row IDs for a data evolution table when partition "
                        + "row-id ranges overlap.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  "
                        + IDENTIFIER
                        + " --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> [--table_conf <key>=<value>] "
                        + "[--catalog_conf <key>=<value>] "
                        + "[--partition <key>=<value>[,<key>=<value>] ...]");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  "
                        + IDENTIFIER
                        + " --warehouse hdfs:///path/to/warehouse "
                        + "--database test_db --table test_table "
                        + "--partition dt=2026-05-19");
    }
}
