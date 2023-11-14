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

package org.apache.paimon.flink.action.cdc.rabbitmq;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.cdc.TypeMapping;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.ArrayList;
import java.util.Optional;

/** Factory to create {@link RabbitmqSyncTableAction}. */
public class RabbitmqSyncTableActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "rabbitmq-sync-table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);
        checkRequiredArgument(params, "rabbitmq-conf");

        RabbitmqSyncTableAction action =
                new RabbitmqSyncTableAction(
                        tablePath.f0,
                        tablePath.f1,
                        tablePath.f2,
                        optionalConfigMap(params, "catalog-conf"),
                        optionalConfigMap(params, "rabbitmq-conf"));
        action.withTableConfig(optionalConfigMap(params, "table-conf"));

        if (params.has("partition-keys")) {
            action.withPartitionKeys(params.get("partition-keys").split(","));
        }

        if (params.has("primary-keys")) {
            action.withPrimaryKeys(params.get("primary-keys").split(","));
        }

        if (params.has("computed-column")) {
            action.withComputedColumnArgs(
                    new ArrayList<>(params.getMultiParameter("computed-column")));
        }

        if (params.has("type-mapping")) {
            String[] options = params.get("type-mapping").split(",");
            action.withTypeMapping(TypeMapping.parse(options));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"rabbitmq-sync-table\" creates a streaming job "
                        + "with a Flink rabbitmq CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();
    }
}
