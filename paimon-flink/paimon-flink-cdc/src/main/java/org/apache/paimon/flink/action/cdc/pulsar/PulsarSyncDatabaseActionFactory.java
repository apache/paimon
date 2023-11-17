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

package org.apache.paimon.flink.action.cdc.pulsar;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.cdc.TypeMapping;

import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Optional;

/** Factory to create {@link PulsarSyncDatabaseAction}. */
public class PulsarSyncDatabaseActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "pulsar-sync-database";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        checkRequiredArgument(params, "pulsar-conf");

        PulsarSyncDatabaseAction action =
                new PulsarSyncDatabaseAction(
                        getRequiredValue(params, "warehouse"),
                        getRequiredValue(params, "database"),
                        optionalConfigMap(params, "catalog-conf"),
                        optionalConfigMap(params, "pulsar-conf"));

        action.withTableConfig(optionalConfigMap(params, "table-conf"))
                .withTablePrefix(params.get("table-prefix"))
                .withTableSuffix(params.get("table-suffix"))
                .includingTables(params.get("including-tables"))
                .excludingTables(params.get("excluding-tables"));

        if (params.has("type-mapping")) {
            String[] options = params.get("type-mapping").split(",");
            action.withTypeMapping(TypeMapping.parse(options));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"pulsar-sync-database\" creates a streaming job "
                        + "with a Flink Pulsar source and multiple Paimon table sinks "
                        + "to synchronize multiple tables into one Paimon database.\n"
                        + "Only tables with primary keys will be considered. ");
        System.out.println();

        // TODO
    }
}
