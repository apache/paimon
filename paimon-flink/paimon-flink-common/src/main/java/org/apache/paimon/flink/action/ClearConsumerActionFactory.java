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

/** Factory to create {@link ClearConsumerAction}. */
public class ClearConsumerActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "clear_consumers";

    private static final String INCLUDING_CONSUMERS = "including_consumers";
    private static final String EXCLUDING_CONSUMERS = "excluding_consumers";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        ClearConsumerAction action =
                new ClearConsumerAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params));

        if (params.has(INCLUDING_CONSUMERS)) {
            action.withIncludingConsumers(params.get(INCLUDING_CONSUMERS));
        }

        if (params.has(EXCLUDING_CONSUMERS)) {
            action.withExcludingConsumers(params.get(EXCLUDING_CONSUMERS));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"clear_consumers\" clear consumers with including consumers and excluding consumers.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  clear_consumers \\\n"
                        + "--warehouse <warehouse_path> \\\n"
                        + "--database <database_name> \\\n"
                        + "--table <table_name> \\\n"
                        + "[--including_consumers <including_pattern> --excluding_consumers <excluding_pattern>]");

        System.out.println();
        System.out.println("Note:");
        System.out.println(
                "  use '' as placeholder for including_consumers if you want to clear all consumers except excludingConsumers in the table.");
        System.out.println();
    }
}
