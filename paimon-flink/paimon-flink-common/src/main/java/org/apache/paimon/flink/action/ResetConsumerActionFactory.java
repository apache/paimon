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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link ResetConsumerAction}. */
public class ResetConsumerActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "reset-consumer";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        checkRequiredArgument(params, "consumer-id");

        Tuple3<String, String, String> tablePath = getTablePath(params);
        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");
        String consumerId = params.get("consumer-id");

        ResetConsumerAction action =
                new ResetConsumerAction(
                        tablePath.f0, tablePath.f1, tablePath.f2, catalogConfig, consumerId);

        if (params.has("next-snapshot")) {
            action.withNextSnapshotIds(Long.parseLong(params.get("next-snapshot")));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"reset-consumer\" reset a consumer with a given consumer ID and next snapshot ID and delete a consumer with a given consumer ID.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  reset-consumer --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> --consumer-id <consumer-id> [--next-snapshot <next-snapshot-id>]");

        System.out.println();
        System.out.println("Note:");
        System.out.println(
                "  please don't specify --next-snapshot parameter if you want to delete the consumer.");
        System.out.println();
    }
}
