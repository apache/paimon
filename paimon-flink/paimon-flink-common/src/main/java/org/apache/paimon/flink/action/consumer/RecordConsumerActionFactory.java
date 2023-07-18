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

package org.apache.paimon.flink.action.consumer;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link RecordConsumerAction}. */
public class RecordConsumerActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "record-consumer";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        checkRequiredArgument(params, "consumer-id");
        checkRequiredArgument(params, "snapshot");

        Tuple3<String, String, String> tablePath = getTablePath(params);
        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");
        String consumerId = params.get("consumer-id");
        long snapshot = Long.parseLong(params.get("snapshot"));

        RecordConsumerAction action =
                new RecordConsumerAction(
                        tablePath.f0,
                        tablePath.f1,
                        tablePath.f2,
                        catalogConfig,
                        consumerId,
                        snapshot);
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"record-consumer\" record a consumer from the given next snapshot.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  record-consumer --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> --consumer-id <consumer-id> --snapshot <next-snapshot-id>");
        System.out.println();
    }
}
