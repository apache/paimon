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

/** Factory to create {@link ResetConsumerAction}. */
public class ResetConsumerActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "reset_consumer";

    private static final String CONSUMER_ID = "consumer_id";
    private static final String NEXT_SNAPSHOT = "next_snapshot";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        ResetConsumerAction action =
                new ResetConsumerAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params),
                        params.getRequired(CONSUMER_ID));

        if (params.has(NEXT_SNAPSHOT)) {
            action.withNextSnapshotIds(Long.parseLong(params.get(NEXT_SNAPSHOT)));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"reset_consumer\" reset a consumer with a given consumer ID and next snapshot ID and delete a consumer with a given consumer ID.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  reset_consumer --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> --consumer_id <consumer_id> [--next_snapshot <next_snapshot_id>]");

        System.out.println();
        System.out.println("Note:");
        System.out.println(
                "  please don't specify --next_snapshot parameter if you want to delete the consumer.");
        System.out.println();
    }
}
