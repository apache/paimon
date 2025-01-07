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

/** Factory to create {@link RollbackToTimestampAction}. */
public class RollbackToTimestampActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "rollback_to_timestamp";

    private static final String TIMESTAMP = "timestamp";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        RollbackToTimestampAction action =
                new RollbackToTimestampAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        Long.parseLong(params.getRequired(TIMESTAMP)),
                        catalogConfigMap(params));

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"rollback_to_timestamp\" roll back a table to a specific timestamp.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  rollback_to --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> --timestamp <timestamp_string>");
        System.out.println("  <timestamp_string> can be a long value representing a timestamp.");
        System.out.println();
    }
}
