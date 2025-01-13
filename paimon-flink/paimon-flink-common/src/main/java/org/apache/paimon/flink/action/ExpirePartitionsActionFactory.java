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

/** Factory to create {@link ExpirePartitionsAction}. */
public class ExpirePartitionsActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "expire_partitions";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {

        return Optional.of(
                new ExpirePartitionsAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params),
                        params.get(EXPIRATIONTIME),
                        params.get(TIMESTAMPFORMATTER),
                        params.get(TIMESTAMP_PATTERN),
                        params.get(EXPIRE_STRATEGY)));
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"expire_partitions\" expire table partitions by name.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  expire_partitions --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> --tag_name <tag_name> --expiration_time <expiration_time> --timestamp_formatter <timestamp_formatter>"
                        + "[--timestamp_pattern <timestamp_pattern>] [--expire_strategy <expire_strategy>]");
        System.out.println();
    }
}
