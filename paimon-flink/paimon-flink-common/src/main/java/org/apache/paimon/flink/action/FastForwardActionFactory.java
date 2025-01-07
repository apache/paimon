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

/** Factory to create {@link FastForwardAction}. */
public class FastForwardActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "fast_forward";

    private static final String BRANCH_NAME = "branch_name";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        FastForwardAction action =
                new FastForwardAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params),
                        params.getRequired(BRANCH_NAME));
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"fast_forward\" fast_forward a branch by name.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  fast_forward --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> --branch_name <branch_name>");
        System.out.println();
    }
}
