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

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link RepairAction}. */
public class RepairActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "repair_table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {

        Tuple3<String, String, String> tablePath = getTablePath(params);
        Map<String, String> catalogConfig = optionalConfigMap(params, CATALOG_CONF);

        RepairAction action =
                new RepairAction(tablePath.f0, tablePath.f1, tablePath.f2, catalogConfig);

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"repair_table\" repair table with a given table name.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  repair_table --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name>");
        System.out.println();
    }
}
