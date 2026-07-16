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

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link CompactChainTableAction}. */
public class CompactChainTableActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "compact_chain_table";

    private static final String PARTITION = "partition";
    private static final String OVERWRITE = "overwrite";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String database = params.getRequired(DATABASE);
        String table = params.getRequired(TABLE);
        Map<String, String> catalogConfig = catalogConfigMap(params);
        String partition = params.getRequired(PARTITION);
        boolean overwrite = params.getBoolean(OVERWRITE, false);
        return Optional.of(
                new CompactChainTableAction(database, table, catalogConfig, partition, overwrite));
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"compact_chain_table\" compacts a chain table partition.");
        System.out.println();
        System.out.println("Syntax:");
        System.out.println(
                "  compact_chain_table --warehouse <warehouse_path> --database <db> --table <table> --partition <partition> [--overwrite true]");
        System.out.println("Examples:");
        System.out.println(
                "  compact_chain_table --warehouse hdfs:///path/to/warehouse --database test_db --table test_table --partition dt=20250810");
        System.out.println(
                "  compact_chain_table --warehouse hdfs:///path/to/warehouse --database test_db --table test_table --partition dt=20250810,hour=22 --overwrite true");
    }
}
