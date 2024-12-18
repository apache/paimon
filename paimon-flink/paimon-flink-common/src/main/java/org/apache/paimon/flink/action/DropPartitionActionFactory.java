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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory to create {@link DropPartitionAction}. */
public class DropPartitionActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "drop_partition";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        checkArgument(
                params.has(PARTITION),
                "Argument '%s' is required. Run '<action> --help' for help.",
                PARTITION);
        List<Map<String, String>> partitions = getPartitions(params);

        return Optional.of(
                new DropPartitionAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        partitions,
                        catalogConfigMap(params)));
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"drop_partition\" drops data of specified partitions for a table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  drop_partition --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> --partition <partition_name> [--partition <partition_name> ...]");
        System.out.println(
                "  drop_partition --path <table_path> --partition <partition_name> [--partition <partition_name> ...]");
        System.out.println();

        System.out.println("Partition name syntax:");
        System.out.println("  key1=value1,key2=value2,...");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  drop_partition --warehouse hdfs:///path/to/warehouse --database test_db --table test_table --partition dt=20221126,hh=08");
        System.out.println(
                "  drop_partition --path hdfs:///path/to/warehouse/test_db.db/test_table --partition dt=20221126,hh=08 --partition dt=20221127,hh=09");
    }
}
