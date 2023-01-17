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

package org.apache.flink.table.store.connector.action;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Abstract class for Flink actions. */
public interface Action {

    /** The execution method of the action. */
    void run() throws Exception;

    @Nullable
    static Path getTablePath(MultipleParameterTool params) {
        String warehouse = params.get("warehouse");
        String database = params.get("database");
        String table = params.get("table");
        String path = params.get("path");

        Path tablePath = null;
        int count = 0;
        if (warehouse != null || database != null || table != null) {
            if (warehouse == null || database == null || table == null) {
                System.err.println(
                        "Warehouse, database and table must be specified all at once.\n"
                                + "Run <action> --help for help.");
                return null;
            }
            tablePath = new Path(new Path(warehouse, database + ".db"), table);
            count++;
        }
        if (path != null) {
            tablePath = new Path(path);
            count++;
        }

        if (count != 1) {
            System.err.println(
                    "Please specify either \"warehouse, database and table\" or \"path\".\n"
                            + "Run <action> --help for help.");
            return null;
        }

        return tablePath;
    }

    @Nullable
    static List<Map<String, String>> getPartitions(MultipleParameterTool params) {
        List<Map<String, String>> partitions = new ArrayList<>();
        for (String partition : params.getMultiParameter("partition")) {
            Map<String, String> kvs = new HashMap<>();
            for (String kvString : partition.split(",")) {
                String[] kv = kvString.split("=");
                if (kv.length != 2) {
                    System.err.print(
                            "Invalid key-value pair \""
                                    + kvString
                                    + "\".\n"
                                    + "Run <action> --help for help.");
                    return null;
                }
                kvs.put(kv[0], kv[1]);
            }
            partitions.add(kvs);
        }

        return partitions;
    }

    /** Factory to create {@link Action}. */
    class Factory {

        // supported actions
        private static final String COMPACT = "compact";
        private static final String DROP_PARTITION = "drop-partition";

        public static Optional<Action> create(String[] args) {
            String action = args[0].toLowerCase();
            String[] actionArgs = Arrays.copyOfRange(args, 1, args.length);

            switch (action) {
                case COMPACT:
                    return CompactAction.create(actionArgs);
                case DROP_PARTITION:
                    return DropPartitionAction.create(actionArgs);
                default:
                    System.err.println("Unknown action \"" + action + "\"");
                    printHelp();
                    return Optional.empty();
            }
        }

        static void printHelp() {
            System.out.println("Usage: <action> [OPTIONS]");
            System.out.println();

            System.out.println("Available actions:");
            System.out.println("  " + COMPACT);
            System.out.println("  " + DROP_PARTITION);
            System.out.println();

            System.out.println("For detailed options of each action, run <action> --help");
        }
    }
}
