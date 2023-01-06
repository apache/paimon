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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.connector.sink.CompactorSinkBuilder;
import org.apache.flink.table.store.connector.source.CompactorSourceBuilder;
import org.apache.flink.table.store.connector.utils.StreamExecutionEnvironmentUtils;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Table compact action for Flink. */
public class CompactAction {

    private final CompactorSourceBuilder sourceBuilder;
    private final CompactorSinkBuilder sinkBuilder;

    CompactAction(Path tablePath) {
        Configuration tableOptions = new Configuration();
        tableOptions.set(CoreOptions.PATH, tablePath.toString());
        tableOptions.set(CoreOptions.WRITE_ONLY, false);
        FileStoreTable table = FileStoreTableFactory.create(tableOptions);

        sourceBuilder = new CompactorSourceBuilder(tablePath.toString(), table);
        sinkBuilder = new CompactorSinkBuilder(table);
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    public CompactAction withPartitions(List<Map<String, String>> partitions) {
        sourceBuilder.withPartitions(partitions);
        return this;
    }

    public void build(StreamExecutionEnvironment env) {
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;

        DataStreamSource<RowData> source =
                sourceBuilder.withEnv(env).withContinuousMode(isStreaming).build();
        sinkBuilder.withInput(source).build();
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    public static Optional<CompactAction> create(MultipleParameterTool params) {
        if (params.has("help")) {
            printHelp();
            return Optional.empty();
        }

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
                                + "Run compact --help for help.");
                return Optional.empty();
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
                            + "Run compact --help for help.");
            return Optional.empty();
        }

        CompactAction action = new CompactAction(tablePath);

        if (params.has("partition")) {
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
                                        + "Run compact --help for help.");
                        return Optional.empty();
                    }
                    kvs.put(kv[0], kv[1]);
                }
                partitions.add(kvs);
            }
            action.withPartitions(partitions);
        }

        return Optional.of(action);
    }

    public static void printHelp() {
        System.out.println(
                "Action \"compact\" runs a dedicated job for compacting specified table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  compact --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> [--partition <partition-name>]");
        System.out.println("  compact --path <table-path> [--partition <partition-name>]");
        System.out.println();

        System.out.println("Partition name syntax:");
        System.out.println("  key1=value1,key2=value2,...");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table");
        System.out.println(
                "  compact --path hdfs:///path/to/warehouse/test_db.db/test_table --partition dt=20221126,hh=08");
        System.out.println(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table "
                        + "--partition dt=20221126,hh=08 --partition dt=20221127,hh=09");
    }
}
