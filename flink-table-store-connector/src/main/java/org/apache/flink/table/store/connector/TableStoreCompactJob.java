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

package org.apache.flink.table.store.connector;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
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

/** Stand-alone compact job for table store. */
public class TableStoreCompactJob {

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 4) {
            printHelp();
            System.exit(1);
        }

        Path location;
        if (args.length <= 2) {
            location = new Path(args[0]);
        } else {
            location = new Path(new Path(args[0], args[1] + ".db"), args[2]);
        }

        String partitionsSpecString = null;
        if (args.length == 2) {
            partitionsSpecString = args[1];
        } else if (args.length == 4) {
            partitionsSpecString = args[3];
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        runCompactJob(location, partitionsSpecString, env);
    }

    @VisibleForTesting
    public static JobClient runCompactJob(
            Path location, String partitionsSpecString, StreamExecutionEnvironment env)
            throws Exception {
        Configuration tableOptions = new Configuration();
        tableOptions.set(CoreOptions.PATH, location.toString());
        tableOptions.set(CoreOptions.WRITE_COMPACTION_SKIP, false);
        FileStoreTable table = FileStoreTableFactory.create(tableOptions);

        List<Map<String, String>> specifiedPartition = parsePartitionSpec(partitionsSpecString);

        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;

        DataStreamSource<RowData> source =
                new CompactorSourceBuilder(location.toString(), table)
                        .withEnv(env)
                        .withContinuousMode(isStreaming)
                        .withPartitions(specifiedPartition)
                        .build();
        new CompactorSinkBuilder(table).withInput(source).build();
        JobClient client = env.executeAsync("Compaction Job: " + location);
        System.out.println(
                "Compaction job for "
                        + location
                        + " successfully submitted with job ID: "
                        + client.getJobID());
        return client;
    }

    private static List<Map<String, String>> parsePartitionSpec(String partitionsSpecString) {
        if (partitionsSpecString == null) {
            return null;
        }

        List<Map<String, String>> result = new ArrayList<>();
        for (String partition : partitionsSpecString.split("/")) {
            Map<String, String> kvs = new HashMap<>();
            for (String kvString : partition.split(",")) {
                String[] kv = kvString.split("=");
                if (kv.length != 2) {
                    System.err.print("Invalid key-value pair " + kvString + "\n\n");
                    printHelp();
                    System.exit(1);
                }
                kvs.put(kv[0], kv[1]);
            }
            result.add(kvs);
        }

        if (result.isEmpty()) {
            return null;
        } else {
            return result;
        }
    }

    private static void printHelp() {
        String partitionSpec =
                "[partition1-key1=partition1-value1[,partition1-key2=partition1-value2,...]"
                        + "[/partition2-key1=partition2-value1[,partition2-key2=partition2-value2,...]]]";
        System.err.print(
                "Arguments should be:\n"
                        + "<warehouse-path> <database-name> <table-name> "
                        + partitionSpec
                        + "\n"
                        + "or\n"
                        + "<table-path> "
                        + partitionSpec
                        + "\n\n"
                        + "Examples:\n"
                        + "hdfs:///path/to/warehouse test_db test_table\n"
                        + "hdfs:///path/to/warehouse/test_db.db/test_table dt=20221126,hh=08\n"
                        + "hdfs:///path/to/warehouse test_db test_table dt=20221126,hh=08/dt=20221127,hh=09\n");
    }
}
