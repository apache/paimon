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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.sink.CompactorSinkBuilder;
import org.apache.paimon.flink.source.CompactorSourceBuilder;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.Action.getPartitions;
import static org.apache.paimon.flink.action.Action.getTablePath;
import static org.apache.paimon.flink.action.Action.optionalConfigMap;

/** Table compact action for Flink. */
public class CompactAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(CompactAction.class);

    private final CompactorSourceBuilder sourceBuilder;
    private final CompactorSinkBuilder sinkBuilder;

    public CompactAction(String warehouse, String database, String tableName) {
        this(warehouse, database, tableName, Collections.emptyMap());
    }

    public CompactAction(
            String warehouse,
            String database,
            String tableName,
            Map<String, String> catalogConfig) {
        super(warehouse, database, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                            table.getClass().getName()));
        }
        table = table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
        sourceBuilder =
                new CompactorSourceBuilder(identifier.getFullName(), (FileStoreTable) table);
        sinkBuilder = new CompactorSinkBuilder((FileStoreTable) table);
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

    public static Optional<Action> create(String[] args) {
        LOG.info("Compact job args: {}", String.join(" ", args));

        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        if (params.has("help")) {
            printHelp();
            return Optional.empty();
        }

        Tuple3<String, String, String> tablePath = getTablePath(params);

        if (tablePath == null) {
            return Optional.empty();
        }

        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");

        CompactAction action =
                new CompactAction(tablePath.f0, tablePath.f1, tablePath.f2, catalogConfig);

        if (params.has("partition")) {
            List<Map<String, String>> partitions = getPartitions(params);
            if (partitions == null) {
                return Optional.empty();
            }

            action.withPartitions(partitions);
        }

        return Optional.of(action);
    }

    private static void printHelp() {
        System.out.println(
                "Action \"compact\" runs a dedicated job for compacting specified table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  compact --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> [--partition <partition-name>]");
        System.out.println(
                "  compact --warehouse s3://path/to/warehouse --database <database-name> "
                        + "--table <table-name> [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]]");
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
        System.out.println(
                "  compact --warehouse s3:///path/to/warehouse "
                        + "--database test_db "
                        + "--table test_table "
                        + "--catalog-conf s3.endpoint=https://****.com "
                        + "--catalog-conf s3.access-key=***** "
                        + "--catalog-conf s3.secret-key=***** ");
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        execute(env, "Compact job");
    }
}
