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
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/** Table compact action for Flink. */
public class CompactAction extends ActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(CompactAction.class);

    private final CompactorSourceBuilder sourceBuilder;
    private final CompactorSinkBuilder sinkBuilder;

    public CompactAction(String warehouse, String database, String tableName) {
        super(warehouse, database, tableName, new Options().set(CoreOptions.WRITE_ONLY, false));
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                            table.getClass().getName()));
        }

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

    public static StringBuilder printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Action \"compact\" runs a dedicated job for compacting specified table.");
        sb.append("/n");

        sb.append("Syntax:");
        sb.append(
                "  compact --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> [--partition <partition-name>]");
        sb.append("  compact --path <table-path> [--partition <partition-name>]");
        sb.append("/n");

        sb.append("Partition name syntax:");
        sb.append("  key1=value1 key2=value2 ...");
        sb.append("/n");

        sb.append("Examples:");
        sb.append(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table");
        sb.append(
                "  compact --path hdfs:///path/to/warehouse/test_db.db/test_table --partition dt=20221126 hh=08");
        sb.append(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table "
                        + "--partition dt=20221126 hh=08 --partition dt=20221127 hh=09");

        return sb;
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        env.execute("Compact job");
    }
}
