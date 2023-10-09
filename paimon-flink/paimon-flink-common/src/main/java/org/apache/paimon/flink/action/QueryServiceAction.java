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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.query.AddressServerFunction;
import org.apache.paimon.flink.query.FileMonitorSourceFunction;
import org.apache.paimon.flink.query.QueryExecutorOperator;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.regex.Pattern;

/** Database compact action for Flink. */
public class QueryServiceAction extends ActionBase {

    private Pattern includingPattern = Pattern.compile(".*");
    @Nullable private Pattern excludingPattern;
    private Pattern databasePattern = Pattern.compile(".*");

    private Options tableOptions = new Options();

    public QueryServiceAction(String warehouse, Map<String, String> catalogConfig) {
        super(warehouse, catalogConfig);
    }

    public QueryServiceAction includingDatabases(@Nullable String includingDatabases) {
        if (includingDatabases != null) {
            this.databasePattern = Pattern.compile(includingDatabases);
        }
        return this;
    }

    public QueryServiceAction includingTables(@Nullable String includingTables) {
        if (includingTables != null) {
            this.includingPattern = Pattern.compile(includingTables);
        }
        return this;
    }

    public QueryServiceAction excludingTables(@Nullable String excludingTables) {
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        return this;
    }

    public QueryServiceAction withTableOptions(Map<String, String> tableOptions) {
        this.tableOptions = Options.fromMap(tableOptions);
        return this;
    }

    @Override
    public void build(StreamExecutionEnvironment env) {
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        Preconditions.checkArgument(
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING,
                "Query Service only supports streaming mode.");
        DataStream<InternalRow> source =
                FileMonitorSourceFunction.build(
                        env,
                        catalogLoader(),
                        databasePattern,
                        includingPattern,
                        excludingPattern,
                        tableOptions.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis());

        QueryExecutorOperator executorOperator = new QueryExecutorOperator(catalogLoader());
        InternalTypeInfo<InternalRow> addressTypeInfo =
                InternalTypeInfo.fromRowType(QueryExecutorOperator.outputType());
        source.transform("Executor", addressTypeInfo, executorOperator)
                .addSink(new AddressServerFunction(catalogLoader()))
                .setParallelism(1)
                .setMaxParallelism(1);
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        execute(env, "Query Service job");
    }
}
