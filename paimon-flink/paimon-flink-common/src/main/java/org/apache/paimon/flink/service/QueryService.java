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

package org.apache.paimon.flink.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/** A class to build Query Service topology. */
public class QueryService {

    public static void build(StreamExecutionEnvironment env, Table table, int parallelism) {
        ReadableConfig conf = env.getConfiguration();
        Preconditions.checkArgument(
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING,
                "Query Service only supports streaming mode.");

        FileStoreTable storeTable = (FileStoreTable) table;
        if (storeTable.bucketMode() != BucketMode.HASH_FIXED
                || storeTable.schema().primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "The bucket mode of "
                            + table.name()
                            + " is not fixed or the table has no primary key.");
        }

        DataStream<InternalRow> stream = QueryFileMonitor.build(env, table);
        stream = partition(stream, QueryFileMonitor.createChannelComputer(), parallelism);

        QueryExecutorOperator executorOperator = new QueryExecutorOperator(table);
        DataStreamSink<?> sink =
                stream.transform(
                                "Executor",
                                InternalTypeInfo.fromRowType(QueryExecutorOperator.outputType()),
                                executorOperator)
                        .setParallelism(parallelism)
                        .addSink(new QueryAddressRegister(table))
                        .setParallelism(1);

        sink.getTransformation().setMaxParallelism(1);
    }
}
