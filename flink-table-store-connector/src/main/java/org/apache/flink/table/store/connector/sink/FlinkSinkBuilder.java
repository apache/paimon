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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.connector.FlinkConnectorOptions;
import org.apache.flink.table.store.file.catalog.CatalogLock;
import org.apache.flink.table.store.file.utils.JsonSerdeUtil;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.LogSinkFunction;

import javax.annotation.Nullable;

import java.util.Map;

/** Sink builder to build a flink sink from input. */
public class FlinkSinkBuilder {

    private final ObjectIdentifier tableIdentifier;
    private final FileStoreTable table;
    private final Configuration conf;

    private DataStream<RowData> input;
    @Nullable private CatalogLock.Factory lockFactory;
    @Nullable private Map<String, String> overwritePartition;
    @Nullable private LogSinkFunction logSinkFunction;
    @Nullable private Integer parallelism;

    public FlinkSinkBuilder(ObjectIdentifier tableIdentifier, FileStoreTable table) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.conf = Configuration.fromMap(table.schema().options());
    }

    public FlinkSinkBuilder withInput(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    public FlinkSinkBuilder withLockFactory(CatalogLock.Factory lockFactory) {
        this.lockFactory = lockFactory;
        return this;
    }

    public FlinkSinkBuilder withOverwritePartition(Map<String, String> overwritePartition) {
        this.overwritePartition = overwritePartition;
        return this;
    }

    public FlinkSinkBuilder withLogSinkFunction(@Nullable LogSinkFunction logSinkFunction) {
        this.logSinkFunction = logSinkFunction;
        return this;
    }

    public FlinkSinkBuilder withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private Map<String, String> getCompactPartSpec() {
        String json = conf.get(FlinkConnectorOptions.COMPACTION_PARTITION_SPEC);
        if (json == null) {
            return null;
        }
        return JsonSerdeUtil.fromJson(json, Map.class);
    }

    public DataStreamSink<?> build() {
        int numBucket = conf.get(CoreOptions.BUCKET);

        BucketStreamPartitioner partitioner =
                new BucketStreamPartitioner(numBucket, table.schema());
        PartitionTransformation<RowData> partitioned =
                new PartitionTransformation<>(input.getTransformation(), partitioner);
        if (parallelism != null) {
            partitioned.setParallelism(parallelism);
        }

        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        StoreSink sink =
                new StoreSink(
                        tableIdentifier,
                        table,
                        conf.get(FlinkConnectorOptions.COMPACTION_MANUAL_TRIGGERED),
                        getCompactPartSpec(),
                        lockFactory,
                        overwritePartition,
                        logSinkFunction);
        return sink.sinkFrom(new DataStream<>(env, partitioned));
    }
}
