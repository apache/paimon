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

package org.apache.paimon.flink.sink;

import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.createPartitionTransformation;

/** Builder for {@link FileStoreSink}. */
public class FlinkSinkBuilder {

    private final FileStoreTable table;

    private DataStream<RowData> input;
    private Lock.Factory lockFactory = Lock.emptyFactory();
    @Nullable private Map<String, String> overwritePartition;
    @Nullable private LogSinkFunction logSinkFunction;
    @Nullable private Integer parallelism;

    public FlinkSinkBuilder(FileStoreTable table) {
        this.table = table;
    }

    public FlinkSinkBuilder withInput(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    public FlinkSinkBuilder withLockFactory(Lock.Factory lockFactory) {
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

    public DataStreamSink<?> build() {
        BucketMode bucketMode = table.bucketMode();
        switch (bucketMode) {
            case FIXED:
                return buildForFixedBucket();
            case DYNAMIC:
            case UNAWARE:
            default:
                throw new UnsupportedOperationException("Unsupported bucket mode: " + bucketMode);
        }
    }

    private DataStreamSink<?> buildForFixedBucket() {
        DataStream<RowData> partitioned =
                createPartitionTransformation(
                        input,
                        new RowDataChannelComputer(table.schema(), logSinkFunction != null),
                        parallelism);
        FileStoreSink sink =
                new FileStoreSink(table, lockFactory, overwritePartition, logSinkFunction);
        return sink.sinkFrom(partitioned);
    }
}
