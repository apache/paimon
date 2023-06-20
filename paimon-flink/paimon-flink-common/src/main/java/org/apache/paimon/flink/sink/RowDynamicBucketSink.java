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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;

/** Sink for dynamic bucket table. */
public class RowDynamicBucketSink extends DynamicBucketSink<RowData> {

    private static final long serialVersionUID = 1L;

    public RowDynamicBucketSink(
            FileStoreTable table,
            Lock.Factory lockFactory,
            @Nullable Map<String, String> overwritePartition) {
        super(table, lockFactory, overwritePartition);
    }

    @Override
    protected ChannelComputer<RowData> channelComputer1() {
        return new RowHashKeyChannelComputer(table.schema());
    }

    @Override
    protected ChannelComputer<Tuple2<RowData, Integer>> channelComputer2() {
        return new RowWithBucketChannelComputer(table.schema());
    }

    @Override
    protected SerializableFunction<TableSchema, PartitionKeyExtractor<RowData>>
            extractorFunction() {
        return RowDataPartitionKeyExtractor::new;
    }

    @Override
    protected OneInputStreamOperator<Tuple2<RowData, Integer>, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new DynamicBucketRowWriteOperator(table, writeProvider, commitUser);
    }
}
