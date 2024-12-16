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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import javax.annotation.Nullable;

import java.util.Map;

/** Sink for dynamic bucket table. */
public class RowDynamicBucketSink extends DynamicBucketSink<InternalRow> {

    private static final long serialVersionUID = 1L;

    public RowDynamicBucketSink(
            FileStoreTable table, @Nullable Map<String, String> overwritePartition) {
        super(table, overwritePartition);
    }

    @Override
    protected ChannelComputer<InternalRow> assignerChannelComputer(Integer numAssigners) {
        return new RowAssignerChannelComputer(table.schema(), numAssigners);
    }

    @Override
    protected ChannelComputer<Tuple2<InternalRow, Integer>> channelComputer2() {
        return new RowWithBucketChannelComputer(table.schema());
    }

    @Override
    protected SerializableFunction<TableSchema, PartitionKeyExtractor<InternalRow>>
            extractorFunction() {
        return RowPartitionKeyExtractor::new;
    }

    @Override
    protected OneInputStreamOperatorFactory<Tuple2<InternalRow, Integer>, Committable>
            createWriteOperatorFactory(StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new DynamicBucketRowWriteOperator.Factory(table, writeProvider, commitUser);
    }
}
