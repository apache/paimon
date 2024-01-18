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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.DynamicBucketSink;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

/** Sink for dynamic bucket table. */
public class CdcDynamicBucketSink extends DynamicBucketSink<CdcRecord> {

    private static final long serialVersionUID = 1L;

    public CdcDynamicBucketSink(FileStoreTable table) {
        super(table, null);
    }

    @Override
    protected ChannelComputer<CdcRecord> assignerChannelComputer(Integer numAssigners) {
        return new CdcAssignerChannelComputer(table.schema(), numAssigners);
    }

    @Override
    protected ChannelComputer<Tuple2<CdcRecord, Integer>> channelComputer2() {
        return new CdcWithBucketChannelComputer(table.schema());
    }

    @Override
    protected SerializableFunction<TableSchema, PartitionKeyExtractor<CdcRecord>>
            extractorFunction() {
        return schema -> {
            CdcRecordKeyAndBucketExtractor extractor = new CdcRecordKeyAndBucketExtractor(schema);
            return new PartitionKeyExtractor<CdcRecord>() {
                @Override
                public BinaryRow partition(CdcRecord record) {
                    extractor.setRecord(record);
                    return extractor.partition();
                }

                @Override
                public BinaryRow trimmedPrimaryKey(CdcRecord record) {
                    extractor.setRecord(record);
                    return extractor.trimmedPrimaryKey();
                }
            };
        };
    }

    @Override
    protected OneInputStreamOperator<Tuple2<CdcRecord, Integer>, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new CdcDynamicBucketWriteOperator(table, writeProvider, commitUser);
    }
}
