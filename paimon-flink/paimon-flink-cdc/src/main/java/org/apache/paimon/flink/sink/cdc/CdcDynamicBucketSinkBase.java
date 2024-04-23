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
import org.apache.paimon.flink.sink.DynamicBucketSink;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.utils.MathUtils;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.java.tuple.Tuple2;

import static org.apache.paimon.index.BucketAssigner.computeAssigner;

/** CDC sink for dynamic bucket table. */
public abstract class CdcDynamicBucketSinkBase<T> extends DynamicBucketSink<T> {

    public CdcDynamicBucketSinkBase(FileStoreTable table) {
        super(table, null);
    }

    @Override
    protected ChannelComputer<T> assignerChannelComputer(Integer numAssigners) {
        return new AssignerChannelComputer(numAssigners);
    }

    @Override
    protected ChannelComputer<Tuple2<T, Integer>> channelComputer2() {
        return new RecordWithBucketChannelComputer();
    }

    @Override
    protected SerializableFunction<TableSchema, PartitionKeyExtractor<T>> extractorFunction() {
        return schema -> {
            KeyAndBucketExtractor<T> extractor = createExtractor(schema);
            return new PartitionKeyExtractor<T>() {
                @Override
                public BinaryRow partition(T record) {
                    extractor.setRecord(record);
                    return extractor.partition();
                }

                @Override
                public BinaryRow trimmedPrimaryKey(T record) {
                    extractor.setRecord(record);
                    return extractor.trimmedPrimaryKey();
                }
            };
        };
    }

    protected abstract KeyAndBucketExtractor<T> createExtractor(TableSchema schema);

    private class AssignerChannelComputer implements ChannelComputer<T> {

        private Integer numAssigners;

        private transient int numChannels;
        private transient KeyAndBucketExtractor<T> extractor;

        public AssignerChannelComputer(Integer numAssigners) {
            this.numAssigners = numAssigners;
        }

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
            this.numAssigners = MathUtils.min(numAssigners, numChannels);
            this.extractor = createExtractor(table.schema());
        }

        @Override
        public int channel(T record) {
            extractor.setRecord(record);
            int partitionHash = extractor.partition().hashCode();
            int keyHash = extractor.trimmedPrimaryKey().hashCode();
            return computeAssigner(partitionHash, keyHash, numChannels, numAssigners);
        }

        @Override
        public String toString() {
            return "shuffle by key hash";
        }
    }

    private class RecordWithBucketChannelComputer implements ChannelComputer<Tuple2<T, Integer>> {

        private transient int numChannels;
        private transient KeyAndBucketExtractor<T> extractor;

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
            this.extractor = createExtractor(table.schema());
        }

        @Override
        public int channel(Tuple2<T, Integer> record) {
            extractor.setRecord(record.f0);
            return ChannelComputer.select(extractor.partition(), record.f1, numChannels);
        }

        @Override
        public String toString() {
            return "shuffle by partition & bucket";
        }
    }
}
