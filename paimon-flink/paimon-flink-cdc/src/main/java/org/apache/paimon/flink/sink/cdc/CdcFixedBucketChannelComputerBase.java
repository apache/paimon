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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;

/**
 * {@link ChannelComputer} for distributing CDC records into writers for fixed-bucket mode tables.
 */
public abstract class CdcFixedBucketChannelComputerBase<T> implements ChannelComputer<T> {

    protected final TableSchema schema;

    private transient int numChannels;
    private transient KeyAndBucketExtractor<T> extractor;

    public CdcFixedBucketChannelComputerBase(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.extractor = createExtractor();
    }

    protected abstract KeyAndBucketExtractor<T> createExtractor();

    @Override
    public int channel(T record) {
        extractor.setRecord(record);
        return channel(extractor.partition(), extractor.bucket());
    }

    @VisibleForTesting
    int channel(BinaryRow partition, int bucket) {
        return ChannelComputer.select(partition, bucket, numChannels);
    }

    @Override
    public String toString() {
        return "shuffle by bucket";
    }
}
