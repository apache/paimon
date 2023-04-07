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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;

import java.io.Serializable;
import java.util.Objects;

/**
 * A utility class to compute which downstream channel a given record should be sent to.
 *
 * @param <T> type of record
 */
public abstract class AbstractChannelComputer<T> {

    private final int numChannels;
    private final KeyAndBucketExtractor<T> extractor;
    protected final boolean shuffleByPartitionEnable;

    public AbstractChannelComputer(
            int numChannels, KeyAndBucketExtractor<T> extractor, boolean shuffleByPartitionEnable) {
        this.numChannels = numChannels;
        this.extractor = extractor;
        this.shuffleByPartitionEnable = shuffleByPartitionEnable;
    }

    public abstract int channel(T record);

    protected int channelImpl(T record, Object... otherChannelKeys) {
        extractor.setRecord(record);
        int bucket = extractor.bucket();
        int otherChannelKeysHash = Objects.hash(otherChannelKeys);

        if (shuffleByPartitionEnable) {
            BinaryRow partition = extractor.partition();
            return Math.abs(Objects.hash(bucket, partition, otherChannelKeysHash)) % numChannels;
        } else {
            return Math.abs(Objects.hash(bucket, otherChannelKeysHash)) % numChannels;
        }
    }

    /**
     * Provider of {@link AbstractChannelComputer}.
     *
     * @param <T> type of record
     */
    public interface Provider<T> extends Serializable {

        AbstractChannelComputer<T> provide(int numChannels);

        String toString();
    }
}
