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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.SerializableFunction;

import java.io.Serializable;

/**
 * A utility class to compute which downstream channel a given record should be sent to.
 *
 * @param <T> type of record
 */
public interface ChannelComputer<T> extends Serializable {

    void setup(int numChannels);

    int channel(T record);

    static int select(BinaryRow partition, int bucket, int numChannels) {
        return (startChannel(partition, numChannels) + bucket) % numChannels;
    }

    static int select(int bucket, int numChannels) {
        return bucket % numChannels;
    }

    static int startChannel(BinaryRow partition, int numChannels) {
        int hashCode = partition.hashCode();
        if (hashCode == Integer.MIN_VALUE) {
            hashCode = Integer.MAX_VALUE;
        }
        // Due to backward compatibility (Flink users may recover from state),
        // we need to use this formula.
        // However, if hashCode equals Integer.MIN_VALUE,
        // Math.abs will still return Integer.MIN_VALUE,
        // and this formula will produce a negative integer.
        // So we specially handle this case above.
        return Math.abs(hashCode) % numChannels;
    }

    static <T, R> ChannelComputer<R> transform(
            ChannelComputer<T> input, SerializableFunction<R, T> converter) {
        return new ChannelComputer<R>() {
            @Override
            public void setup(int numChannels) {
                input.setup(numChannels);
            }

            @Override
            public int channel(R record) {
                return input.channel(converter.apply(record));
            }
        };
    }
}
