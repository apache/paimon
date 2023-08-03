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
        int startChannel = Math.abs(partition.hashCode()) % numChannels;
        return (startChannel + bucket) % numChannels;
    }

    static int select(int bucket, int numChannels) {
        return bucket % numChannels;
    }

    static <T> ChannelComputer<T> channelComputer(SerializableFunction<T, BinaryRow> keyExtractor) {
        return new KeyChannelComputer<>(keyExtractor);
    }

    class KeyChannelComputer<T> implements ChannelComputer<T> {

        private static final long serialVersionUID = 1L;

        private final SerializableFunction<T, BinaryRow> keyExtractor;

        private transient int numChannels;

        private KeyChannelComputer(SerializableFunction<T, BinaryRow> keyExtractor) {
            this.keyExtractor = keyExtractor;
        }

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
        }

        @Override
        public int channel(T record) {
            return Math.abs(keyExtractor.apply(record).hashCode() % numChannels);
        }
    }
}
