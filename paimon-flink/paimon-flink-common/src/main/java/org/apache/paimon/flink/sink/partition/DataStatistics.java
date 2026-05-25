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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.data.BinaryRow;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Data statistics tracking partition key to frequency/weight. */
public class DataStatistics {

    private final Map<BinaryRow, Long> partitionFrequency;

    public DataStatistics() {
        this.partitionFrequency = new HashMap<>();
    }

    public DataStatistics(Map<BinaryRow, Long> partitionFrequency) {
        this.partitionFrequency = partitionFrequency;
    }

    public boolean isEmpty() {
        return partitionFrequency.isEmpty();
    }

    public void add(BinaryRow partition, long value) {
        partitionFrequency.merge(partition, value, Long::sum);
    }

    public Map<BinaryRow, Long> result() {
        return partitionFrequency;
    }

    @Override
    public String toString() {
        return "DataStatistics{" + "partitionFrequency=" + partitionFrequency + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DataStatistics)) {
            return false;
        }
        DataStatistics that = (DataStatistics) o;
        return Objects.equals(partitionFrequency, that.partitionFrequency);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitionFrequency);
    }
}
