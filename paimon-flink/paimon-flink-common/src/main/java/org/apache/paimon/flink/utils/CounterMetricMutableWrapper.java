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

package org.apache.paimon.flink.utils;

import org.apache.flink.metrics.Counter;

/**
 * {@link Counter} for getting the current value of a Paimon {@link
 * org.apache.paimon.metrics.Counter} metric.
 */
public class CounterMetricMutableWrapper implements Counter {
    private org.apache.paimon.metrics.Counter counter;

    public CounterMetricMutableWrapper(org.apache.paimon.metrics.Counter counter) {
        this.counter = counter;
    }

    @Override
    public void inc() {
        throw new UnsupportedOperationException(
                "Inc operation is not supported in the mutable flink metrics wrapper.");
    }

    @Override
    public void inc(long l) {
        throw new UnsupportedOperationException(
                "Inc operation is not supported in the mutable flink metrics wrapper.");
    }

    @Override
    public void dec() {
        throw new UnsupportedOperationException(
                "Dec operation is not supported in the mutable flink metrics wrapper.");
    }

    @Override
    public void dec(long l) {
        throw new UnsupportedOperationException(
                "Dec operation is not supported in the mutable flink metrics wrapper.");
    }

    @Override
    public long getCount() {
        return counter.getCount();
    }
}
