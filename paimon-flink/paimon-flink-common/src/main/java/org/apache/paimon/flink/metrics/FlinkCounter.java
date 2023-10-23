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

package org.apache.paimon.flink.metrics;

import org.apache.paimon.metrics.Counter;

/** {@link Counter} which wraps a Flink's {@link org.apache.flink.metrics.Counter}. */
public class FlinkCounter implements Counter {

    private final org.apache.flink.metrics.Counter wrapped;

    public FlinkCounter(org.apache.flink.metrics.Counter wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void inc() {
        wrapped.inc();
    }

    @Override
    public void inc(long n) {
        wrapped.inc(n);
    }

    @Override
    public void dec() {
        wrapped.dec();
    }

    @Override
    public void dec(long n) {
        wrapped.dec(n);
    }

    @Override
    public long getCount() {
        return wrapped.getCount();
    }
}
