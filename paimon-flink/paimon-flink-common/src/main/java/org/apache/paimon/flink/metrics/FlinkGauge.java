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

import org.apache.paimon.metrics.Gauge;

/** {@link Gauge} which wraps a Flink's {@link org.apache.flink.metrics.Gauge}. */
public class FlinkGauge<T> implements Gauge<T> {

    private final org.apache.flink.metrics.Gauge<T> wrapped;

    public FlinkGauge(org.apache.flink.metrics.Gauge<T> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public T getValue() {
        return wrapped.getValue();
    }
}
