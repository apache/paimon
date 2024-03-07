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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

import java.util.function.Function;
import java.util.function.Supplier;

/** Metrics for writer buffer. */
public class WriterBufferMetric {

    private static final String GROUP_NAME = "writerBuffer";
    private static final String BUFFER_PREEMPT_COUNT = "bufferPreemptCount";
    private static final String USED_WRITE_BUFFER_SIZE = "usedWriteBufferSizeByte";
    private static final String TOTAL_WRITE_BUFFER_SIZE = "totalWriteBufferSizeByte";

    private final MetricGroup metricGroup;

    public WriterBufferMetric(
            Supplier<MemoryPoolFactory> memoryPoolFactorySupplier,
            MetricRegistry metricRegistry,
            String tableName) {
        metricGroup = metricRegistry.tableMetricGroup(GROUP_NAME, tableName);
        metricGroup.gauge(
                BUFFER_PREEMPT_COUNT,
                () ->
                        getMetricValue(
                                memoryPoolFactorySupplier, MemoryPoolFactory::bufferPreemptCount));
        metricGroup.gauge(
                USED_WRITE_BUFFER_SIZE,
                () -> getMetricValue(memoryPoolFactorySupplier, MemoryPoolFactory::usedBufferSize));
        metricGroup.gauge(
                TOTAL_WRITE_BUFFER_SIZE,
                () ->
                        getMetricValue(
                                memoryPoolFactorySupplier, MemoryPoolFactory::totalBufferSize));
    }

    private long getMetricValue(
            Supplier<MemoryPoolFactory> memoryPoolFactorySupplier,
            Function<MemoryPoolFactory, Long> function) {
        MemoryPoolFactory memoryPoolFactory = memoryPoolFactorySupplier.get();
        return memoryPoolFactory == null ? -1 : function.apply(memoryPoolFactory);
    }

    public void close() {
        this.metricGroup.close();
    }
}
