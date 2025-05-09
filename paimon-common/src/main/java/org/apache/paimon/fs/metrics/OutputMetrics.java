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

package org.apache.paimon.fs.metrics;

import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;

/** Collects and monitors outp stream metrics. */
public class OutputMetrics {
    public static final String GROUP_NAME = "sink";
    public static final String WRITE_BYTES = "write.bytes";
    public static final String WRITE_OPERATIONS = "write.operations";
    private final MetricGroup metricGroup;
    private final AtomicLong writeBytes = new AtomicLong(0);
    private final AtomicLong writeOperations = new AtomicLong(0);

    public OutputMetrics(MetricRegistry registry, String tableName) {
        metricGroup = registry.createTableMetricGroup(GROUP_NAME, tableName);
        registerMetrics();
    }

    private void registerMetrics() {
        metricGroup.gauge(WRITE_BYTES, this::getWriteBytes);
        metricGroup.gauge(WRITE_OPERATIONS, this::getWriteOperations);
    }

    public void recordWriteEvent(long bytes) {
        writeBytes.addAndGet(bytes);
        writeOperations.incrementAndGet();
    }

    public long getWriteBytes() {
        return writeBytes.get();
    }

    public long getWriteOperations() {
        return writeOperations.get();
    }
}
