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

import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

import java.util.function.Supplier;

/** Metrics for writer. */
public class WriterMetrics {

    private static final String GROUP_NAME = "writer";

    private static final int WINDOW_SAMPLE_SIZE = 10000;
    private static final String WRITE_RECORD_NUM = "writeRecordCount";

    private static final String BUFFER_PREEMPT_COUNT = "bufferPreemptCount";

    private static final String USED_WRITE_BUFFER_SIZE = "usedWriteBufferSizeByte";

    private static final String TOTAL_WRITE_BUFFER_SIZE = "totalWriteBufferSizeByte";

    private static final String FLUSH_COST_MILLIS = "flushCostMillis";

    public static final String PREPARE_COMMIT_COST_MILLIS = "prepareCommitCostMillis";

    private final Counter writeRecordNumCounter;

    private final Histogram bufferFlushCostMillis;

    private final Histogram prepareCommitCostMillis;

    private MetricGroup metricGroup;

    public WriterMetrics(MetricRegistry registry, String tableName, String commitUser) {
        metricGroup = registry.tableMetricGroup(GROUP_NAME, tableName, commitUser);
        writeRecordNumCounter = metricGroup.counter(WRITE_RECORD_NUM);

        // cost
        bufferFlushCostMillis = metricGroup.histogram(FLUSH_COST_MILLIS, WINDOW_SAMPLE_SIZE);

        // prepareCommittime
        prepareCommitCostMillis =
                metricGroup.histogram(PREPARE_COMMIT_COST_MILLIS, WINDOW_SAMPLE_SIZE);
    }

    public void incWriteRecordNum() {
        writeRecordNumCounter.inc();
    }

    public void updateBufferFlushCostMS(long bufferFlushCost) {
        bufferFlushCostMillis.update(bufferFlushCost);
    }

    public void updatePrepareCommitCostMS(long cost) {
        this.prepareCommitCostMillis.update(cost);
    }

    public void setMemoryPreemptCount(Supplier<Long> bufferPreemptNumSupplier) {
        metricGroup.gauge(BUFFER_PREEMPT_COUNT, bufferPreemptNumSupplier::get);
    }

    public void setUsedWriteBufferSize(Supplier<Long> usedWriteBufferSize) {
        metricGroup.gauge(USED_WRITE_BUFFER_SIZE, usedWriteBufferSize::get);
    }

    public void setTotaldWriteBufferSize(Supplier<Long> totaldWriteBufferSize) {
        metricGroup.gauge(TOTAL_WRITE_BUFFER_SIZE, totaldWriteBufferSize::get);
    }
}
