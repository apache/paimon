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
import org.apache.paimon.metrics.Gauge;
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

    public static final String WRITE_COST_MS = "writeCostMS";

    private static final String FLUSH_COST_MS = "flushCostMS";

    public static final String PREPARE_COMMIT_COST = "prepareCommitCostMS";

    public static final String SYNC_LASTEST_COMPACTION_COST_MS = "syncLastestCompactionCostMS";

    private final Counter writeRecordNumCounter;

    private final Gauge<Long> memoryPreemptCount;

    private final Gauge<Long> usedWriteBufferSizeGauge;

    private final Gauge<Long> totalWriteBufferSizeGauge;

    private final Histogram writeCostMS;

    private final Histogram bufferFlushCostMS;

    private final Histogram prepareCommitCostMS;

    private final Histogram syncLastestCompactionCostMS;

    private Stats stats;

    public WriterMetrics(MetricRegistry registry, String tableName, String commitUser) {
        stats = new Stats();
        MetricGroup metricGroup = registry.tableMetricGroup(GROUP_NAME, tableName, commitUser);
        writeRecordNumCounter = metricGroup.counter(WRITE_RECORD_NUM);

        // buffer
        memoryPreemptCount =
                metricGroup.gauge(BUFFER_PREEMPT_COUNT, () -> stats.bufferPreemptCount.get());

        usedWriteBufferSizeGauge =
                metricGroup.gauge(USED_WRITE_BUFFER_SIZE, () -> stats.usedWriteBufferSize.get());

        totalWriteBufferSizeGauge =
                metricGroup.gauge(TOTAL_WRITE_BUFFER_SIZE, () -> stats.totalWriteBufferSize.get());

        // cost
        writeCostMS = metricGroup.histogram(WRITE_COST_MS, WINDOW_SAMPLE_SIZE);
        bufferFlushCostMS = metricGroup.histogram(FLUSH_COST_MS, WINDOW_SAMPLE_SIZE);

        // prepareCommittime
        prepareCommitCostMS = metricGroup.histogram(PREPARE_COMMIT_COST, WINDOW_SAMPLE_SIZE);

        syncLastestCompactionCostMS =
                metricGroup.histogram(SYNC_LASTEST_COMPACTION_COST_MS, WINDOW_SAMPLE_SIZE);
    }

    public void incWriteRecordNum() {
        writeRecordNumCounter.inc();
    }

    public void updateWriteCostMS(long bufferAppendCost) {
        writeCostMS.update(bufferAppendCost);
    }

    public void updateBufferFlushCostMS(long bufferFlushCost) {
        bufferFlushCostMS.update(bufferFlushCost);
    }

    public void updatePrepareCommitCostMS(long cost) {
        this.prepareCommitCostMS.update(cost);
    }

    public void updateSyncLastestCompactionCostMS(long cost) {
        this.syncLastestCompactionCostMS.update(cost);
    }

    public void setMemoryPreemptCount(Supplier<Long> bufferPreemptNumSupplier) {
        this.stats.bufferPreemptCount = bufferPreemptNumSupplier;
    }

    public void setUsedWriteBufferSize(Supplier<Long> usedWriteBufferSize) {
        this.stats.usedWriteBufferSize = usedWriteBufferSize;
    }

    public void setTotaldWriteBufferSize(Supplier<Long> totaldWriteBufferSize) {
        this.stats.totalWriteBufferSize = totaldWriteBufferSize;
    }

    /** buffer stat for metric. */
    public class Stats {
        private Supplier<Long> bufferPreemptCount = () -> -1L;
        private Supplier<Long> totalWriteBufferSize = () -> -1L;
        private Supplier<Long> usedWriteBufferSize = () -> -1L;
    }
}
