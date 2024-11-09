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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/** Metrics to measure a compaction. */
public class CompactionMetrics {

    private static final String GROUP_NAME = "compaction";

    public static final String MAX_LEVEL0_FILE_COUNT = "maxLevel0FileCount";
    public static final String AVG_LEVEL0_FILE_COUNT = "avgLevel0FileCount";
    public static final String COMPACTION_THREAD_BUSY = "compactionThreadBusy";
    public static final String AVG_COMPACTION_TIME = "avgCompactionTime";
    public static final String COMPACTION_COMPLETED_COUNT = "compactionCompletedCount";
    public static final String COMPACTION_QUEUED_COUNT = "compactionQueuedCount";
    public static final String COMPACTION_DROP_DELETED_RECORD_COUNT =
            "compactionDropDeletedRecordCount";
    public static final String MAX_COMPACTION_INPUT_SIZE = "maxCompactionInputSize";
    public static final String MAX_COMPACTION_OUTPUT_SIZE = "maxCompactionOutputSize";
    public static final String AVG_COMPACTION_INPUT_SIZE = "avgCompactionInputSize";
    public static final String AVG_COMPACTION_OUTPUT_SIZE = "avgCompactionOutputSize";
    private static final long BUSY_MEASURE_MILLIS = 60_000;
    private static final int COMPACTION_TIME_WINDOW = 100;

    private final MetricGroup metricGroup;
    private final Map<PartitionAndBucket, ReporterImpl> reporters;
    private final Map<Long, CompactTimer> compactTimers;
    private final Queue<Long> compactionTimes;
    private Counter compactionCompletedCounter;
    private Counter compactionQueuedCounter;
    private Counter compactionDropDeletedRecordCounter;

    public CompactionMetrics(MetricRegistry registry, String tableName) {
        this.metricGroup = registry.tableMetricGroup(GROUP_NAME, tableName);
        this.reporters = new HashMap<>();
        this.compactTimers = new ConcurrentHashMap<>();
        this.compactionTimes = new ConcurrentLinkedQueue<>();

        registerGenericCompactionMetrics();
    }

    @VisibleForTesting
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    private void registerGenericCompactionMetrics() {
        metricGroup.gauge(MAX_LEVEL0_FILE_COUNT, () -> getLevel0FileCountStream().max().orElse(-1));
        metricGroup.gauge(
                AVG_LEVEL0_FILE_COUNT, () -> getLevel0FileCountStream().average().orElse(-1));
        metricGroup.gauge(
                MAX_COMPACTION_INPUT_SIZE, () -> getCompactionInputSizeStream().max().orElse(-1));
        metricGroup.gauge(
                MAX_COMPACTION_OUTPUT_SIZE, () -> getCompactionOutputSizeStream().max().orElse(-1));
        metricGroup.gauge(
                AVG_COMPACTION_INPUT_SIZE,
                () -> getCompactionInputSizeStream().average().orElse(-1));
        metricGroup.gauge(
                AVG_COMPACTION_OUTPUT_SIZE,
                () -> getCompactionOutputSizeStream().average().orElse(-1));

        metricGroup.gauge(
                AVG_COMPACTION_TIME, () -> getCompactionTimeStream().average().orElse(0.0));
        metricGroup.gauge(COMPACTION_THREAD_BUSY, () -> getCompactBusyStream().sum());

        compactionCompletedCounter = metricGroup.counter(COMPACTION_COMPLETED_COUNT);
        compactionQueuedCounter = metricGroup.counter(COMPACTION_QUEUED_COUNT);
        compactionDropDeletedRecordCounter =
                metricGroup.counter(COMPACTION_DROP_DELETED_RECORD_COUNT);
    }

    private LongStream getLevel0FileCountStream() {
        return reporters.values().stream().mapToLong(r -> r.level0FileCount);
    }

    private LongStream getCompactionInputSizeStream() {
        return reporters.values().stream().mapToLong(r -> r.compactionInputSize);
    }

    private LongStream getCompactionOutputSizeStream() {
        return reporters.values().stream().mapToLong(r -> r.compactionOutputSize);
    }

    private DoubleStream getCompactBusyStream() {
        return compactTimers.values().stream()
                .mapToDouble(t -> 100.0 * t.calculateLength() / BUSY_MEASURE_MILLIS);
    }

    private DoubleStream getCompactionTimeStream() {
        return compactionTimes.stream().mapToDouble(Long::doubleValue);
    }

    public void close() {
        metricGroup.close();
    }

    /** Report metrics value to the {@link CompactionMetrics} object. */
    public interface Reporter {

        CompactTimer getCompactTimer();

        void reportLevel0FileCount(long count);

        void reportCompactionTime(long time);

        void increaseCompactionsCompletedCount();

        void increaseCompactionsQueuedCount();

        void decreaseCompactionsQueuedCount();

        void reportDropDeletedRecordCount(long dropDeletedRecordCount);

        void reportCompactionInputSize(long bytes);

        void reportCompactionOutputSize(long bytes);

        void unregister();
    }

    private class ReporterImpl implements Reporter {

        private final PartitionAndBucket key;
        private long level0FileCount;
        private long compactionInputSize = 0;
        private long compactionOutputSize = 0;

        private ReporterImpl(PartitionAndBucket key) {
            this.key = key;
            this.level0FileCount = 0;
        }

        @Override
        public CompactTimer getCompactTimer() {
            return compactTimers.computeIfAbsent(
                    Thread.currentThread().getId(),
                    ignore -> new CompactTimer(BUSY_MEASURE_MILLIS));
        }

        @Override
        public void reportCompactionTime(long time) {
            synchronized (compactionTimes) {
                compactionTimes.add(time);
                if (compactionTimes.size() > COMPACTION_TIME_WINDOW) {
                    compactionTimes.poll();
                }
            }
        }

        @Override
        public void reportCompactionInputSize(long bytes) {
            this.compactionInputSize = bytes;
        }

        @Override
        public void reportCompactionOutputSize(long bytes) {
            this.compactionOutputSize = bytes;
        }

        @Override
        public void reportLevel0FileCount(long count) {
            this.level0FileCount = count;
        }

        @Override
        public void increaseCompactionsCompletedCount() {
            compactionCompletedCounter.inc();
        }

        @Override
        public void increaseCompactionsQueuedCount() {
            compactionQueuedCounter.inc();
        }

        @Override
        public void decreaseCompactionsQueuedCount() {
            compactionQueuedCounter.dec();
        }

        @Override
        public void reportDropDeletedRecordCount(long dropDeletedRecordCount) {
            compactionDropDeletedRecordCounter.inc(dropDeletedRecordCount);
        }

        @Override
        public void unregister() {
            reporters.remove(key);
        }
    }

    public Reporter createReporter(BinaryRow partition, int bucket) {
        PartitionAndBucket key = new PartitionAndBucket(partition, bucket);
        ReporterImpl reporter = new ReporterImpl(key);
        reporters.put(key, reporter);
        return reporter;
    }

    private static class PartitionAndBucket {

        private final BinaryRow partition;
        private final int bucket;

        private PartitionAndBucket(BinaryRow partition, int bucket) {
            this.partition = partition;
            this.bucket = bucket;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartitionAndBucket)) {
                return false;
            }
            PartitionAndBucket other = (PartitionAndBucket) o;
            return Objects.equals(partition, other.partition) && bucket == other.bucket;
        }
    }
}
