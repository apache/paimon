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

package org.apache.paimon.flink.source;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.SnapshotNotExistPlan;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.snapshot.StartingContext;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.source.coordinator.ExecutorNotifier;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFileTestUtils.row;

/** Base class for split enumerators. */
public abstract class FileSplitEnumeratorTestBase<SplitT extends FileStoreSourceSplit> {

    protected TestingSplitEnumeratorContext<SplitT> getSplitEnumeratorContext(int parallelism) {
        return getSplitEnumeratorContext(parallelism, parallelism);
    }

    protected TestingSplitEnumeratorContext<SplitT> getSplitEnumeratorContext(
            int parallelism, int registeredReaders) {
        final TestingSplitEnumeratorContext<SplitT> context =
                new TestingSplitEnumeratorContext<>(parallelism);
        for (int i = 0; i < registeredReaders; i++) {
            context.registerReader(i, "test-host");
        }
        return context;
    }

    protected SplitT createSnapshotSplit(int snapshotId, int bucket, List<DataFileMeta> files) {
        return createSnapshotSplit(snapshotId, bucket, files, 1);
    }

    protected abstract SplitT createSnapshotSplit(
            int snapshotId, int bucket, List<DataFileMeta> files, int... partitions);

    protected void scanNextSnapshot(TestingAsyncSplitEnumeratorContext<SplitT> context) {
        context.workerExecutor.triggerPeriodicScheduledTasks();
        context.triggerAlCoordinatorAction();
    }

    protected List<DataSplit> toDataSplits(Collection<SplitT> splits) {
        return splits.stream()
                .map(SplitT::split)
                .map(split -> (DataSplit) split)
                .collect(Collectors.toList());
    }

    protected static DataSplit createDataSplit(
            long snapshotId, int bucket, List<DataFileMeta> files) {
        return DataSplit.builder()
                .withSnapshot(snapshotId)
                .withPartition(row(1))
                .withBucket(bucket)
                .withDataFiles(files)
                .isStreaming(true)
                .rawConvertible(false)
                .withBucketPath("") // not used
                .build();
    }

    /** A mock {@link StreamTableScan} that can manually specify generated plans. */
    protected static class MockScan implements StreamDataTableScan {

        private final TreeMap<Long, Plan> results;
        private @Nullable Long nextSnapshotId;
        private boolean allowEnd = true;
        private Long nextSnapshotIdForConsumer;

        public MockScan(TreeMap<Long, Plan> results) {
            this.results = results;
            this.nextSnapshotId = null;
            this.nextSnapshotIdForConsumer = null;
        }

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InnerTableScan withMetricRegistry(MetricRegistry registry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Plan plan() {
            Map.Entry<Long, Plan> planEntry = results.pollFirstEntry();
            if (planEntry == null) {
                if (allowEnd) {
                    throw new EndOfScanException();
                } else {
                    return SnapshotNotExistPlan.INSTANCE;
                }
            }
            nextSnapshotId = planEntry.getKey() + 1;
            return planEntry.getValue();
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long checkpoint() {
            return nextSnapshotId;
        }

        @Override
        public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {
            nextSnapshotIdForConsumer = nextSnapshot;
        }

        @Nullable
        @Override
        public Long watermark() {
            return null;
        }

        @Override
        public void restore(Long state) {}

        public void allowEnd(boolean allowEnd) {
            this.allowEnd = allowEnd;
        }

        public Long getNextSnapshotIdForConsumer() {
            return nextSnapshotIdForConsumer;
        }

        @Override
        public StartingContext startingContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void restore(@Nullable Long nextSnapshotId, boolean scanAllSnapshot) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A {@link TestingSplitEnumeratorContext} that supports manually controlling asynchronous
     * calls.
     */
    protected static class TestingAsyncSplitEnumeratorContext<SplitT extends SourceSplit>
            extends TestingSplitEnumeratorContext<SplitT> {

        private final ManuallyTriggeredScheduledExecutorService workerExecutor;
        private final ExecutorNotifier notifier;

        public TestingAsyncSplitEnumeratorContext(int parallelism) {
            super(parallelism);
            this.workerExecutor = new ManuallyTriggeredScheduledExecutorService();
            this.notifier = new ExecutorNotifier(workerExecutor, super.getExecutorService());
        }

        @Override
        public <SplitT> void callAsync(
                Callable<SplitT> callable, BiConsumer<SplitT, Throwable> handler) {
            notifier.notifyReadyAsync(callable, handler);
        }

        @Override
        public <SplitT> void callAsync(
                Callable<SplitT> callable,
                BiConsumer<SplitT, Throwable> handler,
                long initialDelay,
                long period) {
            notifier.notifyReadyAsync(callable, handler, initialDelay, period);
        }

        public void triggerAllWorkerAction() {
            this.workerExecutor.triggerPeriodicScheduledTasks();
            this.workerExecutor.triggerAll();
        }

        public void triggerAlCoordinatorAction() {
            super.triggerAllActions();
        }

        public void triggerNextCoordinatorAction() {
            super.getExecutorService().trigger();
        }
    }
}
