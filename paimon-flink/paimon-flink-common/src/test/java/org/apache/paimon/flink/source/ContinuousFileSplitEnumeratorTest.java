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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext.SplitAssignmentState;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link ContinuousFileSplitEnumerator}. */
public class ContinuousFileSplitEnumeratorTest {

    @Test
    public void testSplitAllocationIsOrdered() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(1);
        context.registerReader(0, "test-host");

        List<FileStoreSourceSplit> initialSplits = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            initialSplits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
        }
        List<FileStoreSourceSplit> expectedSplits = new ArrayList<>(initialSplits);
        final ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(initialSplits)
                        .setDiscoveryInterval(3)
                        .build();

        // The first time split is allocated, split1 and split2 should be allocated
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        List<FileStoreSourceSplit> assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).hasSameElementsAs(expectedSplits.subList(0, 2));

        // split1 and split2 is added back
        enumerator.addSplitsBack(assignedSplits, 0);
        context.getSplitAssignments().clear();
        assertThat(context.getSplitAssignments()).isEmpty();

        // The split is allocated for the second time, and split1 is allocated first
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).hasSameElementsAs(expectedSplits.subList(0, 2));

        // continuing to allocate split
        context.getSplitAssignments().clear();
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).hasSameElementsAs(expectedSplits.subList(2, 4));
    }

    @Test
    public void testSplitWithBatch() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(1);
        context.registerReader(0, "test-host");

        List<FileStoreSourceSplit> initialSplits = new ArrayList<>();
        for (int i = 1; i <= 18; i++) {
            initialSplits.add(createSnapshotSplit(i, i, Collections.emptyList()));
        }
        final ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(initialSplits)
                        .setDiscoveryInterval(3)
                        .build();

        // The first time split is allocated, split1 and split2 should be allocated
        enumerator.handleSplitRequest(0, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).getAssignedSplits()).hasSize(18);

        // test second batch assign
        enumerator.handleSplitRequest(0, "test-host");

        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).getAssignedSplits()).hasSize(18);

        // test third batch assign
        enumerator.handleSplitRequest(0, "test-host");

        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).getAssignedSplits()).hasSize(18);
    }

    @Test
    public void testSplitAllocationIsFair() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(1);
        context.registerReader(0, "test-host");

        List<FileStoreSourceSplit> initialSplits = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            initialSplits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
            initialSplits.add(createSnapshotSplit(i, 1, Collections.emptyList()));
        }

        List<FileStoreSourceSplit> expectedSplits = new ArrayList<>(initialSplits);

        final ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(initialSplits)
                        .setDiscoveryInterval(3)
                        .build();

        // each time a split is allocated from bucket-0 and bucket-1
        enumerator.handleSplitRequest(0, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        List<FileStoreSourceSplit> assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).hasSameElementsAs(expectedSplits.subList(0, 2));

        // clear assignments
        context.getSplitAssignments().clear();
        assertThat(context.getSplitAssignments()).isEmpty();

        // continuing to allocate the rest splits
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).hasSameElementsAs(expectedSplits.subList(2, 4));
    }

    @Test
    public void testSnapshotEnumerator() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(2);
        context.registerReader(0, "test-host");
        context.registerReader(1, "test-host");

        Queue<TableScan.Plan> results = new LinkedBlockingQueue<>();
        StreamTableScan scan = new MockScan(results);
        ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(1)
                        .setScan(scan)
                        .build();
        enumerator.start();

        long snapshot = 0;
        List<DataSplit> splits = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            splits.add(createDataSplit(snapshot, i, Collections.emptyList()));
        }
        results.add(new DataFilePlan(splits));
        context.triggerAllActions();

        // assign to task 0
        enumerator.handleSplitRequest(0, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()))
                .containsExactly(splits.get(0), splits.get(2));

        // no more splits task 0
        enumerator.handleSplitRequest(0, "test-host");
        context.triggerAllActions();
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).hasReceivedNoMoreSplitsSignal()).isTrue();
        assignments.clear();

        // assign to task 1
        enumerator.handleSplitRequest(1, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(1);
        assertThat(toDataSplits(assignments.get(1).getAssignedSplits()))
                .containsExactly(splits.get(1), splits.get(3));

        // no more splits task 1
        enumerator.handleSplitRequest(1, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(1);
        assertThat(assignments.get(1).hasReceivedNoMoreSplitsSignal()).isTrue();
    }

    @Test
    public void testUnawareBucketEnumeratorWithBucket() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(3);
        context.registerReader(0, "test-host");

        Queue<TableScan.Plan> results = new LinkedBlockingQueue<>();
        StreamTableScan scan = new MockScan(results);
        ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(1)
                        .setScan(scan)
                        .withBucketMode(BucketMode.UNAWARE)
                        .build();
        enumerator.start();

        long snapshot = 0;
        List<DataSplit> splits = new ArrayList<>();
        splits.add(createDataSplit(snapshot, 1, Collections.emptyList()));
        results.add(new DataFilePlan(splits));
        context.triggerAllActions();

        // assign to task 0
        enumerator.handleSplitRequest(0, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()).size()).isEqualTo(1);

        splits.clear();
        splits.add(createDataSplit(snapshot, 2, Collections.emptyList()));
        results.add(new DataFilePlan(splits));
        context.triggerAllActions();

        // assign to task 0
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()).size()).isEqualTo(2);
    }

    @Test
    public void testUnawareBucketEnumeratorLot() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        context.registerReader(0, "test-host");
        context.registerReader(1, "test-host");
        context.registerReader(2, "test-host");
        context.registerReader(3, "test-host");

        Queue<TableScan.Plan> results = new LinkedBlockingQueue<>();
        StreamTableScan scan = new MockScan(results);
        ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(1)
                        .setScan(scan)
                        .withBucketMode(BucketMode.UNAWARE)
                        .build();
        enumerator.start();

        long snapshot = 0;
        List<DataSplit> splits = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            splits.add(createDataSplit(snapshot, 0, Collections.emptyList()));
        }
        results.add(new DataFilePlan(splits));
        context.triggerAllActions();

        // assign to task 0
        enumerator.handleSplitRequest(0, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()).size()).isEqualTo(1);

        // assign to task 1
        enumerator.handleSplitRequest(1, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1);
        assertThat(toDataSplits(assignments.get(1).getAssignedSplits()).size()).isEqualTo(1);

        // assign to task 2
        enumerator.handleSplitRequest(2, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1, 2);
        assertThat(toDataSplits(assignments.get(2).getAssignedSplits()).size()).isEqualTo(1);

        for (int i = 0; i < 97; i++) {
            enumerator.handleSplitRequest(3, "test-host");
            assignments = context.getSplitAssignments();
            assertThat(assignments).containsOnlyKeys(0, 1, 2, 3);
            assertThat(toDataSplits(assignments.get(3).getAssignedSplits()).size())
                    .isEqualTo(i + 1);
        }

        enumerator.handleSplitRequest(3, "test-host");
        context.triggerAllActions();
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1, 2, 3);
        assertThat(assignments.get(3).hasReceivedNoMoreSplitsSignal()).isTrue();
    }

    private static List<DataSplit> toDataSplits(List<FileStoreSourceSplit> splits) {
        return splits.stream()
                .map(FileStoreSourceSplit::split)
                .map(split -> (DataSplit) split)
                .collect(Collectors.toList());
    }

    public static FileStoreSourceSplit createSnapshotSplit(
            int snapshotId, int bucket, List<DataFileMeta> files) {
        return new FileStoreSourceSplit(
                UUID.randomUUID().toString(),
                new DataSplit(snapshotId, row(1), bucket, files, true),
                0);
    }

    private static DataSplit createDataSplit(
            long snapshotId, int bucket, List<DataFileMeta> files) {
        return new DataSplit(snapshotId, row(1), bucket, files, true);
    }

    private static class Builder {
        private SplitEnumeratorContext<FileStoreSourceSplit> context;
        private Collection<FileStoreSourceSplit> initialSplits = Collections.emptyList();
        private long discoveryInterval = Long.MAX_VALUE;

        private StreamTableScan scan;
        private BucketMode bucketMode = BucketMode.FIXED;

        public Builder setSplitEnumeratorContext(
                SplitEnumeratorContext<FileStoreSourceSplit> context) {
            this.context = context;
            return this;
        }

        public Builder setInitialSplits(Collection<FileStoreSourceSplit> initialSplits) {
            this.initialSplits = initialSplits;
            return this;
        }

        public Builder setDiscoveryInterval(long discoveryInterval) {
            this.discoveryInterval = discoveryInterval;
            return this;
        }

        public Builder setScan(StreamTableScan scan) {
            this.scan = scan;
            return this;
        }

        public Builder withBucketMode(BucketMode bucketMode) {
            this.bucketMode = bucketMode;
            return this;
        }

        public ContinuousFileSplitEnumerator build() {
            return new ContinuousFileSplitEnumerator(
                    context, initialSplits, null, discoveryInterval, scan, bucketMode);
        }
    }

    private static class MockScan implements StreamTableScan {
        private final Queue<Plan> results;

        public MockScan(Queue<Plan> results) {
            this.results = results;
        }

        @Override
        public Plan plan() {
            Plan plan = results.poll();
            if (plan == null) {
                throw new EndOfScanException();
            }
            return plan;
        }

        @Override
        public List<BinaryRow> listPartitions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long checkpoint() {
            return null;
        }

        @Override
        public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {}

        @Nullable
        @Override
        public Long watermark() {
            return null;
        }

        @Override
        public void restore(Long state) {}
    }
}
