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
import org.apache.paimon.table.source.SnapshotNotExistPlan;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.source.coordinator.ExecutorNotifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext.SplitAssignmentState;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Unit tests for the {@link ContinuousFileSplitEnumerator}. */
public class ContinuousFileSplitEnumeratorTest extends FileSplitEnumeratorTestBase {

    @Test
    public void testSplitAllocationIsOrdered() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(1);

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
                getSplitEnumeratorContext(1);

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
        assertThat(assignments.get(0).getAssignedSplits()).hasSize(1);

        // test second batch assign
        enumerator.handleSplitRequest(0, "test-host");

        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).getAssignedSplits()).hasSize(2);

        // test third batch assign
        enumerator.handleSplitRequest(0, "test-host");

        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).getAssignedSplits()).hasSize(3);
    }

    @Test
    public void testSplitAllocationIsFair() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(1);

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
                getSplitEnumeratorContext(2);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
        MockScan scan = new MockScan(results);
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
        results.put(1L, new DataFilePlan(splits));
        context.triggerAllActions();

        // assign to task 0
        enumerator.handleSplitRequest(0, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()))
                .containsExactly(splits.get(0));

        // assign to task 0
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
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
                .containsExactly(splits.get(1));

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
                getSplitEnumeratorContext(3, 1);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
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
        results.put(1L, new DataFilePlan(splits));
        context.triggerAllActions();

        // assign to task 0
        enumerator.handleSplitRequest(0, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()).size()).isEqualTo(1);

        splits.clear();
        splits.add(createDataSplit(snapshot, 2, Collections.emptyList()));
        results.put(2L, new DataFilePlan(splits));
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
                getSplitEnumeratorContext(4);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
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
        results.put(1L, new DataFilePlan(splits));
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

    @Test
    public void testUnawareBucketEnumeratorAssignLater() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(4);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
        MockScan scan = new MockScan(results);
        ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(1)
                        .setScan(scan)
                        .withBucketMode(BucketMode.UNAWARE)
                        .build();
        enumerator.start();

        // assign to task 0, but no assigned. add to wait list
        scan.allowEnd(false);
        enumerator.handleSplitRequest(0, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments.size()).isEqualTo(0);

        // assign to task 1, but no assigned. add to wait list
        enumerator.handleSplitRequest(1, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments.size()).isEqualTo(0);

        long snapshot = 0;
        List<DataSplit> splits = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            splits.add(createDataSplit(snapshot, 0, Collections.emptyList()));
        }
        results.put(1L, new DataFilePlan(splits));
        // trigger assign task 0 and task 1 will get their assignment
        context.triggerAllActions();

        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1);
        assertThat(assignments.get(0).getAssignedSplits().size()).isEqualTo(1);
        assertThat(assignments.get(1).getAssignedSplits().size()).isEqualTo(1);

        // assign to task 2
        enumerator.handleSplitRequest(2, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1, 2);
        assertThat(toDataSplits(assignments.get(2).getAssignedSplits()).size()).isEqualTo(1);

        // assign to task 3
        enumerator.handleSplitRequest(3, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1, 2, 3);
        assertThat(toDataSplits(assignments.get(3).getAssignedSplits()).size()).isEqualTo(1);
    }

    @Test
    public void testEnumeratorDeregisteredByContext() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(2);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
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
        for (int i = 0; i < 4; i++) {
            splits.add(createDataSplit(snapshot, i, Collections.emptyList()));
        }
        results.put(1L, new DataFilePlan(splits));
        context.triggerAllActions();

        // assign to task 0
        context.registeredReaders().remove(0);
        enumerator.handleSplitRequest(0, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments.size()).isEqualTo(0);

        // assign to task 1
        enumerator.handleSplitRequest(1, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(1);
        assertThat(toDataSplits(assignments.get(1).getAssignedSplits()).size()).isEqualTo(1);
    }

    @Test
    public void testRemoveReadersAwaitSuccessful() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(2);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
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
        enumerator.handleSplitRequest(1, "test-host");

        long snapshot = 0;
        List<DataSplit> splits = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            splits.add(createDataSplit(snapshot, i, Collections.emptyList()));
        }
        results.put(1L, new DataFilePlan(splits));

        context.registeredReaders().remove(1);
        // assign to task 0
        assertThatCode(() -> enumerator.handleSplitRequest(0, "test-host"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testTriggerScanByTaskRequest() throws Exception {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(2);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
        MockScan scan = new MockScan(results);
        scan.allowEnd(false);
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
        results.put(1L, new DataFilePlan(splits));

        // request directly
        enumerator.handleSplitRequest(0, "test-host");
        context.getExecutorService().triggerAllNonPeriodicTasks();
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        List<FileStoreSourceSplit> assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(toDataSplits(assignedSplits)).containsExactly(splits.get(0));

        enumerator.handleSplitRequest(1, "test-host");
        context.getExecutorService().triggerAllNonPeriodicTasks();
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1);
        assignedSplits = assignments.get(1).getAssignedSplits();
        assertThat(toDataSplits(assignedSplits)).containsExactly(splits.get(1));
    }

    @Test
    public void testNoTriggerWhenReadLatest() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(4);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
        MockScan scan = new MockScan(results);
        scan.allowEnd(false);
        ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(1)
                        .setScan(scan)
                        .build();
        enumerator.start();
        enumerator.handleSplitRequest(0, "test-host");
        context.getExecutorService().triggerAllNonPeriodicTasks();

        long snapshot = 0;
        List<DataSplit> splits = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            splits.add(createDataSplit(snapshot, i, Collections.emptyList()));
        }
        results.put(1L, new DataFilePlan(splits));

        // will not trigger scan here
        enumerator.handleSplitRequest(0, "test-host");
        context.getExecutorService().triggerAllNonPeriodicTasks();
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments).isEmpty();

        enumerator.handleSplitRequest(1, "test-host");
        context.getExecutorService().triggerAllNonPeriodicTasks();
        assignments = context.getSplitAssignments();
        assertThat(assignments).isEmpty();

        // trigger all actions, we will scan anyway
        context.triggerAllActions();

        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1);
        List<FileStoreSourceSplit> assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(toDataSplits(assignedSplits)).containsExactly(splits.get(0));
        assignedSplits = assignments.get(1).getAssignedSplits();
        assertThat(toDataSplits(assignedSplits)).containsExactly(splits.get(1));

        splits.clear();
        for (int i = 2; i < 4; i++) {
            splits.add(createDataSplit(snapshot, i, Collections.emptyList()));
        }
        results.put(2L, new DataFilePlan(splits));
        // because blockScanByRequest = false, so this request will trigger scan
        enumerator.handleSplitRequest(2, "test-host");
        context.getExecutorService().triggerAllNonPeriodicTasks();
        enumerator.handleSplitRequest(3, "test-host");
        context.getExecutorService().triggerAllNonPeriodicTasks();
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1, 2, 3);
        assignedSplits = assignments.get(2).getAssignedSplits();
        assertThat(toDataSplits(assignedSplits)).containsExactly(splits.get(0));
        assignedSplits = assignments.get(3).getAssignedSplits();
        assertThat(toDataSplits(assignedSplits)).containsExactly(splits.get(1));

        // this will trigger scan, and then set blockScanByRequest = true
        enumerator.handleSplitRequest(3, "test-host");
        context.getExecutorService().triggerAllNonPeriodicTasks();
        splits.clear();
        splits.add(createDataSplit(snapshot, 7, Collections.emptyList()));
        results.put(3L, new DataFilePlan(splits));

        // this won't trigger scan, cause blockScanByRequest = true
        enumerator.handleSplitRequest(3, "test-host");
        context.getExecutorService().triggerAllNonPeriodicTasks();
        assignments = context.getSplitAssignments();
        assignedSplits = assignments.get(3).getAssignedSplits();
        assertThat(toDataSplits(assignedSplits)).doesNotContain(splits.get(0));

        // forcely enable trigger scan, so the split request below will trigger scan
        enumerator.enableTriggerScan();
        // trigger scan here
        enumerator.handleSplitRequest(3, "test-host");
        context.getExecutorService().triggerAllNonPeriodicTasks();
        assignments = context.getSplitAssignments();
        assignedSplits = assignments.get(3).getAssignedSplits();
        // get expected split
        assertThat(toDataSplits(assignedSplits)).contains(splits.get(0));
    }

    @Test
    public void testEnumeratorWithCheckpoint() {
        final TestingAsyncSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingAsyncSplitEnumeratorContext<>(1);
        context.registerReader(0, "test-host");

        // prepare test data
        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
        Map<Long, List<DataSplit>> expectedResults = new HashMap<>(4);
        StreamTableScan scan = new MockScan(results);
        for (int i = 1; i <= 4; i++) {
            List<DataSplit> dataSplits =
                    Collections.singletonList(createDataSplit(i, 0, Collections.emptyList()));
            results.put((long) i, new DataFilePlan(dataSplits));
            expectedResults.put((long) i, dataSplits);
        }

        final ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(1)
                        .setScan(scan)
                        .build();
        enumerator.start();

        PendingSplitsCheckpoint state;
        final AtomicReference<PendingSplitsCheckpoint> checkpoint = new AtomicReference<>();

        // empty plan
        context.runInCoordinatorThread(
                () -> checkpoint.set(checkpointWithoutException(enumerator, 1L))); // checkpoint
        context.triggerAlCoordinatorAction();
        state = checkpoint.getAndSet(null);
        assertThat(state).isNotNull();
        assertThat(state.currentSnapshotId()).isNull();
        assertThat(state.splits()).isEmpty();

        // scan first plan
        context.triggerAllWorkerAction(); // scan next plan
        context.triggerAlCoordinatorAction(); // processDiscoveredSplits
        context.runInCoordinatorThread(
                () -> checkpoint.set(checkpointWithoutException(enumerator, 2L))); // snapshotState
        context.triggerAlCoordinatorAction();
        state = checkpoint.getAndSet(null);
        assertThat(state).isNotNull();
        assertThat(state.currentSnapshotId()).isEqualTo(2L);
        assertThat(toDataSplits(state.splits())).containsExactlyElementsOf(expectedResults.get(1L));

        // assign first plan's splits
        enumerator.handleSplitRequest(0, "test");
        context.triggerAlCoordinatorAction();

        // multiple plans happen before processDiscoveredSplits
        context.triggerAllWorkerAction(); // scan next plan
        context.runInCoordinatorThread(
                () -> checkpoint.set(checkpointWithoutException(enumerator, 3L))); // snapshotState
        context.triggerAllWorkerAction(); // scan next plan
        context.triggerNextCoordinatorAction(); // process first discovered splits
        context.triggerNextCoordinatorAction(); // checkpoint
        state = checkpoint.getAndSet(null);
        assertThat(state).isNotNull();
        assertThat(state.currentSnapshotId()).isEqualTo(3L);
        assertThat(toDataSplits(state.splits())).containsExactlyElementsOf(expectedResults.get(2L));
    }

    @Test
    public void testEnumeratorWithConsumer() throws Exception {
        final TestingAsyncSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingAsyncSplitEnumeratorContext<>(3);
        for (int i = 0; i < 3; i++) {
            context.registerReader(i, "test-host");
        }

        // prepare test data
        TreeMap<Long, TableScan.Plan> dataSplits = new TreeMap<>();
        for (int i = 1; i <= 2; i++) {
            dataSplits.put(
                    (long) i,
                    new DataFilePlan(
                            Arrays.asList(
                                    createDataSplit(i, 0, Collections.emptyList()),
                                    createDataSplit(i, 2, Collections.emptyList()))));
        }
        MockScan scan = new MockScan(dataSplits);

        final ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(1)
                        .setScan(scan)
                        .build();
        enumerator.start();

        long checkpointId = 1L;

        // request for splits
        for (int i = 0; i < 3; i++) {
            enumerator.handleSplitRequest(i, "test-host");
        }

        // checkpoint is triggered for the first time and no snapshot is found
        triggerCheckpointAndComplete(enumerator, checkpointId++);
        assertThat(scan.getNextSnapshotIdForConsumer()).isNull();

        // find a new snapshot and trigger for the second checkpoint, but no snapshot is consumed
        scanNextSnapshot(context);
        triggerCheckpointAndComplete(enumerator, checkpointId++);
        assertThat(scan.getNextSnapshotIdForConsumer()).isEqualTo(1L);

        // subtask-0 has consumed the snapshot-1 and trigger for the next checkpoint
        enumerator.handleSourceEvent(0, new ReaderConsumeProgressEvent(1L));
        triggerCheckpointAndComplete(enumerator, checkpointId++);
        assertThat(scan.getNextSnapshotIdForConsumer()).isEqualTo(1L);

        // subtask-2 has consumed the snapshot-1 and trigger for the next checkpoint
        enumerator.handleSourceEvent(2, new ReaderConsumeProgressEvent(1L));
        triggerCheckpointAndComplete(enumerator, checkpointId++);
        assertThat(scan.getNextSnapshotIdForConsumer()).isEqualTo(1L);

        // subtask-0 and subtask-2 request for the next splits but there are no new snapshot
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(2, "test-host");
        triggerCheckpointAndComplete(enumerator, checkpointId++);
        assertThat(scan.getNextSnapshotIdForConsumer()).isEqualTo(2L);

        // find next snapshot and trigger for the next checkpoint, subtask-0 and subtask-2 has been
        // assigned new snapshot
        scanNextSnapshot(context);
        triggerCheckpointAndComplete(enumerator, checkpointId++);
        assertThat(scan.getNextSnapshotIdForConsumer()).isEqualTo(2L);
    }

    @Test
    public void testEnumeratorSplitMax() throws Exception {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(2);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
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
        for (int i = 0; i < 16; i++) {
            splits.add(createDataSplit(snapshot++, i, Collections.emptyList()));
        }
        results.put(1L, new DataFilePlan(splits));
        context.triggerAllActions();

        splits = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            splits.add(createDataSplit(snapshot++, i, Collections.emptyList()));
        }
        results.put(2L, new DataFilePlan(splits));
        context.triggerAllActions();

        splits = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            splits.add(createDataSplit(snapshot++, i, Collections.emptyList()));
        }
        results.put(3L, new DataFilePlan(splits));
        context.triggerAllActions();

        Assertions.assertThat(enumerator.splitAssigner.remainingSplits().size()).isEqualTo(16 * 2);
        Assertions.assertThat(enumerator.splitAssigner.numberOfRemainingSplits()).isEqualTo(16 * 2);

        enumerator.handleSplitRequest(0, "test");
        enumerator.handleSplitRequest(1, "test");

        Assertions.assertThat(enumerator.splitAssigner.remainingSplits().size()).isEqualTo(15 * 2);
        Assertions.assertThat(enumerator.splitAssigner.numberOfRemainingSplits()).isEqualTo(15 * 2);
    }

    private void triggerCheckpointAndComplete(
            ContinuousFileSplitEnumerator enumerator, long checkpointId) throws Exception {
        enumerator.snapshotState(checkpointId);
        enumerator.notifyCheckpointComplete(checkpointId);
    }

    private void scanNextSnapshot(
            TestingAsyncSplitEnumeratorContext<FileStoreSourceSplit> context) {
        context.workerExecutor.triggerPeriodicScheduledTasks();
        context.triggerAlCoordinatorAction();
    }

    private static PendingSplitsCheckpoint checkpointWithoutException(
            ContinuousFileSplitEnumerator enumerator, long checkpointId) {
        try {
            return enumerator.snapshotState(checkpointId);
        } catch (Exception e) {
            return null;
        }
    }

    private static List<DataSplit> toDataSplits(Collection<FileStoreSourceSplit> splits) {
        return splits.stream()
                .map(FileStoreSourceSplit::split)
                .map(split -> (DataSplit) split)
                .collect(Collectors.toList());
    }

    private static DataSplit createDataSplit(
            long snapshotId, int bucket, List<DataFileMeta> files) {
        return DataSplit.builder()
                .withSnapshot(snapshotId)
                .withPartition(row(1))
                .withBucket(bucket)
                .withDataFiles(files)
                .isStreaming(true)
                .build();
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
                    context, initialSplits, null, discoveryInterval, scan, bucketMode, 10);
        }
    }

    private static class MockScan implements StreamTableScan {

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
        public List<BinaryRow> listPartitions() {
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
    }

    private static class TestingAsyncSplitEnumeratorContext<SplitT extends SourceSplit>
            extends TestingSplitEnumeratorContext<SplitT> {

        private final ManuallyTriggeredScheduledExecutorService workerExecutor;
        private final ExecutorNotifier notifier;

        public TestingAsyncSplitEnumeratorContext(int parallelism) {
            super(parallelism);
            this.workerExecutor = new ManuallyTriggeredScheduledExecutorService();
            this.notifier = new ExecutorNotifier(workerExecutor, super.getExecutorService());
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            notifier.notifyReadyAsync(callable, handler);
        }

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
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
