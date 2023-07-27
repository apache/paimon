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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.PendingSplitsCheckpoint;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.testutils.assertj.AssertionUtils;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.source.ContinuousFileSplitEnumeratorTest.createSnapshotSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for the {@link AlignedContinuousFileSplitEnumerator}. */
public class AlignedContinuousFileSplitEnumeratorTest {

    @Test
    public void testSplitsAssignedBySnapshot() throws Exception {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(2);
        context.registerReader(0, "test-host");
        context.registerReader(1, "test-host");

        List<FileStoreSourceSplit> initialSplits = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            initialSplits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
        }
        initialSplits.add(createSnapshotSplit(2, 1, Collections.emptyList()));
        List<FileStoreSourceSplit> expectedSplits = new ArrayList<>(initialSplits);

        final AlignedContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(initialSplits)
                        .setDiscoveryInterval(3)
                        .build();

        // first request
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(1, "test-host");
        Map<Integer, TestingSplitEnumeratorContext.SplitAssignmentState<FileStoreSourceSplit>>
                assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        List<FileStoreSourceSplit> assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).containsExactly(expectedSplits.get(0));

        // second request
        context.getSplitAssignments().clear();
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(1, "test-host");
        assertThat(context.getSplitAssignments()).isEmpty();

        // snapshot state
        enumerator.snapshotState(1L);
        assertThat(context.getSplitAssignments()).isEmpty();

        // third request
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(1, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1);
        assertThat(assignments.get(0).getAssignedSplits()).containsExactly(expectedSplits.get(1));
        assertThat(assignments.get(1).getAssignedSplits()).containsExactly(expectedSplits.get(2));
    }

    @Test
    public void testEnumeratorSnapshotState() throws Exception {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(1);
        context.registerReader(0, "test-host");

        final AlignedContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(3)
                        .setAlignedTimeout(10L)
                        .build();
        assertThatThrownBy(() -> enumerator.snapshotState(1L))
                .satisfies(
                        AssertionUtils.anyCauseMatches(
                                "Timeout while waiting for snapshot from paimon source."));

        List<FileStoreSourceSplit> splits = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            splits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
        }
        enumerator.addSplits(splits);
        enumerator.handleSplitRequest(0, "test-host");

        Map<Integer, TestingSplitEnumeratorContext.SplitAssignmentState<FileStoreSourceSplit>>
                assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).getAssignedSplits()).containsExactly(splits.get(0));
        PendingSplitsCheckpoint checkpoint = enumerator.snapshotState(1L);
        assertThat(checkpoint.splits()).containsExactly(splits.get(1));
    }

    private static class Builder {
        private SplitEnumeratorContext<FileStoreSourceSplit> context;
        private Collection<FileStoreSourceSplit> initialSplits = Collections.emptyList();
        private long discoveryInterval = Long.MAX_VALUE;

        private StreamTableScan scan;
        private BucketMode bucketMode = BucketMode.FIXED;

        private long timeout = 30000L;

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

        public Builder setAlignedTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public AlignedContinuousFileSplitEnumerator build() {
            return new AlignedContinuousFileSplitEnumerator(
                    context, initialSplits, null, discoveryInterval, scan, bucketMode, timeout);
        }
    }
}
