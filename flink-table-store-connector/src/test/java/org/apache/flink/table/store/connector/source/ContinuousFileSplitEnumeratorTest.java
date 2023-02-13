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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.snapshot.SnapshotEnumerator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext.SplitAssignmentState;
import static org.apache.flink.table.store.file.mergetree.compact.MergeTreeCompactManagerTest.row;

/** Unit tests for the {@link ContinuousFileSplitEnumerator}. */
public class ContinuousFileSplitEnumeratorTest {

    @Test
    public void testSplitAllocationIsOrdered() throws Exception {
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
        Assertions.assertThat(assignments.size()).isEqualTo(1);
        Assertions.assertThat(assignments.containsKey(0)).isTrue();
        List<FileStoreSourceSplit> assignedSplits = assignments.get(0).getAssignedSplits();
        Assertions.assertThat(assignedSplits.size()).isEqualTo(2);
        for (int i = 0; i < 2; i++) {
            Assertions.assertThat(assignedSplits.get(i)).isEqualTo(expectedSplits.get(i));
        }

        // split1 and split2 is added back
        enumerator.addSplitsBack(assignedSplits, 0);
        context.getSplitAssignments().clear();
        Assertions.assertThat(context.getSplitAssignments().size()).isEqualTo(0);

        // The split is allocated for the second time, and split1 is allocated first
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        Assertions.assertThat(assignments.size()).isEqualTo(1);
        Assertions.assertThat(assignments.containsKey(0)).isTrue();
        assignedSplits = assignments.get(0).getAssignedSplits();
        Assertions.assertThat(assignedSplits.size()).isEqualTo(2);
        for (int i = 0; i < 2; i++) {
            Assertions.assertThat(assignedSplits.get(i)).isEqualTo(expectedSplits.get(i));
        }

        // continuing to allocate split
        context.getSplitAssignments().clear();
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        Assertions.assertThat(assignments.size()).isEqualTo(1);
        Assertions.assertThat(assignments.containsKey(0));
        assignedSplits = assignments.get(0).getAssignedSplits();
        Assertions.assertThat(assignedSplits.size()).isEqualTo(2);
        for (int i = 0; i < 2; i++) {
            Assertions.assertThat(assignedSplits.get(i)).isEqualTo(expectedSplits.get(i + 2));
        }
    }

    private static FileStoreSourceSplit createSnapshotSplit(
            int snapshotId, int bucket, List<DataFileMeta> files) {
        return new FileStoreSourceSplit(
                UUID.randomUUID().toString(),
                new DataSplit(snapshotId, row(1), bucket, files, true),
                0);
    }

    private static class Builder {
        private SplitEnumeratorContext<FileStoreSourceSplit> context;
        private Collection<FileStoreSourceSplit> initialSplits = Collections.emptyList();
        private Long nextSnapshotId;
        private long discoveryInterval = Long.MAX_VALUE;
        private SnapshotEnumerator snapshotEnumerator;

        public Builder setSplitEnumeratorContext(
                SplitEnumeratorContext<FileStoreSourceSplit> context) {
            this.context = context;
            return this;
        }

        public Builder setInitialSplits(Collection<FileStoreSourceSplit> initialSplits) {
            this.initialSplits = initialSplits;
            return this;
        }

        public Builder setNextSnapshotId(Long nextSnapshotId) {
            this.nextSnapshotId = nextSnapshotId;
            return this;
        }

        public Builder setDiscoveryInterval(long discoveryInterval) {
            this.discoveryInterval = discoveryInterval;
            return this;
        }

        public Builder setSnapshotEnumerator(SnapshotEnumerator snapshotEnumerator) {
            this.snapshotEnumerator = snapshotEnumerator;
            return this;
        }

        public ContinuousFileSplitEnumerator build() {
            return new ContinuousFileSplitEnumerator(
                    context, initialSplits, nextSnapshotId, discoveryInterval, snapshotEnumerator);
        }
    }
}
