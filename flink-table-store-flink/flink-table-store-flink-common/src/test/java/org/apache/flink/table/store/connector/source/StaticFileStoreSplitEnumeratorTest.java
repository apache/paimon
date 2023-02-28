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

import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext.SplitAssignmentState;
import static org.apache.flink.table.store.connector.source.ContinuousFileSplitEnumeratorTest.createSnapshotSplit;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link StaticFileStoreSplitEnumerator}. */
public class StaticFileStoreSplitEnumeratorTest {

    @Test
    public void testSplitAllocation() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(2);
        context.registerReader(0, "test-host");
        context.registerReader(1, "test-host");

        List<FileStoreSourceSplit> splits = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            splits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
        }
        StaticFileStoreSplitEnumerator enumerator =
                new StaticFileStoreSplitEnumerator(context, null, splits);

        // test assign
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(1, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1);
        assertThat(assignments.get(0).getAssignedSplits())
                .containsExactly(splits.get(0), splits.get(2));
        assertThat(assignments.get(1).getAssignedSplits())
                .containsExactly(splits.get(1), splits.get(3));

        // test addSplitsBack
        enumerator.addSplitsBack(assignments.get(0).getAssignedSplits(), 0);
        context.getSplitAssignments().clear();
        assertThat(context.getSplitAssignments()).isEmpty();
        enumerator.handleSplitRequest(0, "test-host");
        assertThat(assignments.get(0).getAssignedSplits())
                .containsExactly(splits.get(0), splits.get(2));
    }

    @Test
    public void testSplitAllocationNotEvenly() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(2);
        context.registerReader(0, "test-host");
        context.registerReader(1, "test-host");

        List<FileStoreSourceSplit> splits = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            splits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
        }
        StaticFileStoreSplitEnumerator enumerator =
                new StaticFileStoreSplitEnumerator(context, null, splits);

        // test assign
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(1, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1);
        assertThat(assignments.get(0).getAssignedSplits())
                .containsExactly(splits.get(0), splits.get(2));
        assertThat(assignments.get(1).getAssignedSplits()).containsExactly(splits.get(1));
    }

    @Test
    public void testSplitAllocationSomeEmpty() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(3);
        context.registerReader(0, "test-host");
        context.registerReader(1, "test-host");
        context.registerReader(2, "test-host");

        List<FileStoreSourceSplit> splits = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            splits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
        }
        StaticFileStoreSplitEnumerator enumerator =
                new StaticFileStoreSplitEnumerator(context, null, splits);

        // test assign
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(1, "test-host");
        enumerator.handleSplitRequest(2, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1, 2);
        assertThat(assignments.get(0).getAssignedSplits()).containsExactly(splits.get(0));
        assertThat(assignments.get(1).getAssignedSplits()).containsExactly(splits.get(1));
        assertThat(assignments.get(2).getAssignedSplits()).isEmpty();
    }
}
