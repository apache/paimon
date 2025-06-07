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

package org.apache.paimon.flink.source.shardread;

import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.StaticFileStoreSplitEnumerator;
import org.apache.paimon.flink.source.StaticFileStoreSplitEnumeratorTestBase;

import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext.SplitAssignmentState;
import static org.apache.paimon.flink.FlinkConnectorOptions.SplitAssignMode;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StaticFileStoreSplitEnumerator} with {@link SplitAssignMode#SHARD_READ}. */
public class ShardReadAssignModeTest extends StaticFileStoreSplitEnumeratorTestBase {

    @Test
    public void testSplitAllocation() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(4);

        List<FileStoreSourceSplit> splits = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            splits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
        }
        StaticFileStoreSplitEnumerator enumerator = getSplitEnumerator(context, splits);

        // test assign
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(1, "test-host");
        enumerator.handleSplitRequest(2, "test-host");
        enumerator.handleSplitRequest(3, "test-host");
        Map<Integer, SplitAssignmentState<FileStoreSourceSplit>> assignments =
                context.getSplitAssignments();

        assertThat(assignments).containsOnlyKeys(0, 1, 2, 3);
        assertThat(assignments.get(0).getAssignedSplits()).containsExactly(splits.get(0));
        assertThat(assignments.get(1).getAssignedSplits()).containsExactly(splits.get(1));
        assertThat(assignments.get(2).getAssignedSplits()).containsExactly(splits.get(2));
        assertThat(assignments.get(3).getAssignedSplits()).containsExactly(splits.get(3));
    }

    @Test
    public void testSplitAllocationWithException() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(4);

        List<FileStoreSourceSplit> splits = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            splits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
        }

        try {
            getSplitEnumerator(context, splits);
        } catch (IllegalArgumentException e) {
            assertThat(e)
                    .hasMessageContaining("Error, splits.size() must be equal with numReaders");
        }
    }

    @Override
    public void assertResultOftestDynamicPartitionFilteringAfterStarted(
            Map<Integer, TestingSplitEnumeratorContext.SplitAssignmentState<FileStoreSourceSplit>>
                    assignments,
            List<FileStoreSourceSplit> initialSplits,
            StaticFileStoreSplitEnumerator enumerator) {
        assertThat(assignments.get(0).getAssignedSplits().size()).isEqualTo(1);
        Assertions.assertTrue(
                assignments.get(0).getAssignedSplits().get(0)
                        instanceof FileStoreSourceSplitWithDpp);
        assertThat(enumerator.getSplitAssigner().remainingSplits().size()).isEqualTo(3);
    }

    @Override
    public void assertResultOfTestDynamicPartitionFilteringWithProjection(
            Map<Integer, TestingSplitEnumeratorContext.SplitAssignmentState<FileStoreSourceSplit>>
                    assignments,
            List<FileStoreSourceSplit> initialSplits,
            StaticFileStoreSplitEnumerator enumerator) {
        assertThat(assignments.get(0).getAssignedSplits().size()).isEqualTo(1);
        Assertions.assertTrue(
                assignments.get(0).getAssignedSplits().get(0)
                        instanceof FileStoreSourceSplitWithDpp);
        assertThat(enumerator.getSplitAssigner().remainingSplits().size()).isEqualTo(3);
    }

    @Override
    protected SplitAssignMode splitAssignMode() {
        return SplitAssignMode.SHARD_READ;
    }

    @Override
    public int getParallelism() {
        return 4;
    }
}
