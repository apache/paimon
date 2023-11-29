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

import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.source.StaticFileStoreSource.createSplitAssigner;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

/** Base test class of {@link StaticFileStoreSplitEnumerator}. */
public abstract class StaticFileStoreSplitEnumeratorTestBase extends FileSplitEnumeratorTestBase {

    @Test
    public void testDynamicPartitionFilteringAfterStarted() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(1);

        List<FileStoreSourceSplit> initialSplits = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            initialSplits.add(createSnapshotSplit(i + 1, 0, Collections.emptyList(), i / 2, i % 2));
        }

        final StaticFileStoreSplitEnumerator enumerator =
                getSplitEnumerator(
                        context,
                        initialSplits,
                        RowType.of(DataTypes.INT(), DataTypes.INT()),
                        Arrays.asList("f0", "f1"));
        Map<Integer, TestingSplitEnumeratorContext.SplitAssignmentState<FileStoreSourceSplit>>
                assignments = context.getSplitAssignments();

        // request one split and check
        enumerator.handleSplitRequest(0, "test-host");

        assertThat(assignments.get(0).getAssignedSplits()).containsExactly(initialSplits.get(0));
        assertThat(enumerator.getSplitAssigner().remainingSplits())
                .containsExactly(initialSplits.get(1), initialSplits.get(2), initialSplits.get(3));
        assignments.clear();

        // send dynamic filtering event
        DynamicFilteringData dynamicFilteringData =
                new MockDynamicFilteringData(
                        org.apache.flink.table.types.logical.RowType.of(
                                new IntType(), new IntType()),
                        new RowData[] {GenericRowData.of(1, 1)});
        enumerator.handleSourceEvent(0, new DynamicFilteringEvent(dynamicFilteringData));

        // request one split and check
        enumerator.handleSplitRequest(0, "test-host");

        assertThat(assignments.get(0).getAssignedSplits()).containsExactly(initialSplits.get(3));
        assertThat(enumerator.getSplitAssigner().remainingSplits()).isEmpty();
    }

    @Test
    public void testDynamicPartitionFilteringWithProjection() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                getSplitEnumeratorContext(1);

        List<FileStoreSourceSplit> initialSplits = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            initialSplits.add(createSnapshotSplit(i + 1, 0, Collections.emptyList(), i / 2, i % 2));
        }

        final StaticFileStoreSplitEnumerator enumerator =
                getSplitEnumerator(
                        context,
                        initialSplits,
                        RowType.of(DataTypes.INT(), DataTypes.INT()),
                        Collections.singletonList("f0"));
        Map<Integer, TestingSplitEnumeratorContext.SplitAssignmentState<FileStoreSourceSplit>>
                assignments = context.getSplitAssignments();

        // send dynamic filtering event
        DynamicFilteringData dynamicFilteringData =
                new MockDynamicFilteringData(
                        org.apache.flink.table.types.logical.RowType.of(new IntType()),
                        new RowData[] {GenericRowData.of(0)});
        enumerator.handleSourceEvent(0, new DynamicFilteringEvent(dynamicFilteringData));

        // request two splits
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");

        // check
        assertThat(assignments.get(0).getAssignedSplits())
                .containsExactly(initialSplits.get(0), initialSplits.get(1));
        assertThat(enumerator.getSplitAssigner().remainingSplits()).isEmpty();
    }

    protected StaticFileStoreSplitEnumerator getSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            List<FileStoreSourceSplit> splits) {
        return new StaticFileStoreSplitEnumerator(
                context, null, createSplitAssigner(context, 10, splitAssignMode(), splits));
    }

    private StaticFileStoreSplitEnumerator getSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            List<FileStoreSourceSplit> splits,
            RowType partitionRowProjection,
            List<String> dynamicPartitionFilteringFields) {
        FlinkConnectorOptions.SplitAssignMode mode = splitAssignMode();
        // make sure one request assigns one split in FAIR mode
        int splitBatchSize = mode == FlinkConnectorOptions.SplitAssignMode.FAIR ? 1 : 10;
        return new StaticFileStoreSplitEnumerator(
                context,
                null,
                createSplitAssigner(context, splitBatchSize, splitAssignMode(), splits),
                new DynamicPartitionFilteringInfo(
                        partitionRowProjection, dynamicPartitionFilteringFields));
    }

    protected abstract FlinkConnectorOptions.SplitAssignMode splitAssignMode();

    private static class MockDynamicFilteringData extends DynamicFilteringData {

        private final org.apache.flink.table.types.logical.RowType rowType;
        private final RowData[] neededPartitions;

        public MockDynamicFilteringData(
                org.apache.flink.table.types.logical.RowType rowType, RowData[] neededPartitions) {
            super(new GenericTypeInfo<>(RowData.class), rowType, Collections.emptyList(), true);
            this.rowType = rowType;
            this.neededPartitions = neededPartitions;
        }

        public boolean contains(RowData row) {
            if (!isFiltering()) {
                return true;
            } else {
                checkArgument(rowType.getFieldCount() == row.getArity());
                for (RowData mayMatch : neededPartitions) {
                    if (matchRow(row, mayMatch)) {
                        return true;
                    }
                }
                return false;
            }
        }

        private boolean matchRow(RowData row, RowData mayMatch) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (row.getInt(i) != mayMatch.getInt(i)) {
                    return false;
                }
            }
            return true;
        }
    }
}
