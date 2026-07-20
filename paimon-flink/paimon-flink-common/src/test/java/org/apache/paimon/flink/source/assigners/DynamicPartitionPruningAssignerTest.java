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

package org.apache.paimon.flink.source.assigners;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;

import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for dynamic partition pruning assigners. */
public class DynamicPartitionPruningAssignerTest {

    private static final Projection IDENTITY_PROJECTION = input -> (BinaryRow) input;

    @Test
    public void testPreAssignQueryAuthSplits() {
        FileStoreSourceSplit selected = queryAuthSplit("selected", 1);
        FileStoreSourceSplit filtered = queryAuthSplit("filtered", 2);

        PreAssignSplitAssigner assigner =
                new PreAssignSplitAssigner(
                        10,
                        1,
                        Arrays.asList(selected, filtered),
                        IDENTITY_PROJECTION,
                        new TestDynamicFilteringData(1),
                        split -> 1L);

        assertThat(assigner.getNext(0, null)).containsExactly(selected);
        assertThat(selected.split()).isInstanceOf(QueryAuthSplit.class);
    }

    @Test
    public void testPreemptiveQueryAuthSplits() {
        FileStoreSourceSplit filtered = queryAuthSplit("filtered", 2);
        FileStoreSourceSplit selected = queryAuthSplit("selected", 1);
        DynamicPartitionPruningAssigner assigner =
                new DynamicPartitionPruningAssigner(
                        new FIFOSplitAssigner(Arrays.asList(filtered, selected)),
                        IDENTITY_PROJECTION,
                        new TestDynamicFilteringData(1));

        assertThat(assigner.remainingSplits()).containsExactly(selected);
        assertThat(assigner.getNext(0, null)).containsExactly(selected);
        assertThat(selected.split()).isInstanceOf(QueryAuthSplit.class);
    }

    private static FileStoreSourceSplit queryAuthSplit(String id, int partition) {
        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(row(partition))
                        .withBucket(0)
                        .withDataFiles(Collections.emptyList())
                        .isStreaming(false)
                        .withBucketPath("")
                        .build();
        return new FileStoreSourceSplit(
                id,
                new QueryAuthSplit(
                        dataSplit,
                        new TableQueryAuthResult(null, Collections.singletonMap("f0", "mask"))));
    }

    private static class TestDynamicFilteringData extends DynamicFilteringData {

        private final int selectedPartition;

        private TestDynamicFilteringData(int selectedPartition) {
            super(
                    new GenericTypeInfo<>(RowData.class),
                    org.apache.flink.table.types.logical.RowType.of(new IntType()),
                    Collections.emptyList(),
                    true);
            this.selectedPartition = selectedPartition;
        }

        @Override
        public boolean contains(RowData row) {
            return row.getInt(0) == selectedPartition;
        }
    }
}
