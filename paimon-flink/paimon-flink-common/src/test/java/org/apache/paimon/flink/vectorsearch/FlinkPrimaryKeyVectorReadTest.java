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

package org.apache.paimon.flink.vectorsearch;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.BucketVectorSearchSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PrimaryKeyVectorRead;
import org.apache.paimon.table.source.PrimaryKeyVectorScan;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link FlinkPrimaryKeyVectorRead}. */
public class FlinkPrimaryKeyVectorReadTest {

    @Test
    public void testDistributesSerializedBucketGroups() {
        List<BucketVectorSearchSplit> splits = new ArrayList<>();
        for (int bucket = 0; bucket < 4; bucket++) {
            DataSplit dataSplit =
                    DataSplit.builder()
                            .withSnapshot(1L)
                            .withPartition(BinaryRow.EMPTY_ROW)
                            .withBucket(bucket)
                            .withBucketPath("bucket-" + bucket)
                            .withDataFiles(Collections.emptyList())
                            .isStreaming(false)
                            .rawConvertible(false)
                            .build();
            splits.add(new BucketVectorSearchSplit(dataSplit, Collections.emptyList()));
        }

        TestingFlinkPrimaryKeyVectorRead read = new TestingFlinkPrimaryKeyVectorRead(splits);
        GlobalIndexResult result = read.read(() -> Collections.emptyList());

        assertThat(result.results()).isEmpty();
        assertThat(read.taskGroupSizes).containsExactly(2, 2);
        assertThat(read.dispatchedBuckets).containsExactlyInAnyOrder(0, 1, 2, 3);
        assertThat(read.operatorParallelism).isEqualTo(2);
        assertThat(read.createResultCalled).isTrue();
    }

    @Test
    public void testFlinkJobNameContainsFullTableName() {
        TestingFlinkPrimaryKeyVectorRead read =
                new TestingFlinkPrimaryKeyVectorRead(Collections.emptyList());

        assertThat(read.flinkJobName("Primary-Key Vector Search"))
                .isEqualTo("Vector Search - Primary-Key Vector Search : default.PK_T");
    }

    private static FileStoreTable table() {
        Map<String, String> options = new HashMap<>();
        options.put("fields.vector.pk-vector.index.type", "test-vector-ann");
        options.put("fields.vector.pk-vector.distance.metric", "l2");
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.coreOptions()).thenReturn(new CoreOptions(options));
        when(table.options()).thenReturn(options);
        when(table.fullName()).thenReturn("default.PK_T");
        return table;
    }

    private static class TestingFlinkPrimaryKeyVectorRead extends FlinkPrimaryKeyVectorRead {

        private final PrimaryKeyVectorScan.Plan primaryKeyPlan;
        private final List<BucketVectorSearchSplit> splits;
        private final List<Integer> taskGroupSizes = new ArrayList<>();
        private final List<Integer> dispatchedBuckets = new ArrayList<>();
        private int operatorParallelism;
        private boolean createResultCalled;

        private TestingFlinkPrimaryKeyVectorRead(List<BucketVectorSearchSplit> splits) {
            super(
                    table(),
                    new DataField(1, "vector", new VectorType(2, new FloatType())),
                    new float[] {0.0f, 0.0f},
                    1,
                    Collections.emptyMap(),
                    null,
                    mock(StreamExecutionEnvironment.class));
            this.primaryKeyPlan = mock(PrimaryKeyVectorScan.Plan.class);
            this.splits = splits;
        }

        @Override
        protected PrimaryKeyVectorScan.Plan primaryKeyPlan(VectorScan.Plan plan) {
            return primaryKeyPlan;
        }

        @Override
        protected List<BucketVectorSearchSplit> bucketSplits(PrimaryKeyVectorScan.Plan plan) {
            assertThat(plan).isSameAs(primaryKeyPlan);
            return splits;
        }

        @Override
        protected int flinkParallelism() {
            return 2;
        }

        @Override
        protected List<byte[]> executeBucketSearchGroups(
                List<List<byte[]>> groups, int parallelism) {
            operatorParallelism = parallelism;
            taskGroupSizes.addAll(groups.stream().map(List::size).collect(Collectors.toList()));
            return groups.stream().map(this::executeBucketSearchGroup).collect(Collectors.toList());
        }

        @Override
        protected SearchResult searchBuckets(List<BucketVectorSearchSplit> taskSplits) {
            dispatchedBuckets.addAll(
                    taskSplits.stream()
                            .map(split -> split.dataSplit().bucket())
                            .collect(Collectors.toList()));
            return new SearchResult(Collections.emptyList(), Collections.emptyList());
        }

        @Override
        protected GlobalIndexResult createResult(
                PrimaryKeyVectorScan.Plan plan, PrimaryKeyVectorRead.SearchResult searchResult) {
            assertThat(plan).isSameAs(primaryKeyPlan);
            assertThat(searchResult.indexedCandidates()).isEmpty();
            assertThat(searchResult.exactCandidates()).isEmpty();
            createResultCalled = true;
            return GlobalIndexResult.createEmpty();
        }
    }
}
