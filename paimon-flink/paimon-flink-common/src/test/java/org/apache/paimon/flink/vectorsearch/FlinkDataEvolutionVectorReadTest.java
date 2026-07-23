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

import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.IndexVectorSearchSplit;
import org.apache.paimon.table.source.RawVectorSearchSplit;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.table.source.VectorSearchSplit;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link FlinkDataEvolutionVectorRead}. */
public class FlinkDataEvolutionVectorReadTest {

    @Test
    public void testExecutesSerializedRawSearchTasks() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        ExecutingFlinkVectorRead read = new ExecutingFlinkVectorRead(env);

        ScoredGlobalIndexResult result =
                read.readRawSplitsInFlink(Collections.singletonList(rawSplit(0, 63)), null, null);
        ScoredGlobalIndexResult secondResult =
                read.readRawSplitsInFlink(
                        Collections.singletonList(rawSplit(100, 103)), null, null);

        assertThat(result.results().getLongCardinality()).isEqualTo(64);
        assertThat(secondResult.results().getLongCardinality()).isEqualTo(4);
    }

    @Test
    public void testRawSearchUsesFlinkPath() {
        TestingFlinkVectorRead read = new TestingFlinkVectorRead();
        RawVectorSearchSplit rawSplit =
                new RawVectorSearchSplit(
                        Collections.singletonList(new Range(42, 42)),
                        Collections.emptyList(),
                        null);
        VectorScan.Plan plan = () -> Collections.singletonList(rawSplit);

        GlobalIndexResult result = read.read(plan);

        assertThat(read.rawFlinkPathUsed).isTrue();
        assertThat(result.results().contains(42L)).isTrue();
    }

    @Test
    public void testRawSearchSplitsRangesAcrossFlinkTasks() {
        RecordingFlinkVectorRead read = new RecordingFlinkVectorRead();
        RawVectorSearchSplit rawSplit =
                new RawVectorSearchSplit(
                        Collections.singletonList(new Range(0, 63)), Collections.emptyList(), null);

        ScoredGlobalIndexResult result =
                read.readRawSplitsInFlink(Collections.singletonList(rawSplit), null, null);

        assertThat(read.rawSearchRanges).containsExactly(new Range(0, 31), new Range(32, 63));
        assertThat(read.flinkParallelism).isEqualTo(2);
        assertThat(result.results().getLongCardinality()).isEqualTo(64);
    }

    @Test
    public void testRawSearchScopesPreFilterToTaskRanges() {
        PreFilterRecordingFlinkVectorRead read = new PreFilterRecordingFlinkVectorRead();
        RoaringNavigableMap64 preFilter = new RoaringNavigableMap64();
        preFilter.addRange(new Range(0, 29));
        preFilter.add(35L);
        preFilter.add(39L);

        read.readRawSplitsInFlink(Collections.singletonList(rawSplit(0, 89)), null, preFilter);

        assertThat(read.rawSearchRanges)
                .containsExactly(new Range(0, 29), new Range(30, 59), new Range(60, 89));
        assertThat(read.rawSearchPreFilters)
                .containsExactly(null, Arrays.asList(35L, 39L), Collections.emptyList());
    }

    @Test
    public void testDistributedIndexRefinesAfterGlobalMerge() {
        DistributedRefineFlinkVectorRead read = new DistributedRefineFlinkVectorRead();

        ScoredGlobalIndexResult result =
                read.readIndexSplitsInFlink(indexSplits("test-vector-ann", 4), new L2Indexer());

        assertThat(read.flinkParallelism).isEqualTo(2);
        assertThat(read.rawSearchCandidateRows).containsExactly(0L, 2L);
        assertThat(result.results().getLongCardinality()).isEqualTo(1);
        assertThat(result.results().contains(0L)).isTrue();
    }

    @Test
    public void testMixedSearchUsesCombinedFlinkJob() {
        MixedFlinkVectorRead read = new MixedFlinkVectorRead();
        List<VectorSearchSplit> splits = new ArrayList<>(indexSplits("test-vector-ann", 4));
        splits.add(rawSplit(42, 105));

        GlobalIndexResult result = read.read(() -> splits);

        assertThat(read.combinedExecution).isTrue();
        assertThat(result.results().contains(0L)).isTrue();
        assertThat(result.results().contains(42L)).isTrue();
    }

    @Test
    public void testMixedResultTagsKeepSearchParallelism() {
        StreamExecutionEnvironment env = environment();
        env.setParallelism(8);
        DataStream<byte[]> searchResults =
                StreamExecutionEnvironmentUtils.fromData(
                                env,
                                Collections.singletonList(new byte[] {1}),
                                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                        .map((MapFunction<byte[], byte[]>) bytes -> bytes)
                        .returns(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                        .setParallelism(2);

        DataStream<byte[]> tagged =
                FlinkDataEvolutionVectorRead.tagResults(
                        searchResults, (byte) 0, "Tag Vector Search Results");

        assertThat(tagged.getParallelism()).isEqualTo(2);
    }

    @Test
    public void testFlinkJobNameContainsFullTableName() {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.fullName()).thenReturn("default.T");
        FlinkDataEvolutionVectorRead read =
                new FlinkDataEvolutionVectorRead(
                        table,
                        null,
                        null,
                        1,
                        new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                        new float[] {1.0f},
                        null,
                        environment());

        assertThat(read.flinkJobName("Vector Index Search"))
                .isEqualTo("Vector Search - Vector Index Search : default.T");
    }

    private static StreamExecutionEnvironment environment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static RawVectorSearchSplit rawSplit(long from, long to) {
        return new RawVectorSearchSplit(
                Collections.singletonList(new Range(from, to)), Collections.emptyList(), null);
    }

    private static List<IndexVectorSearchSplit> indexSplits(String indexType, int count) {
        List<IndexVectorSearchSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            GlobalIndexMeta globalIndexMeta = new GlobalIndexMeta(i, i, 0, null, new byte[0]);
            IndexFileMeta indexFile =
                    new IndexFileMeta(indexType, "index-" + i, 1L, 1L, globalIndexMeta, null);
            splits.add(
                    new IndexVectorSearchSplit(
                            i, i, Collections.singletonList(indexFile), Collections.emptyList()));
        }
        return splits;
    }

    private static ScoredGlobalIndexResult scoredResult(long rowId, float score) {
        RoaringNavigableMap64 rows = new RoaringNavigableMap64();
        rows.add(rowId);
        return ScoredGlobalIndexResult.create(rows, candidate -> score);
    }

    private static class TestingFlinkVectorRead extends FlinkDataEvolutionVectorRead {

        private boolean rawFlinkPathUsed;

        private TestingFlinkVectorRead() {
            super(
                    null,
                    null,
                    null,
                    10,
                    new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[] {1.0f},
                    null,
                    environment());
        }

        @Override
        protected GlobalIndexResult readSplits(List<? extends VectorSearchSplit> splits) {
            throw new AssertionError("Raw search should not fall back to local vector read.");
        }

        @Override
        protected ScoredGlobalIndexResult readIndexSplitsInFlink(
                List<IndexVectorSearchSplit> splits, @Nullable GlobalIndexer globalIndexer) {
            throw new AssertionError("Index search is not part of this test.");
        }

        @Override
        protected ScoredGlobalIndexResult readRawSplitsInFlink(
                List<RawVectorSearchSplit> splits,
                @Nullable GlobalIndexer globalIndexer,
                @Nullable RoaringNavigableMap64 preFilter) {
            rawFlinkPathUsed = true;
            assertThat(splits).hasSize(1);
            assertThat(globalIndexer).isNull();
            assertThat(preFilter).isNull();

            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            rows.add(42L);
            return ScoredGlobalIndexResult.create(rows, rowId -> 1.0f);
        }
    }

    private static class DistributedRefineFlinkVectorRead extends FlinkDataEvolutionVectorRead {

        private int flinkParallelism;
        private List<Long> rawSearchCandidateRows = Collections.emptyList();

        private DistributedRefineFlinkVectorRead() {
            super(
                    null,
                    null,
                    null,
                    1,
                    new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[] {0.0f},
                    Collections.singletonMap("refine_factor", "2"),
                    environment());
        }

        @Override
        protected int flinkParallelism() {
            return 2;
        }

        @Override
        protected List<byte[]> executeIndexSearchGroups(
                List<List<SerializedSplit>> groups, int searchLimit, int parallelism) {
            flinkParallelism = parallelism;
            assertThat(groups).hasSize(2);
            assertThat(searchLimit).isEqualTo(2);
            try {
                GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
                return Arrays.asList(
                        serializer.serialize(scoredResult(2L, 100.0f)),
                        serializer.serialize(scoredResult(0L, 1.0f)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected ScoredGlobalIndexResult readRawRefineSearch(
                RoaringNavigableMap64 candidates,
                @Nullable GlobalIndexer globalIndexer,
                float[] queryVector) {
            assertThat(globalIndexer).isInstanceOf(VectorGlobalIndexer.class);
            assertThat(((VectorGlobalIndexer) globalIndexer).metric()).isEqualTo("l2");
            assertThat(queryVector).containsExactly(0.0f);
            rawSearchCandidateRows = new ArrayList<>();
            for (long rowId : candidates) {
                rawSearchCandidateRows.add(rowId);
            }

            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            for (long rowId : candidates) {
                rows.add(rowId);
            }
            return ScoredGlobalIndexResult.create(
                            rows, rowId -> rowId == 0L ? 1.0f : 1.0f / (1.0f + rowId * rowId))
                    .topK(1);
        }
    }

    private static class MixedFlinkVectorRead extends FlinkDataEvolutionVectorRead {

        private boolean combinedExecution;

        private MixedFlinkVectorRead() {
            super(
                    null,
                    null,
                    null,
                    2,
                    new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[] {0.0f},
                    Collections.singletonMap("test.vector.metric", "l2"),
                    environment());
        }

        @Override
        protected GlobalIndexer createGlobalIndexer(List<IndexVectorSearchSplit> splits) {
            return new L2Indexer();
        }

        @Override
        protected int flinkParallelism() {
            return 2;
        }

        @Override
        protected List<byte[]> executeIndexAndRawSearchGroups(
                List<List<SerializedSplit>> indexGroups,
                int searchLimit,
                List<List<SerializedSplit>> rawGroups,
                String rawMetric,
                int parallelism) {
            combinedExecution = true;
            assertThat(indexGroups).hasSize(2);
            assertThat(rawGroups).hasSize(2);
            assertThat(searchLimit).isEqualTo(2);
            assertThat(rawMetric).isEqualTo("l2");
            assertThat(parallelism).isEqualTo(2);
            try {
                GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
                return Arrays.asList(
                        tagResult((byte) 0, serializer.serialize(scoredResult(0L, 1.0f))),
                        tagResult((byte) 1, serializer.serialize(scoredResult(42L, 0.9f))));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected List<byte[]> executeIndexSearchGroups(
                List<List<SerializedSplit>> groups, int searchLimit, int parallelism) {
            throw new AssertionError("Mixed search must not submit an indexed-only Flink job.");
        }

        @Override
        protected List<byte[]> executeRawSearchGroups(
                List<List<SerializedSplit>> groups, String metric, int parallelism) {
            throw new AssertionError("Mixed search must not submit a raw-only Flink job.");
        }
    }

    private static class RecordingFlinkVectorRead extends FlinkDataEvolutionVectorRead {

        private final AtomicInteger nextTask = new AtomicInteger();
        private final List<Range> rawSearchRanges = Collections.synchronizedList(new ArrayList<>());
        private int flinkParallelism;

        private RecordingFlinkVectorRead() {
            super(
                    null,
                    null,
                    null,
                    100,
                    new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[] {1.0f},
                    Collections.singletonMap("test.vector.metric", "l2"),
                    environment());
        }

        @Override
        protected int flinkParallelism() {
            return 2;
        }

        @Override
        protected List<byte[]> executeRawSearchGroups(
                List<List<SerializedSplit>> groups, String metric, int parallelism) {
            flinkParallelism = parallelism;
            return groups.stream()
                    .map(group -> executeRawSearchGroup(group, metric))
                    .collect(Collectors.toList());
        }

        @Override
        protected ScoredGlobalIndexResult readRawSearch(
                List<Range> rawRowRanges,
                @Nullable RoaringNavigableMap64 preFilter,
                String metric,
                float[] queryVector) {
            assertThat(preFilter).isNull();
            assertThat(metric).isEqualTo("l2");
            assertThat(queryVector).containsExactly(1.0f);
            rawSearchRanges.addAll(rawRowRanges);

            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            int scoreBase = nextTask.getAndIncrement();
            for (Range range : rawRowRanges) {
                rows.addRange(range);
            }
            return ScoredGlobalIndexResult.create(rows, rowId -> scoreBase + (float) rowId);
        }
    }

    private static class PreFilterRecordingFlinkVectorRead extends FlinkDataEvolutionVectorRead {

        private final List<Range> rawSearchRanges = new ArrayList<>();
        private final List<List<Long>> rawSearchPreFilters = new ArrayList<>();

        private PreFilterRecordingFlinkVectorRead() {
            super(
                    null,
                    null,
                    null,
                    100,
                    new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[] {1.0f},
                    Collections.singletonMap("test.vector.metric", "l2"),
                    environment());
        }

        @Override
        protected int flinkParallelism() {
            return 3;
        }

        @Override
        protected List<byte[]> executeRawSearchGroups(
                List<List<SerializedSplit>> groups, String metric, int parallelism) {
            return groups.stream()
                    .map(group -> executeRawSearchGroup(group, metric))
                    .collect(Collectors.toList());
        }

        @Override
        protected ScoredGlobalIndexResult readRawSearch(
                List<Range> rawRowRanges,
                @Nullable RoaringNavigableMap64 preFilter,
                String metric,
                float[] queryVector) {
            assertThat(rawRowRanges).hasSize(1);
            assertThat(metric).isEqualTo("l2");
            assertThat(queryVector).containsExactly(1.0f);
            rawSearchRanges.add(rawRowRanges.get(0));
            if (preFilter == null) {
                rawSearchPreFilters.add(null);
            } else {
                List<Long> rowIds = new ArrayList<>();
                for (long rowId : preFilter) {
                    rowIds.add(rowId);
                }
                rawSearchPreFilters.add(rowIds);
            }
            return ScoredGlobalIndexResult.createEmpty();
        }
    }

    private static class ExecutingFlinkVectorRead extends FlinkDataEvolutionVectorRead {

        private ExecutingFlinkVectorRead(StreamExecutionEnvironment env) {
            super(
                    null,
                    null,
                    null,
                    100,
                    new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[] {1.0f},
                    Collections.singletonMap("test.vector.metric", "l2"),
                    env);
        }

        @Override
        protected int flinkParallelism() {
            return 2;
        }

        @Override
        protected String flinkJobName(String operatorName) {
            return "Vector Search Test - " + operatorName;
        }

        @Override
        protected ScoredGlobalIndexResult readRawSearch(
                List<Range> rawRowRanges,
                @Nullable RoaringNavigableMap64 preFilter,
                String metric,
                float[] queryVector) {
            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            for (Range range : rawRowRanges) {
                rows.addRange(range);
            }
            return ScoredGlobalIndexResult.create(rows, rowId -> (float) rowId);
        }
    }

    private static class L2Indexer implements VectorGlobalIndexer {

        @Override
        public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public GlobalIndexReader createReader(
                GlobalIndexFileReader fileReader,
                List<GlobalIndexIOMeta> files,
                ExecutorService executor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String metric() {
            return "l2";
        }
    }
}
