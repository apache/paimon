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

package org.apache.paimon.spark.read;

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
import org.apache.paimon.table.source.IndexVectorSearchSplit;
import org.apache.paimon.table.source.RawVectorSearchSplit;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.table.source.VectorSearchSplit;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.SerializableFunction;

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

/** Tests for {@link SparkVectorReadImpl}. */
public class SparkVectorReadImplTest {

    @Test
    public void testRawSearchUsesSparkPath() {
        TestingSparkVectorRead read = new TestingSparkVectorRead();
        RawVectorSearchSplit rawSplit =
                new RawVectorSearchSplit(
                        Collections.singletonList(new Range(42, 42)),
                        Collections.emptyList(),
                        null);
        VectorScan.Plan plan = () -> Collections.singletonList(rawSplit);

        GlobalIndexResult result = read.read(plan);

        assertThat(read.rawSparkPathUsed).isTrue();
        assertThat(result.results().contains(42L)).isTrue();
    }

    @Test
    public void testRawSearchSplitsRangesAcrossSparkTasks() {
        RecordingSparkVectorRead read = new RecordingSparkVectorRead();
        RawVectorSearchSplit rawSplit =
                new RawVectorSearchSplit(
                        Collections.singletonList(new Range(0, 63)), Collections.emptyList(), null);

        ScoredGlobalIndexResult result =
                read.readRawSplitsInSpark(Collections.singletonList(rawSplit), null, null);

        assertThat(read.rawSearchRanges).containsExactly(new Range(0, 31), new Range(32, 63));
        assertThat(read.sparkParallelism).isEqualTo(2);
        assertThat(result.results().getLongCardinality()).isEqualTo(64);
    }

    @Test
    public void testDistributedIndexRefinesAfterGlobalMerge() {
        DistributedRefineSparkVectorRead read = new DistributedRefineSparkVectorRead();

        ScoredGlobalIndexResult result =
                read.readIndexSplitsInSpark(indexSplits("test-vector-ann", 4), new L2Indexer());

        assertThat(read.sparkParallelism).isEqualTo(2);
        assertThat(read.rawSearchCandidateRows).containsExactly(0L, 2L);
        assertThat(result.results().getLongCardinality()).isEqualTo(1);
        assertThat(result.results().contains(0L)).isTrue();
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

    private static class TestingSparkVectorRead extends SparkVectorReadImpl {

        private boolean rawSparkPathUsed;

        private TestingSparkVectorRead() {
            super(
                    null,
                    null,
                    null,
                    10,
                    new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[] {1.0f},
                    null);
        }

        @Override
        protected GlobalIndexResult readSplits(List<? extends VectorSearchSplit> splits) {
            throw new AssertionError("Raw search should not fall back to local vector read.");
        }

        @Override
        protected ScoredGlobalIndexResult readIndexSplitsInSpark(
                List<IndexVectorSearchSplit> splits, @Nullable GlobalIndexer globalIndexer) {
            throw new AssertionError("Index search is not part of this test.");
        }

        @Override
        protected ScoredGlobalIndexResult readRawSplitsInSpark(
                List<RawVectorSearchSplit> splits,
                @Nullable GlobalIndexer globalIndexer,
                @Nullable RoaringNavigableMap64 preFilter) {
            rawSparkPathUsed = true;
            assertThat(splits).hasSize(1);
            assertThat(globalIndexer).isNull();
            assertThat(preFilter).isNull();

            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            rows.add(42L);
            return ScoredGlobalIndexResult.create(rows, rowId -> 1.0f);
        }
    }

    private static class DistributedRefineSparkVectorRead extends SparkVectorReadImpl {

        private int sparkParallelism;
        private List<Long> rawSearchCandidateRows = Collections.emptyList();

        private DistributedRefineSparkVectorRead() {
            super(
                    null,
                    null,
                    null,
                    1,
                    new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[] {0.0f},
                    Collections.singletonMap("refine_factor", "2"));
        }

        @Override
        protected int sparkParallelism() {
            return 2;
        }

        @Override
        protected <I, O> List<O> mapInSpark(
                List<I> data, SerializableFunction<I, O> func, int parallelism) {
            sparkParallelism = parallelism;
            assertThat(data).hasSize(2);
            try {
                GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
                return Arrays.asList(
                        uncheckedCast(serializer.serialize(scoredResult(2L, 100.0f))),
                        uncheckedCast(serializer.serialize(scoredResult(0L, 1.0f))));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected ScoredGlobalIndexResult readRawSearch(
                List<Range> rawRowRanges,
                @Nullable RoaringNavigableMap64 preFilter,
                @Nullable GlobalIndexer globalIndexer,
                float[] queryVector) {
            assertThat(globalIndexer).isInstanceOf(VectorGlobalIndexer.class);
            assertThat(((VectorGlobalIndexer) globalIndexer).metric()).isEqualTo("l2");
            assertThat(queryVector).containsExactly(0.0f);
            assertThat(preFilter).isNotNull();
            rawSearchCandidateRows = new ArrayList<>();
            for (long rowId : preFilter) {
                rawSearchCandidateRows.add(rowId);
            }

            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            for (long rowId : preFilter) {
                rows.add(rowId);
            }
            return ScoredGlobalIndexResult.create(
                            rows, rowId -> rowId == 0L ? 1.0f : 1.0f / (1.0f + rowId * rowId))
                    .topK(1);
        }

        @SuppressWarnings("unchecked")
        private <O> O uncheckedCast(byte[] value) {
            return (O) value;
        }

        private static ScoredGlobalIndexResult scoredResult(long rowId, float score) {
            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            rows.add(rowId);
            return ScoredGlobalIndexResult.create(rows, candidate -> score);
        }
    }

    private static class RecordingSparkVectorRead extends SparkVectorReadImpl {

        private final AtomicInteger nextTask = new AtomicInteger();
        private final List<Range> rawSearchRanges =
                Collections.synchronizedList(new java.util.ArrayList<>());
        private int sparkParallelism;

        private RecordingSparkVectorRead() {
            super(
                    null,
                    null,
                    null,
                    100,
                    new DataField(0, "vec", new ArrayType(DataTypes.FLOAT())),
                    new float[] {1.0f},
                    Collections.singletonMap("test.vector.metric", "l2"));
        }

        @Override
        protected int sparkParallelism() {
            return 2;
        }

        @Override
        protected <I, O> List<O> mapInSpark(
                List<I> data,
                org.apache.paimon.utils.SerializableFunction<I, O> func,
                int parallelism) {
            sparkParallelism = parallelism;
            return data.stream().map(func::apply).collect(Collectors.toList());
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
