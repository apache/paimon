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

import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.table.source.IndexVectorSearchSplit;
import org.apache.paimon.table.source.RawVectorSearchSplit;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.table.source.VectorSearchSplit;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
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
}
