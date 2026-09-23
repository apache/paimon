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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.predicate.HybridSearchRoute;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for snapshot-scoped hybrid search on primary-key physical positions. */
class PrimaryKeyHybridSearchTest {

    @Test
    void testRrfFusesPhysicalRoutesAndDeduplicatesSourceFiles() {
        PrimaryKeySearchPosition a = position("a", 0, 10F);
        PrimaryKeySearchPosition b = position("b", 0, 9F);
        PrimaryKeySearchPosition c = position("c", 0, 8F);
        PrimaryKeyScoredResult vectorResult =
                result(7, new String[] {"a", "b"}, new PrimaryKeySearchPosition[] {a, b});
        PrimaryKeyScoredResult textResult =
                result(
                        7,
                        new String[] {"b", "c"},
                        new PrimaryKeySearchPosition[] {b.withScore(100F), c});
        HybridSearchRoute vectorRoute = new HybridSearchRoute("vector", new float[] {1F}, 2, 1F);
        HybridSearchRoute textRoute = HybridSearchRoute.fullText("text", "query", 2, 2F, null);
        HybridSearchBuilderImpl builder =
                (HybridSearchBuilderImpl)
                        new HybridSearchBuilderImpl(null)
                                .addRoute(vectorRoute)
                                .addRoute(textRoute)
                                .withLimit(3)
                                .withRrfRanker();

        ScoredGlobalIndexResult ranked =
                builder.rank(
                        Arrays.asList(
                                new HybridSearchBuilder.RouteResult(vectorRoute, vectorResult),
                                new HybridSearchBuilder.RouteResult(textRoute, textResult)));

        assertThat(ranked).isInstanceOf(PrimaryKeyScoredResult.class);
        PrimaryKeyScoredResult physical = (PrimaryKeyScoredResult) ranked;
        assertThat(physical.snapshotId()).isEqualTo(7);
        assertThat(physical.positions())
                .extracting(PrimaryKeySearchPosition::dataFileName)
                .containsExactly("b", "c", "a");
        assertThat(physical.splits()).hasSize(3);
        assertThat(physical.positions().get(0).score())
                .isCloseTo(
                        (float) (1D / 62D + 2D / 61D),
                        org.assertj.core.data.Offset.offset(0.000001F));
    }

    @Test
    void testRejectsPhysicalRoutesFromDifferentSnapshots() {
        PrimaryKeyScoredResult first =
                result(
                        7,
                        new String[] {"a"},
                        new PrimaryKeySearchPosition[] {position("a", 0, 1F)});
        PrimaryKeyScoredResult second =
                result(
                        8,
                        new String[] {"a"},
                        new PrimaryKeySearchPosition[] {position("a", 0, 1F)});
        HybridSearchRoute firstRoute = new HybridSearchRoute("first", new float[] {1F}, 1, 1F);
        HybridSearchRoute secondRoute = new HybridSearchRoute("second", new float[] {1F}, 1, 1F);
        HybridSearchBuilderImpl builder =
                (HybridSearchBuilderImpl)
                        new HybridSearchBuilderImpl(null)
                                .addRoute(firstRoute)
                                .addRoute(secondRoute)
                                .withLimit(1);

        assertThatThrownBy(
                        () ->
                                builder.rank(
                                        Arrays.asList(
                                                new HybridSearchBuilder.RouteResult(
                                                        firstRoute, first),
                                                new HybridSearchBuilder.RouteResult(
                                                        secondRoute, second))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same snapshot");
    }

    @Test
    void testRejectsMixedPhysicalAndGlobalAddressSpaces() {
        PrimaryKeyScoredResult physical =
                result(
                        7,
                        new String[] {"a"},
                        new PrimaryKeySearchPosition[] {position("a", 0, 1F)});
        RoaringNavigableMap64 rowIds = new RoaringNavigableMap64();
        rowIds.add(1L);
        ScoredGlobalIndexResult global = ScoredGlobalIndexResult.create(rowIds, ignored -> 1F);
        HybridSearchRoute physicalRoute =
                new HybridSearchRoute("physical", new float[] {1F}, 1, 1F);
        HybridSearchRoute globalRoute = new HybridSearchRoute("global", new float[] {1F}, 1, 1F);
        HybridSearchBuilderImpl builder =
                (HybridSearchBuilderImpl)
                        new HybridSearchBuilderImpl(null)
                                .addRoute(physicalRoute)
                                .addRoute(globalRoute)
                                .withLimit(1);

        assertThatThrownBy(
                        () ->
                                builder.rank(
                                        Arrays.asList(
                                                new HybridSearchBuilder.RouteResult(
                                                        physicalRoute, physical),
                                                new HybridSearchBuilder.RouteResult(
                                                        globalRoute, global))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("address spaces");
    }

    private static PrimaryKeyScoredResult result(
            long snapshotId, String[] files, PrimaryKeySearchPosition[] positions) {
        DataSplit source =
                DataSplit.builder()
                        .withSnapshot(snapshotId)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withTotalBuckets(1)
                        .withDataFiles(
                                Arrays.stream(files)
                                        .map(PrimaryKeyHybridSearchTest::dataFile)
                                        .collect(java.util.stream.Collectors.toList()))
                        .build();
        return new PrimaryKeyScoredResult(
                snapshotId, Collections.singletonList(source), Arrays.asList(positions));
    }

    private static PrimaryKeySearchPosition position(String file, long row, float score) {
        return new PrimaryKeySearchPosition(BinaryRow.EMPTY_ROW, 0, file, row, score);
    }

    private static DataFileMeta dataFile(String fileName) {
        return DataFileMeta.forAppend(
                fileName,
                100,
                5,
                SimpleStats.EMPTY_STATS,
                0,
                1,
                1,
                Collections.emptyList(),
                null,
                FileSource.COMPACT,
                null,
                null,
                null,
                null);
    }
}
