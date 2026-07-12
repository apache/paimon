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

package org.apache.paimon.globalindex;

import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link HybridSearchRanker}. */
public class HybridSearchRankerTest {

    @Test
    public void testRrfFavorsRowsReturnedByMultipleRoutes() {
        ScoredGlobalIndexResult first = result(new long[] {1, 2}, new float[] {0.9f, 0.8f});
        ScoredGlobalIndexResult second = result(new long[] {2, 3}, new float[] {0.7f, 0.6f});

        ScoredGlobalIndexResult ranked =
                HybridSearchRanker.rrf(Arrays.asList(first, second), new float[] {1.0f, 1.0f}, 2);

        assertThat(ranked.results().getIntCardinality()).isEqualTo(2);
        assertThat(ranked.results()).contains(2L);
        assertThat(ranked.scoreGetter().score(2L)).isGreaterThan(ranked.scoreGetter().score(1L));
    }

    @Test
    public void testRrfBreaksRouteScoreTiesByRowId() {
        ScoredGlobalIndexResult result =
                result(new long[] {3, 1, 2}, new float[] {1.0f, 1.0f, 1.0f}, new long[] {3, 1, 2});

        ScoredGlobalIndexResult ranked =
                HybridSearchRanker.rrf(Collections.singletonList(result), new float[] {1.0f}, 2);

        assertThat(ranked.results()).contains(1L, 2L);
        assertThat(ranked.results()).doesNotContain(3L);
        assertThat(ranked.scoreGetter().score(1L)).isCloseTo(1.0f / 61.0f, within(0.000001f));
        assertThat(ranked.scoreGetter().score(2L)).isCloseTo(1.0f / 62.0f, within(0.000001f));
    }

    @Test
    public void testWeightedScoreUsesAlignedWeightsAfterEmptyRouteIsSkipped() {
        ScoredGlobalIndexResult result = result(new long[] {1, 2}, new float[] {0.3f, 0.2f});

        ScoredGlobalIndexResult ranked =
                HybridSearchRanker.weightedScore(
                        Collections.singletonList(result), new float[] {3.0f}, 1);

        assertThat(ranked.results().getIntCardinality()).isEqualTo(1);
        assertThat(ranked.results()).contains(1L);
        // Within the route {0.3, 0.2} min-max maps 0.3 -> 1.0, so score = weight (3.0) * 1.0.
        assertThat(ranked.scoreGetter().score(1L)).isCloseTo(3.0f, within(0.000001f));
    }

    @Test
    public void testWeightedScoreNormalizesHeterogeneousRouteScales() {
        // Vector-like route: rowId 1 strongly matches, rowId 2 barely matches (bounded ~[0, 1]).
        ScoredGlobalIndexResult vectorRoute = result(new long[] {1, 2}, new float[] {0.95f, 0.10f});
        // BM25-like route: rowId 2 strongly matches, rowId 1 weakly (unbounded, larger magnitude).
        ScoredGlobalIndexResult textRoute = result(new long[] {1, 2}, new float[] {2.0f, 25.0f});

        // Vector route weighted 5x. Without normalization the BM25 magnitude would dominate; with
        // normalization each route maps its best hit to 1.0 and worst to 0.0, so weights decide.
        ScoredGlobalIndexResult ranked =
                HybridSearchRanker.weightedScore(
                        Arrays.asList(vectorRoute, textRoute), new float[] {5.0f, 1.0f}, 2);

        // rowId 1: 5 * 1.0 (vector best) + 1 * 0.0 (text worst) = 5.0
        // rowId 2: 5 * 0.0 (vector worst) + 1 * 1.0 (text best) = 1.0
        assertThat(ranked.scoreGetter().score(1L)).isCloseTo(5.0f, within(0.000001f));
        assertThat(ranked.scoreGetter().score(2L)).isCloseTo(1.0f, within(0.000001f));
        assertThat(ranked.scoreGetter().score(1L)).isGreaterThan(ranked.scoreGetter().score(2L));
    }

    @Test
    public void testWeightedScoreSingleHitRouteMapsToFullWeight() {
        // A route with a single hit has no spread (min == max); it must not produce NaN/Infinity
        // and must keep contributing rather than being zeroed out.
        ScoredGlobalIndexResult singleHit = result(new long[] {7}, new float[] {42.0f});

        ScoredGlobalIndexResult ranked =
                HybridSearchRanker.weightedScore(
                        Collections.singletonList(
                                new HybridSearchRanker.WeightedResult(singleHit, 2.0f)),
                        1);

        assertThat(ranked.scoreGetter().score(7L)).isCloseTo(2.0f, within(0.000001f));
    }

    @Test
    public void testWeightedScoreAllTiedRouteMapsEachHitToFullWeight() {
        // All hits share the same score (no relative signal); each maps to 1.0 -> weight.
        ScoredGlobalIndexResult tied = result(new long[] {1, 2, 3}, new float[] {5.0f, 5.0f, 5.0f});

        ScoredGlobalIndexResult ranked =
                HybridSearchRanker.weightedScore(
                        Collections.singletonList(
                                new HybridSearchRanker.WeightedResult(tied, 2.0f)),
                        3);

        assertThat(ranked.scoreGetter().score(1L)).isCloseTo(2.0f, within(0.000001f));
        assertThat(ranked.scoreGetter().score(2L)).isCloseTo(2.0f, within(0.000001f));
        assertThat(ranked.scoreGetter().score(3L)).isCloseTo(2.0f, within(0.000001f));
    }

    @Test
    public void testWeightedScoreTopKBreaksBoundaryTiesByRowId() {
        ScoredGlobalIndexResult tied =
                result(
                        new long[] {4, 3, 2, 1},
                        new float[] {5.0f, 5.0f, 5.0f, 5.0f},
                        new long[] {4, 3, 2, 1});

        ScoredGlobalIndexResult ranked =
                HybridSearchRanker.weightedScore(
                        Collections.singletonList(
                                new HybridSearchRanker.WeightedResult(tied, 2.0f)),
                        2);

        assertThat(ranked.results()).contains(1L, 2L);
        assertThat(ranked.results()).doesNotContain(3L, 4L);
        assertThat(ranked.scoreGetter().score(1L)).isCloseTo(2.0f, within(0.000001f));
        assertThat(ranked.scoreGetter().score(2L)).isCloseTo(2.0f, within(0.000001f));
    }

    @Test
    public void testMrrFavorsRowsWithStrongRanksAcrossRoutes() {
        ScoredGlobalIndexResult first = result(new long[] {1, 2}, new float[] {0.9f, 0.8f});
        ScoredGlobalIndexResult second = result(new long[] {2, 3}, new float[] {0.7f, 0.6f});

        assertThat(HybridSearchRanker.normalizeRanker("mrr"))
                .isEqualTo(HybridSearchRanker.MRR_RANKER);

        ScoredGlobalIndexResult ranked =
                HybridSearchRanker.rank(
                        HybridSearchRanker.MRR_RANKER,
                        Arrays.asList(
                                new HybridSearchRanker.WeightedResult(first, 1.0f),
                                new HybridSearchRanker.WeightedResult(second, 2.0f)),
                        2);

        assertThat(ranked.results()).contains(1L, 2L);
        assertThat(ranked.results()).doesNotContain(3L);
        assertThat(ranked.scoreGetter().score(2L)).isCloseTo(2.5f, within(0.000001f));
        assertThat(ranked.scoreGetter().score(1L)).isCloseTo(1.0f, within(0.000001f));
        assertThat(ranked.scoreGetter().score(2L)).isGreaterThan(ranked.scoreGetter().score(1L));
    }

    @Test
    public void testRejectNonFiniteWeights() {
        ScoredGlobalIndexResult result = result(new long[] {1}, new float[] {1.0f});

        assertThatThrownBy(
                        () ->
                                HybridSearchRanker.rrf(
                                        Collections.singletonList(result),
                                        new float[] {Float.NaN},
                                        1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Weight must be finite and positive");

        assertThatThrownBy(
                        () ->
                                HybridSearchRanker.weightedScore(
                                        Collections.singletonList(result),
                                        new float[] {Float.POSITIVE_INFINITY},
                                        1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Weight must be finite and positive");

        assertThatThrownBy(
                        () ->
                                new HybridSearchRanker.WeightedResult(
                                        result, Float.NEGATIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Weight must be finite and positive");
    }

    private ScoredGlobalIndexResult result(long[] rowIds, float[] scores) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        return result(rowIds, scores, bitmap);
    }

    private ScoredGlobalIndexResult result(long[] rowIds, float[] scores, long[] iterationOrder) {
        RoaringNavigableMap64 bitmap =
                new RoaringNavigableMap64() {
                    @Override
                    public Iterator<Long> iterator() {
                        return Arrays.stream(iterationOrder).boxed().iterator();
                    }
                };
        return result(rowIds, scores, bitmap);
    }

    private ScoredGlobalIndexResult result(
            long[] rowIds, float[] scores, RoaringNavigableMap64 bitmap) {
        Map<Long, Float> scoreMap = new HashMap<>();
        for (int i = 0; i < rowIds.length; i++) {
            bitmap.add(rowIds[i]);
            scoreMap.put(rowIds[i], scores[i]);
        }
        return ScoredGlobalIndexResult.create(bitmap, scoreMap::get);
    }
}
