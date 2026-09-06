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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ScoredGlobalIndexResult}. */
public class ScoredGlobalIndexResultTest {

    @Test
    public void testMergeEmptyResults() {
        ScoredGlobalIndexResult merged = ScoredGlobalIndexResult.merge(Collections.emptyList());

        assertThat(merged.results()).isEmpty();
    }

    @Test
    public void testMergeSingleResult() {
        ScoredGlobalIndexResult result = result(new long[] {1}, new float[] {0.1f});

        assertThat(ScoredGlobalIndexResult.merge(Collections.singletonList(result)))
                .isSameAs(result);
    }

    @Test
    public void testMergeDisjointResults() {
        ScoredGlobalIndexResult first = result(new long[] {1, 3}, new float[] {0.1f, 0.3f});
        ScoredGlobalIndexResult second = result(new long[] {2, 4}, new float[] {0.2f, 0.4f});

        ScoredGlobalIndexResult merged =
                ScoredGlobalIndexResult.merge(Arrays.asList(first, second));

        assertThat(merged.results().getIntCardinality()).isEqualTo(4);
        assertThat(merged.results()).contains(1L, 2L, 3L, 4L);
        assertThat(merged.scoreGetter().score(1L)).isEqualTo(0.1f);
        assertThat(merged.scoreGetter().score(2L)).isEqualTo(0.2f);
        assertThat(merged.scoreGetter().score(3L)).isEqualTo(0.3f);
        assertThat(merged.scoreGetter().score(4L)).isEqualTo(0.4f);
    }

    @Test
    public void testMergeKeepsFirstScoreForDuplicateRowId() {
        ScoredGlobalIndexResult first = result(new long[] {1, 2}, new float[] {0.1f, 0.2f});
        ScoredGlobalIndexResult second = result(new long[] {2, 3}, new float[] {2.0f, 0.3f});

        ScoredGlobalIndexResult merged =
                ScoredGlobalIndexResult.merge(Arrays.asList(first, second));

        assertThat(merged.results().getIntCardinality()).isEqualTo(3);
        assertThat(merged.scoreGetter().score(2L)).isEqualTo(0.2f);
    }

    @Test
    public void testMergeManyResultsMaterializesScoresBeforeTopK() {
        int resultCount = 5000;
        AtomicInteger scoreCalls = new AtomicInteger();
        List<ScoredGlobalIndexResult> results = new ArrayList<>(resultCount);
        for (int i = 0; i < resultCount; i++) {
            RoaringNavigableMap64 rowIds = new RoaringNavigableMap64();
            rowIds.add(i);
            final float score = i;
            results.add(
                    ScoredGlobalIndexResult.create(
                            rowIds,
                            ignored -> {
                                scoreCalls.incrementAndGet();
                                return score;
                            }));
        }

        ScoredGlobalIndexResult merged = ScoredGlobalIndexResult.merge(results);
        int scoreCallsAfterMerge = scoreCalls.get();
        RoaringNavigableMap64 topK = merged.topK(1).results();

        assertThat(merged.results().getIntCardinality()).isEqualTo(resultCount);
        assertThat(scoreCallsAfterMerge).isEqualTo(resultCount);
        assertThat(scoreCalls).hasValue(scoreCallsAfterMerge);
        assertThat(topK.getIntCardinality()).isEqualTo(1);
        assertThat(topK).contains(resultCount - 1L);
    }

    @Test
    public void testTopKBreaksBoundaryTiesByRowId() {
        ScoredGlobalIndexResult result =
                result(new long[] {1, 2, 3, 4}, new float[] {0.5f, 0.5f, 0.5f, 0.9f});

        RoaringNavigableMap64 topK = result.topK(2).results();

        assertThat(topK.getIntCardinality()).isEqualTo(2);
        assertThat(topK).contains(1L, 4L);
        assertThat(topK).doesNotContain(2L, 3L);
    }

    @Test
    public void testTopKBreaksTieGroupBySmallerRowId() {
        ScoredGlobalIndexResult result =
                result(new long[] {1, 2, 3, 4, 5}, new float[] {0.5f, 0.5f, 0.5f, 0.5f, 0.9f});

        // The high-scored row 5 forces a heap eviction within the 0.5 tie group, so this
        // distinguishes the score-only heap ({2, 3, 5}) from the fixed heap ({1, 2, 5}).
        RoaringNavigableMap64 topK = result.topK(3).results();

        assertThat(topK.getIntCardinality()).isEqualTo(3);
        assertThat(topK).contains(1L, 2L, 5L);
        assertThat(topK).doesNotContain(3L, 4L);
    }

    @Test
    public void testTopKReturnsSameResultWhenCardinalityDoesNotExceedK() {
        ScoredGlobalIndexResult result =
                result(new long[] {1, 2, 3}, new float[] {0.1f, 0.2f, 0.3f});

        assertThat(result.topK(3)).isSameAs(result);
        assertThat(result.topK(5)).isSameAs(result);
    }

    private ScoredGlobalIndexResult result(long[] rowIds, float[] scores) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        Map<Long, Float> scoreMap = new HashMap<>();
        for (int i = 0; i < rowIds.length; i++) {
            bitmap.add(rowIds[i]);
            scoreMap.put(rowIds[i], scores[i]);
        }
        return ScoredGlobalIndexResult.create(bitmap, scoreMap::get);
    }
}
