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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link MultiVectorSearchRanker}. */
public class MultiVectorSearchRankerTest {

    @Test
    public void testRrfFavorsRowsReturnedByMultipleRoutes() {
        ScoredGlobalIndexResult first = result(new long[] {1, 2}, new float[] {0.9f, 0.8f});
        ScoredGlobalIndexResult second = result(new long[] {2, 3}, new float[] {0.7f, 0.6f});

        ScoredGlobalIndexResult ranked =
                MultiVectorSearchRanker.rrf(
                        Arrays.asList(first, second), new float[] {1.0f, 1.0f}, 2);

        assertThat(ranked.results().getIntCardinality()).isEqualTo(2);
        assertThat(ranked.results()).contains(2L);
        assertThat(ranked.scoreGetter().score(2L)).isGreaterThan(ranked.scoreGetter().score(1L));
    }

    @Test
    public void testWeightedScoreUsesAlignedWeightsAfterEmptyRouteIsSkipped() {
        ScoredGlobalIndexResult result = result(new long[] {1, 2}, new float[] {0.3f, 0.2f});

        ScoredGlobalIndexResult ranked =
                MultiVectorSearchRanker.weightedScore(
                        Collections.singletonList(result), new float[] {3.0f}, 1);

        assertThat(ranked.results().getIntCardinality()).isEqualTo(1);
        assertThat(ranked.results()).contains(1L);
        assertThat(ranked.scoreGetter().score(1L)).isCloseTo(0.9f, within(0.000001f));
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
