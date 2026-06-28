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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ScoredGlobalIndexResult}. */
public class ScoredGlobalIndexResultTest {

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
