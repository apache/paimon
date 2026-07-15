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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for deterministic reciprocal-rank fusion of physical search positions. */
class PrimaryKeySearchRankerTest {

    @Test
    void testFusesDuplicatePositionsAndAssignsTheSameRankToLocalTies() {
        PrimaryKeySearchPosition a = position("a", 0, 10F);
        PrimaryKeySearchPosition b = position("b", 0, 10F);
        PrimaryKeySearchPosition c = position("c", 0, 5F);
        List<PrimaryKeySearchPosition> fused =
                PrimaryKeySearchRanker.rrf(
                        Arrays.asList(
                                Arrays.asList(c, b, a),
                                Arrays.asList(a.withScore(1F), c.withScore(8F))),
                        3);

        assertThat(fused)
                .extracting(PrimaryKeySearchPosition::dataFileName)
                .containsExactly("a", "c", "b");
        assertThat(fused.get(0).score())
                .isCloseTo((float) (1D / 61D + 1D / 62D), within(0.000001F));
        assertThat(fused.get(1).score())
                .isCloseTo((float) (1D / 63D + 1D / 61D), within(0.000001F));
        assertThat(fused.get(2).score()).isCloseTo((float) (1D / 61D), within(0.000001F));
    }

    @Test
    void testUsesRouteWeightsAndDeterministicPhysicalTieBreaking() {
        PrimaryKeySearchPosition a = position("a", 0, 1F);
        PrimaryKeySearchPosition b = position("b", 0, 1F);
        List<PrimaryKeySearchPosition> fused =
                PrimaryKeySearchRanker.weightedRrf(
                        Arrays.asList(
                                new PrimaryKeySearchRanker.Ranking(
                                        Collections.singletonList(a), 2D),
                                new PrimaryKeySearchRanker.Ranking(
                                        Collections.singletonList(b), 2D)),
                        1);

        assertThat(fused).hasSize(1);
        assertThat(fused.get(0).dataFileName()).isEqualTo("a");
        assertThat(fused.get(0).score()).isCloseTo((float) (2D / 61D), within(0.000001F));
    }

    @Test
    void testWeightedScoreNormalizesEachPhysicalRoute() {
        PrimaryKeySearchPosition a = position("a", 0, 0F);
        PrimaryKeySearchPosition b = position("b", 0, 10F);
        PrimaryKeySearchPosition c = position("c", 0, 0F);

        List<PrimaryKeySearchPosition> fused =
                PrimaryKeySearchRanker.weightedScore(
                        Arrays.asList(
                                new PrimaryKeySearchRanker.Ranking(Arrays.asList(a, b), 1D),
                                new PrimaryKeySearchRanker.Ranking(
                                        Arrays.asList(a.withScore(100F), c), 2D)),
                        3);

        assertThat(fused)
                .extracting(PrimaryKeySearchPosition::dataFileName)
                .containsExactly("a", "b", "c");
        assertThat(fused).extracting(PrimaryKeySearchPosition::score).containsExactly(2F, 1F, 0F);
    }

    private static PrimaryKeySearchPosition position(
            String dataFileName, long rowPosition, float score) {
        return new PrimaryKeySearchPosition(
                BinaryRow.EMPTY_ROW, 0, dataFileName, rowPosition, score);
    }

    private static org.assertj.core.data.Offset<Float> within(float value) {
        return org.assertj.core.data.Offset.offset(value);
    }
}
