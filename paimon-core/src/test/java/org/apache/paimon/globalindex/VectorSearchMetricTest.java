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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** Tests for {@link VectorSearchMetric}. */
class VectorSearchMetricTest {

    @Test
    void testNormalizeAndSupportedMetrics() {
        assertThat(VectorSearchMetric.normalize("INNER-PRODUCT")).isEqualTo("inner_product");
        assertThat(VectorSearchMetric.isSupported("L2")).isTrue();
        assertThat(VectorSearchMetric.isSupported("Cosine")).isTrue();
        assertThat(VectorSearchMetric.isSupported("INNER-PRODUCT")).isTrue();
        assertThat(VectorSearchMetric.isSupported("manhattan")).isFalse();
        assertThat(VectorSearchMetric.isSupported(null)).isFalse();
    }

    @Test
    void testScoreAndDistanceForSupportedMetrics() {
        float[] query = {2, 0};
        float[] stored = {1, 0};

        assertThat(VectorSearchMetric.computeScore(query, stored, "l2")).isEqualTo(0.5F);
        assertThat(VectorSearchMetric.computeDistance(query, stored, "l2")).isEqualTo(1F);
        assertThat(VectorSearchMetric.computeScore(query, stored, "cosine")).isEqualTo(1F);
        assertThat(VectorSearchMetric.computeDistance(query, stored, "cosine")).isEqualTo(0F);
        assertThat(VectorSearchMetric.computeScore(query, stored, "inner_product")).isEqualTo(2F);
        assertThat(VectorSearchMetric.computeDistance(query, stored, "inner_product"))
                .isEqualTo(-2F);
    }

    @Test
    void testZeroVectorCosine() {
        float[] zero = {0, 0};
        float[] stored = {1, 2};

        assertThat(VectorSearchMetric.computeScore(zero, stored, "cosine")).isZero();
        assertThat(VectorSearchMetric.computeDistance(zero, stored, "cosine")).isEqualTo(1F);
    }

    @Test
    void testCosineScoreIsUnclampedButDistanceIsBounded() {
        float[] query = {0.34558418F, 0.82161814F};
        float[] stored = {0.3455843F, 0.82161707F};

        assertThat(VectorSearchMetric.computeScore(query, stored, "cosine")).isEqualTo(1.0000001F);
        assertThat(VectorSearchMetric.computeDistance(query, stored, "cosine")).isZero();
    }

    @Test
    void testRepresentativeAndLargeL2UseFloatAccumulation() {
        assertThat(
                        VectorSearchMetric.computeDistance(
                                new float[] {0.1F, 0.2F, 0.3F},
                                new float[] {0.4F, 0.5F, 0.6F},
                                "l2"))
                .isEqualTo(0.27F);

        float[] query = {100_000F, 100_000F, 1F};
        float[] stored = {-100_000F, -100_000F, 0F};
        float distance = VectorSearchMetric.computeDistance(query, stored, "l2");
        assertThat(distance).isEqualTo(80_000_000_000F);
        assertThat(VectorSearchMetric.computeScore(query, stored, "l2"))
                .isEqualTo(1F / (1F + distance));
    }

    @Test
    void testScoreToDistance() {
        assertThat(VectorSearchMetric.scoreToDistance(0.25F, "l2")).isEqualTo(3F);
        assertThat(VectorSearchMetric.scoreToDistance(0.25F, "cosine")).isEqualTo(0.75F);
        assertThat(VectorSearchMetric.scoreToDistance(1.0000001F, "cosine")).isZero();
        assertThat(VectorSearchMetric.scoreToDistance(-1.0000001F, "cosine")).isEqualTo(2F);
        assertThat(VectorSearchMetric.scoreToDistance(0.25F, "inner_product")).isEqualTo(-0.25F);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> VectorSearchMetric.scoreToDistance(1F, "manhattan"))
                .withMessageContaining("Unknown vector search metric");
    }
}
