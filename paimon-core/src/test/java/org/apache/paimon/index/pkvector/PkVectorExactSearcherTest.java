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

package org.apache.paimon.index.pkvector;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** Tests for {@link PkVectorExactSearcher}. */
class PkVectorExactSearcherTest {

    @Test
    void testDistancesForSupportedMetrics() throws Exception {
        assertThat(distance("l2")).isEqualTo(1F);
        assertThat(distance("cosine")).isEqualTo(0F);
        assertThat(distance("inner_product")).isEqualTo(-2F);
    }

    @Test
    void testRejectsInvalidSearchInput() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> search(new float[] {1}, "l2", 1))
                .withMessageContaining("dimension");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> search(new float[] {1, 0}, "l2", 0))
                .withMessageContaining("positive");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> search(new float[] {1, 0}, "manhattan", 1))
                .withMessageContaining("Unsupported");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> search(new float[] {Float.NaN, 0}, "l2", 1))
                .withMessageContaining("finite");
    }

    @Test
    void testPreservesNullAndDeletedPhysicalPositions() throws Exception {
        float[][] vectors = {{3, 0}, null, {1, 0}, {2, 0}};
        List<PkVectorSearchResult> results;
        try (PkVectorReader reader = new ArrayReader(vectors)) {
            results =
                    PkVectorExactSearcher.search(
                            "data-file",
                            reader,
                            new float[] {0, 0},
                            "l2",
                            2,
                            position -> position == 2);
        }

        assertThat(results)
                .extracting(
                        PkVectorSearchResult::dataFileName,
                        PkVectorSearchResult::rowPosition,
                        PkVectorSearchResult::distance)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("data-file", 3L, 4F),
                        org.assertj.core.groups.Tuple.tuple("data-file", 0L, 9F));
    }

    private static float distance(String metric) throws IOException {
        try (PkVectorReader reader = new ArrayReader(new float[][] {{1, 0}})) {
            return PkVectorExactSearcher.search(
                            "data-file", reader, new float[] {2, 0}, metric, 1, position -> false)
                    .get(0)
                    .distance();
        }
    }

    private static void search(float[] query, String metric, int limit) throws IOException {
        try (PkVectorReader reader = new ArrayReader(new float[][] {{1, 0}})) {
            PkVectorExactSearcher.search(
                    "data-file", reader, query, metric, limit, position -> false);
        }
    }

    private static class ArrayReader implements PkVectorReader {

        private final float[][] vectors;
        private int position;

        private ArrayReader(float[][] vectors) {
            this.vectors = vectors;
        }

        @Override
        public int dimension() {
            return 2;
        }

        @Override
        public long rowCount() {
            return vectors.length;
        }

        @Override
        public boolean readNextVector(float[] reuse) {
            float[] vector = vectors[position++];
            if (vector == null) {
                return false;
            }
            System.arraycopy(vector, 0, reuse, 0, reuse.length);
            return true;
        }

        @Override
        public void close() throws IOException {}
    }
}
