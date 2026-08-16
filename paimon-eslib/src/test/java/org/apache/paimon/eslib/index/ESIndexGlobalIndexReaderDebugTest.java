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

package org.apache.paimon.eslib.index;

import org.apache.paimon.predicate.VectorSearch;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ESIndexGlobalIndexReaderDebugTest {

    @Test
    void samplesLongArraysWithTruncation() {
        assertEquals(
                "[11, 22, 33, ...(+2)]",
                ESIndexGlobalIndexReader.sample(new long[] {11L, 22L, 33L, 44L, 55L}, 5, 3));
    }

    @Test
    void samplesFloatArraysWithTruncation() {
        assertEquals(
                "[0.1, 0.2, 0.3, ...(+1)]",
                ESIndexGlobalIndexReader.sample(new float[] {0.1f, 0.2f, 0.3f, 0.4f}, 4, 3));
    }

    @Test
    void resolvesVectorSearchTopKFromOptions() {
        assertEquals(
                1000,
                ESIndexGlobalIndexReader.vectorSearchTopK(
                        new VectorSearch(
                                new float[] {1.0f},
                                10,
                                "embedding",
                                Map.of("hnsw.num_candidates", "1000"))));
        assertEquals(
                64,
                ESIndexGlobalIndexReader.vectorSearchTopK(
                        new VectorSearch(
                                new float[] {1.0f},
                                10,
                                "embedding",
                                Map.of("hnsw.ef_search", "64"))));
        assertEquals(
                10,
                ESIndexGlobalIndexReader.vectorSearchTopK(
                        new VectorSearch(
                                new float[] {1.0f},
                                10,
                                "embedding",
                                Map.of("hnsw.num_candidates", "5"))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ESIndexGlobalIndexReader.vectorSearchTopK(
                                new VectorSearch(
                                        new float[] {1.0f},
                                        10,
                                        "embedding",
                                        Map.of("hnsw.num_candidates", "bad"))));
    }
}
