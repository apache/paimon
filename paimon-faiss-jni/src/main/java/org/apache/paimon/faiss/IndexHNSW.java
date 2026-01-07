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

package org.apache.paimon.faiss;

/**
 * Utility class for HNSW (Hierarchical Navigable Small World) index operations.
 *
 * <p>HNSW indexes build a graph structure for fast approximate nearest neighbor search. The key
 * parameters are:
 *
 * <ul>
 *   <li>{@code M} - The number of neighbors in the graph. Higher values increase memory usage and
 *       build time but improve search accuracy.
 *   <li>{@code efConstruction} - The size of the dynamic candidate list during construction. Higher
 *       values increase build time but can improve the graph quality.
 *   <li>{@code efSearch} - The size of the dynamic candidate list during search. Higher values
 *       increase search accuracy at the cost of speed.
 * </ul>
 */
public final class IndexHNSW {

    private IndexHNSW() {
        // Static utility class
    }

    /**
     * Get the efSearch parameter.
     *
     * <p>This controls the size of the dynamic candidate list during search. Higher values give
     * more accurate results but slower search.
     *
     * @param index the HNSW index
     * @return the current efSearch value
     * @throws IllegalArgumentException if the index is not an HNSW index
     */
    public static int getEfSearch(Index index) {
        return FaissNative.hnswGetEfSearch(index.getNativeHandle());
    }

    /**
     * Set the efSearch parameter.
     *
     * <p>This should be at least k (the number of neighbors requested in search). Typical values
     * range from 16 to 256. Higher values give more accurate results but slower search.
     *
     * @param index the HNSW index
     * @param efSearch the efSearch value
     * @throws IllegalArgumentException if the index is not an HNSW index
     */
    public static void setEfSearch(Index index, int efSearch) {
        if (efSearch <= 0) {
            throw new IllegalArgumentException("efSearch must be positive: " + efSearch);
        }
        FaissNative.hnswSetEfSearch(index.getNativeHandle(), efSearch);
    }

    /**
     * Get the efConstruction parameter.
     *
     * <p>This was the size of the dynamic candidate list during index construction. It cannot be
     * changed after the index is built.
     *
     * @param index the HNSW index
     * @return the efConstruction value
     * @throws IllegalArgumentException if the index is not an HNSW index
     */
    public static int getEfConstruction(Index index) {
        return FaissNative.hnswGetEfConstruction(index.getNativeHandle());
    }

    /**
     * Set the efConstruction parameter.
     *
     * <p>This controls the size of the dynamic candidate list during construction. It must be set
     * before adding any vectors to the index. Higher values give more accurate results but slower
     * construction. Typical values range from 40 to 400.
     *
     * @param index the HNSW index
     * @param efConstruction the efConstruction value
     * @throws IllegalArgumentException if the index is not an HNSW index or efConstruction is not
     *     positive
     */
    public static void setEfConstruction(Index index, int efConstruction) {
        if (efConstruction <= 0) {
            throw new IllegalArgumentException(
                    "efConstruction must be positive: " + efConstruction);
        }
        FaissNative.hnswSetEfConstruction(index.getNativeHandle(), efConstruction);
    }
}
