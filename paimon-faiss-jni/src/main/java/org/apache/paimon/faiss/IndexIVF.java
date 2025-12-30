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
 * Utility class for IVF (Inverted File) index operations.
 *
 * <p>IVF indexes partition the vector space into clusters (cells) and only
 * search a subset of clusters during search. The {@code nprobe} parameter
 * controls how many clusters to search, trading off between speed and accuracy.
 *
 * <p>Example usage:
 * <pre>{@code
 * Index index = IndexFactory.createIVFFlat(128, 1000, MetricType.L2);
 * index.train(trainingVectors);
 * index.add(vectors);
 *
 * // Set number of clusters to probe during search
 * IndexIVF.setNprobe(index, 10);  // Search 10 out of 1000 clusters
 *
 * SearchResult result = index.search(queries, 10);
 * }</pre>
 */
public final class IndexIVF {

    private IndexIVF() {
        // Static utility class
    }

    /**
     * Get the number of clusters to probe during search (nprobe).
     *
     * @param index the IVF index
     * @return the current nprobe value
     * @throws IllegalArgumentException if the index is not an IVF index
     */
    public static int getNprobe(Index index) {
        return FaissNative.ivfGetNprobe(index.getNativeHandle());
    }

    /**
     * Set the number of clusters to probe during search (nprobe).
     *
     * <p>Higher values increase accuracy but decrease search speed.
     * A good starting point is 1-10% of the total number of clusters.
     *
     * @param index the IVF index
     * @param nprobe the number of clusters to probe
     * @throws IllegalArgumentException if the index is not an IVF index
     */
    public static void setNprobe(Index index, int nprobe) {
        if (nprobe <= 0) {
            throw new IllegalArgumentException("nprobe must be positive: " + nprobe);
        }
        FaissNative.ivfSetNprobe(index.getNativeHandle(), nprobe);
    }

    /**
     * Get the total number of clusters (nlist) in the index.
     *
     * @param index the IVF index
     * @return the number of clusters
     * @throws IllegalArgumentException if the index is not an IVF index
     */
    public static int getNlist(Index index) {
        return FaissNative.ivfGetNlist(index.getNativeHandle());
    }
}

