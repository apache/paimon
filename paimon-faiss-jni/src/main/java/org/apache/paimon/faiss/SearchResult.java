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

import java.util.Arrays;

/**
 * Result of a k-nearest neighbor search operation.
 *
 * <p>Contains the labels (IDs) and distances of the k nearest neighbors for each query vector.
 */
public class SearchResult {

    private final int numQueries;
    private final int k;
    private final long[] labels;
    private final float[] distances;

    /**
     * Create a new SearchResult.
     *
     * @param numQueries the number of query vectors
     * @param k the number of neighbors per query
     * @param labels the neighbor labels (numQueries * k)
     * @param distances the distances to neighbors (numQueries * k)
     */
    public SearchResult(int numQueries, int k, long[] labels, float[] distances) {
        this.numQueries = numQueries;
        this.k = k;
        this.labels = labels;
        this.distances = distances;
    }

    /**
     * Get the number of query vectors.
     *
     * @return the number of queries
     */
    public int getNumQueries() {
        return numQueries;
    }

    /**
     * Get the number of neighbors per query.
     *
     * @return k value
     */
    public int getK() {
        return k;
    }

    /**
     * Get all labels as a flat array.
     *
     * <p>The array is organized as: [query0_neighbor0, query0_neighbor1, ..., query1_neighbor0,
     * ...]
     *
     * @return the labels array
     */
    public long[] getLabels() {
        return labels;
    }

    /**
     * Get all distances as a flat array.
     *
     * <p>The array is organized as: [query0_dist0, query0_dist1, ..., query1_dist0, ...]
     *
     * @return the distances array
     */
    public float[] getDistances() {
        return distances;
    }

    /**
     * Get the labels for a specific query.
     *
     * @param queryIndex the query index
     * @return the labels for this query
     */
    public long[] getLabelsForQuery(int queryIndex) {
        if (queryIndex < 0 || queryIndex >= numQueries) {
            throw new IndexOutOfBoundsException("Query index out of bounds: " + queryIndex);
        }
        int start = queryIndex * k;
        return Arrays.copyOfRange(labels, start, start + k);
    }

    /**
     * Get the distances for a specific query.
     *
     * @param queryIndex the query index
     * @return the distances for this query
     */
    public float[] getDistancesForQuery(int queryIndex) {
        if (queryIndex < 0 || queryIndex >= numQueries) {
            throw new IndexOutOfBoundsException("Query index out of bounds: " + queryIndex);
        }
        int start = queryIndex * k;
        return Arrays.copyOfRange(distances, start, start + k);
    }

    /**
     * Get the label of a specific neighbor for a specific query.
     *
     * @param queryIndex the query index
     * @param neighborIndex the neighbor index (0 = closest)
     * @return the label
     */
    public long getLabel(int queryIndex, int neighborIndex) {
        if (queryIndex < 0 || queryIndex >= numQueries) {
            throw new IndexOutOfBoundsException("Query index out of bounds: " + queryIndex);
        }
        if (neighborIndex < 0 || neighborIndex >= k) {
            throw new IndexOutOfBoundsException("Neighbor index out of bounds: " + neighborIndex);
        }
        return labels[queryIndex * k + neighborIndex];
    }

    /**
     * Get the distance of a specific neighbor for a specific query.
     *
     * @param queryIndex the query index
     * @param neighborIndex the neighbor index (0 = closest)
     * @return the distance
     */
    public float getDistance(int queryIndex, int neighborIndex) {
        if (queryIndex < 0 || queryIndex >= numQueries) {
            throw new IndexOutOfBoundsException("Query index out of bounds: " + queryIndex);
        }
        if (neighborIndex < 0 || neighborIndex >= k) {
            throw new IndexOutOfBoundsException("Neighbor index out of bounds: " + neighborIndex);
        }
        return distances[queryIndex * k + neighborIndex];
    }

    @Override
    public String toString() {
        return "SearchResult{"
                + "numQueries="
                + numQueries
                + ", k="
                + k
                + ", labels="
                + Arrays.toString(labels)
                + ", distances="
                + Arrays.toString(distances)
                + '}';
    }
}
