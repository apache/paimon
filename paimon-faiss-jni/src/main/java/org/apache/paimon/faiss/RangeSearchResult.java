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
 * Result of a range search operation.
 *
 * <p>Unlike k-NN search which returns a fixed number of neighbors per query, range search returns
 * all neighbors within a given radius, which can vary per query.
 */
public class RangeSearchResult implements AutoCloseable {

    private long nativeHandle;
    private final int numQueries;
    private long[] limits;
    private long[] labels;
    private float[] distances;

    /**
     * Create a new RangeSearchResult from a native handle.
     *
     * @param nativeHandle the native handle
     * @param numQueries the number of query vectors
     */
    RangeSearchResult(long nativeHandle, int numQueries) {
        this.nativeHandle = nativeHandle;
        this.numQueries = numQueries;
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
     * Get the number of results for a specific query.
     *
     * @param queryIndex the query index
     * @return the number of results
     */
    public long getResultCount(int queryIndex) {
        ensureLimitsLoaded();
        if (queryIndex < 0 || queryIndex >= numQueries) {
            throw new IndexOutOfBoundsException("Query index out of bounds: " + queryIndex);
        }
        return limits[queryIndex + 1] - limits[queryIndex];
    }

    /**
     * Get the total number of results across all queries.
     *
     * @return the total number of results
     */
    public long getTotalResultCount() {
        ensureLimitsLoaded();
        return limits[numQueries];
    }

    /**
     * Get the labels for a specific query.
     *
     * @param queryIndex the query index
     * @return the labels for this query
     */
    public long[] getLabelsForQuery(int queryIndex) {
        ensureFullyLoaded();
        if (queryIndex < 0 || queryIndex >= numQueries) {
            throw new IndexOutOfBoundsException("Query index out of bounds: " + queryIndex);
        }
        int start = (int) limits[queryIndex];
        int end = (int) limits[queryIndex + 1];
        return Arrays.copyOfRange(labels, start, end);
    }

    /**
     * Get the distances for a specific query.
     *
     * @param queryIndex the query index
     * @return the distances for this query
     */
    public float[] getDistancesForQuery(int queryIndex) {
        ensureFullyLoaded();
        if (queryIndex < 0 || queryIndex >= numQueries) {
            throw new IndexOutOfBoundsException("Query index out of bounds: " + queryIndex);
        }
        int start = (int) limits[queryIndex];
        int end = (int) limits[queryIndex + 1];
        return Arrays.copyOfRange(distances, start, end);
    }

    /**
     * Get all labels as a flat array.
     *
     * @return all labels
     */
    public long[] getAllLabels() {
        ensureFullyLoaded();
        return labels;
    }

    /**
     * Get all distances as a flat array.
     *
     * @return all distances
     */
    public float[] getAllDistances() {
        ensureFullyLoaded();
        return distances;
    }

    private void ensureLimitsLoaded() {
        if (limits == null && nativeHandle != 0) {
            limits = FaissNative.rangeSearchResultGetLimits(nativeHandle);
        }
    }

    private void ensureFullyLoaded() {
        ensureLimitsLoaded();
        if (labels == null && nativeHandle != 0) {
            labels = FaissNative.rangeSearchResultGetLabels(nativeHandle);
            distances = FaissNative.rangeSearchResultGetDistances(nativeHandle);
        }
    }

    @Override
    public void close() {
        if (nativeHandle != 0) {
            FaissNative.rangeSearchResultDestroy(nativeHandle);
            nativeHandle = 0;
        }
    }
}
