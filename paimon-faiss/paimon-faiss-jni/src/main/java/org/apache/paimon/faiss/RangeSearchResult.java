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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;

/**
 * Result of a range search operation with zero-copy support.
 *
 * <p>Unlike k-NN search which returns a fixed number of neighbors per query, range search returns
 * all neighbors within a given radius, which can vary per query.
 *
 * <p>This class uses direct ByteBuffers to avoid memory copies when retrieving results from native
 * code.
 */
public class RangeSearchResult implements AutoCloseable {

    private long nativeHandle;
    private final int numQueries;
    private ByteBuffer limitsBuffer;
    private ByteBuffer labelsBuffer;
    private ByteBuffer distancesBuffer;
    private long totalSize = -1;

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
        LongBuffer limits = limitsBuffer.asLongBuffer();
        return limits.get(queryIndex + 1) - limits.get(queryIndex);
    }

    /**
     * Get the total number of results across all queries.
     *
     * @return the total number of results
     */
    public long getTotalResultCount() {
        if (totalSize < 0 && nativeHandle != 0) {
            totalSize = FaissNative.rangeSearchResultGetTotalSize(nativeHandle);
        }
        return totalSize;
    }

    /**
     * Get the limits buffer containing the start/end indices for each query.
     *
     * <p>The limits buffer has (numQueries + 1) longs. For query i, results are in the range
     * [limits[i], limits[i+1]).
     *
     * @return the limits buffer as a LongBuffer view
     */
    public LongBuffer getLimitsBuffer() {
        ensureLimitsLoaded();
        limitsBuffer.rewind();
        return limitsBuffer.asLongBuffer();
    }

    /**
     * Get the labels buffer containing all result labels.
     *
     * @return the labels buffer as a LongBuffer view
     */
    public LongBuffer getLabelsBuffer() {
        ensureFullyLoaded();
        labelsBuffer.rewind();
        return labelsBuffer.asLongBuffer();
    }

    /**
     * Get the distances buffer containing all result distances.
     *
     * @return the distances buffer as a FloatBuffer view
     */
    public FloatBuffer getDistancesBuffer() {
        ensureFullyLoaded();
        distancesBuffer.rewind();
        return distancesBuffer.asFloatBuffer();
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
        LongBuffer limits = limitsBuffer.asLongBuffer();
        int start = (int) limits.get(queryIndex);
        int end = (int) limits.get(queryIndex + 1);
        int count = end - start;

        long[] result = new long[count];
        LongBuffer labels = labelsBuffer.asLongBuffer();
        labels.position(start);
        labels.get(result);
        return result;
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
        LongBuffer limits = limitsBuffer.asLongBuffer();
        int start = (int) limits.get(queryIndex);
        int end = (int) limits.get(queryIndex + 1);
        int count = end - start;

        float[] result = new float[count];
        FloatBuffer distances = distancesBuffer.asFloatBuffer();
        distances.position(start);
        distances.get(result);
        return result;
    }

    private void ensureLimitsLoaded() {
        if (limitsBuffer == null && nativeHandle != 0) {
            limitsBuffer =
                    ByteBuffer.allocateDirect((numQueries + 1) * Long.BYTES)
                            .order(ByteOrder.nativeOrder());
            FaissNative.rangeSearchResultGetLimits(nativeHandle, limitsBuffer);
        }
    }

    private void ensureFullyLoaded() {
        ensureLimitsLoaded();
        if (labelsBuffer == null && nativeHandle != 0) {
            long total = getTotalResultCount();
            labelsBuffer =
                    ByteBuffer.allocateDirect((int) total * Long.BYTES)
                            .order(ByteOrder.nativeOrder());
            distancesBuffer =
                    ByteBuffer.allocateDirect((int) total * Float.BYTES)
                            .order(ByteOrder.nativeOrder());
            FaissNative.rangeSearchResultGetLabels(nativeHandle, labelsBuffer);
            FaissNative.rangeSearchResultGetDistances(nativeHandle, distancesBuffer);
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
