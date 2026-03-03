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

package org.apache.paimon.lumina;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Java wrapper for Lumina's native LuminaSearcher.
 *
 * <p>Lifecycle: create → open → search (repeatable) → close.
 *
 * <p>Thread Safety: Searcher instances are NOT thread-safe.
 */
public class LuminaSearcher implements AutoCloseable {

    private long nativeHandle;
    private volatile boolean closed = false;

    private LuminaSearcher(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    /**
     * Create a new Lumina searcher.
     *
     * @param indexType the index type (e.g. "diskann")
     * @param dimension the vector dimension
     * @param metric the distance metric
     * @return the created searcher
     */
    public static LuminaSearcher create(String indexType, int dimension, MetricType metric) {
        return create(indexType, dimension, metric, new LinkedHashMap<>());
    }

    /**
     * Create a new Lumina searcher with additional options.
     *
     * @param indexType the index type (e.g. "diskann")
     * @param dimension the vector dimension
     * @param metric the distance metric
     * @param extraOptions additional Lumina options
     * @return the created searcher
     */
    public static LuminaSearcher create(
            String indexType,
            int dimension,
            MetricType metric,
            Map<String, String> extraOptions) {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("index.type", indexType);
        opts.put("index.dimension", String.valueOf(dimension));
        opts.put("distance.metric", metric.getLuminaValue());
        opts.putAll(extraOptions);

        String[] keys = opts.keySet().toArray(new String[0]);
        String[] values = opts.values().toArray(new String[0]);

        long handle = LuminaNative.searcherCreate(keys, values);
        return new LuminaSearcher(handle);
    }

    /**
     * Open a persisted index file.
     *
     * @param path the index file path
     */
    public void open(String path) {
        checkNotClosed();
        LuminaNative.searcherOpen(nativeHandle, path);
    }

    /**
     * Open a persisted index file.
     *
     * @param file the index file
     */
    public void open(File file) {
        open(file.getAbsolutePath());
    }

    /**
     * Search for the top-k nearest neighbors.
     *
     * @param queryVectors array containing query vectors (n * dim floats)
     * @param n the number of query vectors
     * @param topk the number of nearest neighbors to find
     * @param distances output array for distances (n * topk floats)
     * @param ids output array for IDs (n * topk longs)
     */
    public void search(int n, float[] queryVectors, int topk, float[] distances, long[] ids) {
        search(n, queryVectors, topk, distances, ids, new LinkedHashMap<>());
    }

    /**
     * Search for the top-k nearest neighbors with additional search options.
     *
     * @param n the number of query vectors
     * @param queryVectors array containing query vectors (n * dim floats)
     * @param topk the number of nearest neighbors to find
     * @param distances output array for distances (n * topk floats)
     * @param ids output array for IDs (n * topk longs)
     * @param searchOptions additional search-time options (e.g., "list_size")
     */
    public void search(
            int n,
            float[] queryVectors,
            int topk,
            float[] distances,
            long[] ids,
            Map<String, String> searchOptions) {
        checkNotClosed();
        if (distances.length < n * topk) {
            throw new IllegalArgumentException(
                    "Distances array too small: required " + (n * topk) + ", got " + distances.length);
        }
        if (ids.length < n * topk) {
            throw new IllegalArgumentException(
                    "IDs array too small: required " + (n * topk) + ", got " + ids.length);
        }

        String[] keys = searchOptions.keySet().toArray(new String[0]);
        String[] values = searchOptions.values().toArray(new String[0]);

        LuminaNative.searcherSearch(
                nativeHandle, queryVectors, n, topk, distances, ids, keys, values);
    }

    /** Get the total number of vectors in the opened index. */
    public long getCount() {
        checkNotClosed();
        return LuminaNative.searcherGetCount(nativeHandle);
    }

    /** Get the vector dimension of the opened index. */
    public int getDimension() {
        checkNotClosed();
        return LuminaNative.searcherGetDimension(nativeHandle);
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Searcher has been closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (nativeHandle != 0) {
                LuminaNative.searcherClose(nativeHandle);
                LuminaNative.searcherDestroy(nativeHandle);
                nativeHandle = 0;
            }
        }
    }
}
