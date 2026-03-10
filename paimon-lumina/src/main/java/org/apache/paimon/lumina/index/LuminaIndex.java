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

package org.apache.paimon.lumina.index;

import org.aliyun.lumina.LuminaBuilder;
import org.aliyun.lumina.LuminaFileInput;
import org.aliyun.lumina.LuminaFileOutput;
import org.aliyun.lumina.LuminaSearcher;
import org.aliyun.lumina.MetricType;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A high-level wrapper for Lumina index operations (build and search).
 *
 * <p>This class provides a safe Java API for building and searching Lumina vector indices. It
 * manages the lifecycle of native LuminaBuilder and LuminaSearcher objects.
 */
public class LuminaIndex implements Closeable {

    private LuminaBuilder builder;
    private LuminaSearcher searcher;
    private final int dimension;
    private final LuminaVectorMetric metric;
    private volatile boolean closed = false;

    /** All lumina options (prefix-stripped), stored for building search options at query time. */
    private Map<String, String> allOptions;

    private LuminaIndex(int dimension, LuminaVectorMetric metric) {
        this.dimension = dimension;
        this.metric = metric;
    }

    /** Create a new index for building. */
    public static LuminaIndex createForBuild(
            int dimension, LuminaVectorMetric metric, Map<String, String> extraOptions) {
        LuminaIndex index = new LuminaIndex(dimension, metric);

        Map<String, String> opts = new LinkedHashMap<>(extraOptions);
        index.builder =
                LuminaBuilder.create(
                        LuminaVectorIndexOptions.INDEX_TYPE.defaultValue(),
                        dimension,
                        toMetricType(metric),
                        opts);
        return index;
    }

    /**
     * Open an existing index from a streaming file input for searching.
     *
     * <p>The native searcher reads on-demand from the provided input. The caller must keep the
     * underlying stream open until this index is closed.
     */
    public static LuminaIndex fromStream(
            LuminaFileInput fileInput,
            long fileSize,
            int dimension,
            LuminaVectorMetric metric,
            Map<String, String> extraOptions) {
        LuminaIndex index = new LuminaIndex(dimension, metric);
        index.searcher =
                LuminaSearcher.create(
                        LuminaVectorIndexOptions.INDEX_TYPE.defaultValue(),
                        dimension,
                        toMetricType(metric),
                        extraOptions);
        index.searcher.open(fileInput, fileSize);
        index.allOptions = extraOptions;
        return index;
    }

    /** Pretrain the index with sample vectors before insertion. */
    public void pretrain(ByteBuffer vectorBuffer, int n) {
        ensureOpen();
        ensureBuilder();
        builder.pretrain(vectorBuffer, n);
    }

    /** Insert a batch of vectors with IDs (zero-copy). */
    public void insertBatch(ByteBuffer vectorBuffer, ByteBuffer idBuffer, int n) {
        ensureOpen();
        ensureBuilder();
        builder.insertBatch(vectorBuffer, idBuffer, n);
    }

    /** Dump (serialize) the built index to a streaming file output. */
    public void dump(LuminaFileOutput fileOutput) {
        ensureOpen();
        ensureBuilder();
        builder.dump(fileOutput);
    }

    /** Search for k nearest neighbors. */
    public void search(
            float[] queryVectors,
            int n,
            int k,
            float[] distances,
            long[] labels,
            Map<String, String> searchOptions) {
        ensureOpen();
        ensureSearcher();
        searcher.search(n, queryVectors, k, distances, labels, filterSearchOptions(searchOptions));
    }

    /** Search for k nearest neighbors with native pre-filtering on vector IDs. */
    public void searchWithFilter(
            float[] queryVectors,
            int n,
            int k,
            float[] distances,
            long[] labels,
            long[] filterIds,
            Map<String, String> searchOptions) {
        ensureOpen();
        ensureSearcher();
        searcher.searchWithFilter(
                n,
                queryVectors,
                k,
                distances,
                labels,
                filterIds,
                filterSearchOptions(searchOptions));
    }

    /** Get the number of vectors (searcher mode). */
    public long size() {
        ensureOpen();
        ensureSearcher();
        return searcher.getCount();
    }

    public int dimension() {
        return dimension;
    }

    public LuminaVectorMetric metric() {
        return metric;
    }

    public static ByteBuffer allocateVectorBuffer(int numVectors, int dimension) {
        long size = (long) numVectors * dimension * Float.BYTES;
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector buffer size exceeds Integer.MAX_VALUE: %d * %d * %d = %d",
                            numVectors, dimension, Float.BYTES, size));
        }
        return ByteBuffer.allocateDirect((int) size).order(ByteOrder.nativeOrder());
    }

    public static ByteBuffer allocateIdBuffer(int numIds) {
        return ByteBuffer.allocateDirect(numIds * Long.BYTES).order(ByteOrder.nativeOrder());
    }

    /**
     * Filters an options map to only include keys valid for Lumina SearchOptions. This mirrors
     * paimon-cpp's {@code NormalizeSearchOptions} which extracts only search-relevant keys.
     *
     * <p>Valid search option prefixes: {@code search.*} (core search options) and {@code
     * diskann.search.*} (DiskANN-specific search options).
     */
    private static Map<String, String> filterSearchOptions(Map<String, String> options) {
        Map<String, String> searchOpts = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("search.") || key.startsWith("diskann.search.")) {
                searchOpts.put(key, entry.getValue());
            }
        }
        return searchOpts;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
    }

    private void ensureBuilder() {
        if (builder == null) {
            throw new IllegalStateException("Index was not opened for building");
        }
    }

    private void ensureSearcher() {
        if (searcher == null) {
            throw new IllegalStateException("Index was not opened for searching");
        }
    }

    private static MetricType toMetricType(LuminaVectorMetric metric) {
        switch (metric) {
            case L2:
                return MetricType.L2;
            case COSINE:
                return MetricType.COSINE;
            case INNER_PRODUCT:
                return MetricType.INNER_PRODUCT;
            default:
                throw new IllegalArgumentException(String.format("Unknown metric: %s", metric));
        }
    }

    @Override
    public void close() {
        if (!closed) {
            synchronized (this) {
                if (!closed) {
                    if (builder != null) {
                        builder.close();
                        builder = null;
                    }
                    if (searcher != null) {
                        searcher.close();
                        searcher = null;
                    }
                    closed = true;
                }
            }
        }
    }
}
