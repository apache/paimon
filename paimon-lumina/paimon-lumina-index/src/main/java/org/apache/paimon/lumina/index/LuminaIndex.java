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

import org.apache.paimon.lumina.LuminaBuilder;
import org.apache.paimon.lumina.LuminaSearcher;
import org.apache.paimon.lumina.MetricType;

import java.io.Closeable;
import java.io.File;
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
    private final LuminaIndexType indexType;
    private volatile boolean closed = false;

    private LuminaIndex(int dimension, LuminaVectorMetric metric, LuminaIndexType indexType) {
        this.dimension = dimension;
        this.metric = metric;
        this.indexType = indexType;
    }

    /** Create a new index for building. */
    public static LuminaIndex createForBuild(
            int dimension,
            LuminaVectorMetric metric,
            LuminaIndexType indexType,
            Map<String, String> extraOptions) {
        LuminaIndex index = new LuminaIndex(dimension, metric, indexType);

        Map<String, String> opts = new LinkedHashMap<>(extraOptions);
        index.builder =
                LuminaBuilder.create(
                        indexType.getLuminaName(), dimension, toMetricType(metric), opts);
        return index;
    }

    /** Open an existing index from a file for searching. */
    public static LuminaIndex fromFile(
            File file,
            int dimension,
            LuminaVectorMetric metric,
            LuminaIndexType indexType,
            Map<String, String> extraOptions) {
        LuminaIndex index = new LuminaIndex(dimension, metric, indexType);

        Map<String, String> opts = new LinkedHashMap<>(extraOptions);
        index.searcher =
                LuminaSearcher.create(
                        indexType.getLuminaName(), dimension, toMetricType(metric), opts);
        index.searcher.open(file);
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

    /** Dump (serialize) the built index to a file. */
    public void dump(String path) {
        ensureOpen();
        ensureBuilder();
        builder.dump(path);
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
        searcher.search(n, queryVectors, k, distances, labels, searchOptions);
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

    public LuminaIndexType indexType() {
        return indexType;
    }

    public static ByteBuffer allocateVectorBuffer(int numVectors, int dimension) {
        return ByteBuffer.allocateDirect(numVectors * dimension * Float.BYTES)
                .order(ByteOrder.nativeOrder());
    }

    public static ByteBuffer allocateIdBuffer(int numIds) {
        return ByteBuffer.allocateDirect(numIds * Long.BYTES).order(ByteOrder.nativeOrder());
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
                throw new IllegalArgumentException("Unknown metric: " + metric);
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
