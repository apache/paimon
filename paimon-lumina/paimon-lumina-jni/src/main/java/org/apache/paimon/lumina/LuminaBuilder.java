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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Java wrapper for Lumina's native LuminaBuilder.
 *
 * <p>Lifecycle: create → pretrain (optional) → insertBatch (repeatable) → dump → close.
 *
 * <p>Thread Safety: Builder instances are NOT thread-safe.
 */
public class LuminaBuilder implements AutoCloseable {

    private long nativeHandle;
    private final int dimension;
    private volatile boolean closed = false;

    private LuminaBuilder(long nativeHandle, int dimension) {
        this.nativeHandle = nativeHandle;
        this.dimension = dimension;
    }

    /**
     * Create a new Lumina builder.
     *
     * @param indexType the index type (e.g., "bruteforce", "ivf", "diskann", "hnsw")
     * @param dimension the vector dimension
     * @param metric the distance metric
     * @return the created builder
     */
    public static LuminaBuilder create(String indexType, int dimension, MetricType metric) {
        return create(indexType, dimension, metric, new LinkedHashMap<>());
    }

    /**
     * Create a new Lumina builder with additional options.
     *
     * @param indexType the index type
     * @param dimension the vector dimension
     * @param metric the distance metric
     * @param extraOptions additional Lumina options (key-value pairs)
     * @return the created builder
     */
    public static LuminaBuilder create(
            String indexType,
            int dimension,
            MetricType metric,
            Map<String, String> extraOptions) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("Dimension must be positive: " + dimension);
        }

        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("index.type", indexType);
        opts.put("index.dimension", String.valueOf(dimension));
        opts.put("distance.metric", metric.getLuminaValue());
        opts.putAll(extraOptions);

        String[] keys = opts.keySet().toArray(new String[0]);
        String[] values = opts.values().toArray(new String[0]);

        long handle = LuminaNative.builderCreate(keys, values);
        return new LuminaBuilder(handle, dimension);
    }

    /**
     * Pretrain the index on a set of sample vectors (zero-copy).
     *
     * <p>Required for IVF-based indices before inserting vectors.
     *
     * @param vectorBuffer direct ByteBuffer containing training vectors (n * dimension floats)
     * @param n the number of training vectors
     */
    public void pretrain(ByteBuffer vectorBuffer, long n) {
        checkNotClosed();
        validateDirectBuffer(vectorBuffer, n * dimension * Float.BYTES, "vector");
        LuminaNative.builderPretrain(nativeHandle, vectorBuffer, n);
    }

    /**
     * Insert a batch of vectors with IDs (zero-copy).
     *
     * @param vectorBuffer direct ByteBuffer containing vectors (n * dimension floats)
     * @param idBuffer direct ByteBuffer containing IDs (n * 8 bytes)
     * @param n the number of vectors
     */
    public void insertBatch(ByteBuffer vectorBuffer, ByteBuffer idBuffer, long n) {
        checkNotClosed();
        validateDirectBuffer(vectorBuffer, n * dimension * Float.BYTES, "vector");
        validateDirectBuffer(idBuffer, n * Long.BYTES, "id");
        LuminaNative.builderInsertBatch(nativeHandle, vectorBuffer, idBuffer, n);
    }

    /**
     * Dump (serialize) the built index to a file.
     *
     * @param path the output file path
     */
    public void dump(String path) {
        checkNotClosed();
        LuminaNative.builderDump(nativeHandle, path);
    }

    public int getDimension() {
        return dimension;
    }

    /** Allocate a direct ByteBuffer suitable for vector data. */
    public static ByteBuffer allocateVectorBuffer(int numVectors, int dimension) {
        return ByteBuffer.allocateDirect(numVectors * dimension * Float.BYTES)
                .order(ByteOrder.nativeOrder());
    }

    /** Allocate a direct ByteBuffer suitable for ID data. */
    public static ByteBuffer allocateIdBuffer(int numIds) {
        return ByteBuffer.allocateDirect(numIds * Long.BYTES).order(ByteOrder.nativeOrder());
    }

    private void validateDirectBuffer(ByteBuffer buffer, long requiredBytes, String name) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException(name + " buffer must be a direct buffer");
        }
        if (buffer.capacity() < requiredBytes) {
            throw new IllegalArgumentException(
                    name
                            + " buffer too small: required "
                            + requiredBytes
                            + " bytes, got "
                            + buffer.capacity());
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Builder has been closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (nativeHandle != 0) {
                LuminaNative.builderDestroy(nativeHandle);
                nativeHandle = 0;
            }
        }
    }
}
