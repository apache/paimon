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

package org.apache.paimon.faiss.index;

import org.apache.paimon.faiss.Index;
import org.apache.paimon.faiss.IndexFactory;
import org.apache.paimon.faiss.IndexHNSW;
import org.apache.paimon.faiss.IndexIVF;
import org.apache.paimon.faiss.MetricType;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A wrapper class for FAISS index with zero-copy support.
 *
 * <p>This class provides a safe Java API for interacting with native FAISS indices using direct
 * ByteBuffers for zero-copy data transfer. This eliminates memory duplication and improves
 * performance for large-scale vector operations.
 */
public class FaissIndex implements Closeable {

    private final Index index;
    private final int dimension;
    private final FaissVectorMetric metric;
    private final FaissIndexType indexType;
    private volatile boolean closed = false;

    private FaissIndex(
            Index index, int dimension, FaissVectorMetric metric, FaissIndexType indexType) {
        this.index = index;
        this.dimension = dimension;
        this.metric = metric;
        this.indexType = indexType;
    }

    /**
     * Create a flat index (exact search).
     *
     * @param dimension the dimension of vectors
     * @param metric the distance metric
     * @return the created index
     */
    public static FaissIndex createFlatIndex(int dimension, FaissVectorMetric metric) {
        MetricType metricType = toMetricType(metric);
        Index index = IndexFactory.create(dimension, "IDMap,Flat", metricType);
        return new FaissIndex(index, dimension, metric, FaissIndexType.FLAT);
    }

    /**
     * Create an HNSW index.
     *
     * @param dimension the dimension of vectors
     * @param m the number of connections per layer
     * @param efConstruction the size of the dynamic candidate list for construction
     * @param metric the distance metric
     * @return the created index
     */
    public static FaissIndex createHnswIndex(
            int dimension, int m, int efConstruction, FaissVectorMetric metric) {
        MetricType metricType = toMetricType(metric);
        String description = String.format("IDMap2,HNSW%d", m);
        Index index = IndexFactory.create(dimension, description, metricType);
        // Set efConstruction before adding any vectors
        IndexHNSW.setEfConstruction(index, efConstruction);
        return new FaissIndex(index, dimension, metric, FaissIndexType.HNSW);
    }

    /**
     * Create an IVF index.
     *
     * @param dimension the dimension of vectors
     * @param nlist the number of inverted lists (clusters)
     * @param metric the distance metric
     * @return the created index
     */
    public static FaissIndex createIvfIndex(int dimension, int nlist, FaissVectorMetric metric) {
        MetricType metricType = toMetricType(metric);
        String description = String.format("IDMap,IVF%d,Flat", nlist);
        Index index = IndexFactory.create(dimension, description, metricType);
        return new FaissIndex(index, dimension, metric, FaissIndexType.IVF);
    }

    /**
     * Create an IVF-PQ index.
     *
     * @param dimension the dimension of vectors
     * @param nlist the number of inverted lists (clusters)
     * @param m the number of sub-quantizers
     * @param nbits the number of bits per sub-quantizer
     * @param metric the distance metric
     * @return the created index
     */
    public static FaissIndex createIvfPqIndex(
            int dimension, int nlist, int m, int nbits, FaissVectorMetric metric) {
        MetricType metricType = toMetricType(metric);
        String description = String.format("IDMap,IVF%d,PQ%dx%d", nlist, m, nbits);
        Index index = IndexFactory.create(dimension, description, metricType);
        return new FaissIndex(index, dimension, metric, FaissIndexType.IVF_PQ);
    }

    /**
     * Create an IVF-SQ8 index (Inverted File with Scalar Quantization 8-bit).
     *
     * @param dimension the dimension of vectors
     * @param nlist the number of inverted lists (clusters)
     * @param metric the distance metric
     * @return the created index
     */
    public static FaissIndex createIvfSq8Index(int dimension, int nlist, FaissVectorMetric metric) {
        MetricType metricType = toMetricType(metric);
        String description = String.format("IDMap,IVF%d,SQ8", nlist);
        Index index = IndexFactory.create(dimension, description, metricType);
        return new FaissIndex(index, dimension, metric, FaissIndexType.IVF_SQ8);
    }

    /**
     * Load an index from a local file using memory-mapped I/O.
     *
     * <p>This method uses FAISS's native file reading which can leverage mmap for efficient memory
     * usage with large indices.
     *
     * @param file the local file containing the FAISS index
     * @return the loaded index
     */
    public static FaissIndex fromFile(File file) {
        Index index = Index.readFromFile(file);
        return new FaissIndex(
                index, index.getDimension(), FaissVectorMetric.L2, FaissIndexType.UNKNOWN);
    }

    /**
     * Search for k nearest neighbors.
     *
     * @param queryVectors array containing query vectors (n * dimension floats)
     * @param n the number of queries
     * @param k the number of nearest neighbors
     * @param distances output array for distances (n * k floats)
     * @param labels output array for labels (n * k longs)
     */
    public void search(float[] queryVectors, int n, int k, float[] distances, long[] labels) {
        ensureOpen();
        if (queryVectors.length < n * dimension) {
            throw new IllegalArgumentException(
                    "Query vectors array too small: required "
                            + (n * dimension)
                            + ", got "
                            + queryVectors.length);
        }
        if (distances.length < n * k) {
            throw new IllegalArgumentException(
                    "Distances array too small: required " + (n * k) + ", got " + distances.length);
        }
        if (labels.length < n * k) {
            throw new IllegalArgumentException(
                    "Labels array too small: required " + (n * k) + ", got " + labels.length);
        }
        index.search(n, queryVectors, k, distances, labels);
    }

    /**
     * Train the index (zero-copy).
     *
     * <p>Required for IVF-based indices before adding vectors.
     *
     * @param vectorBuffer direct ByteBuffer containing training vectors (n * dimension floats)
     * @param n the number of training vectors
     */
    public void train(ByteBuffer vectorBuffer, int n) {
        ensureOpen();
        validateVectorBuffer(vectorBuffer, n);
        index.train(n, vectorBuffer);
    }

    /**
     * Add vectors to the index (zero-copy).
     *
     * @param vectorBuffer direct ByteBuffer containing vectors (n * dimension floats)
     * @param n the number of vectors
     */
    public void add(ByteBuffer vectorBuffer, int n) {
        ensureOpen();
        validateVectorBuffer(vectorBuffer, n);
        index.add(n, vectorBuffer);
    }

    /**
     * Add vectors with IDs to the index (zero-copy).
     *
     * @param vectorBuffer direct ByteBuffer containing vectors (n * dimension floats)
     * @param idBuffer direct ByteBuffer containing IDs (n longs)
     * @param n the number of vectors
     */
    public void addWithIds(ByteBuffer vectorBuffer, ByteBuffer idBuffer, int n) {
        ensureOpen();
        validateVectorBuffer(vectorBuffer, n);
        validateIdBuffer(idBuffer, n);
        index.addWithIds(n, vectorBuffer, idBuffer);
    }

    /**
     * Check if the index is trained.
     *
     * @return true if the index is trained
     */
    public boolean isTrained() {
        ensureOpen();
        return index.isTrained();
    }

    /**
     * Get the number of vectors in the index.
     *
     * @return the number of vectors
     */
    public long size() {
        ensureOpen();
        return index.getCount();
    }

    /** Reset the index (remove all vectors). */
    public void reset() {
        ensureOpen();
        index.reset();
    }

    // ==================== Index Configuration ====================

    /**
     * Set HNSW search parameter efSearch.
     *
     * @param efSearch the size of the dynamic candidate list for search
     */
    public void setHnswEfSearch(int efSearch) {
        ensureOpen();
        IndexHNSW.setEfSearch(index, efSearch);
    }

    /**
     * Set IVF search parameter nprobe.
     *
     * @param nprobe the number of clusters to visit during search
     */
    public void setIvfNprobe(int nprobe) {
        ensureOpen();
        IndexIVF.setNprobe(index, nprobe);
    }

    // ==================== Index Properties ====================

    /**
     * Get the dimension of vectors in the index.
     *
     * @return the dimension
     */
    public int dimension() {
        return dimension;
    }

    /**
     * Get the metric used by this index.
     *
     * @return the metric
     */
    public FaissVectorMetric metric() {
        return metric;
    }

    /**
     * Get the type of this index.
     *
     * @return the index type
     */
    public FaissIndexType indexType() {
        return indexType;
    }

    // ==================== Serialization ====================

    /**
     * Get the size in bytes needed to serialize this index.
     *
     * @return the serialization size
     */
    public long serializeSize() {
        ensureOpen();
        return index.serializeSize();
    }

    /**
     * Serialize the index to a direct ByteBuffer (zero-copy).
     *
     * @param buffer direct ByteBuffer to write to (must have sufficient capacity)
     * @return the number of bytes written
     */
    public long serialize(ByteBuffer buffer) {
        ensureOpen();
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be a direct buffer");
        }
        return index.serialize(buffer);
    }

    // ==================== Buffer Allocation Utilities ====================

    /**
     * Allocate a direct ByteBuffer suitable for vector data.
     *
     * @param numVectors number of vectors
     * @param dimension vector dimension
     * @return a direct ByteBuffer in native byte order
     */
    public static ByteBuffer allocateVectorBuffer(int numVectors, int dimension) {
        return ByteBuffer.allocateDirect(numVectors * dimension * Float.BYTES)
                .order(ByteOrder.nativeOrder());
    }

    /**
     * Allocate a direct ByteBuffer suitable for ID data.
     *
     * @param numIds number of IDs
     * @return a direct ByteBuffer in native byte order
     */
    public static ByteBuffer allocateIdBuffer(int numIds) {
        return ByteBuffer.allocateDirect(numIds * Long.BYTES).order(ByteOrder.nativeOrder());
    }

    // ==================== Internal Methods ====================

    private void validateVectorBuffer(ByteBuffer buffer, int numVectors) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Vector buffer must be a direct buffer");
        }
        int requiredBytes = numVectors * dimension * Float.BYTES;
        if (buffer.capacity() < requiredBytes) {
            throw new IllegalArgumentException(
                    "Vector buffer too small: required "
                            + requiredBytes
                            + " bytes, got "
                            + buffer.capacity());
        }
    }

    private void validateIdBuffer(ByteBuffer buffer, int numIds) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("ID buffer must be a direct buffer");
        }
        int requiredBytes = numIds * Long.BYTES;
        if (buffer.capacity() < requiredBytes) {
            throw new IllegalArgumentException(
                    "ID buffer too small: required "
                            + requiredBytes
                            + " bytes, got "
                            + buffer.capacity());
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
    }

    private static MetricType toMetricType(FaissVectorMetric metric) {
        switch (metric) {
            case L2:
                return MetricType.L2;
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
                    index.close();
                    closed = true;
                }
            }
        }
    }
}
