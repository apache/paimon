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

import org.apache.paimon.faiss.jni.FaissJNI;

import java.io.Closeable;

/**
 * A wrapper class for FAISS index that manages the native index pointer.
 *
 * <p>This class provides a safe Java API for interacting with native FAISS indices, including
 * automatic resource management through the {@link Closeable} interface.
 */
public class FaissIndex implements Closeable {

    private long indexPtr;
    private final int dimension;
    private final FaissVectorMetric metric;
    private final FaissIndexType indexType;
    private volatile boolean closed = false;

    private FaissIndex(
            long indexPtr, int dimension, FaissVectorMetric metric, FaissIndexType indexType) {
        this.indexPtr = indexPtr;
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
        FaissJNI.ensureLoaded();
        long ptr = FaissJNI.createIndex(dimension, "Flat", metric.getValue());
        return new FaissIndex(ptr, dimension, metric, FaissIndexType.FLAT);
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
        FaissJNI.ensureLoaded();
        long ptr = FaissJNI.createHnswIndex(dimension, m, efConstruction, metric.getValue());
        return new FaissIndex(ptr, dimension, metric, FaissIndexType.HNSW);
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
        FaissJNI.ensureLoaded();
        long ptr = FaissJNI.createIvfIndex(dimension, nlist, metric.getValue());
        return new FaissIndex(ptr, dimension, metric, FaissIndexType.IVF);
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
        FaissJNI.ensureLoaded();
        long ptr = FaissJNI.createIvfPqIndex(dimension, nlist, m, nbits, metric.getValue());
        return new FaissIndex(ptr, dimension, metric, FaissIndexType.IVF_PQ);
    }

    /**
     * Load an index from serialized data.
     *
     * @param data the serialized index data
     * @return the loaded index
     */
    public static FaissIndex fromBytes(byte[] data) {
        FaissJNI.ensureLoaded();
        long ptr = FaissJNI.readIndex(data);
        int dimension = FaissJNI.getIndexDimension(ptr);
        // Note: metric and type are not stored in serialized form, use defaults
        return new FaissIndex(ptr, dimension, FaissVectorMetric.L2, FaissIndexType.UNKNOWN);
    }

    /**
     * Add vectors to the index.
     *
     * @param vectors the vectors to add (each row is a vector)
     */
    public void add(float[][] vectors) {
        ensureOpen();
        if (vectors.length == 0) {
            return;
        }
        float[] flattened = flatten(vectors);
        FaissJNI.addVectors(indexPtr, flattened, vectors.length);
    }

    /**
     * Add vectors with IDs to the index.
     *
     * @param vectors the vectors to add (each row is a vector)
     * @param ids the IDs for the vectors
     */
    public void addWithIds(float[][] vectors, long[] ids) {
        ensureOpen();
        if (vectors.length == 0) {
            return;
        }
        if (vectors.length != ids.length) {
            throw new IllegalArgumentException(
                    "Number of vectors and IDs must match: "
                            + vectors.length
                            + " vs "
                            + ids.length);
        }
        float[] flattened = flatten(vectors);
        FaissJNI.addVectorsWithIds(indexPtr, flattened, ids, vectors.length);
    }

    /**
     * Add a single vector to the index.
     *
     * @param vector the vector to add
     */
    public void add(float[] vector) {
        ensureOpen();
        checkDimension(vector);
        FaissJNI.addVectors(indexPtr, vector, 1);
    }

    /**
     * Add a single vector with ID to the index.
     *
     * @param vector the vector to add
     * @param id the ID for the vector
     */
    public void addWithId(float[] vector, long id) {
        ensureOpen();
        checkDimension(vector);
        FaissJNI.addVectorsWithIds(indexPtr, vector, new long[] {id}, 1);
    }

    /**
     * Train the index (required for IVF-based indices).
     *
     * @param trainingVectors the training vectors
     */
    public void train(float[][] trainingVectors) {
        ensureOpen();
        if (trainingVectors.length == 0) {
            return;
        }
        float[] flattened = flatten(trainingVectors);
        FaissJNI.trainIndex(indexPtr, flattened, trainingVectors.length);
    }

    /**
     * Check if the index is trained.
     *
     * @return true if the index is trained
     */
    public boolean isTrained() {
        ensureOpen();
        return FaissJNI.isTrained(indexPtr);
    }

    /**
     * Search for k nearest neighbors.
     *
     * @param queries the query vectors
     * @param k the number of nearest neighbors to return
     * @return search results containing distances and IDs
     */
    public SearchResult search(float[][] queries, int k) {
        ensureOpen();
        if (queries.length == 0) {
            return new SearchResult(new float[0], new long[0], 0, k);
        }
        float[] flattened = flatten(queries);
        float[] distances = new float[queries.length * k];
        long[] labels = new long[queries.length * k];
        FaissJNI.search(indexPtr, flattened, queries.length, k, distances, labels);
        return new SearchResult(distances, labels, queries.length, k);
    }

    /**
     * Search for k nearest neighbors for a single query.
     *
     * @param query the query vector
     * @param k the number of nearest neighbors to return
     * @return search results containing distances and IDs
     */
    public SearchResult search(float[] query, int k) {
        ensureOpen();
        checkDimension(query);
        float[] distances = new float[k];
        long[] labels = new long[k];
        FaissJNI.search(indexPtr, query, 1, k, distances, labels);
        return new SearchResult(distances, labels, 1, k);
    }

    /**
     * Set HNSW search parameter efSearch.
     *
     * @param efSearch the size of the dynamic candidate list for search
     */
    public void setHnswEfSearch(int efSearch) {
        ensureOpen();
        FaissJNI.setHnswEfSearch(indexPtr, efSearch);
    }

    /**
     * Set IVF search parameter nprobe.
     *
     * @param nprobe the number of clusters to visit during search
     */
    public void setIvfNprobe(int nprobe) {
        ensureOpen();
        FaissJNI.setIvfNprobe(indexPtr, nprobe);
    }

    /**
     * Get the number of vectors in the index.
     *
     * @return the number of vectors
     */
    public long size() {
        ensureOpen();
        return FaissJNI.getIndexSize(indexPtr);
    }

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

    /**
     * Serialize the index to a byte array.
     *
     * @return the serialized index
     */
    public byte[] toBytes() {
        ensureOpen();
        return FaissJNI.writeIndex(indexPtr);
    }

    /** Reset the index (remove all vectors). */
    public void reset() {
        ensureOpen();
        FaissJNI.resetIndex(indexPtr);
    }

    @Override
    public void close() {
        if (!closed) {
            synchronized (this) {
                if (!closed) {
                    FaissJNI.freeIndex(indexPtr);
                    indexPtr = 0;
                    closed = true;
                }
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
    }

    private void checkDimension(float[] vector) {
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "Vector dimension mismatch: expected " + dimension + ", got " + vector.length);
        }
    }

    private float[] flatten(float[][] vectors) {
        int n = vectors.length;
        int d = vectors[0].length;
        float[] result = new float[n * d];
        for (int i = 0; i < n; i++) {
            if (vectors[i].length != d) {
                throw new IllegalArgumentException(
                        "All vectors must have the same dimension: expected "
                                + d
                                + ", got "
                                + vectors[i].length
                                + " at index "
                                + i);
            }
            System.arraycopy(vectors[i], 0, result, i * d, d);
        }
        return result;
    }

    /** Result of a search operation. */
    public static class SearchResult {
        private final float[] distances;
        private final long[] labels;
        private final int numQueries;
        private final int k;

        public SearchResult(float[] distances, long[] labels, int numQueries, int k) {
            this.distances = distances;
            this.labels = labels;
            this.numQueries = numQueries;
            this.k = k;
        }

        public float[] getDistances() {
            return distances;
        }

        public long[] getLabels() {
            return labels;
        }

        public int getNumQueries() {
            return numQueries;
        }

        public int getK() {
            return k;
        }

        /**
         * Get distances for a specific query.
         *
         * @param queryIndex the query index
         * @return the distances for that query
         */
        public float[] getDistancesForQuery(int queryIndex) {
            float[] result = new float[k];
            System.arraycopy(distances, queryIndex * k, result, 0, k);
            return result;
        }

        /**
         * Get labels for a specific query.
         *
         * @param queryIndex the query index
         * @return the labels for that query
         */
        public long[] getLabelsForQuery(int queryIndex) {
            long[] result = new long[k];
            System.arraycopy(labels, queryIndex * k, result, 0, k);
            return result;
        }
    }
}
