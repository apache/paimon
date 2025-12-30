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
 * Factory for creating Faiss indexes.
 *
 * <p>This class provides static methods for creating various types of Faiss indexes
 * using Faiss's index factory syntax.
 *
 * <p><b>Index Description Syntax</b></p>
 *
 * <p>The index description string follows Faiss's index factory format:
 *
 * <ul>
 *   <li>{@code "Flat"} - Flat (brute-force) index, exact search</li>
 *   <li>{@code "IVF100,Flat"} - IVF index with 100 clusters and flat quantizer</li>
 *   <li>{@code "IVF100,PQ8"} - IVF index with PQ compression (8 bytes per vector)</li>
 *   <li>{@code "HNSW32"} - HNSW graph index with 32 neighbors per node</li>
 *   <li>{@code "HNSW32,Flat"} - HNSW with flat storage</li>
 *   <li>{@code "PQ16"} - Product quantization with 16 bytes per vector</li>
 *   <li>{@code "OPQ16,PQ16"} - Optimized PQ with rotation</li>
 *   <li>{@code "IVF100,PQ16x4"} - IVF with 4-bit PQ</li>
 *   <li>{@code "IDMap,Flat"} - Flat index with ID mapping support</li>
 *   <li>{@code "IDMap2,Flat"} - Flat index with ID mapping and removal support</li>
 * </ul>
 *
 * <p><b>Preprocessing Options</b></p>
 *
 * <p>Preprocessing can be added before the main index:
 * <ul>
 *   <li>{@code "PCA64,Flat"} - PCA dimensionality reduction to 64 dims</li>
 *   <li>{@code "L2norm,Flat"} - L2 normalization before indexing</li>
 *   <li>{@code "ITQ64,Flat"} - ITQ rotation to 64 dims</li>
 * </ul>
 *
 * <p><b>Example Usage</b></p>
 *
 * <pre>{@code
 * // Create a flat index for exact search
 * Index flatIndex = IndexFactory.create(128, "Flat", MetricType.L2);
 *
 * // Create an IVF index for approximate search
 * Index ivfIndex = IndexFactory.create(128, "IVF1000,Flat", MetricType.L2);
 * ivfIndex.train(trainingVectors);  // Training required for IVF
 *
 * // Create an HNSW index
 * Index hnswIndex = IndexFactory.create(128, "HNSW32", MetricType.INNER_PRODUCT);
 *
 * // Create a flat index with ID mapping
 * Index idMapIndex = IndexFactory.create(128, "IDMap,Flat", MetricType.L2);
 * }</pre>
 *
 * @see Index
 * @see MetricType
 */
public final class IndexFactory {

    private IndexFactory() {
        // Static utility class
    }

    /**
     * Create a Faiss index using the index factory.
     *
     * @param dimension the dimension of the vectors
     * @param description the index description string
     * @param metricType the metric type for similarity computation
     * @return the created index
     */
    public static Index create(int dimension, String description, MetricType metricType) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("Dimension must be positive: " + dimension);
        }
        if (description == null || description.isEmpty()) {
            throw new IllegalArgumentException("Index description cannot be null or empty");
        }
        if (metricType == null) {
            throw new IllegalArgumentException("Metric type cannot be null");
        }

        long handle = FaissNative.indexFactoryCreate(dimension, description, metricType.getValue());
        return new Index(handle, dimension);
    }

    /**
     * Create a Faiss index with L2 (Euclidean) metric.
     *
     * @param dimension the dimension of the vectors
     * @param description the index description string
     * @return the created index
     */
    public static Index create(int dimension, String description) {
        return create(dimension, description, MetricType.L2);
    }

    /**
     * Create a flat (brute-force) index.
     *
     * <p>Flat indexes provide exact search results but have O(n) search complexity.
     * Suitable for small datasets (up to ~100K vectors).
     *
     * @param dimension the dimension of the vectors
     * @param metricType the metric type
     * @return the created index
     */
    public static Index createFlat(int dimension, MetricType metricType) {
        return create(dimension, "Flat", metricType);
    }

    /**
     * Create a flat index with L2 metric.
     *
     * @param dimension the dimension of the vectors
     * @return the created index
     */
    public static Index createFlat(int dimension) {
        return createFlat(dimension, MetricType.L2);
    }

    /**
     * Create a flat index with ID mapping support.
     *
     * <p>This allows adding vectors with explicit IDs using {@link Index#addWithIds}.
     *
     * @param dimension the dimension of the vectors
     * @param metricType the metric type
     * @return the created index
     */
    public static Index createFlatWithIds(int dimension, MetricType metricType) {
        return create(dimension, "IDMap,Flat", metricType);
    }

    /**
     * Create an IVF (Inverted File) index.
     *
     * <p>IVF indexes partition the vector space into clusters for faster search.
     * They require training before use.
     *
     * @param dimension the dimension of the vectors
     * @param nlist the number of clusters (typically sqrt(n) to 4*sqrt(n))
     * @param metricType the metric type
     * @return the created index
     */
    public static Index createIVFFlat(int dimension, int nlist, MetricType metricType) {
        return create(dimension, "IVF" + nlist + ",Flat", metricType);
    }

    /**
     * Create an IVF index with product quantization.
     *
     * <p>IVF-PQ provides a good balance between search speed, memory usage, and accuracy.
     *
     * @param dimension the dimension of the vectors
     * @param nlist the number of clusters
     * @param m the number of sub-vectors for PQ (dimension must be divisible by m)
     * @param metricType the metric type
     * @return the created index
     */
    public static Index createIVFPQ(int dimension, int nlist, int m, MetricType metricType) {
        if (dimension % m != 0) {
            throw new IllegalArgumentException(
                    "Dimension " + dimension + " must be divisible by m " + m);
        }
        return create(dimension, "IVF" + nlist + ",PQ" + m, metricType);
    }

    /**
     * Create an HNSW (Hierarchical Navigable Small World) index.
     *
     * <p>HNSW provides excellent search performance with good recall.
     * It does not require training.
     *
     * @param dimension the dimension of the vectors
     * @param m the number of neighbors in the graph (typically 16-64)
     * @param metricType the metric type
     * @return the created index
     */
    public static Index createHNSW(int dimension, int m, MetricType metricType) {
        return create(dimension, "HNSW" + m, metricType);
    }

    /**
     * Create an HNSW index with flat storage.
     *
     * @param dimension the dimension of the vectors
     * @param m the number of neighbors in the graph
     * @param metricType the metric type
     * @return the created index
     */
    public static Index createHNSWFlat(int dimension, int m, MetricType metricType) {
        return create(dimension, "HNSW" + m + ",Flat", metricType);
    }

    /**
     * Create a product quantization index.
     *
     * <p>PQ indexes provide significant memory savings at the cost of some accuracy.
     * They require training.
     *
     * @param dimension the dimension of the vectors
     * @param m the number of sub-vectors (dimension must be divisible by m)
     * @param metricType the metric type
     * @return the created index
     */
    public static Index createPQ(int dimension, int m, MetricType metricType) {
        if (dimension % m != 0) {
            throw new IllegalArgumentException(
                    "Dimension " + dimension + " must be divisible by m " + m);
        }
        return create(dimension, "PQ" + m, metricType);
    }

    /**
     * Create a scalar quantizer index.
     *
     * <p>Scalar quantization compresses vectors by quantizing each dimension.
     *
     * @param dimension the dimension of the vectors
     * @param bits the number of bits per dimension (4 or 8)
     * @param metricType the metric type
     * @return the created index
     */
    public static Index createScalarQuantizer(int dimension, int bits, MetricType metricType) {
        if (bits != 4 && bits != 8) {
            throw new IllegalArgumentException("Bits must be 4 or 8, got: " + bits);
        }
        return create(dimension, "SQ" + bits, metricType);
    }
}

