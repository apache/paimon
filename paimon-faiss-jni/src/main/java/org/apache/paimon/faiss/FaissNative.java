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
 * Native method declarations for Faiss JNI.
 *
 * <p>This class contains all the native method declarations that are implemented in the JNI C++
 * layer. These methods directly map to Faiss C++ API calls.
 *
 * <p>Users should not call these methods directly. Instead, use the high-level Java API classes
 * like {@link Index} and {@link IndexFactory}.
 */
final class FaissNative {

    static {
        try {
            NativeLibraryLoader.load();
        } catch (FaissException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private FaissNative() {
        // Static utility class
    }

    // ==================== Index Factory ====================

    /**
     * Create an index using an index factory string.
     *
     * @param dimension the dimension of the vectors
     * @param description the index description string (e.g., "Flat", "IVF100,Flat", "HNSW32")
     * @param metricType the metric type (0 = L2, 1 = Inner Product)
     * @return the native handle of the created index
     */
    static native long indexFactoryCreate(int dimension, String description, int metricType);

    // ==================== Index Operations ====================

    /**
     * Destroy an index and free its resources.
     *
     * @param handle the native handle of the index
     */
    static native void indexDestroy(long handle);

    /**
     * Get the dimension of an index.
     *
     * @param handle the native handle of the index
     * @return the dimension
     */
    static native int indexGetDimension(long handle);

    /**
     * Get the number of vectors in an index.
     *
     * @param handle the native handle of the index
     * @return the number of vectors
     */
    static native long indexGetCount(long handle);

    /**
     * Check if an index is trained.
     *
     * @param handle the native handle of the index
     * @return true if trained
     */
    static native boolean indexIsTrained(long handle);

    /**
     * Get the metric type of an index.
     *
     * @param handle the native handle of the index
     * @return the metric type (0 = L2, 1 = Inner Product)
     */
    static native int indexGetMetricType(long handle);

    /**
     * Train an index on a set of training vectors.
     *
     * @param handle the native handle of the index
     * @param n the number of training vectors
     * @param vectors the training vectors (n * dimension floats)
     */
    static native void indexTrain(long handle, long n, float[] vectors);

    /**
     * Add vectors to an index.
     *
     * @param handle the native handle of the index
     * @param n the number of vectors to add
     * @param vectors the vectors to add (n * dimension floats)
     */
    static native void indexAdd(long handle, long n, float[] vectors);

    /**
     * Add vectors with IDs to an index.
     *
     * @param handle the native handle of the index
     * @param n the number of vectors to add
     * @param vectors the vectors to add (n * dimension floats)
     * @param ids the IDs for the vectors (n longs)
     */
    static native void indexAddWithIds(long handle, long n, float[] vectors, long[] ids);

    /**
     * Search for the k nearest neighbors of query vectors.
     *
     * @param handle the native handle of the index
     * @param n the number of query vectors
     * @param queries the query vectors (n * dimension floats)
     * @param k the number of nearest neighbors to find
     * @param distances output array for distances (n * k floats)
     * @param labels output array for labels/IDs (n * k longs)
     */
    static native void indexSearch(
            long handle, long n, float[] queries, int k, float[] distances, long[] labels);

    /**
     * Search for neighbors within a given radius.
     *
     * @param handle the native handle of the index
     * @param n the number of query vectors
     * @param queries the query vectors (n * dimension floats)
     * @param radius the search radius
     * @return a range search result handle
     */
    static native long indexRangeSearch(long handle, long n, float[] queries, float radius);

    /**
     * Remove vectors by IDs from an index.
     *
     * @param handle the native handle of the index
     * @param ids the IDs to remove
     * @return the number of vectors removed
     */
    static native long indexRemoveIds(long handle, long[] ids);

    /**
     * Reset an index (remove all vectors).
     *
     * @param handle the native handle of the index
     */
    static native void indexReset(long handle);

    // ==================== Index I/O ====================

    /**
     * Write an index to a file.
     *
     * @param handle the native handle of the index
     * @param path the file path to write to
     */
    static native void indexWriteToFile(long handle, String path);

    /**
     * Read an index from a file.
     *
     * @param path the file path to read from
     * @return the native handle of the loaded index
     */
    static native long indexReadFromFile(String path);

    /**
     * Serialize an index to a byte array.
     *
     * @param handle the native handle of the index
     * @return the serialized bytes
     */
    static native byte[] indexSerialize(long handle);

    /**
     * Deserialize an index from a byte array.
     *
     * @param data the serialized bytes
     * @return the native handle of the loaded index
     */
    static native long indexDeserialize(byte[] data);

    // ==================== Range Search Result ====================

    /**
     * Destroy a range search result.
     *
     * @param handle the native handle of the range search result
     */
    static native void rangeSearchResultDestroy(long handle);

    /**
     * Get the number of results for each query in a range search.
     *
     * @param handle the native handle of the range search result
     * @return array of result counts per query
     */
    static native long[] rangeSearchResultGetLimits(long handle);

    /**
     * Get all labels from a range search result.
     *
     * @param handle the native handle of the range search result
     * @return array of all labels
     */
    static native long[] rangeSearchResultGetLabels(long handle);

    /**
     * Get all distances from a range search result.
     *
     * @param handle the native handle of the range search result
     * @return array of all distances
     */
    static native float[] rangeSearchResultGetDistances(long handle);

    // ==================== IVF Index Specific ====================

    /**
     * Get the number of probe lists for an IVF index.
     *
     * @param handle the native handle of the index
     * @return the number of probe lists (nprobe)
     */
    static native int ivfGetNprobe(long handle);

    /**
     * Set the number of probe lists for an IVF index.
     *
     * @param handle the native handle of the index
     * @param nprobe the number of probe lists
     */
    static native void ivfSetNprobe(long handle, int nprobe);

    /**
     * Get the number of lists (clusters) in an IVF index.
     *
     * @param handle the native handle of the index
     * @return the number of lists
     */
    static native int ivfGetNlist(long handle);

    // ==================== HNSW Index Specific ====================

    /**
     * Get the efSearch parameter of an HNSW index.
     *
     * @param handle the native handle of the index
     * @return the efSearch value
     */
    static native int hnswGetEfSearch(long handle);

    /**
     * Set the efSearch parameter of an HNSW index.
     *
     * @param handle the native handle of the index
     * @param efSearch the efSearch value
     */
    static native void hnswSetEfSearch(long handle, int efSearch);

    /**
     * Get the efConstruction parameter of an HNSW index.
     *
     * @param handle the native handle of the index
     * @return the efConstruction value
     */
    static native int hnswGetEfConstruction(long handle);

    // ==================== Utility ====================

    /**
     * Get the Faiss library version.
     *
     * @return the version string
     */
    static native String getVersion();

    /**
     * Set the number of threads for parallel operations.
     *
     * @param numThreads the number of threads
     */
    static native void setNumThreads(int numThreads);

    /**
     * Get the number of threads for parallel operations.
     *
     * @return the number of threads
     */
    static native int getNumThreads();
}
