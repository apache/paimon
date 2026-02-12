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

package org.apache.paimon.diskann;

import java.nio.ByteBuffer;

/**
 * Native method declarations for DiskANN JNI with zero-copy support.
 *
 * <p>Users should not call these methods directly. Instead, use the high-level Java API classes
 * like {@link Index}.
 */
final class DiskAnnNative {

    static {
        try {
            NativeLibraryLoader.load();
        } catch (DiskAnnException e) {
            // Library loading failed silently during class init.
            // Native methods will throw UnsatisfiedLinkError if called without the library.
        }
    }

    /** Create a DiskANN index with the given parameters. */
    static native long indexCreate(
            int dimension, int metricType, int indexType, int maxDegree, int buildListSize);

    /** Destroy an index and free its resources. */
    static native void indexDestroy(long handle);

    /** Get the number of vectors in an index. */
    static native long indexGetCount(long handle);

    /** Get the metric type of an index. */
    static native int indexGetMetricType(long handle);

    /** Add vectors to an index using a direct ByteBuffer (zero-copy). */
    static native void indexAdd(long handle, long n, ByteBuffer vectorBuffer);

    /** Build the index graph after adding vectors. */
    static native void indexBuild(long handle, int buildListSize);

    /**
     * Search for the k nearest neighbors.
     *
     * @param handle the native handle of the index
     * @param n the number of query vectors
     * @param queryVectors the query vectors (n * dimension floats)
     * @param k the number of nearest neighbors to find
     * @param searchListSize the size of the search list (DiskANN L parameter)
     * @param distances output array for distances (n * k floats)
     * @param labels output array for labels (n * k longs)
     */
    static native void indexSearch(
            long handle,
            long n,
            float[] queryVectors,
            int k,
            int searchListSize,
            float[] distances,
            long[] labels);

    /**
     * Serialize an index with its graph adjacency lists to a direct ByteBuffer.
     *
     * <p>The format stores the Vamana graph structure alongside vector data, so the graph can be
     * loaded for search without re-building from scratch.
     *
     * @param handle the native handle of the in-memory index.
     * @param buffer a direct ByteBuffer at least {@link #indexSerializeSize} bytes.
     * @return the number of bytes written.
     */
    static native long indexSerialize(long handle, ByteBuffer buffer);

    /** Return the number of bytes needed for serialization. */
    static native long indexSerializeSize(long handle);

    /**
     * Create a <em>search-only</em> index from two on-demand readers: one for the graph structure
     * and one for vectors.
     *
     * <p>Neither the graph data nor the vector data is loaded into Java memory upfront. Instead:
     *
     * <ul>
     *   <li><b>Graph</b>: the Rust side calls {@code graphReader.readNeighbors(int)} via JNI to
     *       fetch neighbor lists on demand during beam search. It also calls getter methods ({@code
     *       getDimension()}, {@code getCount()}, {@code getStartId()}) during initialization.
     *   <li><b>Vectors</b>: the Rust side calls {@code vectorReader.readVector(long)} via JNI.
     * </ul>
     *
     * @param graphReader a Java object providing graph structure on demand.
     * @param vectorReader a Java object with a {@code float[] readVector(long)} method.
     * @param minExtId minimum external ID for this index (for int_id → ext_id conversion).
     * @return a searcher handle (>= 100 000) for use with {@link #indexSearchWithReader}.
     */
    static native long indexCreateSearcherFromReaders(
            Object graphReader, Object vectorReader, long minExtId);

    /**
     * Search on a searcher handle created by {@link #indexCreateSearcherFromReaders}.
     *
     * @see #indexSearch for parameter descriptions — semantics are identical.
     */
    static native void indexSearchWithReader(
            long handle,
            long n,
            float[] queryVectors,
            int k,
            int searchListSize,
            float[] distances,
            long[] labels);

    /** Destroy a searcher handle and free its resources. */
    static native void indexDestroySearcher(long handle);

    /**
     * Train a Product Quantization codebook on the vectors stored in the index and encode all
     * vectors into compact PQ codes.
     *
     * <p>The index must have had vectors added via {@link #indexAddWithIds} before calling this
     * method.
     *
     * @param handle the native index handle.
     * @param numSubspaces number of PQ subspaces (M). Dimension must be divisible by M.
     * @param maxSamples maximum number of vectors sampled for K-Means training.
     * @param kmeansIters number of K-Means iterations.
     * @return {@code byte[2]} where {@code [0]} is the serialized PQ pivots (codebook) and {@code
     *     [1]} is the serialized compressed PQ codes.
     */
    static native byte[][] pqTrainAndEncode(
            long handle, int numSubspaces, int maxSamples, int kmeansIters);
}
