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

import java.io.Closeable;

/**
 * A <b>search-only</b> DiskANN index backed by Paimon FileIO (local, HDFS, S3, OSS, etc.).
 *
 * <p>Both the Vamana graph and full-precision vectors are read on-demand from FileIO-backed storage
 * during beam search. The Rust JNI code invokes Java reader callbacks:
 *
 * <ul>
 *   <li>{@code graphReader.readNeighbors(int)} — fetches graph neighbor lists from the {@code
 *       .index} file via {@code SeekableInputStream}.
 *   <li>{@code vectorReader.readVector(long)} — fetches full-precision vectors from the {@code
 *       .data} file via {@code SeekableInputStream}.
 * </ul>
 *
 * <p>Frequently accessed data is cached (graph neighbors in a {@code DashMap}, vectors in an LRU
 * cache) to reduce FileIO/JNI round-trips.
 *
 * <p>Thread Safety: instances are <b>not</b> thread-safe.
 */
public class IndexSearcher implements AutoCloseable {

    /** Native searcher handle (≥100 000, distinct from Index handles). */
    private long nativeHandle;

    private final Closeable graphReader;

    private final Closeable vectorReader;

    /** Vector dimension. */
    private final int dimension;

    private volatile boolean closed = false;

    private IndexSearcher(
            long nativeHandle, int dimension, Closeable graphReader, Closeable vectorReader) {
        this.nativeHandle = nativeHandle;
        this.dimension = dimension;
        this.graphReader = graphReader;
        this.vectorReader = vectorReader;
    }

    /**
     * Create a search-only index from two on-demand readers.
     *
     * <p>Neither the graph nor the vector data is loaded into Java memory upfront. The Rust JNI
     * code invokes:
     *
     * <ul>
     *   <li>{@code graphReader.readNeighbors(int)} for neighbor lists during beam search
     *   <li>{@code vectorReader.loadVector(long)} for full-precision vectors (zero-copy via
     *       DirectByteBuffer)
     * </ul>
     *
     * <p>When PQ data is provided, beam search uses PQ-reconstructed vectors for approximate
     * distance computation (fully in-memory), and only the final top-K candidates are re-ranked
     * with full-precision vectors from disk I/O.
     *
     * <p>Both readers must implement {@link Closeable}.
     *
     * @param graphReader a graph reader object (e.g. {@code FileIOGraphReader}).
     * @param vectorReader a vector reader object (e.g. {@code FileIOVectorReader}).
     * @param dimension the vector dimension (from {@code DiskAnnIndexMeta}).
     * @param minExtId minimum external ID for this index (for int_id → ext_id conversion).
     * @param pqPivots serialized PQ codebook, or null to disable PQ-accelerated search.
     * @param pqCompressed serialized PQ compressed codes, or null to disable.
     * @return a new IndexSearcher
     */
    public static IndexSearcher createFromReaders(
            Closeable graphReader,
            Closeable vectorReader,
            int dimension,
            long minExtId,
            byte[] pqPivots,
            byte[] pqCompressed) {
        long handle =
                DiskAnnNative.indexCreateSearcherFromReaders(
                        graphReader, vectorReader, minExtId, pqPivots, pqCompressed);
        return new IndexSearcher(handle, dimension, graphReader, vectorReader);
    }

    /** Return the vector dimension of this index. */
    public int getDimension() {
        return dimension;
    }

    /**
     * Search for the k nearest neighbors.
     *
     * <p>Semantics are identical to {@link Index#search}. During beam search the Rust code will
     * invoke the vector reader's {@code readVector} to fetch vectors it has not yet cached.
     */
    public void search(
            long n,
            float[] queryVectors,
            int k,
            int searchListSize,
            float[] distances,
            long[] labels) {
        checkNotClosed();
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
        DiskAnnNative.indexSearchWithReader(
                nativeHandle, n, queryVectors, k, searchListSize, distances, labels);
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("IndexSearcher has been closed");
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            if (nativeHandle != 0) {
                DiskAnnNative.indexDestroySearcher(nativeHandle);
                nativeHandle = 0;
            }
            try {
                if (graphReader != null) {
                    graphReader.close();
                }
                if (vectorReader != null) {
                    vectorReader.close();
                }
            } catch (Exception e) {
                // best-effort
            }
        }
    }
}
