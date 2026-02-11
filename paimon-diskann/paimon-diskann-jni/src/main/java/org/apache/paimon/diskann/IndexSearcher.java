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
 * A <b>search-only</b> DiskANN index whose graph lives in memory and whose vectors are fetched
 * lazily from a vector reader via JNI callbacks.
 *
 * <p>This implements the core DiskANN architecture: the Vamana graph is loaded from serialized data
 * at construction time, while full-precision vectors are read on-demand during beam search. The
 * Rust JNI code invokes the reader's {@code readVector(long)} method for each candidate node whose
 * vector is not yet cached.
 *
 * <p>The vector reader must be any Java object with a {@code float[] readVector(long)} method. The
 * Rust JNI layer calls this method via reflection — no specific interface is required.
 *
 * <p>Thread Safety: instances are <b>not</b> thread-safe.
 */
public class IndexSearcher implements AutoCloseable {

    /** Native searcher handle (≥100 000, distinct from Index handles). */
    private long nativeHandle;

    /** Resources to close when this searcher is closed. */
    private final Closeable[] closeables;

    /** Vector dimension. */
    private final int dimension;

    private volatile boolean closed = false;

    private IndexSearcher(long nativeHandle, int dimension, Closeable... closeables) {
        this.nativeHandle = nativeHandle;
        this.dimension = dimension;
        this.closeables = closeables;
    }

    /**
     * Create a search-only index from serialized data and a vector reader callback.
     *
     * <p>The {@code reader} must expose a {@code float[] readVector(long)} method that the Rust JNI
     * layer invokes via reflection during beam search. It must also implement {@link Closeable} so
     * its resources are released when this searcher is closed.
     *
     * @param data the serialized byte array (header + graph + vector data).
     * @param reader a Java object with a {@code readVector(long)} method, also {@link Closeable}.
     * @return a new IndexSearcher
     * @throws DiskAnnException if deserialization fails
     */
    public static IndexSearcher create(byte[] data, Closeable reader) {
        long handle = DiskAnnNative.indexCreateSearcher(data, reader);
        // Parse dimension from the header (bytes 8..12, little-endian).
        int dim = 0;
        if (data.length >= 12) {
            dim =
                    (data[8] & 0xFF)
                            | ((data[9] & 0xFF) << 8)
                            | ((data[10] & 0xFF) << 16)
                            | ((data[11] & 0xFF) << 24);
        }
        return new IndexSearcher(handle, dim, reader);
    }

    /**
     * Create a search-only index from two on-demand readers.
     *
     * <p>Neither the graph nor the vector data is loaded into Java memory upfront. The Rust JNI
     * code invokes:
     *
     * <ul>
     *   <li>{@code graphReader.readNeighbors(int)} for neighbor lists during beam search
     *   <li>{@code vectorReader.readVector(long)} for full-precision vectors during beam search
     * </ul>
     *
     * <p>Initialization reads header info and ID mappings from the graph reader (via getter
     * methods). Both readers must implement {@link Closeable}.
     *
     * @param graphReader a graph reader object (e.g. {@code FileIOGraphReader}).
     * @param vectorReader a vector reader object (e.g. {@code FileIOVectorReader}).
     * @param dimension the vector dimension (obtained from graph reader header).
     * @return a new IndexSearcher
     */
    public static IndexSearcher createFromReaders(
            Closeable graphReader, Closeable vectorReader, int dimension) {
        long handle = DiskAnnNative.indexCreateSearcherFromReaders(graphReader, vectorReader);
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
            for (Closeable c : closeables) {
                try {
                    c.close();
                } catch (Exception e) {
                    // best-effort
                }
            }
        }
    }
}
