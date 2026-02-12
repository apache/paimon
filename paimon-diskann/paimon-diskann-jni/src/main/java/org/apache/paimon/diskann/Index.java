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
import java.nio.ByteOrder;

/**
 * A DiskANN index for similarity search with zero-copy support.
 *
 * <p>Thread Safety: Index instances are NOT thread-safe. External synchronization is required if an
 * index is accessed from multiple threads.
 */
public class Index implements AutoCloseable {

    /** Native handle to the DiskANN index. */
    private long nativeHandle;

    /** The dimension of vectors in this index. */
    private final int dimension;

    /** Whether this index has been closed. */
    private volatile boolean closed = false;

    Index(long nativeHandle, int dimension) {
        this.nativeHandle = nativeHandle;
        this.dimension = dimension;
    }

    public int getDimension() {
        return dimension;
    }

    public long getCount() {
        checkNotClosed();
        return DiskAnnNative.indexGetCount(nativeHandle);
    }

    public MetricType getMetricType() {
        checkNotClosed();
        return MetricType.fromValue(DiskAnnNative.indexGetMetricType(nativeHandle));
    }

    public void add(long n, ByteBuffer vectorBuffer) {
        checkNotClosed();
        validateDirectBuffer(vectorBuffer, n * dimension * Float.BYTES, "vector");
        DiskAnnNative.indexAdd(nativeHandle, n, vectorBuffer);
    }

    public void build(int buildListSize) {
        checkNotClosed();
        DiskAnnNative.indexBuild(nativeHandle, buildListSize);
    }

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
        DiskAnnNative.indexSearch(
                nativeHandle, n, queryVectors, k, searchListSize, distances, labels);
    }

    /** Return the number of bytes needed for serialization. */
    public long serializeSize() {
        checkNotClosed();
        return DiskAnnNative.indexSerializeSize(nativeHandle);
    }

    /**
     * Serialize the Vamana graph adjacency lists and vectors into the given direct ByteBuffer.
     *
     * <p>The serialized data contains the graph section followed by the vector section, with no
     * header. Metadata (dimension, metric, etc.) is stored separately in {@code DiskAnnIndexMeta}.
     *
     * @return the number of bytes written
     */
    public long serialize(ByteBuffer buffer) {
        checkNotClosed();
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be a direct buffer");
        }
        return DiskAnnNative.indexSerialize(nativeHandle, buffer);
    }

    public static Index create(
            int dimension, MetricType metricType, int indexType, int maxDegree, int buildListSize) {
        long handle =
                DiskAnnNative.indexCreate(
                        dimension, metricType.value(), indexType, maxDegree, buildListSize);
        return new Index(handle, dimension);
    }

    /**
     * Train a PQ codebook on the vectors in this index and encode all vectors.
     *
     * @param numSubspaces number of PQ subspaces (M).
     * @param maxSamples maximum training samples for K-Means.
     * @param kmeansIters number of K-Means iterations.
     * @return {@code byte[2]}: [0] = serialized pivots, [1] = serialized compressed codes.
     */
    public byte[][] pqTrainAndEncode(int numSubspaces, int maxSamples, int kmeansIters) {
        checkNotClosed();
        return DiskAnnNative.pqTrainAndEncode(nativeHandle, numSubspaces, maxSamples, kmeansIters);
    }

    public static ByteBuffer allocateVectorBuffer(int numVectors, int dimension) {
        return ByteBuffer.allocateDirect(numVectors * dimension * Float.BYTES)
                .order(ByteOrder.nativeOrder());
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
            throw new IllegalStateException("Index has been closed");
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            if (nativeHandle != 0) {
                DiskAnnNative.indexDestroy(nativeHandle);
                nativeHandle = 0;
            }
        }
    }
}
