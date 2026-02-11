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

package org.apache.paimon.diskann.index;

import org.apache.paimon.diskann.Index;
import org.apache.paimon.diskann.MetricType;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A wrapper class for DiskANN index with zero-copy support.
 *
 * <p>This class provides a safe Java API for interacting with native DiskANN indices using direct
 * ByteBuffers for zero-copy data transfer.
 */
public class DiskAnnIndex implements Closeable {

    private final Index index;
    private final int dimension;
    private final int buildListSize;
    private volatile boolean closed = false;

    private DiskAnnIndex(Index index, int dimension, int buildListSize) {
        this.index = index;
        this.dimension = dimension;
        this.buildListSize = buildListSize;
    }

    public static DiskAnnIndex create(
            int dimension,
            DiskAnnVectorMetric metric,
            DiskAnnIndexType indexType,
            int maxDegree,
            int buildListSize) {
        MetricType metricType = metric.toMetricType();
        Index index =
                Index.create(dimension, metricType, indexType.value(), maxDegree, buildListSize);
        return new DiskAnnIndex(index, dimension, buildListSize);
    }

    public void addWithIds(ByteBuffer vectorBuffer, ByteBuffer idBuffer, int n) {
        ensureOpen();
        validateVectorBuffer(vectorBuffer, n);
        validateIdBuffer(idBuffer, n);
        index.addWithIds(n, vectorBuffer, idBuffer);
    }

    /**
     * Build the index graph after adding vectors.
     *
     * <p>Uses the buildListSize parameter that was specified during index creation.
     */
    public void build() {
        ensureOpen();
        index.build(buildListSize);
    }

    /** Return the number of bytes needed for serialization. */
    public long serializeSize() {
        ensureOpen();
        return index.serializeSize();
    }

    /**
     * Serialize this index with its Vamana graph adjacency lists into the given direct ByteBuffer.
     *
     * <p>The serialized data is later split into an index file (header + graph) and a data file
     * (raw vectors) by the writer, then loaded by {@link DiskAnnVectorGlobalIndexReader} for
     * search.
     *
     * @param buffer a direct ByteBuffer of at least {@link #serializeSize()} bytes
     * @return the number of bytes written
     */
    public long serialize(ByteBuffer buffer) {
        ensureOpen();
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be a direct buffer");
        }
        return index.serialize(buffer);
    }

    public static ByteBuffer allocateVectorBuffer(int numVectors, int dimension) {
        return ByteBuffer.allocateDirect(numVectors * dimension * Float.BYTES)
                .order(ByteOrder.nativeOrder());
    }

    public static ByteBuffer allocateIdBuffer(int numIds) {
        return ByteBuffer.allocateDirect(numIds * Long.BYTES).order(ByteOrder.nativeOrder());
    }

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
