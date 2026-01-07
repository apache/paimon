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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A Faiss index for similarity search with zero-copy support.
 *
 * <p>This class wraps a native Faiss index and provides methods for adding vectors, searching for
 * nearest neighbors, and managing the index. All vector operations use direct ByteBuffers for
 * zero-copy data transfer between Java and native code.
 *
 * <p>Thread Safety: Index instances are NOT thread-safe. External synchronization is required if an
 * index is accessed from multiple threads.
 *
 * @see IndexFactory
 */
public class Index implements AutoCloseable {

    /** Native handle to the Faiss index. */
    private long nativeHandle;

    /** The dimension of vectors in this index. */
    private final int dimension;

    /** Whether this index has been closed. */
    private volatile boolean closed = false;

    /**
     * Create an Index wrapper around a native handle.
     *
     * @param nativeHandle the native handle
     * @param dimension the vector dimension
     */
    Index(long nativeHandle, int dimension) {
        this.nativeHandle = nativeHandle;
        this.dimension = dimension;
    }

    /**
     * Get the dimension of vectors in this index.
     *
     * @return the vector dimension
     */
    public int getDimension() {
        return dimension;
    }

    /**
     * Get the number of vectors in this index.
     *
     * @return the number of vectors
     */
    public long getCount() {
        checkNotClosed();
        return FaissNative.indexGetCount(nativeHandle);
    }

    /**
     * Check if this index is trained.
     *
     * <p>Some index types (like IVF) require training before vectors can be added. Flat indexes are
     * always considered trained.
     *
     * @return true if the index is trained
     */
    public boolean isTrained() {
        checkNotClosed();
        return FaissNative.indexIsTrained(nativeHandle);
    }

    /**
     * Get the metric type used by this index.
     *
     * @return the metric type
     */
    public MetricType getMetricType() {
        checkNotClosed();
        return MetricType.fromValue(FaissNative.indexGetMetricType(nativeHandle));
    }

    // ==================== Zero-Copy Vector Operations ====================

    /**
     * Train the index on a set of training vectors (zero-copy).
     *
     * <p>This is required for some index types (like IVF) before adding vectors. For flat indexes,
     * this is a no-op.
     *
     * @param n the number of training vectors
     * @param vectorBuffer direct ByteBuffer containing training vectors (n * dimension floats)
     */
    public void train(long n, ByteBuffer vectorBuffer) {
        checkNotClosed();
        validateDirectBuffer(vectorBuffer, n * dimension * Float.BYTES, "vector");
        FaissNative.indexTrain(nativeHandle, n, vectorBuffer);
    }

    /**
     * Add vectors to the index (zero-copy).
     *
     * <p>The vectors are assigned sequential IDs starting from the current count.
     *
     * @param n the number of vectors to add
     * @param vectorBuffer direct ByteBuffer containing vectors (n * dimension floats)
     */
    public void add(long n, ByteBuffer vectorBuffer) {
        checkNotClosed();
        validateDirectBuffer(vectorBuffer, n * dimension * Float.BYTES, "vector");
        FaissNative.indexAdd(nativeHandle, n, vectorBuffer);
    }

    /**
     * Add vectors with explicit IDs to the index (zero-copy).
     *
     * <p>Note: Not all index types support this operation. Flat indexes and IndexIDMap wrapped
     * indexes support it.
     *
     * @param n the number of vectors to add
     * @param vectorBuffer direct ByteBuffer containing vectors (n * dimension floats)
     * @param idBuffer direct ByteBuffer containing IDs (n longs)
     */
    public void addWithIds(long n, ByteBuffer vectorBuffer, ByteBuffer idBuffer) {
        checkNotClosed();
        validateDirectBuffer(vectorBuffer, n * dimension * Float.BYTES, "vector");
        validateDirectBuffer(idBuffer, n * Long.BYTES, "id");
        FaissNative.indexAddWithIds(nativeHandle, n, vectorBuffer, idBuffer);
    }

    /**
     * Search for the k nearest neighbors of query vectors.
     *
     * @param n the number of query vectors
     * @param queryVectors array containing query vectors (n * dimension floats)
     * @param k the number of nearest neighbors to find
     * @param distances output array for distances (n * k floats)
     * @param labels output array for labels (n * k longs)
     */
    public void search(long n, float[] queryVectors, int k, float[] distances, long[] labels) {
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
        FaissNative.indexSearch(nativeHandle, n, queryVectors, k, distances, labels);
    }

    /**
     * Search for all neighbors within a given radius (zero-copy).
     *
     * @param n the number of query vectors
     * @param queryBuffer direct ByteBuffer containing query vectors (n * dimension floats)
     * @param radius the search radius
     * @return the range search result
     */
    public RangeSearchResult rangeSearch(long n, ByteBuffer queryBuffer, float radius) {
        checkNotClosed();
        validateDirectBuffer(queryBuffer, n * dimension * Float.BYTES, "query");
        long resultHandle = FaissNative.indexRangeSearch(nativeHandle, n, queryBuffer, radius);
        return new RangeSearchResult(resultHandle, (int) n);
    }

    /**
     * Remove vectors by their IDs (zero-copy).
     *
     * <p>Note: Not all index types support removal. Check Faiss documentation for details on which
     * index types support this operation.
     *
     * @param n the number of IDs
     * @param idBuffer direct ByteBuffer containing IDs to remove (n longs)
     * @return the number of vectors actually removed
     */
    public long removeIds(long n, ByteBuffer idBuffer) {
        checkNotClosed();
        validateDirectBuffer(idBuffer, n * Long.BYTES, "id");
        return FaissNative.indexRemoveIds(nativeHandle, n, idBuffer);
    }

    /** Reset the index (remove all vectors). */
    public void reset() {
        checkNotClosed();
        FaissNative.indexReset(nativeHandle);
    }

    // ==================== Index I/O ====================

    /**
     * Write the index to a file.
     *
     * @param path the file path
     */
    public void writeToFile(String path) {
        checkNotClosed();
        FaissNative.indexWriteToFile(nativeHandle, path);
    }

    /**
     * Write the index to a file.
     *
     * @param file the file
     */
    public void writeToFile(File file) {
        writeToFile(file.getAbsolutePath());
    }

    /**
     * Read an index from a file.
     *
     * @param path the file path
     * @return the loaded index
     */
    public static Index readFromFile(String path) {
        long handle = FaissNative.indexReadFromFile(path);
        int dimension = FaissNative.indexGetDimension(handle);
        return new Index(handle, dimension);
    }

    /**
     * Read an index from a file.
     *
     * @param file the file
     * @return the loaded index
     */
    public static Index readFromFile(File file) {
        return readFromFile(file.getAbsolutePath());
    }

    /**
     * Get the size in bytes needed to serialize this index.
     *
     * @return the serialization size
     */
    public long serializeSize() {
        checkNotClosed();
        return FaissNative.indexSerializeSize(nativeHandle);
    }

    /**
     * Serialize the index to a direct ByteBuffer (zero-copy).
     *
     * @param buffer direct ByteBuffer to write to (must have sufficient capacity)
     * @return the number of bytes written
     */
    public long serialize(ByteBuffer buffer) {
        checkNotClosed();
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be a direct buffer");
        }
        return FaissNative.indexSerialize(nativeHandle, buffer);
    }

    /**
     * Deserialize an index from a byte array.
     *
     * @param data the serialized index data
     * @return the deserialized index
     */
    public static Index deserialize(byte[] data) {
        long handle = FaissNative.indexDeserialize(data, data.length);
        int dimension = FaissNative.indexGetDimension(handle);
        return new Index(handle, dimension);
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

    /**
     * Get the native handle.
     *
     * <p>This is for internal use only.
     *
     * @return the native handle
     */
    long getNativeHandle() {
        return nativeHandle;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (nativeHandle != 0) {
                FaissNative.indexDestroy(nativeHandle);
                nativeHandle = 0;
            }
        }
    }

    @Override
    public String toString() {
        if (closed) {
            return "Index[closed]";
        }
        return "Index{"
                + "dimension="
                + dimension
                + ", count="
                + getCount()
                + ", trained="
                + isTrained()
                + ", metricType="
                + getMetricType()
                + '}';
    }
}
