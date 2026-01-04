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

/**
 * A Faiss index for similarity search.
 *
 * <p>This class wraps a native Faiss index and provides methods for adding vectors, searching for
 * nearest neighbors, and managing the index.
 *
 * <p>Index instances must be closed when no longer needed to free native resources. It is
 * recommended to use try-with-resources:
 *
 * <pre>{@code
 * try (Index index = IndexFactory.create(128, "Flat", MetricType.L2)) {
 *     index.add(vectors);
 *     SearchResult result = index.search(queries, 10);
 * }
 * }</pre>
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

    /**
     * Train the index on a set of training vectors.
     *
     * <p>This is required for some index types (like IVF) before adding vectors. For flat indexes,
     * this is a no-op.
     *
     * @param vectors the training vectors (n * dimension floats)
     */
    public void train(float[] vectors) {
        checkNotClosed();
        if (vectors.length % dimension != 0) {
            throw new IllegalArgumentException(
                    "Vector array length must be a multiple of dimension " + dimension);
        }
        long n = vectors.length / dimension;
        FaissNative.indexTrain(nativeHandle, n, vectors);
    }

    /**
     * Add vectors to the index.
     *
     * <p>The vectors are assigned sequential IDs starting from the current count.
     *
     * @param vectors the vectors to add (n * dimension floats)
     */
    public void add(float[] vectors) {
        checkNotClosed();
        if (vectors.length % dimension != 0) {
            throw new IllegalArgumentException(
                    "Vector array length must be a multiple of dimension " + dimension);
        }
        long n = vectors.length / dimension;
        FaissNative.indexAdd(nativeHandle, n, vectors);
    }

    /**
     * Add a single vector to the index.
     *
     * @param vector the vector to add (dimension floats)
     */
    public void addSingle(float[] vector) {
        checkNotClosed();
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "Vector length must equal dimension " + dimension + ", got " + vector.length);
        }
        FaissNative.indexAdd(nativeHandle, 1, vector);
    }

    /**
     * Add vectors with explicit IDs to the index.
     *
     * <p>Note: Not all index types support this operation. Flat indexes and IndexIDMap wrapped
     * indexes support it.
     *
     * @param vectors the vectors to add (n * dimension floats)
     * @param ids the IDs for the vectors (n longs)
     */
    public void addWithIds(float[] vectors, long[] ids) {
        checkNotClosed();
        if (vectors.length % dimension != 0) {
            throw new IllegalArgumentException(
                    "Vector array length must be a multiple of dimension " + dimension);
        }
        long n = vectors.length / dimension;
        if (ids.length != n) {
            throw new IllegalArgumentException(
                    "Number of IDs (" + ids.length + ") must match number of vectors (" + n + ")");
        }
        FaissNative.indexAddWithIds(nativeHandle, n, vectors, ids);
    }

    /**
     * Search for the k nearest neighbors of query vectors.
     *
     * @param queries the query vectors (n * dimension floats)
     * @param k the number of nearest neighbors to find
     * @return the search result containing labels and distances
     */
    public SearchResult search(float[] queries, int k) {
        checkNotClosed();
        if (queries.length % dimension != 0) {
            throw new IllegalArgumentException(
                    "Query array length must be a multiple of dimension " + dimension);
        }
        int n = queries.length / dimension;
        long[] labels = new long[n * k];
        float[] distances = new float[n * k];
        FaissNative.indexSearch(nativeHandle, n, queries, k, distances, labels);
        return new SearchResult(n, k, labels, distances);
    }

    /**
     * Search for a single query vector.
     *
     * @param query the query vector (dimension floats)
     * @param k the number of nearest neighbors to find
     * @return the search result
     */
    public SearchResult searchSingle(float[] query, int k) {
        checkNotClosed();
        if (query.length != dimension) {
            throw new IllegalArgumentException(
                    "Query length must equal dimension " + dimension + ", got " + query.length);
        }
        long[] labels = new long[k];
        float[] distances = new float[k];
        FaissNative.indexSearch(nativeHandle, 1, query, k, distances, labels);
        return new SearchResult(1, k, labels, distances);
    }

    /**
     * Search for all neighbors within a given radius.
     *
     * @param queries the query vectors (n * dimension floats)
     * @param radius the search radius
     * @return the range search result
     */
    public RangeSearchResult rangeSearch(float[] queries, float radius) {
        checkNotClosed();
        if (queries.length % dimension != 0) {
            throw new IllegalArgumentException(
                    "Query array length must be a multiple of dimension " + dimension);
        }
        int n = queries.length / dimension;
        long resultHandle = FaissNative.indexRangeSearch(nativeHandle, n, queries, radius);
        return new RangeSearchResult(resultHandle, n);
    }

    /**
     * Remove vectors by their IDs.
     *
     * <p>Note: Not all index types support removal. Check Faiss documentation for details on which
     * index types support this operation.
     *
     * @param ids the IDs of vectors to remove
     * @return the number of vectors actually removed
     */
    public long removeIds(long[] ids) {
        checkNotClosed();
        return FaissNative.indexRemoveIds(nativeHandle, ids);
    }

    /** Reset the index (remove all vectors). */
    public void reset() {
        checkNotClosed();
        FaissNative.indexReset(nativeHandle);
    }

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
     * Serialize the index to a byte array.
     *
     * @return the serialized bytes
     */
    public byte[] serialize() {
        checkNotClosed();
        return FaissNative.indexSerialize(nativeHandle);
    }

    /**
     * Deserialize an index from a byte array.
     *
     * @param data the serialized bytes
     * @return the deserialized index
     */
    public static Index deserialize(byte[] data) {
        long handle = FaissNative.indexDeserialize(data);
        int dimension = FaissNative.indexGetDimension(handle);
        return new Index(handle, dimension);
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
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
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
