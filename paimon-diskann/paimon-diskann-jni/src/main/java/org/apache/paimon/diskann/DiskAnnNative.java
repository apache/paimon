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

    /** Get the dimension of an index. */
    static native int indexGetDimension(long handle);

    /** Get the number of vectors in an index. */
    static native long indexGetCount(long handle);

    /** Get the metric type of an index. */
    static native int indexGetMetricType(long handle);

    /** Add vectors with IDs to an index using direct ByteBuffers (zero-copy). */
    static native void indexAddWithIds(
            long handle, long n, ByteBuffer vectorBuffer, ByteBuffer idBuffer);

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

    /** Serialize an index to a direct ByteBuffer (zero-copy). */
    static native long indexSerialize(long handle, ByteBuffer buffer);

    /** Get the size in bytes needed to serialize an index. */
    static native long indexSerializeSize(long handle);

    /** Deserialize an index from a byte array. */
    static native long indexDeserialize(byte[] data, long length);
}
