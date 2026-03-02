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

package org.apache.paimon.lumina;

import java.nio.ByteBuffer;

/**
 * Native method declarations for Lumina JNI.
 *
 * <p>This class contains all the native method declarations that are implemented in the JNI C++
 * layer mapping to Lumina's LuminaBuilder and LuminaSearcher APIs.
 *
 * <p>Users should not call these methods directly. Instead, use the high-level Java API classes like
 * {@link LuminaBuilder} and {@link LuminaSearcher}.
 */
final class LuminaNative {

    static {
        try {
            NativeLibraryLoader.load();
        } catch (LuminaException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ==================== Builder API ====================

    /**
     * Create a Lumina builder with the given options.
     *
     * @param optionKeys option key array
     * @param optionValues option value array (all values encoded as strings)
     * @return the native handle of the created builder
     */
    static native long builderCreate(String[] optionKeys, String[] optionValues);

    /**
     * Pretrain the builder with a set of sample vectors.
     *
     * @param handle the native builder handle
     * @param vectorBuffer direct ByteBuffer containing training vectors (n * dim floats)
     * @param n the number of training vectors
     */
    static native void builderPretrain(long handle, ByteBuffer vectorBuffer, long n);

    /**
     * Insert a batch of vectors with IDs into the builder.
     *
     * @param handle the native builder handle
     * @param vectorBuffer direct ByteBuffer containing vectors (n * dim floats)
     * @param idBuffer direct ByteBuffer containing IDs (n * 8 bytes, uint64)
     * @param n the number of vectors
     */
    static native void builderInsertBatch(
            long handle, ByteBuffer vectorBuffer, ByteBuffer idBuffer, long n);

    /**
     * Dump (serialize) the built index to a file.
     *
     * @param handle the native builder handle
     * @param path the output file path
     */
    static native void builderDump(long handle, String path);

    /**
     * Destroy a builder and free its resources.
     *
     * @param handle the native builder handle
     */
    static native void builderDestroy(long handle);

    // ==================== Searcher API ====================

    /**
     * Create a Lumina searcher with the given options.
     *
     * @param optionKeys option key array
     * @param optionValues option value array (all values encoded as strings)
     * @return the native handle of the created searcher
     */
    static native long searcherCreate(String[] optionKeys, String[] optionValues);

    /**
     * Open the searcher from a persisted index file.
     *
     * @param handle the native searcher handle
     * @param path the index file path
     */
    static native void searcherOpen(long handle, String path);

    /**
     * Search for the top-k nearest neighbors.
     *
     * @param handle the native searcher handle
     * @param queryVectors array containing query vectors (n * dim floats)
     * @param n the number of query vectors
     * @param topk the number of nearest neighbors to find
     * @param distances output array for distances (n * topk floats)
     * @param ids output array for vector IDs (n * topk longs)
     * @param optionKeys search option keys (e.g., "search.topk", "search.nprobe")
     * @param optionValues search option values
     */
    static native void searcherSearch(
            long handle,
            float[] queryVectors,
            int n,
            int topk,
            float[] distances,
            long[] ids,
            String[] optionKeys,
            String[] optionValues);

    /**
     * Get the total number of vectors in the opened index.
     *
     * @param handle the native searcher handle
     * @return the number of vectors
     */
    static native long searcherGetCount(long handle);

    /**
     * Get the vector dimension of the opened index.
     *
     * @param handle the native searcher handle
     * @return the dimension
     */
    static native int searcherGetDimension(long handle);

    /**
     * Close the searcher (releases the opened index data).
     *
     * @param handle the native searcher handle
     */
    static native void searcherClose(long handle);

    /**
     * Destroy a searcher and free all its resources.
     *
     * @param handle the native searcher handle
     */
    static native void searcherDestroy(long handle);
}
