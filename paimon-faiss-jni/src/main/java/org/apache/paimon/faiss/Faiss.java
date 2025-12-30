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
 * Global Faiss configuration and utilities.
 *
 * <p>This class provides methods for configuring Faiss globally, such as
 * setting the number of threads for parallel operations.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Set the number of threads for Faiss operations
 * Faiss.setNumThreads(4);
 *
 * // Get the Faiss version
 * String version = Faiss.getVersion();
 * }</pre>
 */
public final class Faiss {

    static {
        try {
            NativeLibraryLoader.load();
        } catch (FaissException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private Faiss() {
        // Static utility class
    }

    /**
     * Get the version of the Faiss library.
     *
     * @return the version string
     */
    public static String getVersion() {
        return FaissNative.getVersion();
    }

    /**
     * Set the number of threads for parallel operations.
     *
     * <p>This affects operations like index training, adding vectors,
     * and searching. Set to 1 to disable parallelism.
     *
     * @param numThreads the number of threads (must be positive)
     */
    public static void setNumThreads(int numThreads) {
        if (numThreads <= 0) {
            throw new IllegalArgumentException("Number of threads must be positive: " + numThreads);
        }
        FaissNative.setNumThreads(numThreads);
    }

    /**
     * Get the number of threads for parallel operations.
     *
     * @return the current number of threads
     */
    public static int getNumThreads() {
        return FaissNative.getNumThreads();
    }

    /**
     * Ensure the native library is loaded.
     *
     * <p>This method is called automatically when any Faiss class is used.
     * It can be called explicitly to load the library early and catch
     * any loading errors.
     *
     * @throws FaissException if the native library cannot be loaded
     */
    public static void loadLibrary() throws FaissException {
        NativeLibraryLoader.load();
    }

    /**
     * Check if the native library has been loaded.
     *
     * @return true if the library is loaded
     */
    public static boolean isLibraryLoaded() {
        return NativeLibraryLoader.isLoaded();
    }
}

