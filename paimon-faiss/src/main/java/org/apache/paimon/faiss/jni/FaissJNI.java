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

package org.apache.paimon.faiss.jni;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * JNI bridge for FAISS (Facebook AI Similarity Search) library.
 *
 * <p>This class provides Java bindings to the native FAISS library for efficient similarity search
 * and clustering of dense vectors.
 */
public class FaissJNI {

    private static final Logger LOG = LoggerFactory.getLogger(FaissJNI.class);

    private static final String LIBRARY_NAME = "paimon_faiss_jni";
    private static volatile boolean loaded = false;
    private static Throwable loadError = null;

    // Directory where we extract native libraries (shared across dependency and JNI libs)
    private static Path extractedLibDir = null;

    static {
        try {
            // Try to load libfaiss dependency first (required on Linux)
            tryLoadFaissDependency();
            loadNativeLibrary();
            loaded = true;
            LOG.info("FAISS native library loaded successfully");
        } catch (Throwable t) {
            loadError = t;
            LOG.warn("Failed to load FAISS native library: {}", t.getMessage());
            LOG.warn(
                    "Make sure libfaiss.so is in LD_LIBRARY_PATH. "
                            + "If FAISS is built from source, run: "
                            + "export LD_LIBRARY_PATH=/path/to/faiss/build/faiss:$LD_LIBRARY_PATH");
            LOG.debug("Full stack trace:", t);
        }
    }

    private static void tryLoadFaissDependency() {
        String osName = System.getProperty("os.name").toLowerCase();

        // On macOS, FAISS is typically installed via Homebrew with proper rpath
        if (osName.contains("mac") || osName.contains("darwin")) {
            LOG.debug("On macOS, skipping manual libfaiss loading (uses rpath)");
            return;
        }

        // First, try to load from bundled resources (preferred for distributed environments)
        if (tryLoadBundledFaissLibrary()) {
            return;
        }

        // Try to load libfaiss.so from common locations
        String[] faissPaths = {
            System.getenv("FAISS_BUILD_DIR") != null
                    ? System.getenv("FAISS_BUILD_DIR") + "/faiss/libfaiss.so"
                    : null,
            System.getenv("FAISS_HOME") != null
                    ? System.getenv("FAISS_HOME") + "/build/faiss/libfaiss.so"
                    : null,
            System.getenv("FAISS_HOME") != null
                    ? System.getenv("FAISS_HOME") + "/lib/libfaiss.so"
                    : null,
            "/root/faiss/faiss/build/faiss/libfaiss.so",
            "/usr/local/lib/libfaiss.so",
            "/usr/lib/libfaiss.so",
            "/usr/lib64/libfaiss.so",
            "/usr/lib/x86_64-linux-gnu/libfaiss.so"
        };

        for (String path : faissPaths) {
            if (path != null) {
                File libFile = new File(path);
                if (libFile.exists()) {
                    try {
                        System.load(libFile.getAbsolutePath());
                        LOG.info("Loaded libfaiss.so from: {}", libFile.getAbsolutePath());
                        return; // Success
                    } catch (UnsatisfiedLinkError e) {
                        LOG.debug("Could not load libfaiss from {}: {}", path, e.getMessage());
                    }
                }
            }
        }

        // Try system library path
        try {
            System.loadLibrary("faiss");
            LOG.info("Loaded libfaiss from system library path");
            return; // Success
        } catch (UnsatisfiedLinkError e) {
            LOG.debug("Could not load libfaiss from system: {}", e.getMessage());
        }

        // FAISS might already be loaded or available via rpath in the JNI library
        LOG.debug("libfaiss not loaded directly, will try to load JNI library anyway");
    }

    private static boolean tryLoadBundledFaissLibrary() {
        String osName = System.getProperty("os.name").toLowerCase();
        String osArch = System.getProperty("os.arch").toLowerCase();
        String platformDir = getPlatformDir(osName, osArch);

        // Try to load libfaiss.so from bundled resources
        String faissResourcePath = "/native/" + platformDir + "/libfaiss.so";
        LOG.debug("Trying to load bundled libfaiss from: {}", faissResourcePath);

        try (InputStream is = FaissJNI.class.getResourceAsStream(faissResourcePath)) {
            if (is == null) {
                LOG.debug("Bundled libfaiss.so not found in resources: {}", faissResourcePath);
                return false;
            }

            // Create temp directory if not already created
            if (extractedLibDir == null) {
                extractedLibDir = Files.createTempDirectory("paimon-faiss-");
                extractedLibDir.toFile().deleteOnExit();
            }

            File tempFile = new File(extractedLibDir.toFile(), "libfaiss.so");
            tempFile.deleteOnExit();

            Files.copy(is, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            LOG.debug("Extracted libfaiss.so to: {}", tempFile.getAbsolutePath());

            System.load(tempFile.getAbsolutePath());
            LOG.info("Loaded bundled libfaiss.so from resources");
            return true;
        } catch (IOException e) {
            LOG.debug("Failed to extract bundled libfaiss.so: {}", e.getMessage());
            return false;
        } catch (UnsatisfiedLinkError e) {
            LOG.debug("Failed to load bundled libfaiss.so: {}", e.getMessage());
            return false;
        }
    }

    /** Check if the native library is loaded successfully. */
    public static boolean isLoaded() {
        return loaded;
    }

    /** Get the error that occurred during library loading, if any. */
    public static Throwable getLoadError() {
        return loadError;
    }

    /** Ensure the native library is loaded, throwing an exception if it failed. */
    public static void ensureLoaded() {
        if (!loaded) {
            throw new UnsatisfiedLinkError(
                    "FAISS native library not loaded. Error: "
                            + (loadError != null ? loadError.getMessage() : "unknown"));
        }
    }

    private static void loadNativeLibrary() throws IOException {
        StringBuilder errors = new StringBuilder();

        // Try to load from java.library.path first
        try {
            System.loadLibrary(LIBRARY_NAME);
            LOG.info("Loaded FAISS native library from java.library.path");
            return;
        } catch (UnsatisfiedLinkError e) {
            String msg = "java.library.path: " + e.getMessage();
            LOG.debug("Could not load from java.library.path, trying other locations");
            errors.append(msg).append("; ");
        }

        String osName = System.getProperty("os.name").toLowerCase();
        String osArch = System.getProperty("os.arch").toLowerCase();
        String libraryFileName = getLibraryFileName(osName);
        String platformDir = getPlatformDir(osName, osArch);

        LOG.debug("Looking for {} in platform dir {}", libraryFileName, platformDir);

        // Try to load from build directory or known paths
        String userDir = System.getProperty("user.dir");
        String[] buildPaths = {
            // When running from IDE, user.dir might be module directory
            userDir + "/src/main/native/build/lib",
            userDir + "/src/main/resources/native/" + platformDir,
            // Paths relative to module directory when running from IDE or Maven
            "paimon-faiss/src/main/native/build/lib",
            "paimon-faiss/src/main/resources/native/" + platformDir,
            "src/main/native/build/lib",
            "src/main/resources/native/" + platformDir
        };

        for (String path : buildPaths) {
            File libFile = new File(path, libraryFileName);
            LOG.debug("Checking path: {}", libFile.getAbsolutePath());
            if (libFile.exists()) {
                try {
                    System.load(libFile.getAbsolutePath());
                    LOG.info("Loaded FAISS native library from: {}", libFile.getAbsolutePath());
                    return;
                } catch (UnsatisfiedLinkError e) {
                    String msg =
                            "Could not load from "
                                    + libFile.getAbsolutePath()
                                    + ": "
                                    + e.getMessage();
                    LOG.debug(msg);
                    errors.append(msg).append("; ");
                }
            }
        }

        // Try to load bundled library from resources
        String resourcePath = "/native/" + platformDir + "/" + libraryFileName;
        LOG.debug("Trying to load from resources: {}", resourcePath);

        try (InputStream is = FaissJNI.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                String msg =
                        "Native library not found in resources: "
                                + resourcePath
                                + ". Platform: "
                                + osName
                                + "-"
                                + osArch
                                + ". "
                                + "Please build the native library first: "
                                + "cd paimon-faiss/src/main/native && ./build.sh";
                errors.append(msg);
                throw new UnsatisfiedLinkError(errors.toString());
            }

            // Reuse the same temp directory if we already extracted libfaiss.so there
            Path tempDir;
            if (extractedLibDir != null) {
                tempDir = extractedLibDir;
            } else {
                tempDir = Files.createTempDirectory("paimon-faiss-");
                tempDir.toFile().deleteOnExit();
                extractedLibDir = tempDir;
            }

            File tempFile = new File(tempDir.toFile(), libraryFileName);
            tempFile.deleteOnExit();

            Files.copy(is, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            LOG.debug("Extracted library to: {}", tempFile.getAbsolutePath());

            try {
                System.load(tempFile.getAbsolutePath());
                LOG.info("Loaded FAISS native library from resources: {}", resourcePath);
            } catch (UnsatisfiedLinkError e) {
                String msg =
                        "Library extracted but failed to load from "
                                + tempFile.getAbsolutePath()
                                + ": "
                                + e.getMessage()
                                + ". This usually means libfaiss.so is missing. "
                                + "Set LD_LIBRARY_PATH to include the FAISS library directory.";
                errors.append(msg);
                throw new UnsatisfiedLinkError(errors.toString());
            }
        }
    }

    private static String getLibraryFileName(String osName) {
        if (osName.contains("linux")) {
            return "lib" + LIBRARY_NAME + ".so";
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            return "lib" + LIBRARY_NAME + ".dylib";
        } else if (osName.contains("windows")) {
            return LIBRARY_NAME + ".dll";
        } else {
            throw new UnsupportedOperationException("Unsupported OS: " + osName);
        }
    }

    private static String getPlatformDir(String osName, String osArch) {
        String os;
        if (osName.contains("linux")) {
            os = "linux";
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            os = "darwin";
        } else if (osName.contains("windows")) {
            os = "windows";
        } else {
            throw new UnsupportedOperationException("Unsupported OS: " + osName);
        }

        String arch;
        if (osArch.contains("amd64") || osArch.contains("x86_64")) {
            arch = "x86_64";
        } else if (osArch.contains("aarch64") || osArch.contains("arm64")) {
            arch = "aarch64";
        } else {
            throw new UnsupportedOperationException("Unsupported architecture: " + osArch);
        }

        return os + "-" + arch;
    }

    // =================== Native Methods ===================

    /**
     * Create a new FAISS index.
     *
     * @param dimension the dimension of vectors
     * @param indexType the type of index (e.g., "Flat", "IVF", "HNSW")
     * @param metric the distance metric (0=L2, 1=INNER_PRODUCT)
     * @return the native pointer to the created index
     */
    public static native long createIndex(int dimension, String indexType, int metric);

    /**
     * Create a HNSW index.
     *
     * @param dimension the dimension of vectors
     * @param m the number of connections per layer
     * @param efConstruction the size of the dynamic candidate list for construction
     * @param metric the distance metric (0=L2, 1=INNER_PRODUCT)
     * @return the native pointer to the created index
     */
    public static native long createHnswIndex(int dimension, int m, int efConstruction, int metric);

    /**
     * Create an IVF index.
     *
     * @param dimension the dimension of vectors
     * @param nlist the number of inverted lists (clusters)
     * @param metric the distance metric (0=L2, 1=INNER_PRODUCT)
     * @return the native pointer to the created index
     */
    public static native long createIvfIndex(int dimension, int nlist, int metric);

    /**
     * Create an IVF-PQ index.
     *
     * @param dimension the dimension of vectors
     * @param nlist the number of inverted lists (clusters)
     * @param m the number of sub-quantizers
     * @param nbits the number of bits per sub-quantizer
     * @param metric the distance metric (0=L2, 1=INNER_PRODUCT)
     * @return the native pointer to the created index
     */
    public static native long createIvfPqIndex(
            int dimension, int nlist, int m, int nbits, int metric);

    /**
     * Add vectors to the index.
     *
     * @param indexPtr the native pointer to the index
     * @param vectors the vectors to add (flattened array)
     * @param n the number of vectors
     */
    public static native void addVectors(long indexPtr, float[] vectors, int n);

    /**
     * Add vectors with IDs to the index.
     *
     * @param indexPtr the native pointer to the index
     * @param vectors the vectors to add (flattened array)
     * @param ids the IDs for the vectors
     * @param n the number of vectors
     */
    public static native void addVectorsWithIds(long indexPtr, float[] vectors, long[] ids, int n);

    /**
     * Train the index (required for IVF-based indices).
     *
     * @param indexPtr the native pointer to the index
     * @param vectors the training vectors (flattened array)
     * @param n the number of training vectors
     */
    public static native void trainIndex(long indexPtr, float[] vectors, int n);

    /**
     * Check if the index is trained.
     *
     * @param indexPtr the native pointer to the index
     * @return true if the index is trained
     */
    public static native boolean isTrained(long indexPtr);

    /**
     * Search for nearest neighbors.
     *
     * @param indexPtr the native pointer to the index
     * @param queries the query vectors (flattened array)
     * @param nq the number of query vectors
     * @param k the number of nearest neighbors to return
     * @param distances output array for distances (size: nq * k)
     * @param labels output array for labels/IDs (size: nq * k)
     */
    public static native void search(
            long indexPtr, float[] queries, int nq, int k, float[] distances, long[] labels);

    /**
     * Search for nearest neighbors within a range.
     *
     * @param indexPtr the native pointer to the index
     * @param queries the query vectors (flattened array)
     * @param nq the number of query vectors
     * @param radius the search radius
     * @return array containing [lims, distances, labels] where lims[i+1]-lims[i] is the number of
     *     results for query i
     */
    public static native Object[] rangeSearch(long indexPtr, float[] queries, int nq, float radius);

    /**
     * Set HNSW search parameter efSearch.
     *
     * @param indexPtr the native pointer to the index
     * @param efSearch the size of the dynamic candidate list for search
     */
    public static native void setHnswEfSearch(long indexPtr, int efSearch);

    /**
     * Set IVF search parameter nprobe.
     *
     * @param indexPtr the native pointer to the index
     * @param nprobe the number of clusters to visit during search
     */
    public static native void setIvfNprobe(long indexPtr, int nprobe);

    /**
     * Get the number of vectors in the index.
     *
     * @param indexPtr the native pointer to the index
     * @return the number of vectors
     */
    public static native long getIndexSize(long indexPtr);

    /**
     * Get the dimension of vectors in the index.
     *
     * @param indexPtr the native pointer to the index
     * @return the dimension
     */
    public static native int getIndexDimension(long indexPtr);

    /**
     * Write the index to a byte array.
     *
     * @param indexPtr the native pointer to the index
     * @return the serialized index as a byte array
     */
    public static native byte[] writeIndex(long indexPtr);

    /**
     * Read an index from a byte array.
     *
     * @param data the serialized index data
     * @return the native pointer to the loaded index
     */
    public static native long readIndex(byte[] data);

    /**
     * Free the memory associated with an index.
     *
     * @param indexPtr the native pointer to the index
     */
    public static native void freeIndex(long indexPtr);

    /**
     * Reset the index (remove all vectors).
     *
     * @param indexPtr the native pointer to the index
     */
    public static native void resetIndex(long indexPtr);

    /**
     * Get FAISS version string.
     *
     * @return the FAISS version
     */
    public static native String getVersion();
}
