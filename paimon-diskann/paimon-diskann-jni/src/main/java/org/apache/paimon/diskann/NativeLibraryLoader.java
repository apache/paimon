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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Native library loader for DiskANN JNI.
 *
 * <p>The loader attempts to load the library in the following order:
 *
 * <ol>
 *   <li>From the path specified by the {@code paimon.diskann.lib.path} system property
 *   <li>From the system library path using {@code System.loadLibrary}
 *   <li>From the JAR file bundled with the distribution
 * </ol>
 */
public class NativeLibraryLoader {
    private static final Logger LOG = LoggerFactory.getLogger(NativeLibraryLoader.class);

    /** The name of the native library. */
    private static final String JNI_LIBRARY_NAME = "paimon_diskann_jni";

    /** System property to specify a custom path to the native library. */
    private static final String LIBRARY_PATH_PROPERTY = "paimon.diskann.lib.path";

    /**
     * Dependency libraries that need to be loaded before the main JNI library. These are bundled in
     * the JAR when the build script detects they are dynamically linked.
     *
     * <p>Order matters! Libraries must be loaded before the libraries that depend on them. The Rust
     * {@code cdylib} statically links all Rust code but may dynamically link against the GCC
     * runtime and C++ standard library on Linux.
     */
    private static final String[] DEPENDENCY_LIBRARIES = {
        // GCC runtime (Rust cdylib uses libgcc_s for stack unwinding on Linux)
        "libgcc_s.so.1",
        // C++ standard library (needed if diskann crate compiles C++ code internally)
        "libstdc++.so.6",
        // OpenMP runtime (possible transitive dependency)
        "libgomp.so.1",
    };

    /** Whether the native library has been loaded. */
    private static volatile boolean libraryLoaded = false;

    /** Lock for thread-safe library loading. */
    private static final Object LOAD_LOCK = new Object();

    /** Temporary directory for extracting native libraries. */
    private static Path tempDir;

    private NativeLibraryLoader() {
        // Utility class, no instantiation
    }

    /**
     * Load the native library.
     *
     * @throws DiskAnnException if the library cannot be loaded
     */
    public static void load() throws DiskAnnException {
        if (libraryLoaded) {
            return;
        }

        synchronized (LOAD_LOCK) {
            if (libraryLoaded) {
                return;
            }

            try {
                loadNativeLibrary();
                libraryLoaded = true;
                LOG.info("DiskANN native library loaded successfully");
            } catch (Exception e) {
                throw new DiskAnnException("Failed to load DiskANN native library", e);
            }
        }
    }

    /**
     * Check if the native library has been loaded.
     *
     * @return true if the library is loaded
     */
    public static boolean isLoaded() {
        return libraryLoaded;
    }

    private static void loadNativeLibrary() throws IOException {
        // First, try loading from custom path
        String customPath = System.getProperty(LIBRARY_PATH_PROPERTY);
        if (customPath != null && !customPath.isEmpty()) {
            File customLibrary = new File(customPath);
            if (customLibrary.exists()) {
                System.load(customLibrary.getAbsolutePath());
                LOG.info("Loaded DiskANN native library from custom path: {}", customPath);
                return;
            } else {
                LOG.warn("Custom library path specified but file not found: {}", customPath);
            }
        }

        // Second, try loading from system library path
        try {
            System.loadLibrary(JNI_LIBRARY_NAME);
            LOG.info("Loaded DiskANN native library from system path");
            return;
        } catch (UnsatisfiedLinkError e) {
            LOG.debug(
                    "Could not load from system path, trying bundled library: {}", e.getMessage());
        }

        // Third, try loading from JAR
        loadFromJar();
    }

    private static void loadFromJar() throws IOException {
        String libraryPath = getLibraryResourcePath();
        LOG.debug("Attempting to load native library from JAR: {}", libraryPath);

        try (InputStream is = NativeLibraryLoader.class.getResourceAsStream(libraryPath)) {
            if (is == null) {
                throw new IOException(
                        "Native library not found in JAR: "
                                + libraryPath
                                + ". "
                                + "Make sure you are using the correct JAR for your platform ("
                                + getPlatformIdentifier()
                                + ")");
            }

            // Create temp directory if needed
            if (tempDir == null) {
                tempDir = Files.createTempDirectory("paimon-diskann-native");
                tempDir.toFile().deleteOnExit();
            }

            // Extract and load dependency libraries (if bundled)
            loadDependencyLibraries();

            // Extract native library to temp file
            String fileName = System.mapLibraryName(JNI_LIBRARY_NAME);
            File tempFile = new File(tempDir.toFile(), fileName);
            tempFile.deleteOnExit();

            try (OutputStream os = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    os.write(buffer, 0, bytesRead);
                }
            }

            // Make the file executable (for Unix-like systems)
            if (!tempFile.setExecutable(true)) {
                LOG.warn("Could not set executable permission on native library");
            }

            // Load the library
            System.load(tempFile.getAbsolutePath());
            LOG.info("Loaded DiskANN native library from JAR: {}", libraryPath);
        }
    }

    private static void loadDependencyLibraries() {
        String os = getOsName();
        String arch = getArchName();

        for (String depLib : DEPENDENCY_LIBRARIES) {
            String resourcePath = "/" + os + "/" + arch + "/" + depLib;
            try (InputStream is = NativeLibraryLoader.class.getResourceAsStream(resourcePath)) {
                if (is == null) {
                    LOG.warn("Dependency library not bundled: {}", depLib);
                    continue;
                }

                File tempFile = new File(tempDir.toFile(), depLib);
                tempFile.deleteOnExit();

                try (OutputStream fos = new FileOutputStream(tempFile)) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = is.read(buffer)) != -1) {
                        fos.write(buffer, 0, bytesRead);
                    }
                }

                if (!tempFile.setExecutable(true)) {
                    LOG.warn("Could not set executable permission on: {}", depLib);
                }

                System.load(tempFile.getAbsolutePath());
                LOG.info("Loaded bundled dependency library: {}", depLib);
            } catch (UnsatisfiedLinkError e) {
                LOG.warn("Could not load dependency {}: {}", depLib, e.getMessage());
            } catch (IOException e) {
                LOG.warn("Could not extract dependency {}: {}", depLib, e.getMessage());
            }
        }
    }

    private static String getLibraryResourcePath() {
        String os = getOsName();
        String arch = getArchName();
        String libraryFileName = System.mapLibraryName(JNI_LIBRARY_NAME);
        return "/" + os + "/" + arch + "/" + libraryFileName;
    }

    static String getPlatformIdentifier() {
        return getOsName() + "/" + getArchName();
    }

    private static String getOsName() {
        String osName = System.getProperty("os.name").toLowerCase();

        if (osName.contains("linux")) {
            return "linux";
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            return "darwin";
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported operating system: "
                            + osName
                            + ". Only Linux and macOS are supported.");
        }
    }

    private static String getArchName() {
        String osArch = System.getProperty("os.arch").toLowerCase();

        if (osArch.equals("amd64") || osArch.equals("x86_64")) {
            return "amd64";
        } else if (osArch.equals("aarch64") || osArch.equals("arm64")) {
            return "aarch64";
        } else {
            throw new UnsupportedOperationException("Unsupported architecture: " + osArch);
        }
    }

    public static String getLibraryName() {
        return JNI_LIBRARY_NAME;
    }
}
