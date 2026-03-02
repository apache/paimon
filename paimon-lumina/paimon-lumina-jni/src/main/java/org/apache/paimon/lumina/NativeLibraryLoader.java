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
 * Native library loader for Lumina JNI.
 *
 * <p>The loader attempts to load the library in the following order:
 *
 * <ol>
 *   <li>From the path specified by the {@code paimon.lumina.lib.path} system property
 *   <li>From the system library path using {@code System.loadLibrary}
 *   <li>From the JAR file bundled with the distribution
 * </ol>
 */
public class NativeLibraryLoader {
    private static final Logger LOG = LoggerFactory.getLogger(NativeLibraryLoader.class);

    private static final String JNI_LIBRARY_NAME = "paimon_lumina_jni";

    private static final String LIBRARY_PATH_PROPERTY = "paimon.lumina.lib.path";

    /**
     * Dependency libraries that need to be loaded before the main JNI library. Order matters:
     * libraries must be loaded before the libraries that depend on them.
     */
    private static final String[] DEPENDENCY_LIBRARIES = {
        "libgcc_s.so.1",
        "libstdc++.so.6",
        "libgomp.so.1",
        "liblumina.so",
    };

    private static volatile boolean libraryLoaded = false;

    private static final Object LOAD_LOCK = new Object();

    private static Path tempDir;

    private NativeLibraryLoader() {}

    public static void load() throws LuminaException {
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
                LOG.info("Lumina native library loaded successfully");
            } catch (Exception e) {
                throw new LuminaException("Failed to load Lumina native library", e);
            }
        }
    }

    public static boolean isLoaded() {
        return libraryLoaded;
    }

    private static void loadNativeLibrary() throws IOException {
        String customPath = System.getProperty(LIBRARY_PATH_PROPERTY);
        if (customPath != null && !customPath.isEmpty()) {
            File customLibrary = new File(customPath);
            if (customLibrary.exists()) {
                System.load(customLibrary.getAbsolutePath());
                LOG.info("Loaded Lumina native library from custom path: {}", customPath);
                return;
            } else {
                LOG.warn("Custom library path specified but file not found: {}", customPath);
            }
        }

        try {
            System.loadLibrary(JNI_LIBRARY_NAME);
            LOG.info("Loaded Lumina native library from system path");
            return;
        } catch (UnsatisfiedLinkError e) {
            LOG.debug(
                    "Could not load from system path, trying bundled library: {}", e.getMessage());
        }

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

            if (tempDir == null) {
                tempDir = Files.createTempDirectory("paimon-lumina-native");
                tempDir.toFile().deleteOnExit();
            }

            loadDependencyLibraries();

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

            if (!tempFile.setExecutable(true)) {
                LOG.warn("Could not set executable permission on native library");
            }

            System.load(tempFile.getAbsolutePath());
            LOG.info("Loaded Lumina native library from JAR: {}", libraryPath);
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
