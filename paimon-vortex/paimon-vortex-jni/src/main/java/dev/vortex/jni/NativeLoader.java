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

package dev.vortex.jni;

import org.apache.paimon.shade.guava30.com.google.common.io.ByteStreams;
import java.io.*;
import java.util.Locale;

/**
 * Utility class for loading the native Vortex JNI library.
 * <p>
 * This class handles the platform-specific loading of the native Vortex library
 * by detecting the operating system and architecture, extracting the appropriate
 * native library from the classpath, and loading it into the JVM.
 * </p>
 * <p>
 * The loader supports Windows, macOS, and Linux platforms with automatic
 * detection of the correct library file format (.dll, .dylib, or .so).
 * </p>
 */
public final class NativeLoader {
    private static boolean loaded = false;

    private NativeLoader() {}

    /**
     * Loads the native Vortex JNI library if it hasn't been loaded already.
     * <p>
     * This method performs platform detection, extracts the appropriate native
     * library from the classpath to a temporary file, and loads it using
     * {@link System#load(String)}. The method is thread-safe and will only
     * perform the loading operation once per JVM session.
     * </p>
     * <p>
     * The native library is expected to be located at:
     * {@code /native/{platform}-{arch}/libvortex_jni.{ext}}
     * where platform is one of: win, darwin, linux and ext is the appropriate
     * library extension for the platform.
     * </p>
     *
     * @throws UnsupportedOperationException if the current platform is not supported
     * @throws RuntimeException if the library cannot be extracted or loaded
     */
    public static synchronized void loadJni() {
        if (loaded) {
            return;
        }

        // Load the native library
        String osName = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        String osArch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);
        String libName = "libvortex_jni";

        String osShortName;
        String libExt;
        if (osName.contains("win")) {
            osShortName = "win";
            libExt = ".dll";
            libName += libExt;
        } else if (osName.contains("mac")) {
            osShortName = "darwin";
            libExt = ".dylib";
            libName += libExt;
        } else if (osName.contains("nix") || osName.contains("nux")) {
            osShortName = "linux";
            libExt = ".so";
            libName += libExt;
        } else {
            throw new UnsupportedOperationException("Unsupported OS: " + osName);
        }

        // Extract the library from classpath
        // This assumes the library is in the same package as this class
        String libPath = "/native/" + osShortName + "-" + osArch + "/" + libName;
        try (InputStream in = NativeLoader.class.getResourceAsStream(libPath)) {
            if (in == null) {
                throw new FileNotFoundException("Library not found: " + libPath);
            }
            File tempFile = File.createTempFile("libvortex_jni", libExt);
            tempFile.deleteOnExit();

            try (OutputStream out = new FileOutputStream(tempFile)) {
                ByteStreams.copy(in, out);
            }
            libName = tempFile.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load library: " + e.getMessage(), e);
        }

        // Load the library
        System.load(libName);
        loaded = true;
    }
}
