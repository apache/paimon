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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;

/** Utility class for loading the native Vortex JNI library. */
public final class NativeLoader {
    private static boolean loaded = false;

    private NativeLoader() {}

    public static synchronized void loadJni() {
        if (loaded) {
            return;
        }

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

        System.load(libName);
        loaded = true;
    }
}
