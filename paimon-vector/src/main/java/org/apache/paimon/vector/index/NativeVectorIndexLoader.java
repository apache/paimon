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

package org.apache.paimon.vector.index;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

/** Loads native libraries packaged by paimon-vector-index-java. */
final class NativeVectorIndexLoader {

    private static volatile boolean loaded;

    private NativeVectorIndexLoader() {}

    static void loadJni() {
        if (loaded) {
            return;
        }
        synchronized (NativeVectorIndexLoader.class) {
            if (loaded) {
                return;
            }
            loadFromResource();
            loaded = true;
        }
    }

    private static void loadFromResource() {
        String libraryPath = nativeLibraryPath();
        try (InputStream input = NativeVectorIndexLoader.class.getResourceAsStream(libraryPath)) {
            if (input == null) {
                throw new UnsupportedOperationException(
                        "Vector index native library resource not found: " + libraryPath);
            }

            File temp = File.createTempFile("paimon-vector-", librarySuffix());
            temp.deleteOnExit();
            try (FileOutputStream output = new FileOutputStream(temp)) {
                byte[] buffer = new byte[8192];
                int len;
                while ((len = input.read(buffer)) >= 0) {
                    output.write(buffer, 0, len);
                }
            }
            System.load(temp.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to load vector index native library: " + libraryPath, e);
        }
    }

    private static String nativeLibraryPath() {
        return "/native/" + os() + "/" + arch() + "/" + libraryName();
    }

    private static String os() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        if (os.contains("mac")) {
            return "macos";
        } else if (os.contains("linux")) {
            return "linux";
        } else if (os.contains("windows")) {
            return "windows";
        }
        throw new UnsupportedOperationException("Unsupported vector index native OS: " + os);
    }

    private static String arch() {
        String arch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);
        if ("amd64".equals(arch) || "x86_64".equals(arch)) {
            return "x86_64";
        } else if ("aarch64".equals(arch) || "arm64".equals(arch)) {
            return "aarch64";
        }
        throw new UnsupportedOperationException("Unsupported vector index native arch: " + arch);
    }

    private static String libraryName() {
        if ("windows".equals(os())) {
            return "paimon_vindex_jni.dll";
        }
        return "libpaimon_vindex_jni" + librarySuffix();
    }

    private static String librarySuffix() {
        if ("macos".equals(os())) {
            return ".dylib";
        } else if ("windows".equals(os())) {
            return ".dll";
        }
        return ".so";
    }
}
