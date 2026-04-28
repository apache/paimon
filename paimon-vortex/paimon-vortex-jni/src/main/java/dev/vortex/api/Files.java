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

package dev.vortex.api;

import org.apache.paimon.shade.guava30.com.google.common.base.Preconditions;
import dev.vortex.jni.JNIFile;
import dev.vortex.jni.NativeFileMethods;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Utility class for opening and accessing Vortex files.
 *
 * <p>This class provides static methods to open Vortex files from various sources,
 * including local file system paths and URIs. It supports both simple path-based
 * access and more advanced configuration through properties.
 *
 * <p>All methods in this class are static and the class cannot be instantiated.
 *
 * @since 1.0
 */
public final class Files {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private Files() {}

    /**
     * Opens a Vortex file from the specified path string.
     *
     * @see #open(String, Map)
     */
    public static File open(String path) {
        return open(path, java.util.Collections.emptyMap());
    }

    /**
     * Opens a Vortex file from the specified path string and format parameters.
     *
     * <p>This method provides a convenient way to open Vortex files using either
     * absolute file system paths or URI strings. If the path starts with "/",
     * it is treated as an absolute file system path and converted to a file URI.
     * Otherwise, it is parsed as a URI directly.
     *
     * <p>This is equivalent to calling {@code open(uri, java.util.Collections.emptyMap())} with no additional
     * properties.
     *
     * @param path the path to the Vortex file, either as an absolute file system path
     *             (starting with "/") or as a URI string
     * @return a {@link File} instance representing the opened Vortex file
     * @throws RuntimeException     if the file cannot be opened or the path is invalid
     * @throws NullPointerException if path is null
     * @see #open(URI, Map)
     */
    public static File open(String path, Map<String, String> properties) {
        if (path.startsWith("/")) {
            return open(Paths.get(path).toUri(), properties);
        }
        return open(URI.create(path), properties);
    }

    /**
     * Opens a Vortex file from the specified URI with additional properties.
     *
     * <p>This method provides the most flexible way to open Vortex files, allowing
     * you to specify both the location via a URI and additional configuration
     * properties. The URI can point to local files, remote resources, or other
     * supported storage systems.
     *
     * <p>The properties map can be used to pass configuration options to the
     * underlying file system or storage layer. The specific properties supported
     * depend on the URI scheme and storage backend being used.
     *
     * @param uri        the URI pointing to the Vortex file to open
     * @param properties a map of configuration properties for opening the file;
     *                   may be empty but must not be null
     * @return a {@link File} instance representing the opened Vortex file
     * @throws RuntimeException     if the file cannot be opened, the URI is invalid,
     *                              or the returned native pointer is invalid
     * @throws NullPointerException if uri or properties is null
     * @see #open(String)
     */
    public static File open(URI uri, Map<String, String> properties) {
        long ptr = NativeFileMethods.open(uri.toString(), properties);
        Preconditions.checkArgument(ptr > 0, "Failed to open file: %s", uri);
        return new JNIFile(ptr);
    }
}
