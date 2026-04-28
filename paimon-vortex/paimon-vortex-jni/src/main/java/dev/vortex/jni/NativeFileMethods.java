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

import java.util.List;
import java.util.Map;

public final class NativeFileMethods {
    static {
        NativeLoader.loadJni();
    }

    private NativeFileMethods() {}

    /**
     * List all Vortex files underneath the current file path.
     *
     * @param uri     The URI describing both the object store and the path to the file
     * @param options Any options required to initialize the object store client
     * @return A list of URIs for the Vortex files below the provided path
     */
    public static native List<String> listVortexFiles(String uri, Map<String, String> options);

    /**
     * Delete the files at the provided URIs. Use the options to configure an object store client.
     */
    public static native void delete(String[] uris, Map<String, String> options);

    /**
     * Open a file using the native library with the provided URI and options.
     *
     * @param uri     The URI of the file to open. e.g. "file://path/to/file".
     * @param options A map of options to provide for opening the file.
     * @return A native pointer to the opened file. This will be 0 if the open call failed.
     */
    public static native long open(String uri, Map<String, String> options);

    /**
     * Get the total row count contained in the file associated with the given pointer.
     *
     * @param pointer The native pointer to a file. Must be a value returned by {@link #open(String, Map)}.
     * @return The number of rows of data encoded in the file. This includes null values.
     */
    public static native long rowCount(long pointer);

    /**
     * Get the data type of the file associated with the given pointer.
     *
     * @param pointer The native pointer to a file. Must be a value returned by {@link #open(String, Map)}.
     * @return Native pointer to the DType of the file. This pointer is owned by the file and should not be freed.
     */
    public static native long dtype(long pointer);

    /**
     * Close the file associated with the given pointer.
     *
     * @param pointer The native pointer to a file. Must be a value returned by {@link #open(String, Map)}.
     */
    public static native void close(long pointer);

    /**
     * Build a new native scan operator that will materialize Arrays from the file, pushing down the optional
     * predicate, row range or row indices to perform data skipping.
     */
    public static native long scan(
            long pointer, List<String> columns, byte[] predicateProto, long[] rowRange, long[] rowIndices);
}
