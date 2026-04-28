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

import java.util.Map;

/**
 * Native JNI methods for writing Vortex files.
 */
public final class NativeWriterMethods {

    static {
        NativeLoader.loadJni();
    }

    private NativeWriterMethods() {}

    /**
     * Creates a new native Vortex writer.
     *
     * @param uri     the URI to the file,  e.g. "file://path/to/file".
     * @param dtype   native pointer to a writer schema (Vortex DType)
     * @param options additional writer options. For cloud storage this includes things like credentials.
     * @return a native pointer to the writer, or 0 on failure
     */
    public static native long create(String uri, long dtype, Map<String, String> options);

    /**
     * Writes a batch of Arrow data to the Vortex file.
     *
     * @param writerPtr the native writer pointer
     * @param arrowData the Arrow IPC format data
     * @return true if successful, false otherwise
     */
    public static native boolean writeBatch(long writerPtr, byte[] arrowData);

    /**
     * Writes a batch of Arrow data to the Vortex file directly from Arrow C Data Interface pointers.
     *
     * @param writerPtr       the native writer pointer
     * @param arrowArrayAddr  memory address of the ArrowArray struct
     * @param arrowSchemaAddr memory address of the ArrowSchema struct
     * @return true if successful, false otherwise
     */
    public static native boolean writeBatchFfi(long writerPtr, long arrowArrayAddr, long arrowSchemaAddr);

    /**
     * Close and flush the writer, finalizing it to the storage system.
     *
     * @param writerPtr the native writer pointer
     * @throws RuntimeException if the writer fails to close
     */
    public static native void close(long writerPtr);
}
