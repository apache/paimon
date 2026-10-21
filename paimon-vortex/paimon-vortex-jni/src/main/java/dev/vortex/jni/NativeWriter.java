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

import dev.vortex.api.VortexWriteSummary;

import java.util.Map;

/** Native methods for Vortex writer operations. */
public final class NativeWriter {

    static {
        NativeLoader.loadJni();
    }

    private NativeWriter() {}

    /**
     * Opens a writer at {@code uri}. The native side borrows the Arrow schema at {@code
     * arrowSchemaAddr}, so the caller still owns it and must release it.
     */
    public static native long create(
            long sessionPtr,
            String uri,
            long arrowSchemaAddr,
            Map<String, String> options,
            Map<String, byte[]> metadata);

    /**
     * Opens a writer that streams the file into a caller-provided {@code
     * dev.vortex.io.NativeWritable}. The native side writes and flushes but never closes it.
     */
    public static native long createStream(
            long sessionPtr, Object writable, long arrowSchemaAddr, Map<String, byte[]> metadata);

    /**
     * Moves the Arrow array at {@code arrowArrayAddr} into the writer and borrows the schema at
     * {@code arrowSchemaAddr}. The caller must release the schema after the call.
     */
    public static native boolean writeBatch(
            long writerPtr, long arrowArrayAddr, long arrowSchemaAddr);

    /** Compressed bytes that reached the sink so far. */
    public static native long bytesWritten(long writerPtr);

    /** Uncompressed bytes accepted by the writer that did not reach the sink yet. */
    public static native long bufferedBytes(long writerPtr);

    /** Flushes and closes the writer. Must be called at most once, and not with {@link #finish}. */
    public static native void close(long writerPtr);

    /** Flushes and closes the writer, and returns its summary. Must be called at most once. */
    public static native VortexWriteSummary finish(long writerPtr);
}
