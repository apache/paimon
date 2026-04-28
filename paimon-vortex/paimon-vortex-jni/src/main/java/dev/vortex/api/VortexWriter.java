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

import dev.vortex.jni.JNIDType;
import dev.vortex.jni.JNIWriter;
import dev.vortex.jni.NativeWriterMethods;
import java.io.IOException;
import java.util.Map;

/**
 * Writer for creating Vortex files from Arrow data.
 * <p>
 * This class provides methods to write Arrow VectorSchemaRoot batches
 * to Vortex format files.
 */
public interface VortexWriter extends AutoCloseable {

    /**
     * Creates a new VortexWriter for writing to the specified file path.
     *
     * @param uri     The URI for where the file is opened
     * @param dtype   The Vortex DType for data that gets written
     * @param options additional writer options
     * @return a new VortexWriter instance
     * @throws IOException if the writer cannot be created
     */
    static VortexWriter create(String uri, DType dtype, Map<String, String> options) throws IOException {
        long ptr = NativeWriterMethods.create(uri, ((JNIDType) dtype).getPointer(), options);
        if (ptr <= 0) {
            throw new IOException("Failed to create Vortex writer for: " + uri + " (got ptr=" + ptr + ")");
        }
        return new JNIWriter(ptr);
    }

    /**
     * Writes a batch of Arrow data to the Vortex file.
     *
     * @param arrowData the Arrow data in IPC format as byte array
     * @throws IOException if writing fails
     */
    void writeBatch(byte[] arrowData) throws IOException;

    /**
     * Writes a batch of Arrow data directly from Arrow C Data Interface pointers.
     * <p>
     * This avoids the IPC serialization overhead by accepting raw memory addresses
     * of ArrowArray and ArrowSchema structs.
     *
     * @param arrowArrayAddr  memory address of the ArrowArray struct
     * @param arrowSchemaAddr memory address of the ArrowSchema struct
     * @throws IOException if writing fails
     */
    void writeBatchFfi(long arrowArrayAddr, long arrowSchemaAddr) throws IOException;

    /**
     * Closes the writer and finalizes the Vortex file.
     * <p>
     * This method must be called to ensure the file is properly written
     * with all necessary metadata and footers.
     *
     * @throws IOException if closing fails
     */
    @Override
    void close() throws IOException;
}
