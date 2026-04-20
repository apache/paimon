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

import dev.vortex.api.VortexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.OptionalLong;

/** JNI implementation of VortexWriter. */
public final class JNIWriter implements VortexWriter, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(JNIWriter.class);

    private OptionalLong ptr;

    public JNIWriter(long ptr) {
        this.ptr = OptionalLong.of(ptr);
        logger.debug("Created JNIWriter with ptr={}", ptr);
    }

    @Override
    public void writeBatch(byte[] arrowData) throws IOException {
        logger.trace("Writing batch with {} bytes", arrowData.length);

        boolean success = NativeWriterMethods.writeBatch(ptr.getAsLong(), arrowData);
        if (!success) {
            logger.error("Failed to write batch to Vortex file");
            throw new IOException("Failed to write batch to Vortex file");
        }
    }

    @Override
    public void writeBatchFfi(long arrowArrayAddr, long arrowSchemaAddr) throws IOException {
        logger.trace("Writing batch via FFI with arrayAddr={}, schemaAddr={}", arrowArrayAddr, arrowSchemaAddr);

        boolean success = NativeWriterMethods.writeBatchFfi(ptr.getAsLong(), arrowArrayAddr, arrowSchemaAddr);
        if (!success) {
            logger.error("Failed to write batch via FFI to Vortex file");
            throw new IOException("Failed to write batch via FFI to Vortex file");
        }
    }

    @Override
    public void close() {
        if (!this.ptr.isPresent()) {
            logger.debug("Attempted to close already closed JNIWriter, skipping");
            return;
        }

        long ptr = this.ptr.getAsLong();

        logger.debug("Closing JNIWriter with ptr={}", ptr);
        NativeWriterMethods.close(ptr);
        this.ptr = OptionalLong.empty();
    }
}
