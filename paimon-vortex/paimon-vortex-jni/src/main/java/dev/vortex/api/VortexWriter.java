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

import dev.vortex.jni.NativeWriter;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.Map;

/** A Vortex file writer using the Arrow C Data Interface. */
public final class VortexWriter implements AutoCloseable {

    private final long pointer;
    private boolean closed;

    private VortexWriter(long pointer) {
        this.pointer = pointer;
    }

    public static VortexWriter create(
            Session session, String uri, Schema arrowSchema, Map<String, String> options)
            throws IOException {
        // Use a dedicated allocator for schema export to avoid leaking retained
        // references into the caller's allocator (Arrow C Data Interface retain/release
        // semantics don't fully clean up via ArrowSchema.close in Arrow 15).
        RootAllocator schemaAllocator = new RootAllocator(Long.MAX_VALUE);
        try {
            ArrowSchema cSchema = ArrowSchema.allocateNew(schemaAllocator);
            Data.exportSchema(schemaAllocator, arrowSchema, null, cSchema);
            long ptr =
                    NativeWriter.create(
                            session.nativePointer(), uri, cSchema.memoryAddress(), options);
            cSchema.close();
            if (ptr <= 0) {
                throw new IOException("Failed to create Vortex writer for: " + uri);
            }
            return new VortexWriter(ptr);
        } finally {
            try {
                schemaAllocator.close();
            } catch (IllegalStateException e) {
                if (e.getMessage() == null || !e.getMessage().contains("Memory was leaked")) {
                    throw e;
                }
            }
        }
    }

    public void writeBatch(long arrowArrayAddr, long arrowSchemaAddr) throws IOException {
        if (closed) {
            throw new IllegalStateException("writer already closed");
        }
        boolean ok;
        try {
            ok = NativeWriter.writeBatch(pointer, arrowArrayAddr, arrowSchemaAddr);
        } catch (RuntimeException e) {
            throw new IOException("failed to write batch", e);
        }
        if (!ok) {
            throw new IOException("NativeWriter.writeBatch returned false");
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed && pointer != 0) {
            closed = true;
            try {
                NativeWriter.close(pointer);
            } catch (RuntimeException e) {
                throw new IOException("failed to close writer", e);
            }
        }
    }
}
