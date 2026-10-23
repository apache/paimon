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

import dev.vortex.io.NativeWritable;
import dev.vortex.jni.NativeWriter;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A Vortex file writer using the Arrow C Data Interface.
 *
 * <p>The native writer moves each written Arrow array and keeps references to its buffers until
 * the batch is encoded, which can be after {@link #writeBatch} returns. Up to four batches can be
 * queued before {@link #writeBatch} blocks.
 */
public final class VortexWriter implements AutoCloseable {

    private final long pointer;
    private boolean closed;
    private VortexWriteSummary summary;

    private VortexWriter(long pointer) {
        this.pointer = pointer;
    }

    /** Opens a writer that writes to {@code uri} through a native storage client. */
    public static VortexWriter create(
            Session session,
            String uri,
            Schema arrowSchema,
            BufferAllocator allocator,
            Map<String, String> options)
            throws IOException {
        ArrowSchema cSchema = exportSchema(arrowSchema, allocator);
        try {
            return fromPointer(
                    NativeWriter.create(
                            session.nativePointer(),
                            uri,
                            cSchema.memoryAddress(),
                            options,
                            Collections.<String, byte[]>emptyMap()),
                    uri);
        } finally {
            releaseSchema(cSchema);
        }
    }

    /**
     * Opens a writer that streams the file into {@code writable}. Native code never closes the
     * writable. The caller closes it after {@link #finish()}.
     */
    public static VortexWriter create(
            Session session, NativeWritable writable, Schema arrowSchema, BufferAllocator allocator)
            throws IOException {
        ArrowSchema cSchema = exportSchema(arrowSchema, allocator);
        try {
            return fromPointer(
                    NativeWriter.createStream(
                            session.nativePointer(),
                            writable,
                            cSchema.memoryAddress(),
                            Collections.<String, byte[]>emptyMap()),
                    "stream");
        } finally {
            releaseSchema(cSchema);
        }
    }

    /**
     * Writes one batch. The native side moves the array and borrows the schema, so the caller must
     * still release the schema.
     */
    public void writeBatch(long arrowArrayAddr, long arrowSchemaAddr) throws IOException {
        checkOpen();
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

    /**
     * Compressed bytes that reached the sink so far. Queued batches and data that the layout
     * strategy still buffers are not included.
     */
    public long bytesWritten() {
        if (summary != null) {
            return summary.fileSize();
        }
        checkOpen();
        return NativeWriter.bytesWritten(pointer);
    }

    /** Uncompressed bytes accepted by the writer that did not reach the sink yet. */
    public long bufferedBytes() {
        if (summary != null) {
            return 0;
        }
        checkOpen();
        return NativeWriter.bufferedBytes(pointer);
    }

    /**
     * Flushes all batches, completes the file and returns its summary. This blocks until all
     * queued batches are encoded and released. Later calls return the same summary.
     */
    public VortexWriteSummary finish() throws IOException {
        if (summary != null) {
            return summary;
        }
        checkOpen();
        closed = true;
        try {
            summary = NativeWriter.finish(pointer);
        } catch (RuntimeException e) {
            throw new IOException("failed to finish writer", e);
        }
        if (summary == null) {
            throw new IOException("native writer returned no write summary");
        }
        return summary;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            finish();
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("writer already closed");
        }
    }

    private static VortexWriter fromPointer(long ptr, String destination) throws IOException {
        if (ptr <= 0) {
            throw new IOException("Failed to create Vortex writer for: " + destination);
        }
        return new VortexWriter(ptr);
    }

    private static ArrowSchema exportSchema(Schema arrowSchema, BufferAllocator allocator) {
        ArrowSchema cSchema = ArrowSchema.allocateNew(allocator);
        try {
            Data.exportSchema(allocator, arrowSchema, null, cSchema);
        } catch (RuntimeException e) {
            cSchema.close();
            throw e;
        }
        return cSchema;
    }

    private static void releaseSchema(ArrowSchema cSchema) {
        try {
            // The native side only borrows the schema, so its release callback is still ours.
            if (cSchema.snapshot().release != 0) {
                cSchema.release();
            }
        } finally {
            cSchema.close();
        }
    }
}
