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

package org.apache.paimon.format.vortex;

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.vector.ArrowCStruct;
import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.BundleRecords;

import dev.vortex.api.Session;
import dev.vortex.api.VortexWriter;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** Vortex records writer using the Arrow C Data Interface. */
public class VortexRecordsWriter implements BundleFormatWriter {

    private static final Logger LOG = LoggerFactory.getLogger(VortexRecordsWriter.class);

    private static final double COMPRESSION_RATIO = 0.25;

    private final Supplier<ArrowFormatCWriter> cWriterSupplier;
    private final Session session;
    private final VortexWriter nativeWriter;
    private final String path;

    // Vortex's writeBatch is semi-async: Rust takes zero-copy ownership of buffers
    // via Arc<FFI_ArrowArray> and releases them after the background write completes.
    // Each flush creates a new ArrowFormatCWriter (with its own RootAllocator) so
    // buffers are never reused across batches. The Rust release callback frees most
    // memory; only a small residual (~148 bytes per batch from Arrow 15's incomplete
    // release) remains in each retained resource until nativeWriter.close().
    private final List<AutoCloseable> retainedResources;
    private ArrowFormatCWriter currentWriter;

    private long jniCost = 0;
    private long ffiBytes = 0;

    public VortexRecordsWriter(
            Supplier<ArrowFormatCWriter> cWriterSupplier,
            Path path,
            Map<String, String> storageOptions)
            throws IOException {
        this.cWriterSupplier = cWriterSupplier;
        this.path = path.toUri().toString();
        this.retainedResources = new ArrayList<>();
        this.currentWriter = cWriterSupplier.get();

        this.session = Session.create();
        try {
            Schema arrowSchema = currentWriter.getVectorSchemaRoot().getSchema();
            this.nativeWriter =
                    VortexWriter.create(session, this.path, arrowSchema, storageOptions);
        } catch (Exception e) {
            session.close();
            throw e;
        }
    }

    @Override
    public void addElement(InternalRow internalRow) throws IOException {
        if (!currentWriter.write(internalRow)) {
            flush();
            if (!currentWriter.write(internalRow)) {
                throw new RuntimeException("Exception happens while write to vortex file");
            }
        }
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) throws IOException {
        if (bundleRecords instanceof ArrowBundleRecords) {
            flush();
            writeBundleVsr(((ArrowBundleRecords) bundleRecords).getVectorSchemaRoot());
        } else {
            for (InternalRow row : bundleRecords) {
                addElement(row);
            }
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
        return suggestedCheck && (long) (ffiBytes * COMPRESSION_RATIO) >= targetSize;
    }

    @Override
    public void close() throws IOException {
        long t1 = System.currentTimeMillis();
        Throwable throwable = null;

        try {
            flush();
        } catch (Throwable t) {
            throwable = t;
        }

        // nativeWriter.close() blocks until all async background writes complete.
        try {
            nativeWriter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        // Release all retained resources now that async writes are done.
        for (AutoCloseable res : retainedResources) {
            closeQuietly(res);
        }
        retainedResources.clear();
        closeQuietly(currentWriter);

        try {
            session.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        long closeCost = System.currentTimeMillis() - t1;
        LOG.info("Jni cost: {}ms, close cost: {}ms for file: {}", jniCost, closeCost, path);

        if (throwable != null) {
            rethrow(throwable);
        }
    }

    private void flush() throws IOException {
        currentWriter.flush();
        if (!currentWriter.empty()) {
            ffiBytes += bufferBytes(currentWriter.getVectorSchemaRoot());
            ArrowCStruct cStruct = currentWriter.toCStruct();
            long t1 = System.currentTimeMillis();
            nativeWriter.writeBatch(cStruct.arrayAddress(), cStruct.schemaAddress());
            jniCost += (System.currentTimeMillis() - t1);
            // Each ArrowFormatCWriter has its own RootAllocator and buffers.
            // Retain it so buffer memory stays alive for async Rust reads.
            retainedResources.add(currentWriter);
            currentWriter = cWriterSupplier.get();
        }
    }

    /** Write an external VSR (from writeBundle) via IPC copy into an independent allocator. */
    private void writeBundleVsr(VectorSchemaRoot vsr) throws IOException {
        ffiBytes += bufferBytes(vsr);
        byte[] ipc = org.apache.paimon.arrow.ArrowUtils.serializeToIpc(vsr);
        RootAllocator bundleAllocator = new RootAllocator(Long.MAX_VALUE);
        try {
            ArrowStreamReader ipcReader =
                    new ArrowStreamReader(new java.io.ByteArrayInputStream(ipc), bundleAllocator);
            ipcReader.loadNextBatch();
            VectorSchemaRoot copy = ipcReader.getVectorSchemaRoot();

            ArrowArray arrowArray = ArrowArray.allocateNew(bundleAllocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(bundleAllocator);
            Data.exportVectorSchemaRoot(bundleAllocator, copy, null, arrowArray, arrowSchema);
            long t1 = System.currentTimeMillis();
            nativeWriter.writeBatch(arrowArray.memoryAddress(), arrowSchema.memoryAddress());
            jniCost += (System.currentTimeMillis() - t1);
            // Retain all resources that own the exported C Data buffers and release
            // callbacks. Rust holds async zero-copy references via Arc<FFI_ArrowArray>.
            // Order matters: close ipcReader (owns VectorSchemaRoot) before allocator.
            retainedResources.add(ipcReader);
            retainedResources.add(bundleAllocator);
        } catch (Exception e) {
            closeQuietly(bundleAllocator);
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        }
    }

    private static long bufferBytes(VectorSchemaRoot vsr) {
        long bytes = 0;
        for (int i = 0; i < vsr.getFieldVectors().size(); i++) {
            bytes += vsr.getFieldVectors().get(i).getBufferSize();
        }
        return bytes;
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (IllegalStateException e) {
            if (e.getMessage() == null || !e.getMessage().contains("Memory was leaked")) {
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Throwable addSuppressed(Throwable throwable, Throwable suppressed) {
        if (throwable == null) {
            return suppressed;
        }
        throwable.addSuppressed(suppressed);
        return throwable;
    }

    private static void rethrow(Throwable throwable) throws IOException {
        if (throwable instanceof IOException) {
            throw (IOException) throwable;
        }
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        throw new IOException(throwable);
    }
}
