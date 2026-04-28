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
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.vector.ArrowCStruct;
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.types.RowType;

import dev.vortex.api.DType;
import dev.vortex.api.VortexWriter;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Vortex records writer.
 *
 * <p>Uses the Arrow C Data Interface (FFI) to hand record batches to the native Vortex writer
 * without a serialization copy. The native writer is asynchronous: {@code writeBatchFfi} only
 * enqueues the batch and returns; the actual disk write happens on a separate task. Because
 * vortex's {@code from_arrow} shares buffer references rather than copying, the original Arrow
 * buffers must remain intact until the native writer drains.
 *
 * <p>Two consequences shape the lifecycle below:
 *
 * <ul>
 *   <li>{@link ArrowFormatWriter#reset()} is implemented as in-place {@code zeroVector}; reusing a
 *       single writer across batches would zero buffers that the native side is still reading. Each
 *       batch therefore uses a fresh {@link ArrowFormatWriter}; the previous one is moved to a
 *       pending slot and closed after the next {@code writeBatchFfi} returns.
 *   <li>{@link ArrowSchema}'s release callback is never invoked by vortex's Rust side (it borrows
 *       {@code &*ffi_schema} instead of taking ownership). The exported C structs are therefore
 *       tracked alongside the pending batch and explicitly released after native no longer needs
 *       the batch.
 * </ul>
 */
public class VortexRecordsWriter implements BundleFormatWriter {

    private static final Logger LOG = LoggerFactory.getLogger(VortexRecordsWriter.class);

    private final Supplier<ArrowFormatWriter> arrowFormatWriterSupplier;
    private final VortexWriter nativeWriter;
    private final String path;
    @Nullable private PendingBatch pendingBatch;
    private ArrowFormatWriter currentArrowFormatWriter;
    private long jniCost = 0;

    public VortexRecordsWriter(
            RowType rowType,
            Supplier<ArrowFormatWriter> arrowFormatWriterSupplier,
            Path path,
            Map<String, String> storageOptions)
            throws IOException {
        this.arrowFormatWriterSupplier = arrowFormatWriterSupplier;
        this.currentArrowFormatWriter = arrowFormatWriterSupplier.get();
        this.path = path.toUri().toString();

        DType dtype = VortexTypeUtils.toDType(rowType);
        this.nativeWriter = VortexWriter.create(this.path, dtype, storageOptions);
    }

    @Override
    public void addElement(InternalRow internalRow) throws IOException {
        if (!currentArrowFormatWriter.write(internalRow)) {
            flush();
            if (!currentArrowFormatWriter.write(internalRow)) {
                throw new RuntimeException("Exception happens while write to vortex file");
            }
        }
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) throws IOException {
        if (bundleRecords instanceof ArrowBundleRecords) {
            FfiExport export =
                    writeVsr(
                            ((ArrowBundleRecords) bundleRecords).getVectorSchemaRoot(),
                            currentArrowFormatWriter.getAllocator());
            replacePendingBatch(new PendingBatch(null, export));
        } else {
            for (InternalRow row : bundleRecords) {
                addElement(row);
            }
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
        // Vortex applies its own compression/encoding, so in-memory Arrow size is much larger
        // than the actual file size on disk. Always return false to avoid rolling into small files.
        return false;
    }

    @Override
    public void close() throws IOException {
        IOException ioException = null;
        long t1 = System.currentTimeMillis();
        try {
            flush();
            LOG.info("Jni cost: {}ms for file: {}", jniCost, path);
            nativeWriter.close();
        } catch (IOException e) {
            ioException = e;
        } finally {
            RuntimeException runtimeException = null;
            try {
                closePendingBatch();
            } catch (RuntimeException e) {
                if (runtimeException == null) {
                    runtimeException = e;
                } else {
                    runtimeException.addSuppressed(e);
                }
            }
            try {
                currentArrowFormatWriter.close();
            } catch (RuntimeException e) {
                if (runtimeException == null) {
                    runtimeException = e;
                } else {
                    runtimeException.addSuppressed(e);
                }
            }

            long closeCost = (System.currentTimeMillis() - t1);
            LOG.info("Close cost: {}ms for file: {}", closeCost, path);

            if (runtimeException != null) {
                if (ioException != null) {
                    ioException.addSuppressed(runtimeException);
                } else {
                    throw runtimeException;
                }
            }
        }

        if (ioException != null) {
            throw ioException;
        }
    }

    private void flush() throws IOException {
        currentArrowFormatWriter.flush();
        if (!currentArrowFormatWriter.empty()) {
            ArrowFormatWriter flushedWriter = currentArrowFormatWriter;
            ArrowFormatWriter nextWriter = arrowFormatWriterSupplier.get();
            FfiExport export = null;
            boolean success = false;
            try {
                export =
                        writeVsr(flushedWriter.getVectorSchemaRoot(), flushedWriter.getAllocator());
                currentArrowFormatWriter = nextWriter;
                success = true;
                replacePendingBatch(new PendingBatch(flushedWriter, export));
            } finally {
                if (!success) {
                    nextWriter.close();
                    if (export != null) {
                        export.close();
                    }
                }
            }
        }
    }

    private FfiExport writeVsr(VectorSchemaRoot vsr, BufferAllocator allocator) throws IOException {
        ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        boolean success = false;
        try {
            ArrowCStruct cStruct =
                    ArrowUtils.serializeToCStruct(vsr, arrowArray, arrowSchema, allocator);
            long t1 = System.currentTimeMillis();
            nativeWriter.writeBatchFfi(cStruct.arrayAddress(), cStruct.schemaAddress());
            jniCost += (System.currentTimeMillis() - t1);
            success = true;
            return new FfiExport(arrowArray, arrowSchema);
        } finally {
            if (!success) {
                FfiExport.close(arrowArray, arrowSchema);
            }
        }
    }

    private void replacePendingBatch(PendingBatch batch) {
        PendingBatch previousBatch = pendingBatch;
        pendingBatch = batch;
        closePendingBatch(previousBatch);
    }

    private void closePendingBatch() {
        closePendingBatch(pendingBatch);
        pendingBatch = null;
    }

    private static void closePendingBatch(@Nullable PendingBatch batch) {
        if (batch != null) {
            batch.close();
        }
    }

    private static class PendingBatch implements AutoCloseable {

        @Nullable private final ArrowFormatWriter writer;
        private final FfiExport ffiExport;

        private PendingBatch(@Nullable ArrowFormatWriter writer, FfiExport ffiExport) {
            this.writer = writer;
            this.ffiExport = ffiExport;
        }

        @Override
        public void close() {
            RuntimeException runtimeException = null;
            runtimeException = FfiExport.run(runtimeException, ffiExport::close);
            if (writer != null) {
                runtimeException = FfiExport.run(runtimeException, writer::close);
            }
            if (runtimeException != null) {
                throw runtimeException;
            }
        }
    }

    private static class FfiExport implements AutoCloseable {

        private final ArrowArray arrowArray;
        private final ArrowSchema arrowSchema;

        private FfiExport(ArrowArray arrowArray, ArrowSchema arrowSchema) {
            this.arrowArray = arrowArray;
            this.arrowSchema = arrowSchema;
        }

        @Override
        public void close() {
            close(arrowArray, arrowSchema);
        }

        private static void close(ArrowArray arrowArray, ArrowSchema arrowSchema) {
            RuntimeException runtimeException = null;
            runtimeException = run(runtimeException, () -> releaseArrayIfNeeded(arrowArray));
            runtimeException = run(runtimeException, () -> releaseSchemaIfNeeded(arrowSchema));
            runtimeException = run(runtimeException, arrowArray::close);
            runtimeException = run(runtimeException, arrowSchema::close);
            if (runtimeException != null) {
                throw runtimeException;
            }
        }

        private static void releaseArrayIfNeeded(ArrowArray arrowArray) {
            if (arrowArray.snapshot().release != 0) {
                arrowArray.release();
            }
        }

        private static void releaseSchemaIfNeeded(ArrowSchema arrowSchema) {
            if (arrowSchema.snapshot().release != 0) {
                arrowSchema.release();
            }
        }

        private static RuntimeException run(RuntimeException previous, ThrowingRunnable runnable) {
            try {
                runnable.run();
            } catch (RuntimeException e) {
                if (previous == null) {
                    return e;
                }
                previous.addSuppressed(e);
            }
            return previous;
        }

        private interface ThrowingRunnable {
            void run();
        }
    }
}
