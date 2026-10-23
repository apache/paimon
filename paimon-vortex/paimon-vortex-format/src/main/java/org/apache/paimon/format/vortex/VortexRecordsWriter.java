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
import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.io.BundleRecords;

import dev.vortex.api.VortexWriteSummary;
import dev.vortex.api.VortexWriter;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;

/**
 * Vortex records writer using the Arrow C Data Interface.
 *
 * <p>The native writer moves each exported Arrow array and keeps references to its buffers until
 * the batch is encoded, which can be after {@code writeBatch} returns. The exported array holds its
 * own reference to each buffer, so after the export this writer clears its vectors instead of
 * overwriting buffers that the native writer can still read. All buffers come from one allocator,
 * which is closed only after the native writer is finished.
 */
public class VortexRecordsWriter implements BundleFormatWriter {

    private static final Logger LOG = LoggerFactory.getLogger(VortexRecordsWriter.class);

    /** The compression ratio to assume until the native writer has written enough bytes. */
    private static final double INITIAL_COMPRESSION_RATIO = 0.25;

    /** Bytes on the sink from which the observed compression ratio is used. */
    private static final long MIN_OBSERVED_BYTES = 1024 * 1024;

    private final ArrowFormatCWriter arrowWriter;
    private final VortexWriter nativeWriter;
    @Nullable private final Closeable output;
    private final String path;

    private long acceptedBytes;
    private long writeNanos;
    @Nullable private VortexWriteSummary summary;
    private boolean closed;

    VortexRecordsWriter(ArrowFormatCWriter arrowWriter, VortexOutput output) throws IOException {
        this.arrowWriter = arrowWriter;
        this.output = output.writable();
        this.path = output.name();

        try {
            this.nativeWriter =
                    output.open(
                            arrowWriter.getVectorSchemaRoot().getSchema(),
                            arrowWriter.getAllocator());
        } catch (Throwable t) {
            closeQuietly(this.output, t);
            closeQuietly(arrowWriter, t);
            throw t;
        }
    }

    @Override
    public void addElement(InternalRow internalRow) throws IOException {
        if (!arrowWriter.write(internalRow)) {
            flush();
            if (!arrowWriter.write(internalRow)) {
                throw new RuntimeException("Exception happens while write to vortex file");
            }
        }
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) throws IOException {
        if (bundleRecords instanceof ArrowBundleRecords) {
            ArrowBundleRecords arrowBundle = (ArrowBundleRecords) bundleRecords;
            if (arrowWriter.formatWriter().isArrowBundleSchemaCompatible(arrowBundle)) {
                flush();
                writeBundleRoot(arrowBundle.getVectorSchemaRoot());
                return;
            }
        }

        for (InternalRow row : bundleRecords) {
            addElement(row);
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
        return suggestedCheck && estimatedFileSize() >= targetSize;
    }

    /** The exact file size after close, or the size the file would have if it closed now. */
    long estimatedFileSize() {
        if (summary != null) {
            return summary.fileSize();
        }

        // The native writer writes the file header in the background, so without any rows the
        // byte count depends on timing. An empty file must never count as full.
        if (acceptedBytes == 0 && arrowWriter.empty()) {
            return 0;
        }

        long written = nativeWriter.bytesWritten();
        long javaBuffered = arrowWriter.empty() ? 0 : arrowWriter.formatWriter().memoryUsed();

        // The native counters lag behind the input: batches that are queued or being encoded are
        // neither buffered nor written. So take the compression ratio of the input that has left
        // the native buffers, once enough of it reached the sink, and apply it to all input.
        long flushedInput = acceptedBytes - nativeWriter.bufferedBytes();
        double ratio =
                written >= MIN_OBSERVED_BYTES && flushedInput > 0
                        ? Math.min(1.0, (double) written / flushedInput)
                        : INITIAL_COMPRESSION_RATIO;
        return Math.max(written, (long) ((acceptedBytes + javaBuffered) * ratio));
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        Throwable failure = null;
        try {
            flush();
        } catch (Throwable t) {
            failure = t;
        }

        // finish() blocks until the native writer has encoded and released every batch.
        try {
            summary = nativeWriter.finish();
        } catch (Throwable t) {
            failure = addSuppressed(failure, t);
        }

        failure = closeQuietly(output, failure);
        failure = closeArrowWriter(failure);

        LOG.debug(
                "Wrote {} uncompressed Arrow bytes in {} ms of native calls to {}.",
                acceptedBytes,
                writeNanos / 1_000_000,
                path);

        if (failure != null) {
            rethrow(failure);
        }
    }

    /** The native write summary, available after {@link #close()}. */
    @Nullable
    @Override
    public Object writerMetadata() {
        return summary;
    }

    private void flush() throws IOException {
        arrowWriter.flush();
        if (arrowWriter.empty()) {
            return;
        }

        VectorSchemaRoot root = arrowWriter.getVectorSchemaRoot();
        writeRoot(root, arrowWriter.getAllocator());

        // The exported array keeps the buffers alive for the native writer. Clear the vectors so
        // that the next batch allocates new buffers instead of resetting the exported ones, and
        // size the new buffers like this batch, so they do not grow value by value.
        int rowCount = root.getRowCount();
        double[] densities = new double[root.getFieldVectors().size()];
        for (int i = 0; i < densities.length; i++) {
            FieldVector vector = root.getVector(i);
            if (vector instanceof BaseVariableWidthVector && rowCount > 0) {
                long bytes = ((BaseVariableWidthVector) vector).getStartOffset(rowCount);
                densities[i] = Math.max(1.0, (double) bytes / rowCount);
            }
        }
        root.clear();
        for (int i = 0; i < densities.length; i++) {
            FieldVector vector = root.getVector(i);
            if (densities[i] > 0) {
                ((BaseVariableWidthVector) vector).setInitialCapacity(rowCount, densities[i]);
            } else {
                vector.setInitialCapacity(rowCount);
            }
            vector.allocateNew();
        }
        arrowWriter.reset();
    }

    /** Copies an external root, which its owner can reuse after this call, then writes the copy. */
    private void writeBundleRoot(VectorSchemaRoot root) throws IOException {
        byte[] ipc = ArrowUtils.serializeToIpc(root);
        BufferAllocator allocator = arrowWriter.getAllocator();
        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            if (!reader.loadNextBatch()) {
                throw new IOException("Arrow IPC copy did not contain a record batch.");
            }
            writeRoot(reader.getVectorSchemaRoot(), allocator);
        }
    }

    private void writeRoot(VectorSchemaRoot root, BufferAllocator allocator) throws IOException {
        acceptedBytes += bufferBytes(root);
        try (ArrowArray array = ArrowArray.allocateNew(allocator);
                ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
            Data.exportVectorSchemaRoot(allocator, root, null, array, schema);
            long start = System.nanoTime();
            try {
                nativeWriter.writeBatch(array.memoryAddress(), schema.memoryAddress());
            } finally {
                writeNanos += System.nanoTime() - start;
                // The native writer moves the array and borrows the schema. Release what is
                // still ours.
                ArrowUtils.releaseCDataIfNeeded(array, schema);
            }
        }
    }

    @Nullable
    private Throwable closeArrowWriter(@Nullable Throwable failure) {
        try {
            arrowWriter.close();
        } catch (IllegalStateException e) {
            // Arrow reports buffers that a native release callback has not returned yet as a
            // leak. The native writer is finished, so they are freed when it drops them.
            if (e.getMessage() == null || !e.getMessage().contains("Memory was leaked")) {
                return addSuppressed(failure, e);
            }
            LOG.debug("Arrow buffers of {} were still referenced on close.", path, e);
        } catch (Throwable t) {
            return addSuppressed(failure, t);
        }
        return failure;
    }

    private static long bufferBytes(VectorSchemaRoot root) {
        long bytes = 0;
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            bytes += root.getFieldVectors().get(i).getBufferSize();
        }
        return bytes;
    }

    @Nullable
    private static Throwable closeQuietly(
            @Nullable AutoCloseable closeable, @Nullable Throwable failure) {
        if (closeable == null) {
            return failure;
        }

        try {
            closeable.close();
        } catch (Throwable t) {
            return addSuppressed(failure, t);
        }
        return failure;
    }

    private static Throwable addSuppressed(@Nullable Throwable failure, Throwable suppressed) {
        if (failure == null) {
            return suppressed;
        }
        failure.addSuppressed(suppressed);
        return failure;
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
