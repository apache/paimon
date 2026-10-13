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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.VectorizedBundleRecords;
import org.apache.paimon.mosaic.ColumnStatistics;
import org.apache.paimon.mosaic.MosaicWriter;
import org.apache.paimon.mosaic.WriterOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.OversizedAllocationException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Mosaic records writer. */
public class MosaicRecordsWriter implements BundleFormatWriter {

    private static final int MEMORY_CHECK_INTERVAL = 32;
    private static final int ALLOCATION_HEADROOM_FACTOR = 4;

    private final ArrowFormatWriter arrowFormatWriter;
    private final long writeBatchMemory;
    private final long conversionMemoryLimit;
    private final boolean columnBatchSupported;
    private int vectorBatchRows;
    private final MosaicWriter nativeWriter;
    private final BufferAllocator allocator;
    private final List<String> statsColumnNames;
    private boolean failed;
    @Nullable private MosaicWriterMetadata metadata;

    public MosaicRecordsWriter(
            OutputStream outputStream,
            RowType rowType,
            FileFormatFactory.FormatContext formatContext,
            List<String> statsColumnNames,
            @Nullable Integer numBuckets) {
        this(
                outputStream,
                rowType,
                formatContext,
                statsColumnNames,
                numBuckets,
                new RootAllocator(),
                MosaicWriter::new);
    }

    MosaicRecordsWriter(
            OutputStream outputStream,
            RowType rowType,
            FileFormatFactory.FormatContext formatContext,
            List<String> statsColumnNames,
            @Nullable Integer numBuckets,
            BufferAllocator allocator,
            NativeWriterFactory nativeWriterFactory) {
        this.statsColumnNames = statsColumnNames;
        this.allocator = allocator;

        int writeBatchSize = formatContext.writeBatchSize();
        this.writeBatchMemory = formatContext.writeBatchMemory().getBytes();
        this.conversionMemoryLimit =
                writeBatchMemory > Long.MAX_VALUE / ALLOCATION_HEADROOM_FACTOR
                        ? Long.MAX_VALUE
                        : writeBatchMemory * ALLOCATION_HEADROOM_FACTOR;
        this.columnBatchSupported =
                rowType.getFields().stream().allMatch(field -> supportsColumnBatch(field.type()));
        this.vectorBatchRows = initialVectorBatchRows(rowType, writeBatchSize, writeBatchMemory);

        WriterOptions options = new WriterOptions().zstdLevel(formatContext.zstdLevel());
        if (numBuckets != null) {
            options = options.numBuckets(numBuckets);
        }
        MemorySize blockSize = formatContext.blockSize();
        if (blockSize != null) {
            options = options.rowGroupMaxSize(blockSize.getBytes());
        }
        if (!statsColumnNames.isEmpty()) {
            options.statsColumns(statsColumnNames.toArray(new String[0]));
        }

        ArrowFormatWriter createdArrowWriter = null;
        MosaicWriter createdNativeWriter = null;
        try {
            checkArgument(
                    writeBatchSize > 0,
                    "'write.batch-size' must be greater than 0, but was %s.",
                    writeBatchSize);
            createdArrowWriter =
                    ArrowFormatWriter.forBorrowedAllocator(
                            rowType, writeBatchSize, true, allocator, writeBatchMemory);
            // Only batches smaller than Arrow's default allocation are sized by the batch.
            if (writeBatchSize < BaseValueVector.INITIAL_VALUE_ALLOCATION) {
                for (FieldVector vector :
                        createdArrowWriter.getVectorSchemaRoot().getFieldVectors()) {
                    setInitialCapacity(vector, writeBatchSize);
                }
            }
            Schema arrowSchema = createdArrowWriter.getVectorSchemaRoot().getSchema();
            createdNativeWriter =
                    nativeWriterFactory.create(outputStream, arrowSchema, options, allocator);
        } catch (Throwable t) {
            closeOnConstructionFailure(t, createdNativeWriter, createdArrowWriter, allocator);
            throw rethrowUnchecked(t);
        }

        this.arrowFormatWriter = createdArrowWriter;
        this.nativeWriter = createdNativeWriter;
    }

    private static void setInitialCapacity(FieldVector vector, int capacity) {
        if (vector instanceof BaseRepeatedValueVector) {
            // Avoid Arrow's 5x estimate for fixed- or variable-width element vectors.
            ((BaseRepeatedValueVector) vector).setInitialCapacity(capacity, 1.0);
        } else {
            vector.setInitialCapacity(capacity);
        }
    }

    @Override
    public void addElement(InternalRow internalRow) {
        checkNotFailed();
        if (!arrowFormatWriter.write(internalRow)) {
            flush();
            if (!arrowFormatWriter.write(internalRow)) {
                throw new RuntimeException("Failed to write row to Mosaic file");
            }
        }
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) {
        checkNotFailed();
        if (columnBatchSupported && bundleRecords instanceof VectorizedBundleRecords) {
            writeColumnBatch((VectorizedBundleRecords) bundleRecords);
            return;
        }
        if (bundleRecords instanceof ArrowBundleRecords) {
            ArrowBundleRecords arrowBundle = (ArrowBundleRecords) bundleRecords;
            VectorSchemaRoot root = arrowBundle.getVectorSchemaRoot();
            if (arrowFormatWriter.isArrowBundleSchemaCompatible(arrowBundle)
                    && ArrowUtils.hasSameRootAllocator(root, allocator)) {
                writeArrowBatch(root);
                return;
            }
        }
        for (InternalRow row : bundleRecords) {
            addElement(row);
        }
    }

    /** The conversion buffer owned and closed by this writer. */
    protected final ArrowFormatWriter arrowWriter() {
        return arrowFormatWriter;
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
        if (!suggestedCheck) {
            return false;
        }
        return nativeWriter.estimatedFileSize() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        Throwable throwable = null;

        if (!failed) {
            try {
                flush();
            } catch (Throwable t) {
                throwable = t;
            }
        }

        try {
            nativeWriter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            collectMetadata();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            arrowFormatWriter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            allocator.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        if (throwable != null) {
            rethrow(throwable);
        }
    }

    @Nullable
    @Override
    public Object writerMetadata() {
        return metadata;
    }

    private void collectMetadata() {
        int numRowGroups = nativeWriter.numRowGroups();
        List<Map<String, ColumnStatistics>> allStats = new ArrayList<>(numRowGroups);
        for (int i = 0; i < numRowGroups; i++) {
            allStats.add(nativeWriter.getRowGroupStatistics(i));
        }
        this.metadata = new MosaicWriterMetadata(numRowGroups, allStats, statsColumnNames);
    }

    /**
     * Write a borrowed batch synchronously after flushing buffered rows. The caller must validate
     * schema and allocator compatibility for the native implementation it uses. Ownership remains
     * with the caller, which must keep the batch alive until this method returns.
     */
    private void writeArrowBatch(VectorSchemaRoot root) {
        checkNotFailed();
        flush();
        try {
            nativeWriter.write(root);
        } catch (RuntimeException | Error e) {
            failed = true;
            throw e;
        }
    }

    protected final void flush() {
        if (arrowFormatWriter.empty()) {
            return;
        }
        arrowFormatWriter.flush();
        VectorSchemaRoot vsr = arrowFormatWriter.getVectorSchemaRoot();
        try {
            nativeWriter.write(vsr);
        } catch (RuntimeException | Error e) {
            failed = true;
            throw e;
        } finally {
            arrowFormatWriter.reset();
        }
    }

    protected final void checkNotFailed() {
        if (failed) {
            throw new IllegalStateException("Mosaic writer has failed");
        }
    }

    private void writeColumnBatch(VectorizedBundleRecords vectors) {
        int[] selected = vectors.selected();
        int totalRows = selected == null ? vectors.batch().getNumRows() : selected.length;
        if (totalRows == 0) {
            return;
        }
        flush();
        for (int start = 0; start < totalRows; ) {
            int rows;
            try {
                rows =
                        prepareVectorizedBatch(
                                vectors.batch().columns,
                                selected,
                                start,
                                Math.min(arrowFormatWriter.getBatchSize(), totalRows - start));
            } catch (RuntimeException | Error e) {
                // Conversion can leave partially populated vectors. Never flush those on close.
                failed = true;
                clearArrowWriterForRetry();
                throw e;
            }
            flush();
            start += rows;
        }
    }

    private int prepareVectorizedBatch(
            ColumnVector[] columns, @Nullable int[] selected, int startIndex, int maxBatchRows) {
        // Consume successful slices once and carry their measured size into the next slice.
        // Double at most: a short prefix must not trigger allocation for an entire skewed batch.
        int batchRows = Math.min(vectorBatchRows, maxBatchRows);
        while (true) {
            long memoryUsed;
            try {
                memoryUsed = writeWithinAllocationLimit(columns, selected, startIndex, batchRows);
            } catch (OutOfMemoryException | OversizedAllocationException e) {
                clearArrowWriterForRetry();
                if (batchRows == 1) {
                    throw e;
                }
                // Avoid repeatedly filling the same allocation budget with oversized retries.
                batchRows = Math.max(1, Math.min(MEMORY_CHECK_INTERVAL, batchRows / 2));
                continue;
            }

            if (memoryUsed > writeBatchMemory && batchRows > 1) {
                clearArrowWriterForRetry();
                batchRows = reducedBatchRows(batchRows, memoryUsed, writeBatchMemory);
                continue;
            }

            vectorBatchRows =
                    Math.min(
                            arrowFormatWriter.getBatchSize(),
                            Math.min(
                                    batchRows > Integer.MAX_VALUE / 2
                                            ? Integer.MAX_VALUE
                                            : batchRows * 2,
                                    rowsWithinMemory(batchRows, memoryUsed, writeBatchMemory)));
            return batchRows;
        }
    }

    private long writeWithinAllocationLimit(
            ColumnVector[] columns, @Nullable int[] selected, int startIndex, int batchRows) {
        BufferAllocator allocator = arrowFormatWriter.getAllocator();
        long originalLimit = allocator.getLimit();
        try {
            if (batchRows > 1) {
                // This writer owns the allocator. Allow capacity rounding and overlapping old/new
                // buffers during Arrow growth, but reject large allocations before copying them.
                // Keep existing buffers reusable and leave oversized single rows to the original
                // allocator limit, since write.batch-memory is a soft batch threshold.
                allocator.setLimit(
                        Math.min(
                                originalLimit,
                                Math.max(allocator.getAllocatedMemory(), conversionMemoryLimit)));
            }
            arrowFormatWriter.write(columns, selected, startIndex, batchRows);
            return arrowFormatWriter.memoryUsed();
        } finally {
            // Native writes share this allocator and must retain its original limit.
            allocator.setLimit(originalLimit);
        }
    }

    private void clearArrowWriterForRetry() {
        arrowFormatWriter.reset();
        arrowFormatWriter.getVectorSchemaRoot().clear();
    }

    private static int initialVectorBatchRows(
            RowType rowType, int writeBatchSize, long writeBatchMemory) {
        if (rowType.getFields().stream().anyMatch(field -> !isFixedWidth(field.type()))) {
            return Math.min(writeBatchSize, MEMORY_CHECK_INTERVAL);
        }

        // Include one byte per field as a conservative allowance for validity buffers.
        long estimatedBytesPerRow = (long) rowType.defaultSize() + rowType.getFieldCount();
        return Math.min(
                writeBatchSize,
                rowsWithinMemory(1, Math.max(1, estimatedBytesPerRow), writeBatchMemory));
    }

    private static boolean isFixedWidth(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;
            default:
                return false;
        }
    }

    private static int reducedBatchRows(int batchRows, long memoryUsed, long writeBatchMemory) {
        return Math.min(batchRows - 1, rowsWithinMemory(batchRows, memoryUsed, writeBatchMemory));
    }

    private static int rowsWithinMemory(int batchRows, long memoryUsed, long writeBatchMemory) {
        if (memoryUsed <= 0) {
            return Integer.MAX_VALUE;
        }
        double estimatedRows = (double) writeBatchMemory * batchRows / memoryUsed;
        return Math.max(1, (int) Math.min(Integer.MAX_VALUE, estimatedRows));
    }

    private static boolean supportsColumnBatch(DataType type) {
        // Keep nested types on the existing row path until their shared column converters support
        // sliced child vectors. This does not change the types supported by the Mosaic format.
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return true;
            default:
                return isFixedWidth(type);
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

    private static RuntimeException rethrowUnchecked(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            return (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        return new RuntimeException(throwable);
    }

    private static void closeOnConstructionFailure(
            Throwable throwable,
            @Nullable MosaicWriter nativeWriter,
            @Nullable ArrowFormatWriter arrowFormatWriter,
            BufferAllocator allocator) {
        try {
            if (nativeWriter != null) {
                nativeWriter.close();
            }
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }

        try {
            if (arrowFormatWriter != null) {
                arrowFormatWriter.close();
            }
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }

        try {
            allocator.close();
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }
    }

    /** Creates the native writer using the allocator owned by the records writer. */
    @FunctionalInterface
    interface NativeWriterFactory {

        MosaicWriter create(
                OutputStream outputStream,
                Schema arrowSchema,
                WriterOptions options,
                BufferAllocator allocator);
    }
}
