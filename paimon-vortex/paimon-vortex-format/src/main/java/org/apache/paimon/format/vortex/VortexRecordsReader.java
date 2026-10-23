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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.AllNullColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.VectorizedRowIterator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.LongIterator;
import org.apache.paimon.utils.RoaringBitmap32;

import dev.vortex.api.DataSource;
import dev.vortex.api.Expression;
import dev.vortex.api.ImmutableScanOptions;
import dev.vortex.api.Partition;
import dev.vortex.api.Scan;
import dev.vortex.api.ScanOptions;
import dev.vortex.arrow.ArrowAllocation;
import dev.vortex.arrow.ArrowBatch;
import dev.vortex.arrow.ArrowBatchStream;
import dev.vortex.arrow.ViewColumn;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * File reader for Vortex format.
 *
 * <p>Each Arrow batch from the native scan is a new root that this reader owns until the consumer
 * releases the batch, so a later batch never overwrites one that the consumer still holds. Batches
 * are returned as {@link VectorizedRowIterator}s, so engines can consume them column by column.
 *
 * <p>A pushed-down filter or a row selection drops rows in native code. The file position of each
 * returned row then comes from the Vortex row index, because deletion vectors and row tracking
 * depend on it.
 */
public class VortexRecordsReader implements FileRecordReader<InternalRow> {

    private static final String ROW_INDEX_FIELD = "_PAIMON_VORTEX_ROW_INDEX";

    private final Path filePath;
    private final BufferAllocator allocator;
    @Nullable private final Closeable readable;
    private final DataSource dataSource;
    private final Scan scan;
    private final ArrowBatchReader arrowBatchReader;
    @Nullable private final int[] physicalFieldMapping;
    private final boolean readsRowIndex;
    private final Set<ArrowBatch> activeBatches = new HashSet<>();

    /** Fetches the next batch while the consumer processes the current one. */
    private static final ExecutorService PREFETCH_POOL =
            Executors.newCachedThreadPool(new ExecutorThreadFactory("vortex-batch-prefetch"));

    // Only one thread at a time calls fetchNext: the caller, or the prefetch it waits for.
    @Nullable private ArrowBatchStream currentStream;
    @Nullable private Future<ArrowBatch> prefetched;
    private long nextPosition;
    private boolean closed;

    VortexRecordsReader(
            VortexInput input,
            Path filePath,
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable RoaringBitmap32 selection,
            @Nullable Expression filter) {
        this.filePath = filePath;

        RowType physicalReadRowType = physicalReadRowType(dataSchemaRowType, projectedRowType);
        this.physicalFieldMapping =
                physicalFieldMapping(dataSchemaRowType, physicalReadRowType, projectedRowType);
        this.arrowBatchReader =
                new ArrowBatchReader(physicalReadRowType, true, VortexArrowConverters.INSTANCE);

        List<String> columns = physicalReadRowType.getFieldNames();
        this.readsRowIndex = filter != null || selection != null || columns.isEmpty();

        this.allocator =
                ArrowAllocation.rootAllocator()
                        .newChildAllocator("vortex-reader", 0, Long.MAX_VALUE);

        Closeable openedReadable = null;
        DataSource openedDataSource = null;
        try {
            openedReadable = input.readable();
            openedDataSource = input.open();
            // The scan clones the expressions it is given, so the projection is ours to release
            // once the scan exists.
            try (Expression projection = projection(columns)) {
                this.scan = openedDataSource.scan(scanOptions(projection, selection, filter));
            }
        } catch (Throwable t) {
            closeQuietly(openedDataSource, t);
            closeQuietly(openedReadable, t);
            closeQuietly(allocator, t);
            throw t;
        }

        this.readable = openedReadable;
        this.dataSource = openedDataSource;

        // Start decoding the first batch now. A merge opens all files of a split before reading
        // any of them, so their first batches then decode in parallel.
        this.prefetched = PREFETCH_POOL.submit(this::fetchNext);
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        while (true) {
            ArrowBatch batch = takeNext();
            if (batch == null) {
                return null;
            }

            track(batch);
            if (batch.rowCount() > 0) {
                prefetched = PREFETCH_POOL.submit(this::fetchNext);
                return toBatchIterator(batch);
            }
            releaseBatch(batch);
        }
    }

    @Nullable
    private ArrowBatch takeNext() throws IOException {
        if (prefetched == null) {
            return fetchNext();
        }

        // Keep the prefetch reachable until it completes. An interrupted wait leaves it running,
        // and close must then wait for it before it releases native resources.
        try {
            return await(prefetched);
        } finally {
            if (prefetched.isDone()) {
                prefetched = null;
            }
        }
    }

    /** Reads the next batch, moving on to the next partition when one is exhausted. */
    @Nullable
    private ArrowBatch fetchNext() throws IOException {
        while (true) {
            if (currentStream != null) {
                ArrowBatch batch = currentStream.next();
                if (batch != null) {
                    return batch;
                }
                closeCurrentStream();
            }

            if (!scan.hasNext()) {
                return null;
            }

            try (Partition partition = scan.next()) {
                currentStream = partition.scanArrow(allocator);
            }
        }
    }

    @Nullable
    private static ArrowBatch await(Future<ArrowBatch> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted =
                    new InterruptedIOException("Interrupted while reading a Vortex batch");
            interrupted.initCause(e);
            throw interrupted;
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }

    /**
     * Waits for the prefetch to complete, also when the thread is interrupted, because native IO in
     * the prefetch can still use the stream and the allocator. The interrupt flag is restored after
     * the wait.
     */
    @Nullable
    private static ArrowBatch awaitUninterruptibly(Future<ArrowBatch> future) throws IOException {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return future.get();
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    throw unwrap(e);
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static IOException unwrap(ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new IOException(cause);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        Throwable failure = null;
        if (prefetched != null) {
            // Wait for the prefetch, which may still use the stream, before closing anything.
            Future<ArrowBatch> future = prefetched;
            prefetched = null;
            try {
                ArrowBatch batch = awaitUninterruptibly(future);
                if (batch != null) {
                    batch.close();
                }
            } catch (Throwable t) {
                failure = t;
            }
        }
        try {
            closeCurrentStream();
        } catch (Throwable t) {
            if (failure == null) {
                failure = t;
            } else {
                failure.addSuppressed(t);
            }
        }

        List<ArrowBatch> unreleased;
        synchronized (activeBatches) {
            unreleased = new ArrayList<>(activeBatches);
            activeBatches.clear();
        }
        for (ArrowBatch batch : unreleased) {
            failure = closeQuietly(batch, failure);
        }

        failure = closeQuietly(scan, failure);
        failure = closeQuietly(dataSource, failure);
        failure = closeQuietly(readable, failure);
        failure = closeQuietly(allocator, failure);

        if (failure instanceof IOException) {
            throw (IOException) failure;
        } else if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        } else if (failure instanceof Error) {
            throw (Error) failure;
        } else if (failure != null) {
            throw new IOException(failure);
        }
    }

    private static ScanOptions scanOptions(
            Expression projection,
            @Nullable RoaringBitmap32 selection,
            @Nullable Expression filter) {
        // Paimon merges sorted files and maps deletion vectors by position, so rows must keep
        // their file order.
        ImmutableScanOptions.Builder builder = ImmutableScanOptions.builder().ordered(true);

        if (selection != null) {
            builder.selectionRoaringBitmap(serializeSelection(selection))
                    .selectionMode(ScanOptions.SelectionMode.INCLUDE_ROARING);
        }

        if (filter != null) {
            builder.filter(filter);
        }

        return builder.projection(projection).build();
    }

    private Expression projection(List<String> columns) {
        if (!readsRowIndex) {
            try (Expression root = Expression.root()) {
                return Expression.select(columns.toArray(new String[0]), root);
            }
        }

        String[] names = new String[columns.size() + 1];
        Expression[] values = new Expression[columns.size() + 1];
        try {
            for (int i = 0; i < columns.size(); i++) {
                names[i] = columns.get(i);
                values[i] = Expression.column(columns.get(i));
            }
            names[columns.size()] = ROW_INDEX_FIELD;
            values[columns.size()] = Expression.rowIdx();
            return Expression.pack(names, values, false);
        } finally {
            for (Expression value : values) {
                if (value != null) {
                    value.close();
                }
            }
        }
    }

    private void track(ArrowBatch batch) {
        synchronized (activeBatches) {
            activeBatches.add(batch);
        }
    }

    private void releaseBatch(ArrowBatch batch) {
        boolean owned;
        synchronized (activeBatches) {
            owned = activeBatches.remove(batch);
        }
        if (owned) {
            batch.close();
        }
    }

    private FileRecordIterator<InternalRow> toBatchIterator(ArrowBatch arrowBatch) {
        VectorSchemaRoot root = arrowBatch.root();
        VectorizedColumnBatch physicalBatch = arrowBatchReader.readVectorizedBatch(root);
        // The root follows the projection, so its column i is physical column i.
        for (int i = 0; i < physicalBatch.columns.length; i++) {
            ViewColumn viewColumn = arrowBatch.viewColumn(i);
            if (viewColumn != null) {
                physicalBatch.columns[i] = new ViewBytesColumnVector(viewColumn);
            }
        }
        VectorizedColumnBatch batch = projectedBatch(physicalBatch);
        int rowCount = arrowBatch.rowCount();

        LongIterator positions;
        if (readsRowIndex) {
            positions = rowIndexPositions((UInt8Vector) root.getVector(ROW_INDEX_FIELD), rowCount);
        } else {
            positions = LongIterator.fromRange(nextPosition, nextPosition + rowCount);
            nextPosition += rowCount;
        }

        VectorizedRowIterator iterator =
                new VectorizedRowIterator(
                        filePath, new ColumnarRow(batch), () -> releaseBatch(arrowBatch));
        iterator.reset(positions);
        return iterator;
    }

    private VectorizedColumnBatch projectedBatch(VectorizedColumnBatch physicalBatch) {
        if (physicalFieldMapping == null) {
            return physicalBatch;
        }

        ColumnVector[] columns = new ColumnVector[physicalFieldMapping.length];
        for (int i = 0; i < physicalFieldMapping.length; i++) {
            int index = physicalFieldMapping[i];
            columns[i] = index < 0 ? AllNullColumnVector.INSTANCE : physicalBatch.columns[index];
        }
        return physicalBatch.copy(columns);
    }

    private void closeCurrentStream() {
        if (currentStream != null) {
            ArrowBatchStream stream = currentStream;
            currentStream = null;
            stream.close();
        }
    }

    private static LongIterator rowIndexPositions(UInt8Vector rowIndex, int rowCount) {
        return new LongIterator() {

            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < rowCount;
            }

            @Override
            public long next() {
                return rowIndex.get(i++);
            }
        };
    }

    /**
     * Serializes a 32-bit selection as a portable Roaring64 bitmap: a little-endian count of 32-bit
     * bitmaps, then each bitmap after its little-endian high 32 bits. All Paimon file positions
     * have zero high bits, so there is one bitmap.
     */
    @VisibleForTesting
    static byte[] serializeSelection(RoaringBitmap32 selection) {
        byte[] bitmap = selection.serialize();
        return ByteBuffer.allocate(Long.BYTES + Integer.BYTES + bitmap.length)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(1L)
                .putInt(0)
                .put(bitmap)
                .array();
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
            if (failure == null) {
                return t;
            }
            failure.addSuppressed(t);
        }
        return failure;
    }

    @VisibleForTesting
    static RowType physicalReadRowType(RowType dataSchemaRowType, RowType projectedRowType) {
        if (!hasSynthesizedRowTrackingField(dataSchemaRowType, projectedRowType)) {
            return projectedRowType;
        }

        List<DataField> fields = new ArrayList<>();
        Set<Integer> selectedFieldIds = new HashSet<>();
        Set<String> selectedFieldNames = new HashSet<>();
        for (DataField projectedField : projectedRowType.getFields()) {
            if (isSynthesizedRowTrackingField(dataSchemaRowType, projectedField)) {
                continue;
            }

            DataField physicalField = physicalDataField(dataSchemaRowType, projectedField);
            if (physicalField == null
                    || selectedFieldIds.contains(physicalField.id())
                    || selectedFieldNames.contains(physicalField.name())) {
                continue;
            }
            fields.add(physicalField);
            selectedFieldIds.add(physicalField.id());
            selectedFieldNames.add(physicalField.name());
        }
        return new RowType(fields);
    }

    @Nullable
    @VisibleForTesting
    static int[] physicalFieldMapping(
            RowType dataSchemaRowType, RowType physicalReadRowType, RowType projectedRowType) {
        if (!hasSynthesizedRowTrackingField(dataSchemaRowType, projectedRowType)) {
            return null;
        }

        int[] mapping = new int[projectedRowType.getFieldCount()];
        for (int i = 0; i < projectedRowType.getFieldCount(); i++) {
            DataField field = projectedRowType.getFields().get(i);
            if (isSynthesizedRowTrackingField(dataSchemaRowType, field)) {
                mapping[i] = -1;
            } else {
                if (physicalReadRowType.containsField(field.id())) {
                    mapping[i] = physicalReadRowType.getFieldIndexByFieldId(field.id());
                } else {
                    mapping[i] = physicalReadRowType.getFieldIndex(field.name());
                    if (mapping[i] < 0) {
                        throw new RuntimeException(
                                String.format(
                                        "Cannot find physical field for projected field '%s' with id %s.",
                                        field.name(), field.id()));
                    }
                }
            }
        }
        return mapping;
    }

    @Nullable
    private static DataField physicalDataField(
            RowType dataSchemaRowType, DataField projectedField) {
        if (dataSchemaRowType.containsField(projectedField.id())) {
            return dataSchemaRowType.getField(projectedField.id());
        }
        if (dataSchemaRowType.containsField(projectedField.name())) {
            return dataSchemaRowType.getField(projectedField.name());
        }
        return null;
    }

    private static boolean hasSynthesizedRowTrackingField(
            RowType dataSchemaRowType, RowType rowType) {
        for (DataField field : rowType.getFields()) {
            if (isSynthesizedRowTrackingField(dataSchemaRowType, field)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A row-tracking field is synthesized only when the file does not store it physically. {@code
     * _SEQUENCE_NUMBER} doubles as the primary-key file format's physical sequence column: for
     * key-value reads it must be read like any other data column, not mapped away for synthesis —
     * doing so left its projection index at -1 and crashed every vortex read of a primary-key
     * table.
     */
    private static boolean isSynthesizedRowTrackingField(
            RowType dataSchemaRowType, DataField field) {
        if (!SpecialFields.ROW_ID.name().equals(field.name())
                && !SpecialFields.SEQUENCE_NUMBER.name().equals(field.name())) {
            return false;
        }
        return !dataSchemaRowType.containsField(field.id())
                && dataSchemaRowType.getFieldIndex(field.name()) < 0;
    }
}
