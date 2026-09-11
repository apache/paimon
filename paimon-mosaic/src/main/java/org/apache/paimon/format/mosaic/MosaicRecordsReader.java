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

import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.mosaic.ColumnStatistics;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ExecutorThreadFactory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static org.apache.paimon.format.mosaic.MosaicObjects.convertStatsValue;

/** File reader for Mosaic format. */
public class MosaicRecordsReader implements FileRecordReader<InternalRow> {

    private final MosaicInputFileAdapter inputFileAdapter;
    private final MosaicReader reader;
    private final ArrowBatchReader arrowBatchReader;
    private final Path filePath;
    private final BufferAllocator allocator;
    private final int numRowGroups;
    private final RowType dataSchemaRowType;
    private final int projectedFieldCount;
    private final boolean allProjectedColumnsMissing;
    @Nullable private final List<Predicate> predicates;

    /** Opens upcoming row groups while the current one is consumed; opens are thread-safe. */
    private static final ExecutorService PREFETCH_POOL =
            Executors.newCachedThreadPool(new ExecutorThreadFactory("mosaic-row-group-prefetch"));

    private final int prefetchDepth;
    private final ArrayDeque<RowGroupBatch> pending = new ArrayDeque<>();
    private int nextRowGroupToSchedule;
    private long scheduledRowCount;

    private long returnedPosition = -1;
    private VectorSchemaRoot currentVsr;

    private static final Logger LOG = LoggerFactory.getLogger(MosaicRecordsReader.class);

    // Read statistics reported at debug level when the reader closes.
    private int openedRowGroups;
    private long openNanos;
    private long rowsReturned;
    private final long createdNanos = System.nanoTime();

    public MosaicRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            long fileSize,
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates,
            Path filePath) {
        this(
                inputFileAdapter,
                fileSize,
                dataSchemaRowType,
                projectedRowType,
                predicates,
                filePath,
                MosaicFileFormat.READ_PREFETCH_ROW_GROUPS.defaultValue());
    }

    public MosaicRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            long fileSize,
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates,
            Path filePath,
            int prefetchRowGroups) {
        this(
                inputFileAdapter,
                fileSize,
                dataSchemaRowType,
                projectedRowType,
                predicates,
                filePath,
                new RootAllocator(),
                MosaicReader::open,
                prefetchRowGroups);
    }

    MosaicRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            long fileSize,
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates,
            Path filePath,
            BufferAllocator allocator,
            NativeReaderOpener nativeReaderOpener) {
        this(
                inputFileAdapter,
                fileSize,
                dataSchemaRowType,
                projectedRowType,
                predicates,
                filePath,
                allocator,
                nativeReaderOpener,
                MosaicFileFormat.READ_PREFETCH_ROW_GROUPS.defaultValue());
    }

    MosaicRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            long fileSize,
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates,
            Path filePath,
            BufferAllocator allocator,
            NativeReaderOpener nativeReaderOpener,
            int prefetchRowGroups) {
        this.filePath = filePath;
        this.inputFileAdapter = inputFileAdapter;
        this.dataSchemaRowType = dataSchemaRowType;
        this.projectedFieldCount = projectedRowType.getFieldCount();
        this.predicates = predicates;
        this.allocator = allocator;
        this.prefetchDepth = Math.max(0, prefetchRowGroups);

        MosaicReader createdReader = null;
        int createdNumRowGroups;
        ArrowBatchReader createdArrowBatchReader;
        boolean createdAllProjectedColumnsMissing = false;
        try {
            createdReader = nativeReaderOpener.open(inputFileAdapter, fileSize, allocator);

            Schema fileSchema = createdReader.getSchema();
            Set<String> fileColumnNames = new HashSet<>();
            for (Field field : fileSchema.getFields()) {
                fileColumnNames.add(field.getName());
            }
            List<String> projectedNames = projectedRowType.getFieldNames();
            List<String> existingColumns = new ArrayList<>();
            for (String name : projectedNames) {
                if (fileColumnNames.contains(name)) {
                    existingColumns.add(name);
                }
            }
            createdAllProjectedColumnsMissing = existingColumns.isEmpty();
            if (!existingColumns.isEmpty()) {
                createdReader.project(existingColumns.toArray(new String[0]));
            }

            createdNumRowGroups = createdReader.numRowGroups();
            createdArrowBatchReader = new ArrowBatchReader(projectedRowType, true);
        } catch (Throwable t) {
            closeOnConstructionFailure(t, createdReader, allocator, inputFileAdapter);
            throw rethrowUnchecked(t);
        }

        this.reader = createdReader;
        this.numRowGroups = createdNumRowGroups;
        this.allProjectedColumnsMissing = createdAllProjectedColumnsMissing;
        this.arrowBatchReader = createdArrowBatchReader;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        releaseCurrentVsr();

        RowGroupBatch batch = nextRowGroup();
        if (batch == null) {
            return null;
        }
        // Rows of skipped row groups still count towards the file position.
        returnedPosition = batch.startPosition - 1;
        rowsReturned += batch.numRows;

        if (allProjectedColumnsMissing) {
            return allNullIterator(batch.numRows);
        }

        Iterator<InternalRow> rows = arrowBatchReader.readBatch(currentVsr).iterator();

        return new FileRecordIterator<InternalRow>() {
            @Override
            public long returnedPosition() {
                return returnedPosition;
            }

            @Override
            public Path filePath() {
                return filePath;
            }

            @Nullable
            @Override
            public InternalRow next() {
                if (rows.hasNext()) {
                    returnedPosition++;
                    return rows.next();
                }
                return null;
            }

            @Override
            public void releaseBatch() {
                releaseCurrentVsr();
            }
        };
    }

    /** Returns the next matching row group with its data ready, or null at end of file. */
    @Nullable
    private RowGroupBatch nextRowGroup() throws IOException {
        if (pending.isEmpty()) {
            fillPrefetchQueue(Math.max(1, prefetchDepth));
        }
        RowGroupBatch head = pending.peek();
        if (head == null) {
            return null;
        }
        // The head stays queued until its data has arrived, so close() can still wait for it.
        long waitStart = System.nanoTime();
        VectorSchemaRoot vsr = head.await();
        openNanos += System.nanoTime() - waitStart;
        openedRowGroups++;
        pending.poll();
        currentVsr = vsr;
        if (prefetchDepth > 0) {
            // currentVsr is owned by this reader, so a failure here leaves nothing unreleased.
            fillPrefetchQueue(prefetchDepth);
        }
        return head;
    }

    /** Schedules matching row groups until {@code wanted} of them are queued. */
    private void fillPrefetchQueue(int wanted) {
        while (pending.size() < wanted && nextRowGroupToSchedule < numRowGroups) {
            int index = nextRowGroupToSchedule++;
            int numRows = reader.rowGroupNumRows(index);
            long startPosition = scheduledRowCount;
            scheduledRowCount += numRows;
            if (!matchesRowGroup(index, numRows)) {
                continue;
            }
            Future<VectorSchemaRoot> future = null;
            if (!allProjectedColumnsMissing) {
                FutureTask<VectorSchemaRoot> task =
                        new FutureTask<>(() -> reader.readRowGroup(index, allocator));
                if (prefetchDepth == 0) {
                    task.run();
                } else {
                    PREFETCH_POOL.execute(task);
                }
                future = task;
            }
            pending.add(new RowGroupBatch(index, numRows, startPosition, future));
        }
    }

    /** A row group whose data is being, or has been, loaded. */
    private static final class RowGroupBatch {
        final int index;
        final int numRows;
        final long startPosition;
        @Nullable private final Future<VectorSchemaRoot> future;

        RowGroupBatch(
                int index,
                int numRows,
                long startPosition,
                @Nullable Future<VectorSchemaRoot> future) {
            this.index = index;
            this.numRows = numRows;
            this.startPosition = startPosition;
            this.future = future;
        }

        /** Waits for the data; the batch is not released here even when the wait fails. */
        @Nullable
        VectorSchemaRoot await() throws IOException {
            if (future == null) {
                return null;
            }
            try {
                return future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                InterruptedIOException interrupted =
                        new InterruptedIOException("Interrupted while opening row group " + index);
                interrupted.initCause(e);
                throw interrupted;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                if (cause instanceof Error) {
                    throw (Error) cause;
                }
                throw new IOException("Failed to open row group " + index, cause);
            }
        }

        /**
         * Waits for the read to finish, even if the current thread is interrupted, and releases its
         * data. Returns whether an interrupt was swallowed while waiting.
         */
        boolean discard() {
            if (future == null) {
                return false;
            }
            boolean interrupted = false;
            while (true) {
                try {
                    VectorSchemaRoot vsr = future.get();
                    if (vsr != null) {
                        vsr.close();
                    }
                    return interrupted;
                } catch (InterruptedException e) {
                    // The native read still uses the reader handle; it must complete first.
                    interrupted = true;
                } catch (ExecutionException e) {
                    return interrupted;
                }
            }
        }
    }

    private FileRecordIterator<InternalRow> allNullIterator(int numRows) {
        GenericRow row = new GenericRow(projectedFieldCount);
        return new FileRecordIterator<InternalRow>() {
            private int position;

            @Override
            public long returnedPosition() {
                return returnedPosition;
            }

            @Override
            public Path filePath() {
                return filePath;
            }

            @Nullable
            @Override
            public InternalRow next() {
                if (position < numRows) {
                    position++;
                    returnedPosition++;
                    return row;
                }
                return null;
            }

            @Override
            public void releaseBatch() {}
        };
    }

    private boolean matchesRowGroup(int rowGroupIndex, long rowCount) {
        if (predicates == null || predicates.isEmpty()) {
            return true;
        }

        Map<String, ColumnStatistics> statsMap = reader.getRowGroupStatistics(rowGroupIndex);
        if (statsMap.isEmpty()) {
            return true;
        }

        int fieldCount = dataSchemaRowType.getFieldCount();
        GenericRow minValues = new GenericRow(fieldCount);
        GenericRow maxValues = new GenericRow(fieldCount);
        long[] nullCounts = new long[fieldCount];

        List<DataField> fields = dataSchemaRowType.getFields();
        for (int i = 0; i < fieldCount; i++) {
            String colName = fields.get(i).name();
            ColumnStatistics stats = statsMap.get(colName);
            if (stats == null) {
                continue;
            }

            nullCounts[i] = stats.getNullCount();
            if (stats.hasMinMax()) {
                DataType dataType = fields.get(i).type();
                Object min = convertStatsValue(stats.getMin(), dataType);
                Object max = convertStatsValue(stats.getMax(), dataType);
                minValues.setField(i, min);
                maxValues.setField(i, max);
            }
        }

        for (Predicate predicate : predicates) {
            if (!predicate.test(rowCount, minValues, maxValues, new GenericArray(nullCounts))) {
                return false;
            }
        }
        return true;
    }

    private void releaseCurrentVsr() {
        if (currentVsr != null) {
            currentVsr.close();
            currentVsr = null;
        }
    }

    @Override
    public void close() throws IOException {
        Throwable throwable = null;

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Closing mosaic reader for {}: row groups {} (opened {}), rows {}, "
                            + "waited {} ms for row groups, lifetime {} ms, {}",
                    filePath.getName(),
                    numRowGroups,
                    openedRowGroups,
                    rowsReturned,
                    openNanos / 1_000_000,
                    (System.nanoTime() - createdNanos) / 1_000_000,
                    inputFileAdapter.ioStats());
        }

        try {
            releaseCurrentVsr();
        } catch (Throwable t) {
            throwable = t;
        }

        // Prefetched row groups must finish and be released before the native reader and the
        // allocator go away, even if this thread is interrupted.
        boolean interrupted = false;
        RowGroupBatch batch;
        while ((batch = pending.poll()) != null) {
            try {
                interrupted |= batch.discard();
            } catch (Throwable t) {
                throwable = addSuppressed(throwable, t);
            }
        }

        try {
            reader.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            allocator.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            inputFileAdapter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (throwable != null) {
            rethrow(throwable);
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
            @Nullable MosaicReader reader,
            BufferAllocator allocator,
            MosaicInputFileAdapter inputFileAdapter) {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }

        try {
            allocator.close();
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }

        try {
            inputFileAdapter.close();
        } catch (Throwable t) {
            addSuppressed(throwable, t);
        }
    }

    @FunctionalInterface
    interface NativeReaderOpener {

        MosaicReader open(
                MosaicInputFileAdapter inputFileAdapter, long fileSize, BufferAllocator allocator);
    }
}
