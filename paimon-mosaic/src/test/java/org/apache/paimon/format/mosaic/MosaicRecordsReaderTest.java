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

import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link MosaicRecordsReader}. */
class MosaicRecordsReaderTest {

    @Test
    void testConstructorRuntimeExceptionClosesCreatedResources() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        RuntimeException failure = new RuntimeException("native reader failed");

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsReader(
                                        inputFileAdapter,
                                        0,
                                        rowType(),
                                        rowType(),
                                        null,
                                        new Path("file:/tmp/mosaic-reader-test"),
                                        allocator,
                                        (inputFile, fileSize, bufferAllocator) -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);

        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testConstructorErrorClosesCreatedResources() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        UnsatisfiedLinkError failure = new UnsatisfiedLinkError("native library failed");

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsReader(
                                        inputFileAdapter,
                                        0,
                                        rowType(),
                                        rowType(),
                                        null,
                                        new Path("file:/tmp/mosaic-reader-test"),
                                        allocator,
                                        (inputFile, fileSize, bufferAllocator) -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);

        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testConstructorFailureAfterReaderCreatedClosesReaderAndOtherResources()
            throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RuntimeException failure = new RuntimeException("schema failed");
        doThrow(failure).when(reader).getSchema();

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsReader(
                                        inputFileAdapter,
                                        0,
                                        rowType(),
                                        rowType(),
                                        null,
                                        new Path("file:/tmp/mosaic-reader-test"),
                                        allocator,
                                        (inputFile, fileSize, bufferAllocator) -> reader))
                .isSameAs(failure);

        verify(reader).close();
        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testCloseContinuesWhenReaderCloseThrows() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createReader();
        RuntimeException failure = new RuntimeException("reader close failed");
        doThrow(failure).when(reader).close();

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);

        assertThatThrownBy(recordsReader::close).isSameAs(failure);

        verify(reader).close();
        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testCloseAddsSuppressedExceptionsFromLaterResources() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        RuntimeException allocatorFailure = new RuntimeException("allocator close failed");
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator(allocatorFailure);
        MosaicReader reader = createReader();
        RuntimeException readerFailure = new RuntimeException("reader close failed");
        doThrow(readerFailure).when(reader).close();

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);

        assertThatThrownBy(recordsReader::close)
                .isSameAs(readerFailure)
                .satisfies(t -> assertThat(t.getSuppressed()).containsExactly(allocatorFailure));

        verify(reader).close();
        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testAllProjectedColumnsMissingSkipsRowGroupRead() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createReader();
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(3);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);

        assertThat(readerBatchSize(recordsReader)).isEqualTo(3);
        verify(reader, never()).readRowGroup(anyInt(), any());

        recordsReader.close();
    }

    @Test
    void testDisabledPrefetchReadsRowGroupsOnDemand() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createProjectedReader(allocator, 3);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader, 0);

        FileRecordIterator<InternalRow> first = recordsReader.readBatch();
        assertThat(first).isNotNull();
        // Depth 0 must not read the next row group before the first batch is consumed.
        verify(reader, times(1)).readRowGroup(anyInt(), any());
        assertThat(first.next().getInt(0)).isEqualTo(0);
        assertThat(first.next()).isNull();
        first.releaseBatch();

        List<Integer> values = new ArrayList<>();
        FileRecordIterator<InternalRow> batch;
        while ((batch = recordsReader.readBatch()) != null) {
            InternalRow row;
            while ((row = batch.next()) != null) {
                values.add(row.getInt(0));
            }
            batch.releaseBatch();
        }
        assertThat(values).containsExactly(1, 2);
        verify(reader, times(3)).readRowGroup(anyInt(), any());

        recordsReader.close();
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testInterruptedReadKeepsInFlightRowGroupUntilClose() throws Exception {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createProjectedReader(allocator, 1);
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        doAnswer(
                        invocation -> {
                            readStarted.countDown();
                            releaseRead.await();
                            events.add("read-finished");
                            return rowGroup(allocator, 0);
                        })
                .when(reader)
                .readRowGroup(eq(0), any());
        doAnswer(
                        invocation -> {
                            events.add("reader-closed");
                            return null;
                        })
                .when(reader)
                .close();

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader, 2);
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        Thread consumer =
                new Thread(
                        () -> {
                            try {
                                recordsReader.readBatch();
                            } catch (Throwable t) {
                                readFailure.set(t);
                            }
                        });
        consumer.start();
        readStarted.await();
        consumer.interrupt();
        consumer.join();
        assertThat(readFailure.get()).isInstanceOf(InterruptedIOException.class);

        // The interrupted read is still running natively: close() has to wait for it.
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer = new Thread(() -> closeQuietly(recordsReader, closeFailure));
        closer.start();
        closer.join(200);
        assertThat(closer.isAlive()).isTrue();
        assertThat(events).isEmpty();
        releaseRead.countDown();
        closer.join();

        assertThat(closeFailure.get()).isNull();
        assertThat(events).containsExactly("read-finished", "reader-closed");
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testCloseWithInterruptFlagStillWaitsForInFlightRowGroups() throws Exception {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createProjectedReader(allocator, 2);
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        doAnswer(
                        invocation -> {
                            readStarted.countDown();
                            releaseRead.await();
                            events.add("read-finished");
                            return rowGroup(allocator, 1);
                        })
                .when(reader)
                .readRowGroup(eq(1), any());
        doAnswer(
                        invocation -> {
                            events.add("reader-closed");
                            return null;
                        })
                .when(reader)
                .close();

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader, 1);
        // Consuming row group 0 schedules row group 1, which now blocks in the background.
        assertThat(recordsReader.readBatch()).isNotNull();
        readStarted.await();

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        AtomicBoolean interruptedAfterClose = new AtomicBoolean();
        Thread closer =
                new Thread(
                        () -> {
                            Thread.currentThread().interrupt();
                            closeQuietly(recordsReader, closeFailure);
                            interruptedAfterClose.set(Thread.currentThread().isInterrupted());
                        });
        closer.start();
        closer.join(200);
        assertThat(closer.isAlive()).isTrue();
        assertThat(events).isEmpty();
        releaseRead.countDown();
        closer.join();

        assertThat(closeFailure.get()).isNull();
        assertThat(events).containsExactly("read-finished", "reader-closed");
        assertThat(interruptedAfterClose).isTrue();
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testRefillFailureLeavesCurrentRowGroupReleasable() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createProjectedReader(allocator, 3);
        RuntimeException failure = new RuntimeException("row group 2 metadata failed");
        when(reader.rowGroupNumRows(2)).thenThrow(failure);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader, 1);
        // Row group 0 is handed over; scheduling row group 2 fails while refilling behind it.
        assertThat(recordsReader.readBatch()).isNotNull();
        assertThatThrownBy(recordsReader::readBatch).isSameAs(failure);

        // Row group 0 must still be released, otherwise the allocator reports a leak here.
        recordsReader.close();
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testPrefetchIsBoundedByEstimatedDecodedBytes() throws IOException {
        // One INT column: 5 bytes per row; 1,000 rows per row group is 5,000 bytes.
        assertThat(MosaicRecordsReader.estimatedRowBytes(rowType())).isEqualTo(5);
        for (long budget : new long[] {4_000L, 100_000L}) {
            CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
            MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
            CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
            MosaicReader reader = createProjectedReader(allocator, 4);
            when(reader.rowGroupNumRows(anyInt())).thenReturn(1000);
            MosaicRecordsReader recordsReader =
                    new MosaicRecordsReader(
                            inputFileAdapter,
                            0,
                            rowType(),
                            rowType(),
                            null,
                            new Path("file:/tmp/mosaic-reader-test"),
                            allocator,
                            (inputFile, fileSize, bufferAllocator) -> reader,
                            8,
                            budget);
            assertThat(recordsReader.readBatch()).isNotNull();
            if (budget < 5_000L) {
                // Below one row group: nothing is read ahead of the batch being consumed.
                verify(reader, times(1)).readRowGroup(anyInt(), any());
            } else {
                // The three remaining row groups fit the budget and are read ahead.
                verify(reader, timeout(5_000).times(4)).readRowGroup(anyInt(), any());
            }
            recordsReader.close();
            assertThat(allocator.closeCount()).isEqualTo(1);
        }
    }

    private static void closeQuietly(
            MosaicRecordsReader recordsReader, AtomicReference<Throwable> failure) {
        try {
            recordsReader.close();
        } catch (Throwable t) {
            failure.set(t);
        }
    }

    /** A mocked native reader whose file schema contains the projected column. */
    private static MosaicReader createProjectedReader(BufferAllocator allocator, int numRowGroups) {
        MosaicReader reader = mock(MosaicReader.class);
        when(reader.getSchema())
                .thenReturn(
                        new Schema(
                                Collections.singletonList(
                                        Field.nullable("f0", new ArrowType.Int(32, true)))));
        when(reader.numRowGroups()).thenReturn(numRowGroups);
        when(reader.rowGroupNumRows(anyInt())).thenReturn(1);
        when(reader.readRowGroup(anyInt(), any()))
                .thenAnswer(invocation -> rowGroup(allocator, invocation.getArgument(0)));
        return reader;
    }

    private static VectorSchemaRoot rowGroup(BufferAllocator allocator, int value) {
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType(), allocator);
        IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(1);
        vector.set(0, value);
        vector.setValueCount(1);
        root.setRowCount(1);
        return root;
    }

    private static MosaicInputFileAdapter createInputFileAdapter(
            CloseCountingSeekableInputStream inputStream) throws IOException {
        return new MosaicInputFileAdapter(
                new CloseCountingFileIO(inputStream), new Path("file:/tmp/mosaic-reader-test"));
    }

    private static MosaicRecordsReader createRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            CloseCountingRootAllocator allocator,
            MosaicReader reader) {
        return new MosaicRecordsReader(
                inputFileAdapter,
                0,
                rowType(),
                rowType(),
                null,
                new Path("file:/tmp/mosaic-reader-test"),
                allocator,
                (inputFile, fileSize, bufferAllocator) -> reader);
    }

    private static MosaicRecordsReader createRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            CloseCountingRootAllocator allocator,
            MosaicReader reader,
            int prefetchRowGroups) {
        return new MosaicRecordsReader(
                inputFileAdapter,
                0,
                rowType(),
                rowType(),
                null,
                new Path("file:/tmp/mosaic-reader-test"),
                allocator,
                (inputFile, fileSize, bufferAllocator) -> reader,
                prefetchRowGroups,
                MosaicFileFormat.READ_PREFETCH_MAX_BYTES.defaultValue().getBytes());
    }

    private static MosaicReader createReader() {
        MosaicReader reader = mock(MosaicReader.class);
        when(reader.getSchema()).thenReturn(new Schema(Collections.emptyList()));
        return reader;
    }

    private static int readerBatchSize(MosaicRecordsReader recordsReader) throws IOException {
        int count = 0;
        while (true) {
            FileRecordIterator<InternalRow> batch = recordsReader.readBatch();
            if (batch == null) {
                return count;
            }
            InternalRow row;
            while ((row = batch.next()) != null) {
                assertThat(row.isNullAt(0)).isTrue();
                count++;
            }
            batch.releaseBatch();
        }
    }

    private static RowType rowType() {
        return DataTypes.ROW(DataTypes.INT());
    }

    private static class CloseCountingFileIO extends LocalFileIO {

        private final CloseCountingSeekableInputStream inputStream;

        private CloseCountingFileIO(CloseCountingSeekableInputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public SeekableInputStream newInputStream(Path path) {
            return inputStream;
        }
    }

    private static class CloseCountingSeekableInputStream extends SeekableInputStream {

        private int closeCount;

        @Override
        public void seek(long desired) {}

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            return -1;
        }

        @Override
        public int read() {
            return -1;
        }

        @Override
        public void close() {
            closeCount++;
        }

        int closeCount() {
            return closeCount;
        }
    }

    private static class CloseCountingRootAllocator extends RootAllocator {

        private final RuntimeException closeFailure;
        private int closeCount;

        private CloseCountingRootAllocator() {
            this(null);
        }

        private CloseCountingRootAllocator(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
            super.close();
        }

        int closeCount() {
            return closeCount;
        }
    }
}
