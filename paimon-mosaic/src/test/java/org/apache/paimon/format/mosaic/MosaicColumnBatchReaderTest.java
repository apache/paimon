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
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link MosaicRecordsReader}. */
class MosaicColumnBatchReaderTest {

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
    void testConstructorIOExceptionClosesCreatedResources() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        IOException failure = new IOException("native reader failed");

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
                .isInstanceOf(RuntimeException.class)
                .hasCause(failure);

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
    void testAllMissingBatchesTrackPositionsIndependently() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createReader();
        when(reader.numRowGroups()).thenReturn(2);
        when(reader.rowGroupNumRows(0)).thenReturn(2);
        when(reader.rowGroupNumRows(1)).thenReturn(2);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);
        FileRecordIterator<InternalRow> first = recordsReader.readBatch();
        FileRecordIterator<InternalRow> second = recordsReader.readBatch();

        second.next();
        assertThat(second.returnedPosition()).isEqualTo(2);
        first.next();
        assertThat(first.returnedPosition()).isZero();
        second.next();
        assertThat(second.returnedPosition()).isEqualTo(3);
        first.next();
        assertThat(first.returnedPosition()).isEqualTo(1);

        first.releaseBatch();
        second.releaseBatch();
        verify(reader, never()).readRowGroup(anyInt(), any());
        recordsReader.close();
    }

    @Test
    void testMosaicBatchExposesVectorizedRecordIterator() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        IntVector vector = new IntVector("f0", allocator);
        vector.allocateNew(3);
        vector.setSafe(0, 10);
        vector.setNull(1);
        vector.setSafe(2, 30);
        vector.setValueCount(3);
        VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vector));
        root.setRowCount(3);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(3);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);
        FileRecordIterator<InternalRow> records = recordsReader.readBatch();

        assertThat(records).isInstanceOf(VectorizedRecordIterator.class);
        assertThat(((VectorizedRecordIterator) records).batch().getNumRows()).isEqualTo(3);
        assertThat(records.next().getInt(0)).isEqualTo(10);
        assertThat(records.returnedPosition()).isZero();
        assertThat(records.next().isNullAt(0)).isTrue();
        assertThat(records.returnedPosition()).isEqualTo(1);
        records.releaseBatch();
        assertThat(allocator.getAllocatedMemory()).isZero();

        recordsReader.close();
    }

    @Test
    void testReadingNextBatchDoesNotInvalidateOutstandingBatch() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot firstRoot = intRoot(allocator, 10);
        VectorSchemaRoot secondRoot = intRoot(allocator, 20, 30);
        when(reader.getSchema()).thenReturn(firstRoot.getSchema());
        when(reader.numRowGroups()).thenReturn(2);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.rowGroupNumRows(1)).thenReturn(2);
        when(reader.readRowGroup(0, allocator)).thenReturn(firstRoot);
        when(reader.readRowGroup(1, allocator)).thenReturn(secondRoot);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);
        FileRecordIterator<InternalRow> first = recordsReader.readBatch();
        FileRecordIterator<InternalRow> second = recordsReader.readBatch();

        assertThat(first).isInstanceOf(VectorizedRecordIterator.class);
        assertThat(second).isInstanceOf(VectorizedRecordIterator.class);
        assertThat(((VectorizedRecordIterator) first).batch())
                .isNotSameAs(((VectorizedRecordIterator) second).batch());
        assertThat(((VectorizedRecordIterator) first).batch().getNumRows()).isEqualTo(1);
        assertThat(((VectorizedRecordIterator) second).batch().getNumRows()).isEqualTo(2);
        assertThat(first.next().getInt(0)).isEqualTo(10);
        assertThat(second.next().getInt(0)).isEqualTo(20);
        assertThat(second.next().getInt(0)).isEqualTo(30);
        assertThat(second.next()).isNull();
        assertThat(firstRoot.getVector(0).getObject(0)).isEqualTo(10);

        second.releaseBatch();
        assertThat(firstRoot.getVector(0).getObject(0)).isEqualTo(10);
        assertThat(allocator.getAllocatedMemory()).isGreaterThan(0);
        first.releaseBatch();
        assertThat(allocator.getAllocatedMemory()).isZero();
        recordsReader.close();
    }

    @Test
    void testCloseReleasesOutstandingBatch() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot root = intRoot(allocator, 10);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        MosaicRecordsReader recordsReader =
                createRecordsReader(inputFileAdapter, allocator, reader);
        FileRecordIterator<InternalRow> batch = recordsReader.readBatch();

        assertThat(allocator.getAllocatedMemory()).isGreaterThan(0);
        recordsReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
        batch.releaseBatch();
    }

    @Test
    void testCloseWaitsForConcurrentBatchRelease() throws Exception {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        BlockingRootAllocator allocator = new BlockingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot root = intRoot(allocator, 10);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType(),
                        rowType(),
                        null,
                        new Path("file:/tmp/mosaic-reader-test"),
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        FileRecordIterator<InternalRow> batch = recordsReader.readBatch();
        allocator.blockNextRelease();

        AtomicReference<Throwable> releaseFailure = new AtomicReference<>();
        Thread releaseThread =
                new Thread(
                        () -> {
                            try {
                                batch.releaseBatch();
                            } catch (Throwable t) {
                                releaseFailure.set(t);
                            }
                        });
        releaseThread.start();
        allocator.awaitReleaseStarted();

        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closeThread =
                new Thread(
                        () -> {
                            closeStarted.countDown();
                            try {
                                recordsReader.close();
                            } catch (Throwable t) {
                                closeFailure.set(t);
                            }
                        });
        closeThread.start();
        assertThat(closeStarted.await(5, TimeUnit.SECONDS)).isTrue();
        awaitBlocked(closeThread);
        assertThat(allocator.closeCount()).isZero();

        allocator.allowRelease();
        releaseThread.join(TimeUnit.SECONDS.toMillis(5));
        closeThread.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(releaseThread.isAlive()).isFalse();
        assertThat(closeThread.isAlive()).isFalse();
        assertThat(releaseFailure.get()).isNull();
        assertThat(closeFailure.get()).isNull();
        assertThat(allocator.getAllocatedMemory()).isZero();
        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testRowTrackingFallsBackFromColumnBatch() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field(SpecialFields.ROW_ID.name(), DataTypes.BIGINT())
                        .field(SpecialFields.SEQUENCE_NUMBER.name(), DataTypes.BIGINT())
                        .build();
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
        IntVector idVector = (IntVector) root.getVector(0);
        idVector.allocateNew(1);
        idVector.setSafe(0, 7);
        idVector.setValueCount(1);
        BigIntVector rowIdVector = (BigIntVector) root.getVector(1);
        rowIdVector.allocateNew(1);
        rowIdVector.setNull(0);
        rowIdVector.setValueCount(1);
        BigIntVector sequenceVector = (BigIntVector) root.getVector(2);
        sequenceVector.allocateNew(1);
        sequenceVector.setNull(0);
        sequenceVector.setValueCount(1);
        root.setRowCount(1);
        when(reader.getSchema()).thenReturn(root.getSchema());
        when(reader.numRowGroups()).thenReturn(1);
        when(reader.rowGroupNumRows(0)).thenReturn(1);
        when(reader.readRowGroup(0, allocator)).thenReturn(root);

        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType,
                        rowType,
                        null,
                        new Path("file:/tmp/mosaic-reader-test"),
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        Map<String, Integer> systemFields = new LinkedHashMap<>();
        systemFields.put(SpecialFields.ROW_ID.name(), 1);
        systemFields.put(SpecialFields.SEQUENCE_NUMBER.name(), 2);
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        rowType,
                        recordsReader,
                        false,
                        false,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        true,
                        1000L,
                        123L,
                        systemFields,
                        null,
                        new Path("file:/tmp/mosaic-reader-test"));
        FileRecordIterator<InternalRow> tracked = dataFileReader.readBatch();

        assertThat(tracked).isNotInstanceOf(VectorizedRecordIterator.class);
        InternalRow row = tracked.next();
        assertThat(row.getInt(0)).isEqualTo(7);
        assertThat(row.getLong(1)).isEqualTo(1000L);
        assertThat(row.getLong(2)).isEqualTo(123L);
        assertThat(root.getVector(1).isNull(0)).isTrue();
        assertThat(root.getVector(2).isNull(0)).isTrue();

        tracked.releaseBatch();
        dataFileReader.close();
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

    private static VectorSchemaRoot intRoot(RootAllocator allocator, int... values) {
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType(), allocator);
        IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            vector.setSafe(i, values[i]);
        }
        vector.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
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

    private static class BlockingRootAllocator extends CloseCountingRootAllocator {

        private final CountDownLatch releaseStarted = new CountDownLatch(1);
        private final CountDownLatch allowRelease = new CountDownLatch(1);
        private volatile boolean blockNextRelease;

        private void blockNextRelease() {
            blockNextRelease = true;
        }

        private void awaitReleaseStarted() throws InterruptedException {
            assertThat(releaseStarted.await(5, TimeUnit.SECONDS)).isTrue();
        }

        private void allowRelease() {
            allowRelease.countDown();
        }

        @Override
        public void releaseBytes(long size) {
            if (blockNextRelease) {
                blockNextRelease = false;
                releaseStarted.countDown();
                try {
                    assertThat(allowRelease.await(5, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            super.releaseBytes(size);
        }
    }

    private static void awaitBlocked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(thread.getState()).isEqualTo(Thread.State.BLOCKED);
    }
}
