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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

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
    void testAllProjectedColumnsMissingPreservesSelectedPositionsAcrossRowGroups()
            throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = createReader();
        when(reader.numRowGroups()).thenReturn(3);
        when(reader.rowGroupNumRows(0)).thenReturn(2);
        when(reader.rowGroupNumRows(1)).thenReturn(2);
        when(reader.rowGroupNumRows(2)).thenReturn(2);

        Path filePath = new Path("file:/tmp/mosaic-reader-test");
        RoaringBitmap32 selection = RoaringBitmap32.bitmapOf(1, 5);
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType(),
                        rowType(),
                        null,
                        filePath,
                        selection,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        rowType(),
                        recordsReader,
                        false,
                        false,
                        null,
                        null,
                        null,
                        false,
                        null,
                        0,
                        Collections.emptyMap(),
                        selection,
                        filePath);

        FileRecordIterator<InternalRow> firstBatch = dataFileReader.readBatch();
        assertThat(firstBatch).isNotNull();
        InternalRow firstRow = firstBatch.next();
        assertThat(firstRow).isNotNull();
        assertThat(firstRow.isNullAt(0)).isTrue();
        assertThat(firstBatch.returnedPosition()).isEqualTo(1);
        assertThat(firstBatch.next()).isNull();
        firstBatch.releaseBatch();

        FileRecordIterator<InternalRow> secondBatch = dataFileReader.readBatch();
        assertThat(secondBatch).isNotNull();
        InternalRow secondRow = secondBatch.next();
        assertThat(secondRow).isNotNull();
        assertThat(secondRow.isNullAt(0)).isTrue();
        assertThat(secondBatch.returnedPosition()).isEqualTo(5);
        assertThat(secondBatch.next()).isNull();
        secondBatch.releaseBatch();

        assertThat(dataFileReader.readBatch()).isNull();
        verify(reader, never()).readRowGroup(anyInt(), any());

        dataFileReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @Test
    void testSelectionSkipsUnmatchedRowGroupsAndPreservesPositions() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot firstRoot = intRoot(allocator, 0, 1);
        VectorSchemaRoot thirdRoot = intRoot(allocator, 5, 6);
        VectorSchemaRoot fourthRoot = intRoot(allocator, 7, 8, 9);
        when(reader.getSchema()).thenReturn(firstRoot.getSchema());
        when(reader.numRowGroups()).thenReturn(4);
        when(reader.rowGroupNumRows(0)).thenReturn(2);
        when(reader.rowGroupNumRows(1)).thenReturn(3);
        when(reader.rowGroupNumRows(2)).thenReturn(2);
        when(reader.rowGroupNumRows(3)).thenReturn(3);
        when(reader.readRowGroup(0, allocator)).thenReturn(firstRoot);
        when(reader.readRowGroup(2, allocator)).thenReturn(thirdRoot);
        when(reader.readRowGroup(3, allocator)).thenReturn(fourthRoot);

        Path filePath = new Path("file:/tmp/mosaic-reader-test");
        RoaringBitmap32 selection = RoaringBitmap32.bitmapOf(1, 5, 9);
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType(),
                        rowType(),
                        null,
                        filePath,
                        selection,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);
        DataFileRecordReader dataFileReader =
                new DataFileRecordReader(
                        rowType(),
                        recordsReader,
                        false,
                        false,
                        null,
                        null,
                        null,
                        false,
                        null,
                        0,
                        Collections.emptyMap(),
                        selection,
                        filePath);

        assertSelectedRow(dataFileReader.readBatch(), 1, 1);
        assertSelectedRow(dataFileReader.readBatch(), 5, 5);
        assertSelectedRow(dataFileReader.readBatch(), 9, 9);
        assertThat(dataFileReader.readBatch()).isNull();

        verify(reader).readRowGroup(0, allocator);
        verify(reader, never()).readRowGroup(1, allocator);
        verify(reader).readRowGroup(2, allocator);
        verify(reader).readRowGroup(3, allocator);

        dataFileReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @Test
    void testRowGroupPositionAfterPartiallyConsumedBatch() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        VectorSchemaRoot secondRoot = intRoot(allocator, 3, 4, 5, 6);
        VectorSchemaRoot thirdRoot = intRoot(allocator, 7, 8);
        when(reader.getSchema()).thenReturn(secondRoot.getSchema());
        when(reader.numRowGroups()).thenReturn(3);
        when(reader.rowGroupNumRows(0)).thenReturn(3);
        when(reader.rowGroupNumRows(1)).thenReturn(4);
        when(reader.rowGroupNumRows(2)).thenReturn(2);
        when(reader.readRowGroup(1, allocator)).thenReturn(secondRoot);
        when(reader.readRowGroup(2, allocator)).thenReturn(thirdRoot);

        RoaringBitmap32 selection = RoaringBitmap32.bitmapOf(3, 7);
        MosaicRecordsReader recordsReader =
                new MosaicRecordsReader(
                        inputFileAdapter,
                        0,
                        rowType(),
                        rowType(),
                        null,
                        new Path("file:/tmp/mosaic-reader-test"),
                        selection,
                        allocator,
                        (inputFile, fileSize, bufferAllocator) -> reader);

        FileRecordIterator<InternalRow> secondBatch =
                recordsReader.readBatch().selection(RoaringBitmap32.bitmapOf(3));
        assertThat(secondBatch.next().getInt(0)).isEqualTo(3);
        assertThat(secondBatch.returnedPosition()).isEqualTo(3);
        assertThat(secondBatch.next()).isNull();
        secondBatch.releaseBatch();

        FileRecordIterator<InternalRow> thirdBatch =
                recordsReader.readBatch().selection(RoaringBitmap32.bitmapOf(7));
        assertThat(thirdBatch.next().getInt(0)).isEqualTo(7);
        assertThat(thirdBatch.returnedPosition()).isEqualTo(7);
        assertThat(thirdBatch.next()).isNull();
        thirdBatch.releaseBatch();
        assertThat(recordsReader.readBatch()).isNull();

        verify(reader, never()).readRowGroup(0, allocator);
        verify(reader).readRowGroup(1, allocator);
        verify(reader).readRowGroup(2, allocator);

        recordsReader.close();
        assertThat(allocator.getAllocatedMemory()).isZero();
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

    private static void assertSelectedRow(
            FileRecordIterator<InternalRow> batch, int value, long position) throws IOException {
        assertThat(batch).isNotNull();
        assertThat(batch.next().getInt(0)).isEqualTo(value);
        assertThat(batch.returnedPosition()).isEqualTo(position);
        assertThat(batch.next()).isNull();
        batch.releaseBatch();
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
}
