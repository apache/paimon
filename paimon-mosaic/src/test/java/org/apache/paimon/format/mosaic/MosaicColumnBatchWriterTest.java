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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.AllNullColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.VectorizedBundleRecords;
import org.apache.paimon.mosaic.MosaicWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Test for {@link MosaicRecordsWriter}. */
class MosaicColumnBatchWriterTest {

    @TempDir java.nio.file.Path tempDir;

    private static final FileFormatFactory.FormatContext FORMAT_CONTEXT =
            new FileFormatFactory.FormatContext(new Options(), 1024, 1024);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testArrayFallbackPreservesLogicalOffsets(boolean selected) throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.ARRAY(DataTypes.INT())).build();
        FileFormatFactory.FormatContext formatContext =
                new FileFormatFactory.FormatContext(new Options(), 1024, 2);
        List<List<List<Integer>>> snapshots = new ArrayList<>();
        MosaicWriter nativeWriter =
                capturingWriter(
                        root -> {
                            List<List<Integer>> values = new ArrayList<>();
                            for (InternalRow row :
                                    new ArrowBatchReader(rowType, true).readBatch(root)) {
                                values.add(row.isNullAt(0) ? null : toInts(row.getArray(0)));
                            }
                            snapshots.add(values);
                        });
        try (MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, formatContext)) {
            HeapIntVector elements = heapInts(-1, -2, -3, -4, 30, 31, 32, -5, 40);
            HeapArrayVector vector = new HeapArrayVector(4, elements);
            vector.setNullAt(0);
            vector.putOffsetLength(1, 1, 0);
            vector.putOffsetLength(2, 4, 3);
            vector.putOffsetLength(3, 8, 1);
            VectorizedColumnBatch batch = batch(4, vector);
            writer.writeBundle(
                    new VectorizedBundleRecords(
                            batch, selected ? new int[] {2, 0, 1, 3, 2} : null));
        }

        if (selected) {
            assertThat(snapshots)
                    .containsExactly(
                            Arrays.asList(Arrays.asList(30, 31, 32), null),
                            Arrays.asList(Collections.emptyList(), Collections.singletonList(40)),
                            Collections.singletonList(Arrays.asList(30, 31, 32)));
        } else {
            assertThat(snapshots)
                    .containsExactly(
                            Arrays.asList(null, Collections.emptyList()),
                            Arrays.asList(
                                    Arrays.asList(30, 31, 32), Collections.singletonList(40)));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMapFallbackPreservesLogicalOffsets(boolean selected) throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("m", DataTypes.MAP(DataTypes.INT(), DataTypes.INT()))
                        .build();
        FileFormatFactory.FormatContext formatContext =
                new FileFormatFactory.FormatContext(new Options(), 1024, 2);
        List<List<List<Integer>>> keySnapshots = new ArrayList<>();
        List<List<List<Integer>>> valueSnapshots = new ArrayList<>();
        MosaicWriter nativeWriter =
                capturingWriter(
                        root -> {
                            List<List<Integer>> keys = new ArrayList<>();
                            List<List<Integer>> values = new ArrayList<>();
                            for (InternalRow row :
                                    new ArrowBatchReader(rowType, true).readBatch(root)) {
                                if (row.isNullAt(0)) {
                                    keys.add(null);
                                    values.add(null);
                                } else {
                                    InternalMap map = row.getMap(0);
                                    keys.add(toInts(map.keyArray()));
                                    values.add(toInts(map.valueArray()));
                                }
                            }
                            keySnapshots.add(keys);
                            valueSnapshots.add(values);
                        });
        try (MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, formatContext)) {
            HeapMapVector vector =
                    new HeapMapVector(
                            4,
                            heapInts(-1, -2, -3, -4, 4, 5, 6, -5, 8),
                            heapInts(-10, -20, -30, -40, 40, 50, 60, -50, 80));
            vector.setNullAt(0);
            vector.putOffsetLength(1, 1, 0);
            vector.putOffsetLength(2, 4, 3);
            vector.putOffsetLength(3, 8, 1);
            VectorizedColumnBatch batch = batch(4, vector);
            writer.writeBundle(
                    new VectorizedBundleRecords(
                            batch, selected ? new int[] {2, 0, 1, 3, 2} : null));
        }

        if (selected) {
            assertThat(keySnapshots)
                    .containsExactly(
                            Arrays.asList(Arrays.asList(4, 5, 6), null),
                            Arrays.asList(Collections.emptyList(), Collections.singletonList(8)),
                            Collections.singletonList(Arrays.asList(4, 5, 6)));
            assertThat(valueSnapshots)
                    .containsExactly(
                            Arrays.asList(Arrays.asList(40, 50, 60), null),
                            Arrays.asList(Collections.emptyList(), Collections.singletonList(80)),
                            Collections.singletonList(Arrays.asList(40, 50, 60)));
        } else {
            assertThat(keySnapshots)
                    .containsExactly(
                            Arrays.asList(null, Collections.emptyList()),
                            Arrays.asList(Arrays.asList(4, 5, 6), Collections.singletonList(8)));
            assertThat(valueSnapshots)
                    .containsExactly(
                            Arrays.asList(null, Collections.emptyList()),
                            Arrays.asList(
                                    Arrays.asList(40, 50, 60), Collections.singletonList(80)));
        }
    }

    @Test
    void testVectorizedBundleRespectsWriteBatchMemory() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.STRING()).build();
        MemorySize memoryLimit = MemorySize.parse("64 kb");
        FileFormatFactory.FormatContext formatContext =
                new FileFormatFactory.FormatContext(new Options(), 1024, 1024, memoryLimit);
        List<Integer> batchRowCounts = new ArrayList<>();
        List<Long> batchBufferSizes = new ArrayList<>();
        MosaicWriter nativeWriter =
                capturingWriter(
                        root -> {
                            batchRowCounts.add(root.getRowCount());
                            batchBufferSizes.add(
                                    root.getFieldVectors().stream()
                                            .mapToLong(FieldVector::getBufferSize)
                                            .sum());
                        });
        try (MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, formatContext)) {
            int sourceRows = 128;
            byte[] value = new byte[1024];
            HeapBytesVector vector = new HeapBytesVector(sourceRows);
            for (int i = 0; i < sourceRows; i++) {
                vector.putByteArray(i, value, 0, value.length);
            }
            VectorizedColumnBatch batch = batch(sourceRows, vector);
            writer.writeBundle(new VectorizedBundleRecords(batch, null));
        }

        assertThat(batchRowCounts).hasSizeGreaterThan(1);
        assertThat(batchRowCounts).anyMatch(rows -> rows > 32);
        assertThat(batchRowCounts.stream().mapToInt(Integer::intValue).sum()).isEqualTo(128);
        assertThat(batchBufferSizes).allMatch(size -> size <= memoryLimit.getBytes());
    }

    @Test
    void testSkewedVectorizedBundleBoundsAllocationAndSourceReads() throws Exception {
        int rows = 1024;
        long[] readBytes = {0};
        HeapBytesVector values =
                new HeapBytesVector(rows) {
                    @Override
                    public Bytes getBytes(int row) {
                        readBytes[0] += length[row];
                        return super.getBytes(row);
                    }
                };
        byte[] large = new byte[65536];
        long payloadBytes = 0;
        for (int i = 0; i < rows; i++) {
            int length = i < 32 ? 1 : large.length;
            values.putByteArray(i, large, 0, length);
            payloadBytes += length;
        }
        VectorizedColumnBatch batch = batch(rows, values);
        List<Integer> lengths = new ArrayList<>();
        MosaicWriter nativeWriter =
                capturingWriter(
                        root -> {
                            VarCharVector column = (VarCharVector) root.getVector(0);
                            assertThat(column.getAllocator().getLimit()).isEqualTo(Long.MAX_VALUE);
                            for (int i = 0; i < root.getRowCount(); i++) {
                                lengths.add(column.get(i).length);
                            }
                        });
        HeapBytesVector smallValues = new HeapBytesVector(rows);
        for (int i = 0; i < rows; i++) {
            smallValues.putByteArray(i, large, 0, 1);
        }
        VectorizedColumnBatch smallBatch = batch(rows, smallValues);
        try (RootAllocator allocator = new RootAllocator()) {
            try (MosaicRecordsWriter writer =
                    createWriter(
                            RowType.builder().field("a", DataTypes.STRING()).build(),
                            allocator,
                            nativeWriter,
                            rows,
                            MemorySize.ofMebiBytes(1))) {
                for (int i = 0; i < 5; i++) {
                    writer.writeBundle(new VectorizedBundleRecords(smallBatch, null));
                }
                // Keep calls amortized: flushing every 32 rows would require 160 calls.
                verify(nativeWriter, atMost(32)).write(any(VectorSchemaRoot.class));
                writer.writeBundle(new VectorizedBundleRecords(batch, null));
                assertThat(allocator.getLimit()).isEqualTo(Long.MAX_VALUE);
            }
            assertThat(allocator.getPeakMemoryAllocation()).isLessThanOrEqualTo(4L * 1024 * 1024);
            assertThat(allocator.getAllocatedMemory()).isZero();
            assertThat(allocator.getLimit()).isEqualTo(Long.MAX_VALUE);
        }
        assertThat(readBytes[0]).isLessThan(payloadBytes * 5 / 4);
        assertThat(lengths)
                .containsExactlyElementsOf(
                        IntStream.concat(
                                        IntStream.range(0, rows * 5).map(i -> 1),
                                        IntStream.range(0, rows)
                                                .map(i -> i < 32 ? 1 : large.length))
                                .boxed()
                                .collect(Collectors.toList()));
    }

    @Test
    void testVectorizedBundleRetriesWhenSampleUnderestimatesMemory() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.STRING()).build();
        MemorySize memoryLimit = MemorySize.parse("64 kb");
        FileFormatFactory.FormatContext formatContext =
                new FileFormatFactory.FormatContext(new Options(), 1024, 1024, memoryLimit);
        List<Integer> writtenLengths = new ArrayList<>();
        List<Long> allocatedMemory = new ArrayList<>();
        MosaicWriter nativeWriter =
                capturingWriter(
                        root -> {
                            VarCharVector vector = (VarCharVector) root.getVector("a");
                            for (int i = 0; i < root.getRowCount(); i++) {
                                writtenLengths.add(vector.get(i).length);
                            }
                            allocatedMemory.add(vector.getAllocator().getAllocatedMemory());
                        });
        try (MosaicRecordsWriter writer = createWriter(rowType, nativeWriter, formatContext)) {
            int sourceRows = 64;
            HeapBytesVector vector = new HeapBytesVector(sourceRows);
            for (int i = 0; i < sourceRows; i++) {
                int length = i < 32 ? 1 : 4096;
                vector.putByteArray(i, new byte[length], 0, length);
            }
            VectorizedColumnBatch batch = batch(sourceRows, vector);
            writer.writeBundle(new VectorizedBundleRecords(batch, null));
        }

        assertThat(writtenLengths)
                .containsExactlyElementsOf(
                        IntStream.range(0, 64)
                                .map(i -> i < 32 ? 1 : 4096)
                                .boxed()
                                .collect(Collectors.toList()));
        assertThat(allocatedMemory).allMatch(size -> size <= 2 * memoryLimit.getBytes());
    }

    @Test
    void testAllocatorExhaustionRetriesSmallerSlicesWithoutLosingRows() throws Exception {
        int[] failedAllocations = {0};
        AllocationListener listener =
                new AllocationListener() {
                    @Override
                    public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
                        failedAllocations[0]++;
                        return false;
                    }
                };
        List<Integer> written = new ArrayList<>();
        MosaicWriter nativeWriter =
                capturingWriter(
                        root -> {
                            VarCharVector values = (VarCharVector) root.getVector(0);
                            for (int i = 0; i < root.getRowCount(); i++) {
                                written.add((int) values.get(i)[0]);
                            }
                        });
        HeapBytesVector values = new HeapBytesVector(96);
        for (int i = 0; i < 96; i++) {
            byte[] bytes = new byte[i < 32 ? 1 : 65536];
            bytes[0] = (byte) i;
            values.putByteArray(i, bytes, 0, bytes.length);
        }
        VectorizedColumnBatch batch = batch(96, values);
        try (RootAllocator allocator = new RootAllocator(listener, 512 * 1024)) {
            try (MosaicRecordsWriter writer =
                    createWriter(
                            RowType.builder().field("a", DataTypes.STRING()).build(),
                            nativeWriter,
                            new FileFormatFactory.FormatContext(
                                    new Options(), 1024, 1024, MemorySize.ofMebiBytes(1)),
                            allocator)) {
                writer.writeBundle(new VectorizedBundleRecords(batch, null));
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
        assertThat(failedAllocations[0]).isGreaterThan(0);
        assertThat(written)
                .containsExactlyElementsOf(
                        IntStream.range(0, 96).boxed().collect(Collectors.toList()));
    }

    @Test
    void testRowsLargerThanBatchMemoryUseOriginalAllocatorLimit() throws Exception {
        int rowBytes = 1024 * 1024;
        long allocatorLimit = 8L * 1024 * 1024;
        HeapBytesVector values = new HeapBytesVector(2);
        for (int i = 0; i < 2; i++) {
            byte[] bytes = new byte[rowBytes];
            bytes[0] = (byte) i;
            values.putByteArray(i, bytes, 0, bytes.length);
        }
        VectorizedColumnBatch batch = batch(2, values);
        List<Integer> written = new ArrayList<>();
        MosaicWriter nativeWriter =
                capturingWriter(
                        root -> {
                            VarCharVector column = (VarCharVector) root.getVector(0);
                            assertThat(column.getAllocator().getLimit()).isEqualTo(allocatorLimit);
                            assertThat(root.getRowCount()).isEqualTo(1);
                            byte[] bytes = column.get(0);
                            assertThat(bytes).hasSize(rowBytes);
                            written.add((int) bytes[0]);
                        });
        try (RootAllocator allocator = new RootAllocator(allocatorLimit)) {
            try (MosaicRecordsWriter writer =
                    createWriter(
                            RowType.builder().field("a", DataTypes.STRING()).build(),
                            allocator,
                            nativeWriter,
                            1024,
                            MemorySize.parse("64 kb"))) {
                writer.writeBundle(new VectorizedBundleRecords(batch, null));
                assertThat(allocator.getLimit()).isEqualTo(allocatorLimit);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
        assertThat(written).containsExactly(0, 1);
        verify(nativeWriter, times(2)).write(any(VectorSchemaRoot.class));
    }

    @Test
    void testSingleOversizedRowFailsWithoutWritingOrLeaking() throws Exception {
        HeapBytesVector values = new HeapBytesVector(1);
        values.putByteArray(0, new byte[1024 * 1024], 0, 1024 * 1024);
        VectorizedColumnBatch batch = batch(1, values);
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        try (RootAllocator allocator = new RootAllocator(128 * 1024)) {
            try (MosaicRecordsWriter writer =
                    createWriter(
                            RowType.builder().field("a", DataTypes.STRING()).build(),
                            nativeWriter,
                            new FileFormatFactory.FormatContext(new Options(), 1024, 1024),
                            allocator)) {
                assertThatThrownBy(
                                () -> writer.writeBundle(new VectorizedBundleRecords(batch, null)))
                        .isInstanceOf(OutOfMemoryException.class);
                assertThat(allocator.getLimit()).isEqualTo(128 * 1024);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
        verify(nativeWriter, never()).write(any(VectorSchemaRoot.class));
    }

    @Test
    void testNativeWriteFailureIsNotRetriedDuringClose() throws Exception {
        RowType rowType = RowType.builder().field("a", DataTypes.INT()).build();
        RuntimeException failure = new RuntimeException("native write failed");
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        doThrow(failure).when(nativeWriter).write(any(VectorSchemaRoot.class));
        MosaicRecordsWriter writer = createWriter(rowType, nativeWriter);

        VectorizedColumnBatch batch = batch(1, heapInts(1));

        assertThatThrownBy(() -> writer.writeBundle(new VectorizedBundleRecords(batch, null)))
                .isSameAs(failure);
        assertThatThrownBy(() -> writer.addElement(GenericRow.of(2)))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> writer.writeBundle(new VectorizedBundleRecords(batch, null)))
                .isInstanceOf(IllegalStateException.class);
        writer.close();

        verify(nativeWriter, times(1)).write(any(VectorSchemaRoot.class));
    }

    @Test
    void testEmptyBatchesDoNotFlushBufferedRows() throws Exception {
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        try (MosaicRecordsWriter writer =
                createWriter(
                        RowType.builder().field("a", DataTypes.INT().notNull()).build(),
                        nativeWriter)) {
            writer.addElement(GenericRow.of(1));
            VectorizedColumnBatch batch = batch(1, AllNullColumnVector.INSTANCE);
            writer.writeBundle(new VectorizedBundleRecords(batch, new int[0]));
            batch.setNumRows(0);
            writer.writeBundle(new VectorizedBundleRecords(batch, null));
            verify(nativeWriter, never()).write(any(VectorSchemaRoot.class));
        }
        verify(nativeWriter, times(1)).write(any(VectorSchemaRoot.class));
    }

    @Test
    void testConversionFailureDiscardsPartiallyWrittenColumns() throws Exception {
        RuntimeException failure = new RuntimeException("source column failed");
        HeapIntVector broken =
                new HeapIntVector(2) {
                    @Override
                    public int getInt(int row) {
                        throw failure;
                    }
                };
        VectorizedColumnBatch batch = batch(2, heapInts(1, 2), broken);
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        RootAllocator allocator = new RootAllocator();
        try (MosaicRecordsWriter writer =
                createWriter(
                        RowType.builder()
                                .field("a", DataTypes.INT())
                                .field("b", DataTypes.INT())
                                .build(),
                        nativeWriter,
                        allocator)) {
            assertThatThrownBy(() -> writer.writeBundle(new VectorizedBundleRecords(batch, null)))
                    .isSameAs(failure);
        }
        verify(nativeWriter, never()).write(any(VectorSchemaRoot.class));
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void testColumnWriteRejectsNullsInNotNullColumn(boolean selected, boolean allNull)
            throws Exception {
        RowType type = RowType.builder().field("id", DataTypes.INT().notNull()).build();
        HeapIntVector values = heapInts(7, 8, 9);
        values.setNullAt(1);
        VectorizedColumnBatch batch = batch(3, allNull ? AllNullColumnVector.INSTANCE : values);
        try (MosaicRecordsWriter writer =
                new MosaicRecordsWriter(
                        new ByteArrayOutputStream(),
                        type,
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1),
                        Collections.emptyList(),
                        null)) {
            assertThatThrownBy(
                            () ->
                                    writer.writeBundle(
                                            new VectorizedBundleRecords(
                                                    batch, selected ? new int[] {2, 1} : null)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Field 'id' expected not null but found null value");
            assertThatThrownBy(() -> writer.addElement(GenericRow.of(99)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Mosaic writer has failed");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testColumnWriteChecksNullsOnlyInRequestedRows(boolean selected) throws Exception {
        RowType type = RowType.builder().field("id", DataTypes.INT().notNull()).build();
        HeapIntVector values = heapInts(7, 8, 9, 10);
        values.setNullAt(selected ? 0 : 3);
        values.setNullAt(2);
        VectorizedColumnBatch batch = batch(selected ? 4 : 2, values);
        List<Integer> written = new ArrayList<>();
        MosaicWriter nativeWriter =
                capturingWriter(
                        root -> {
                            assertThat(root.getRowCount()).isEqualTo(1);
                            written.add(((IntVector) root.getVector("id")).get(0));
                        });
        try (MosaicRecordsWriter writer =
                createWriter(
                        type,
                        nativeWriter,
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1))) {
            writer.writeBundle(
                    new VectorizedBundleRecords(batch, selected ? new int[] {3, 1, 3} : null));
        }
        if (selected) {
            assertThat(written).containsExactly(10, 8, 10);
        } else {
            assertThat(written).containsExactly(7, 8);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNativeRoundTripWithMixedRowsAndColumnBatches(boolean selected) throws Exception {
        RowType type =
                RowType.builder()
                        .field("id", DataTypes.INT().notNull())
                        .field("s", DataTypes.STRING())
                        .build();
        FileFormatFactory.FormatContext context =
                new FileFormatFactory.FormatContext(new Options(), 1024, 2);
        MosaicFileFormat format = new MosaicFileFormat(context);
        LocalFileIO io = LocalFileIO.create();
        Path path = new Path(tempDir.resolve("batch.mosaic").toString());
        int[] selection = selected ? new int[] {2, 0, 2, 1, 3} : null;
        List<Integer> batchRowCounts = new ArrayList<>();
        try (MosaicRecordsWriter writer =
                new MosaicRecordsWriter(
                        io.newOutputStream(path, false),
                        type,
                        context,
                        Collections.emptyList(),
                        null,
                        new RootAllocator(),
                        (output, schema, options, allocator) ->
                                new MosaicWriter(output, schema, options, allocator) {
                                    @Override
                                    public void write(VectorSchemaRoot root) {
                                        batchRowCounts.add(root.getRowCount());
                                        super.write(root);
                                    }
                                })) {
            writer.addElement(GenericRow.of(0, BinaryString.fromString("before")));
            HeapBytesVector strings = new HeapBytesVector(4);
            strings.putByteArray(0, new byte[] {65}, 0, 1);
            strings.setNullAt(1);
            strings.putByteArray(2, new byte[] {67}, 0, 1);
            strings.putByteArray(3, new byte[] {68}, 0, 1);
            VectorizedColumnBatch batch = batch(4, heapInts(1, 2, 3, 4), strings);
            writer.writeBundle(
                    new VectorizedBundleRecords(batch, selection) {
                        @Override
                        public Iterator<InternalRow> iterator() {
                            throw new AssertionError("Scalar batches must bypass row iteration");
                        }
                    });
            writer.addElement(GenericRow.of(5, BinaryString.fromString("after")));
        }
        List<Integer> ids = new ArrayList<>();
        List<String> strings = new ArrayList<>();
        try (RecordReaderIterator<InternalRow> rows =
                new RecordReaderIterator<>(
                        format.createReaderFactory(type, type, null)
                                .createReader(
                                        new FormatReaderContext(
                                                io, path, io.getFileSize(path), null, null)))) {
            while (rows.hasNext()) {
                InternalRow row = rows.next();
                ids.add(row.getInt(0));
                strings.add(row.isNullAt(1) ? null : row.getString(1).toString());
            }
        }
        if (selected) {
            assertThat(batchRowCounts).containsExactly(1, 2, 2, 1, 1);
            assertThat(ids).containsExactly(0, 3, 1, 3, 2, 4, 5);
            assertThat(strings).containsExactly("before", "C", "A", "C", null, "D", "after");
        } else {
            assertThat(batchRowCounts).containsExactly(1, 2, 2, 1);
            assertThat(ids).containsExactly(0, 1, 2, 3, 4, 5);
            assertThat(strings).containsExactly("before", "A", null, "C", "D", "after");
        }
    }

    private static VectorizedColumnBatch batch(int rows, ColumnVector... columns) {
        VectorizedColumnBatch batch = new VectorizedColumnBatch(columns);
        batch.setNumRows(rows);
        return batch;
    }

    private static MosaicWriter capturingWriter(Consumer<VectorSchemaRoot> capture) {
        MosaicWriter writer = mock(MosaicWriter.class);
        doAnswer(
                        invocation -> {
                            capture.accept(invocation.getArgument(0));
                            return null;
                        })
                .when(writer)
                .write(any(VectorSchemaRoot.class));
        return writer;
    }

    private static MosaicRecordsWriter createWriter(RowType rowType, MosaicWriter nativeWriter) {
        return createWriter(rowType, nativeWriter, new RootAllocator());
    }

    private static MosaicRecordsWriter createWriter(
            RowType rowType, MosaicWriter nativeWriter, BufferAllocator allocator) {
        return createWriter(rowType, nativeWriter, FORMAT_CONTEXT, allocator);
    }

    private static MosaicRecordsWriter createWriter(
            RowType rowType,
            MosaicWriter nativeWriter,
            FileFormatFactory.FormatContext formatContext) {
        return createWriter(rowType, nativeWriter, formatContext, new RootAllocator());
    }

    private static MosaicRecordsWriter createWriter(
            RowType rowType,
            MosaicWriter nativeWriter,
            FileFormatFactory.FormatContext formatContext,
            BufferAllocator allocator) {
        return new MosaicRecordsWriter(
                new ByteArrayOutputStream(),
                rowType,
                formatContext,
                Collections.emptyList(),
                null,
                allocator,
                (outputStream, arrowSchema, options, bufferAllocator) -> nativeWriter);
    }

    private static HeapIntVector heapInts(int... values) {
        HeapIntVector vector = new HeapIntVector(values.length);
        for (int i = 0; i < values.length; i++) {
            vector.setInt(i, values[i]);
        }
        return vector;
    }

    private static List<Integer> toInts(InternalArray array) {
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            values.add(array.getInt(i));
        }
        return values;
    }

    private static MosaicRecordsWriter createWriter(
            RowType rowType,
            RootAllocator allocator,
            MosaicWriter nativeWriter,
            int writeBatchSize,
            MemorySize writeBatchMemory) {
        return createWriter(
                rowType,
                nativeWriter,
                new FileFormatFactory.FormatContext(
                        new Options(), 1024, writeBatchSize, writeBatchMemory),
                allocator);
    }
}
