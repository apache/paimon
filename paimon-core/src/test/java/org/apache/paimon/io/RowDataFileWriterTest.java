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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link RowDataFileWriter}. */
class RowDataFileWriterTest {

    private static final Path PATH = new Path("file:/tmp/data-file");
    private static final RowType ROW_TYPE = RowType.builder().field("id", DataTypes.INT()).build();

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testEligibleBundleIsForwardedWithoutIteration() throws Exception {
        FileIO fileIO = fileIO();
        TestingBundleFormatWriter formatWriter = new TestingBundleFormatWriter();
        LongCounter sequenceCounter = new LongCounter(5);
        RowDataFileWriter writer =
                createWriter(
                        fileIO,
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        sequenceCounter,
                        new FileIndexOptions());
        TrackingBundleRecords bundle =
                trackingRows(GenericRow.of(1), GenericRow.of(2), GenericRow.of(3));

        writer.writeBundle(bundle);

        assertThat(bundle.iteratorCalls).isZero();
        assertThat(formatWriter.writtenBundle).isSameAs(bundle);
        assertThat(formatWriter.bundleWrites).isEqualTo(1);
        assertThat(formatWriter.rowWrites).isZero();
        assertThat(writer.recordCount()).isEqualTo(3);
        assertThat(sequenceCounter.getValue()).isEqualTo(8);

        writer.close();
        DataFileMeta result = writer.result();
        assertThat(result.rowCount()).isEqualTo(3);
        assertThat(result.minSequenceNumber()).isEqualTo(5);
        assertThat(result.maxSequenceNumber()).isEqualTo(7);
    }

    @Test
    void testExtractorStatsAllowBundleForwarding() throws Exception {
        TestingBundleFormatWriter formatWriter = new TestingBundleFormatWriter();
        TestingExtractStatsProducer statsProducer = new TestingExtractStatsProducer();
        LongCounter sequenceCounter = new LongCounter(5);
        RowDataFileWriter writer =
                createWriter(
                        fileIO(),
                        ROW_TYPE,
                        formatWriter,
                        statsProducer,
                        sequenceCounter,
                        new FileIndexOptions());
        TrackingBundleRecords bundle =
                trackingRows(GenericRow.of(1), GenericRow.of(2), GenericRow.of(3));

        writer.writeBundle(bundle);

        assertThat(bundle.iteratorCalls).isZero();
        assertThat(formatWriter.writtenBundle).isSameAs(bundle);
        assertThat(formatWriter.bundleWrites).isEqualTo(1);
        assertThat(formatWriter.rowWrites).isZero();
        assertThat(writer.recordCount()).isEqualTo(3);
        assertThat(sequenceCounter.getValue()).isEqualTo(8);

        writer.close();
        DataFileMeta result = writer.result();
        assertThat(statsProducer.extractCalls).isEqualTo(1);
        assertThat(result.valueStats().minValues().getInt(0)).isEqualTo(1);
        assertThat(result.valueStats().maxValues().getInt(0)).isEqualTo(3);
        assertThat(result.valueStats().nullCounts().getLong(0)).isZero();
    }

    @Test
    void testPlainFormatWriterFallsBackToRows() throws Exception {
        TestingFormatWriter formatWriter = new TestingFormatWriter();
        LongCounter sequenceCounter = new LongCounter();
        RowDataFileWriter writer =
                createWriter(
                        fileIO(),
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        sequenceCounter,
                        new FileIndexOptions());

        writer.writeBundle(rows(GenericRow.of(1), GenericRow.of(2)));

        assertThat(formatWriter.rowWrites).isEqualTo(2);
        assertThat(writer.recordCount()).isEqualTo(2);
        assertThat(sequenceCounter.getValue()).isEqualTo(2);
    }

    @Test
    void testBundleFormatWriterCanChooseRowFallback() throws Exception {
        TestingFallbackBundleFormatWriter formatWriter = new TestingFallbackBundleFormatWriter();
        LongCounter sequenceCounter = new LongCounter();
        RowDataFileWriter writer =
                createWriter(
                        fileIO(),
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        sequenceCounter,
                        new FileIndexOptions());

        writer.writeBundle(rows(GenericRow.of(1), GenericRow.of(2)));

        assertThat(formatWriter.bundleWrites).isEqualTo(1);
        assertThat(formatWriter.rowWrites).isEqualTo(2);
        assertThat(writer.recordCount()).isEqualTo(2);
        assertThat(sequenceCounter.getValue()).isEqualTo(2);
    }

    @Test
    void testNegativeBundleRowCountIsRejectedBeforeWriting() throws Exception {
        FileIO fileIO = fileIO();
        TestingBundleFormatWriter formatWriter = new TestingBundleFormatWriter();
        LongCounter sequenceCounter = new LongCounter();
        RowDataFileWriter writer =
                createWriter(
                        fileIO,
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        sequenceCounter,
                        new FileIndexOptions());

        assertThatThrownBy(() -> writer.writeBundle(new InvalidRowCountBundleRecords(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row count must not be negative.");

        assertThat(formatWriter.bundleWrites).isZero();
        assertThat(formatWriter.closeCalls).isEqualTo(1);
        assertThat(writer.recordCount()).isZero();
        assertThat(sequenceCounter.getValue()).isZero();
        verify(fileIO).deleteQuietly(PATH);
    }

    @Test
    void testBundleWriteFailureCleansUpWithoutAdvancingMetadata() throws Exception {
        FileIO fileIO = fileIO();
        IOException failure = new IOException("bundle write failed");
        TestingThrowingBundleFormatWriter formatWriter =
                new TestingThrowingBundleFormatWriter(failure);
        LongCounter sequenceCounter = new LongCounter(5);
        RowDataFileWriter writer =
                createWriter(
                        fileIO,
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        sequenceCounter,
                        new FileIndexOptions());

        assertThatThrownBy(() -> writer.writeBundle(rows(GenericRow.of(1)))).isSameAs(failure);

        assertThat(formatWriter.closeCalls).isEqualTo(1);
        assertThat(writer.recordCount()).isZero();
        assertThat(sequenceCounter.getValue()).isEqualTo(5);
        verify(fileIO).deleteQuietly(PATH);
    }

    @Test
    void testPerRecordStatsFallBackToRows() throws Exception {
        TestingBundleFormatWriter formatWriter = new TestingBundleFormatWriter();
        TestingStatsProducer statsProducer = new TestingStatsProducer();
        LongCounter sequenceCounter = new LongCounter();
        RowDataFileWriter writer =
                createWriter(
                        fileIO(),
                        ROW_TYPE,
                        formatWriter,
                        statsProducer,
                        sequenceCounter,
                        bitmapOptions());

        writer.writeBundle(vectorizedRows(1, 2));

        assertThat(formatWriter.bundleWrites).isZero();
        assertThat(formatWriter.rowWrites).isEqualTo(2);
        assertThat(statsProducer.collectedRows).isEqualTo(2);
        assertThat(sequenceCounter.getValue()).isEqualTo(2);
    }

    @Test
    void testRowTrackingFallsBackToRows() throws Exception {
        RowType rowTrackingType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field(SpecialFields.SEQUENCE_NUMBER.name(), DataTypes.BIGINT())
                        .build();
        TestingBundleFormatWriter formatWriter = new TestingBundleFormatWriter();
        LongCounter sequenceCounter = new LongCounter(20);
        RowDataFileWriter writer =
                createWriter(
                        fileIO(),
                        rowTrackingType,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        sequenceCounter,
                        new FileIndexOptions());

        writer.writeBundle(rows(GenericRow.of(1, 7L), GenericRow.of(2, 11L)));

        assertThat(formatWriter.bundleWrites).isZero();
        assertThat(formatWriter.rowWrites).isEqualTo(2);
        assertThat(sequenceCounter.getValue()).isEqualTo(22);

        writer.close();
        DataFileMeta result = writer.result();
        assertThat(result.minSequenceNumber()).isEqualTo(7);
        assertThat(result.maxSequenceNumber()).isEqualTo(11);
    }

    @Test
    void testFileIndexFallsBackToRows() throws Exception {
        Options options = new Options();
        options.set("file-index.bitmap.columns", "id");
        FileIndexOptions fileIndexOptions = new FileIndexOptions(new CoreOptions(options));
        TestingBundleFormatWriter formatWriter = new TestingBundleFormatWriter();
        RowDataFileWriter writer =
                createWriter(
                        fileIO(),
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        new LongCounter(),
                        fileIndexOptions);

        writer.writeBundle(rows(GenericRow.of(1), GenericRow.of(2)));

        assertThat(formatWriter.bundleWrites).isZero();
        assertThat(formatWriter.rowWrites).isEqualTo(2);

        writer.close();
        assertThat(writer.result().embeddedIndex()).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testVectorizedBundlesPreserveBitmapPositions(boolean externalIndex) throws Exception {
        Options options = new Options();
        options.set("file-index.bitmap.columns", "id");
        options.set("file-index.in-manifest-threshold", externalIndex ? "0B" : "1MB");
        FileIndexOptions indexOptions = new FileIndexOptions(new CoreOptions(options));
        LongCounter sequenceCounter = new LongCounter(5);
        List<TestingBundleFormatWriter> formatWriters = new ArrayList<>();
        RollingFileWriterImpl<InternalRow, DataFileMeta> writer =
                new RollingFileWriterImpl<>(
                        () -> {
                            TestingBundleFormatWriter formatWriter =
                                    new TestingBundleFormatWriter();
                            formatWriters.add(formatWriter);
                            Path path =
                                    new Path(
                                            tempDir.resolve("data-" + formatWriters.size())
                                                    .toUri());
                            return createIndexedWriter(
                                    path, formatWriter, sequenceCounter, indexOptions);
                        },
                        Long.MAX_VALUE,
                        7L);
        // The first file mixes row writes, selected batches and an empty batch.
        writer.write(GenericRow.of(1));
        VectorizedBundleRecords source = vectorizedRows(1, null, 2, 1);
        writer.writeBundle(new VectorizedBundleRecords(source.batch(), new int[] {2, 0, 1, 2}));
        writer.writeBundle(vectorizedRows());
        writer.writeBundle(vectorizedRows(2, 1));
        // Index positions must restart at zero after rolling to the second file.
        writer.writeBundle(vectorizedRows(2, 1));
        writer.close();

        assertThat(formatWriters).extracting(w -> w.bundleWrites).containsExactly(3, 1);
        assertThat(formatWriters).extracting(w -> w.rowWrites).containsExactly(1, 0);
        assertThat(writer.recordCount()).isEqualTo(9);
        assertThat(sequenceCounter.getValue()).isEqualTo(14);
        List<DataFileMeta> files = writer.result();
        assertThat(files).extracting(DataFileMeta::rowCount).containsExactly(7L, 2L);
        assertThat(files).extracting(DataFileMeta::minSequenceNumber).containsExactly(5L, 12L);
        assertThat(files).extracting(DataFileMeta::maxSequenceNumber).containsExactly(11L, 13L);
        try (FileIndexFormat.Reader reader = readFileIndex(files.get(0), externalIndex)) {
            FileIndexReader index = reader.readColumnIndex("id").iterator().next();
            assertBitmapPositions(index, 1, 0, 2, 6);
            assertBitmapPositions(index, 2, 1, 4, 5);
            assertBitmapPositions(index, null, 3);
        }
        try (FileIndexFormat.Reader reader = readFileIndex(files.get(1), externalIndex)) {
            FileIndexReader index = reader.readColumnIndex("id").iterator().next();
            assertBitmapPositions(index, 1, 1);
            assertBitmapPositions(index, 2, 0);
            assertBitmapPositions(index, null);
        }
    }

    @Test
    void testIndexedVectorizedBundleAllowsFormatRowFallback() throws Exception {
        TestingFallbackBundleFormatWriter formatWriter = new TestingFallbackBundleFormatWriter();
        RowDataFileWriter writer =
                createWriter(
                        fileIO(),
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        new LongCounter(),
                        bitmapOptions());
        writer.writeBundle(vectorizedRows(1, 2));
        assertThat(formatWriter.bundleWrites).isEqualTo(1);
        assertThat(formatWriter.rowWrites).isEqualTo(2);
        writer.close();
        assertThat(writer.result().embeddedIndex()).isNotNull();
    }

    @Test
    void testIndexFailureAbortsBeforeWritingBatch() throws Exception {
        FileIO fileIO = fileIO();
        TestingBundleFormatWriter formatWriter = new TestingBundleFormatWriter();
        LongCounter sequenceCounter = new LongCounter(5);
        RowDataFileWriter writer =
                createWriter(
                        fileIO,
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        sequenceCounter,
                        bitmapOptions());
        RuntimeException failure = new RuntimeException("index field access failed");
        HeapIntVector vector =
                new HeapIntVector(2) {
                    @Override
                    public int getInt(int rowId) {
                        if (rowId == 1) {
                            throw failure;
                        }
                        return 1;
                    }
                };
        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
        batch.setNumRows(2);
        assertThatThrownBy(() -> writer.writeBundle(new VectorizedBundleRecords(batch, null)))
                .isSameAs(failure);
        assertThat(formatWriter.bundleWrites).isZero();
        assertThat(formatWriter.closeCalls).isEqualTo(1);
        assertThat(writer.recordCount()).isZero();
        assertThat(sequenceCounter.getValue()).isEqualTo(5);
        verify(fileIO).deleteQuietly(PATH);
        verify(fileIO, atLeastOnce()).deleteQuietly(dataFileToFileIndexPath(PATH));
    }

    @Test
    void testClosedWriterRejectsBundleBeforeReadingIndexFields() throws Exception {
        FileIO fileIO = fileIO();
        RowDataFileWriter writer =
                createWriter(
                        fileIO,
                        ROW_TYPE,
                        new TestingBundleFormatWriter(),
                        SimpleStatsProducer.disabledProducer(),
                        new LongCounter(),
                        bitmapOptions());
        writer.close();
        VectorizedBundleRecords bundle =
                new VectorizedBundleRecords(vectorizedRows(1).batch(), null) {
                    @Override
                    public Iterator<InternalRow> iterator() {
                        throw new AssertionError("Closed writer must not consume the index batch");
                    }
                };
        assertThatThrownBy(() -> writer.writeBundle(bundle))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Writer has already closed!");
        verify(fileIO, never()).deleteQuietly(PATH);
        verify(fileIO, never()).deleteQuietly(dataFileToFileIndexPath(PATH));
    }

    @Test
    void testBundleFailureAlsoAbortsPreparedIndex() throws Exception {
        FileIO fileIO = fileIO();
        IOException failure = new IOException("bundle write failed");
        TestingThrowingBundleFormatWriter formatWriter =
                new TestingThrowingBundleFormatWriter(failure);
        LongCounter sequenceCounter = new LongCounter(5);
        RowDataFileWriter writer =
                createWriter(
                        fileIO,
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        sequenceCounter,
                        bitmapOptions());
        assertThatThrownBy(() -> writer.writeBundle(vectorizedRows(1, 2))).isSameAs(failure);
        assertThat(formatWriter.closeCalls).isEqualTo(1);
        assertThat(writer.recordCount()).isZero();
        assertThat(sequenceCounter.getValue()).isEqualTo(5);
        verify(fileIO).deleteQuietly(PATH);
        verify(fileIO, atLeastOnce()).deleteQuietly(dataFileToFileIndexPath(PATH));
    }

    @Test
    void testRowSidecarFallsBackToRows() throws Exception {
        FileIO fileIO = fileIO();
        TestingBundleFormatWriter formatWriter = new TestingBundleFormatWriter();
        TestingFormatWriter sidecarWriter = new TestingFormatWriter();
        FileFormat rowSidecarFormat = mock(FileFormat.class);
        when(rowSidecarFormat.createWriterFactory(ROW_TYPE))
                .thenReturn(new TestingFormatWriterFactory(sidecarWriter));
        Path rowSidecarPath = new Path("file:/tmp/data-file.row");
        RowDataFileWriter writer =
                createWriter(
                        fileIO,
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        new LongCounter(),
                        bitmapOptions(),
                        rowSidecarFormat,
                        rowSidecarPath);

        writer.writeBundle(vectorizedRows(1, 2));

        assertThat(formatWriter.bundleWrites).isZero();
        assertThat(formatWriter.rowWrites).isEqualTo(2);
        assertThat(sidecarWriter.rowWrites).isEqualTo(2);

        writer.close();
        assertThat(writer.result().extraFiles()).containsExactly(rowSidecarPath.getName());
    }

    private static FileIO fileIO() throws IOException {
        FileIO fileIO = mock(FileIO.class);
        when(fileIO.getFileSize(PATH)).thenReturn(123L);
        return fileIO;
    }

    private static RowDataFileWriter createIndexedWriter(
            Path path,
            FormatWriter formatWriter,
            LongCounter sequenceCounter,
            FileIndexOptions indexOptions) {
        return new RowDataFileWriter(
                LocalFileIO.create(),
                new FileWriterContext(
                        (out, compression) -> formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        "none"),
                path,
                ROW_TYPE,
                1L,
                () -> sequenceCounter,
                indexOptions,
                FileSource.APPEND,
                false,
                false,
                false,
                null,
                null,
                null);
    }

    private FileIndexFormat.Reader readFileIndex(DataFileMeta file, boolean externalIndex)
            throws IOException {
        if (externalIndex) {
            assertThat(file.embeddedIndex()).isNull();
            assertThat(file.extraFiles()).hasSize(1);
            return FileIndexFormat.createReader(
                    LocalFileIO.create()
                            .newInputStream(
                                    new Path(tempDir.resolve(file.extraFiles().get(0)).toUri())),
                    ROW_TYPE);
        }
        assertThat(file.extraFiles()).isEmpty();
        assertThat(file.embeddedIndex()).isNotNull();
        return FileIndexFormat.createReader(
                new ByteArraySeekableStream(file.embeddedIndex()), ROW_TYPE);
    }

    private static void assertBitmapPositions(
            FileIndexReader index, Integer value, int... positions) {
        FieldRef field = new FieldRef(0, "id", DataTypes.INT());
        BitmapIndexResult result =
                (BitmapIndexResult)
                        (value == null ? index.visitIsNull(field) : index.visitEqual(field, value));
        assertThat(result.get()).isEqualTo(RoaringBitmap32.bitmapOf(positions));
    }

    private static FileIndexOptions bitmapOptions() {
        Options options = new Options();
        options.set("file-index.bitmap.columns", "id");
        return new FileIndexOptions(new CoreOptions(options));
    }

    private static VectorizedBundleRecords vectorizedRows(Integer... values) {
        HeapIntVector vector = new HeapIntVector(values.length);
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                vector.setNullAt(i);
            } else {
                vector.setInt(i, values[i]);
            }
        }
        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
        batch.setNumRows(values.length);
        return new VectorizedBundleRecords(batch, null);
    }

    private static RowDataFileWriter createWriter(
            FileIO fileIO,
            RowType rowType,
            FormatWriter formatWriter,
            SimpleStatsProducer statsProducer,
            LongCounter sequenceCounter,
            FileIndexOptions fileIndexOptions) {
        return createWriter(
                fileIO,
                rowType,
                formatWriter,
                statsProducer,
                sequenceCounter,
                fileIndexOptions,
                null,
                null);
    }

    private static RowDataFileWriter createWriter(
            FileIO fileIO,
            RowType rowType,
            FormatWriter formatWriter,
            SimpleStatsProducer statsProducer,
            LongCounter sequenceCounter,
            FileIndexOptions fileIndexOptions,
            FileFormat rowSidecarFormat,
            Path rowSidecarPath) {
        return new RowDataFileWriter(
                fileIO,
                new FileWriterContext(
                        new TestingFormatWriterFactory(formatWriter), statsProducer, "none"),
                PATH,
                rowType,
                1L,
                () -> sequenceCounter,
                fileIndexOptions,
                FileSource.APPEND,
                false,
                false,
                false,
                null,
                rowSidecarFormat,
                rowSidecarPath);
    }

    private static BundleRecords rows(InternalRow... rows) {
        return new ListBundleRecords(Arrays.asList(rows));
    }

    private static TrackingBundleRecords trackingRows(InternalRow... rows) {
        return new TrackingBundleRecords(Arrays.asList(rows));
    }

    private static class TestingFormatWriterFactory
            implements FormatWriterFactory, SupportsDirectWrite {

        private final FormatWriter writer;

        private TestingFormatWriterFactory(FormatWriter writer) {
            this.writer = writer;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression) {
            return writer;
        }

        @Override
        public FormatWriter create(FileIO fileIO, Path path, String compression) {
            return writer;
        }
    }

    private static class TestingFormatWriter implements FormatWriter {

        int rowWrites;
        int closeCalls;

        @Override
        public void addElement(InternalRow element) {
            rowWrites++;
        }

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
            return false;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }

    private static class TestingBundleFormatWriter extends TestingFormatWriter
            implements BundleFormatWriter {

        private int bundleWrites;
        private BundleRecords writtenBundle;

        @Override
        public void writeBundle(BundleRecords bundle) {
            bundleWrites++;
            writtenBundle = bundle;
        }
    }

    private static class TestingFallbackBundleFormatWriter extends TestingFormatWriter
            implements BundleFormatWriter {

        private int bundleWrites;

        @Override
        public void writeBundle(BundleRecords bundle) {
            bundleWrites++;
            for (InternalRow row : bundle) {
                addElement(row);
            }
        }
    }

    private static class TestingThrowingBundleFormatWriter extends TestingFormatWriter
            implements BundleFormatWriter {

        private final IOException failure;

        private TestingThrowingBundleFormatWriter(IOException failure) {
            this.failure = failure;
        }

        @Override
        public void writeBundle(BundleRecords bundle) throws IOException {
            throw failure;
        }
    }

    private static class TestingStatsProducer implements SimpleStatsProducer {

        private int collectedRows;

        @Override
        public boolean isStatsDisabled() {
            return false;
        }

        @Override
        public boolean requirePerRecord() {
            return true;
        }

        @Override
        public void collect(InternalRow row) {
            collectedRows++;
        }

        @Override
        public SimpleColStats[] extract(FileIO fileIO, Path path, long length) {
            return new SimpleColStats[] {SimpleColStats.NONE};
        }
    }

    private static class TestingExtractStatsProducer implements SimpleStatsProducer {

        private int extractCalls;

        @Override
        public boolean isStatsDisabled() {
            return false;
        }

        @Override
        public boolean requirePerRecord() {
            return false;
        }

        @Override
        public void collect(InternalRow row) {
            throw new AssertionError("Extractor-backed statistics must not collect rows.");
        }

        @Override
        public SimpleColStats[] extract(FileIO fileIO, Path path, long length) {
            extractCalls++;
            return new SimpleColStats[] {new SimpleColStats(1, 3, 0L)};
        }
    }

    private static class InvalidRowCountBundleRecords implements BundleRecords {

        private final long rowCount;

        private InvalidRowCountBundleRecords(long rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public Iterator<InternalRow> iterator() {
            throw new AssertionError("Invalid row count must be rejected before row iteration.");
        }

        @Override
        public long rowCount() {
            return rowCount;
        }
    }

    private static class TrackingBundleRecords implements BundleRecords {

        private final List<InternalRow> rows;
        private int iteratorCalls;

        private TrackingBundleRecords(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        public Iterator<InternalRow> iterator() {
            iteratorCalls++;
            return rows.iterator();
        }

        @Override
        public long rowCount() {
            return rows.size();
        }
    }

    private static class ListBundleRecords implements BundleRecords {

        private final List<InternalRow> rows;

        private ListBundleRecords(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        public Iterator<InternalRow> iterator() {
            return rows.iterator();
        }

        @Override
        public long rowCount() {
            return rows.size();
        }
    }
}
