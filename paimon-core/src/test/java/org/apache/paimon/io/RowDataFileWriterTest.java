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
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link RowDataFileWriter}. */
class RowDataFileWriterTest {

    private static final Path PATH = new Path("file:/tmp/data-file");
    private static final RowType ROW_TYPE = RowType.builder().field("id", DataTypes.INT()).build();

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

        assertThat(bundle.rowCountCalls).isEqualTo(1);
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

        assertThat(bundle.rowCountCalls).isEqualTo(1);
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
                        new FileIndexOptions());

        writer.writeBundle(rows(GenericRow.of(1), GenericRow.of(2)));

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

    private static FileIO fileIO() throws IOException {
        FileIO fileIO = mock(FileIO.class);
        when(fileIO.getFileSize(PATH)).thenReturn(123L);
        return fileIO;
    }

    private static RowDataFileWriter createWriter(
            FileIO fileIO,
            RowType rowType,
            FormatWriter formatWriter,
            SimpleStatsProducer statsProducer,
            LongCounter sequenceCounter,
            FileIndexOptions fileIndexOptions) {
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
                null,
                null);
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
            implements BundleFormatWriter {}

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
        private int rowCountCalls;

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
            rowCountCalls++;
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
