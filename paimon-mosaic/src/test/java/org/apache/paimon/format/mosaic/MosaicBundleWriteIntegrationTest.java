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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileWriterContext;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.io.SimpleStatsProducer;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mosaic.MosaicWriter;
import org.apache.paimon.mosaic.WriterOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Integration tests for bundle dispatch from core file writers to Mosaic native writes. */
class MosaicBundleWriteIntegrationTest {

    private static final FileFormatFactory.FormatContext FORMAT_CONTEXT =
            new FileFormatFactory.FormatContext(new Options(), 1024, 1024);

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testCompatibleArrowBundleUsesFullDirectWritePath() throws Exception {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");

        RowType rowType = RowType.builder().field("value", DataTypes.INT()).build();
        Path path = newPath("direct");
        LocalFileIO fileIO = new LocalFileIO();
        RootAllocator writerAllocator = new RootAllocator();
        LongCounter sequenceCounter = new LongCounter(5);
        AtomicReference<TrackingMosaicWriter> nativeWriterRef = new AtomicReference<>();

        try (RowDataFileWriter writer =
                createWriter(
                        fileIO, path, rowType, writerAllocator, sequenceCounter, nativeWriterRef)) {
            TrackingMosaicWriter nativeWriter = nativeWriterRef.get();
            assertThat(nativeWriter).isNotNull();

            try (BufferAllocator sourceAllocator =
                            writerAllocator.newChildAllocator(
                                    "mosaic-direct-integration-test", 0, Long.MAX_VALUE);
                    VectorSchemaRoot root =
                            ArrowUtils.createVectorSchemaRoot(rowType, sourceAllocator)) {
                setInts((IntVector) root.getVector("value"), 1, 2, 3);
                root.setRowCount(3);

                TrackingDirectArrowBundleRecords bundle =
                        new TrackingDirectArrowBundleRecords(root, rowType);
                nativeWriter.expectDirectRoot(root);
                writer.writeBundle(bundle);
                nativeWriter.clearExpectedRoot();

                assertThat(bundle.iteratorCalls).isZero();
                assertThat(nativeWriter.directWrites).isEqualTo(1);
                assertThat(writer.recordCount()).isEqualTo(3);
                assertThat(sequenceCounter.getValue()).isEqualTo(8);
            }

            // The borrowed source root has already been released. Closing the native writer must
            // not access it again.
            writer.close();
            DataFileMeta result = writer.result();
            assertThat(result.rowCount()).isEqualTo(3);
            assertThat(result.minSequenceNumber()).isEqualTo(5);
            assertThat(result.maxSequenceNumber()).isEqualTo(7);
        }

        assertThat(readRows(fileIO, path, rowType))
                .containsExactly(
                        Collections.singletonList(1),
                        Collections.singletonList(2),
                        Collections.singletonList(3));
    }

    @Test
    void testIncompatibleArrowSchemaFallsBackThroughFullWritePath() throws Exception {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");

        RowType writerType =
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).build();
        RowType sourceType =
                RowType.builder().field("b", DataTypes.INT()).field("a", DataTypes.INT()).build();
        Path path = newPath("fallback");
        LocalFileIO fileIO = new LocalFileIO();
        RootAllocator writerAllocator = new RootAllocator();
        LongCounter sequenceCounter = new LongCounter();
        AtomicReference<TrackingMosaicWriter> nativeWriterRef = new AtomicReference<>();

        try (RowDataFileWriter writer =
                createWriter(
                        fileIO,
                        path,
                        writerType,
                        writerAllocator,
                        sequenceCounter,
                        nativeWriterRef)) {
            TrackingMosaicWriter nativeWriter = nativeWriterRef.get();
            assertThat(nativeWriter).isNotNull();

            try (BufferAllocator sourceAllocator =
                            writerAllocator.newChildAllocator(
                                    "mosaic-fallback-integration-test", 0, Long.MAX_VALUE);
                    VectorSchemaRoot root =
                            ArrowUtils.createVectorSchemaRoot(sourceType, sourceAllocator)) {
                setInts((IntVector) root.getVector("b"), 20, 21);
                setInts((IntVector) root.getVector("a"), 10, 11);
                root.setRowCount(2);

                TrackingArrowBundleRecords bundle =
                        new TrackingArrowBundleRecords(root, writerType);
                nativeWriter.expectDirectRoot(root);
                writer.writeBundle(bundle);
                nativeWriter.clearExpectedRoot();

                assertThat(bundle.iteratorCalls).isEqualTo(1);
                assertThat(nativeWriter.directWrites).isZero();
                assertThat(writer.recordCount()).isEqualTo(2);
                assertThat(sequenceCounter.getValue()).isEqualTo(2);
            }

            writer.close();
            assertThat(writer.result().rowCount()).isEqualTo(2);
        }

        assertThat(readRows(fileIO, path, writerType))
                .containsExactly(asList(10, 20), asList(11, 21));
    }

    private Path newPath(String prefix) {
        return new Path(tempDir.toUri().toString(), prefix + ".mosaic");
    }

    private static RowDataFileWriter createWriter(
            LocalFileIO fileIO,
            Path path,
            RowType rowType,
            RootAllocator allocator,
            LongCounter sequenceCounter,
            AtomicReference<TrackingMosaicWriter> nativeWriterRef) {
        FormatWriterFactory writerFactory =
                new FormatWriterFactory() {
                    @Override
                    public FormatWriter create(PositionOutputStream out, String compression) {
                        assertThat(compression).isEqualTo("zstd");
                        return new MosaicRecordsWriter(
                                out,
                                rowType,
                                FORMAT_CONTEXT,
                                Collections.emptyList(),
                                null,
                                allocator,
                                (outputStream, arrowSchema, options, bufferAllocator) -> {
                                    TrackingMosaicWriter writer =
                                            new TrackingMosaicWriter(
                                                    outputStream,
                                                    arrowSchema,
                                                    options,
                                                    bufferAllocator);
                                    nativeWriterRef.set(writer);
                                    return writer;
                                });
                    }
                };

        return new RowDataFileWriter(
                fileIO,
                new FileWriterContext(
                        writerFactory, SimpleStatsProducer.disabledProducer(), "zstd"),
                path,
                rowType,
                1L,
                () -> sequenceCounter,
                new FileIndexOptions(),
                FileSource.APPEND,
                false,
                false,
                false,
                null,
                null,
                null);
    }

    private static List<List<Integer>> readRows(LocalFileIO fileIO, Path path, RowType rowType)
            throws IOException {
        MosaicFileFormat format = new MosaicFileFormat(FORMAT_CONTEXT);
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, Collections.emptyList());
        List<List<Integer>> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)))) {
            reader.forEachRemaining(
                    row -> {
                        List<Integer> values = new ArrayList<>(rowType.getFieldCount());
                        for (int i = 0; i < rowType.getFieldCount(); i++) {
                            values.add(row.getInt(i));
                        }
                        rows.add(values);
                    });
        }
        return rows;
    }

    private static List<Integer> asList(int first, int second) {
        List<Integer> values = new ArrayList<>(2);
        values.add(first);
        values.add(second);
        return values;
    }

    private static void setInts(IntVector vector, int... values) {
        vector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            vector.setSafe(i, values[i]);
        }
        vector.setValueCount(values.length);
    }

    private static boolean isNativeAvailable() {
        try {
            Class.forName("org.apache.paimon.mosaic.NativeLib");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private static class TrackingDirectArrowBundleRecords extends ArrowBundleRecords {

        private int iteratorCalls;

        private TrackingDirectArrowBundleRecords(VectorSchemaRoot root, RowType rowType) {
            super(root, rowType, true);
        }

        @Override
        public Iterator<InternalRow> iterator() {
            iteratorCalls++;
            return super.iterator();
        }
    }

    private static class TrackingArrowBundleRecords extends ArrowBundleRecords {

        private int iteratorCalls;

        private TrackingArrowBundleRecords(VectorSchemaRoot root, RowType rowType) {
            super(root, rowType, true);
        }

        @Override
        public Iterator<InternalRow> iterator() {
            iteratorCalls++;
            return super.iterator();
        }
    }

    private static class TrackingMosaicWriter extends MosaicWriter {

        private VectorSchemaRoot expectedDirectRoot;
        private int directWrites;

        private TrackingMosaicWriter(
                OutputStream outputStream,
                Schema schema,
                WriterOptions options,
                BufferAllocator allocator) {
            super(outputStream, schema, options, allocator);
        }

        private void expectDirectRoot(VectorSchemaRoot root) {
            expectedDirectRoot = root;
        }

        private void clearExpectedRoot() {
            expectedDirectRoot = null;
        }

        @Override
        public void write(VectorSchemaRoot root) {
            if (root == expectedDirectRoot) {
                directWrites++;
            }
            super.write(root);
        }
    }
}
