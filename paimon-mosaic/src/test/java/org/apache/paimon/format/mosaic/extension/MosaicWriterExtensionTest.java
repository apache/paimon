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

package org.apache.paimon.format.mosaic.extension;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.mosaic.MosaicFileFormat;
import org.apache.paimon.format.mosaic.MosaicRecordsWriter;
import org.apache.paimon.format.mosaic.MosaicWriterFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Verifies external writer extension through the normal public constructor. */
class MosaicWriterExtensionTest {

    private static final RowType TYPE = RowType.of(DataTypes.INT());
    private static final FileFormatFactory.FormatContext CONTEXT =
            new FileFormatFactory.FormatContext(new Options(), 1024, 1024);

    @TempDir java.nio.file.Path tempDir;

    @Test
    void externalWriterReusesConversionBufferAndCleanup() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path path = new Path(tempDir.resolve("columns.mosaic").toUri());
        BufferAllocator allocator;
        try (ExternalWriter writer = new ExternalWriter(fileIO.newOutputStream(path, false))) {
            allocator = writer.allocator();
            HeapIntVector input = new HeapIntVector(2);
            input.setInt(0, 11);
            input.setInt(1, 22);
            writer.writeColumns(new ColumnVector[] {input}, 2);
        }
        assertThat(allocator.getAllocatedMemory()).isZero();
        assertThat(readValues(fileIO, path)).containsExactly(11, 22);
    }

    @Test
    void factoryHookRetainsCompressionValidation() throws Exception {
        int[] created = {0};
        MosaicWriterFactory factory =
                new MosaicWriterFactory(TYPE, CONTEXT) {
                    @Override
                    protected MosaicRecordsWriter createWriter(
                            OutputStream output,
                            RowType type,
                            FileFormatFactory.FormatContext context,
                            List<String> statsColumns,
                            Integer buckets) {
                        created[0]++;
                        assertThat(type).isEqualTo(TYPE);
                        assertThat(context).isSameAs(CONTEXT);
                        assertThat(statsColumns).isEmpty();
                        assertThat(buckets).isNull();
                        return new ExternalWriter(output);
                    }
                };
        assertThatThrownBy(() -> factory.create(mock(PositionOutputStream.class), "snappy"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(created[0]).isZero();
        LocalFileIO fileIO = new LocalFileIO();
        Path path = new Path(tempDir.resolve("factory.mosaic").toUri());
        try (FormatWriter writer = factory.create(fileIO.newOutputStream(path, false), "zstd")) {
            assertThat(writer).isInstanceOf(ExternalWriter.class);
            assertThat(created[0]).isEqualTo(1);
            HeapIntVector input = new HeapIntVector(1);
            input.setInt(0, 33);
            ((ExternalWriter) writer).writeColumns(new ColumnVector[] {input}, 1);
        }
        assertThat(readValues(fileIO, path)).containsExactly(33);
    }

    private static List<Integer> readValues(LocalFileIO fileIO, Path path) throws Exception {
        List<Integer> values = new ArrayList<>();
        MosaicFileFormat format = new MosaicFileFormat(CONTEXT);
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(TYPE, TYPE, Collections.emptyList())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, path, fileIO.getFileSize(path), null, null))) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        values.add(row.getInt(0));
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        return values;
    }

    private static class ExternalWriter extends MosaicRecordsWriter {

        private ExternalWriter(OutputStream output) {
            super(output, TYPE, CONTEXT, Collections.emptyList(), null);
        }

        private BufferAllocator allocator() {
            return arrowWriter().getAllocator();
        }

        private void writeColumns(ColumnVector[] columns, int rows) {
            checkNotFailed();
            arrowWriter().write(columns, null, 0, rows);
            flush();
        }
    }
}
