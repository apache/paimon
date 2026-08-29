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

package org.apache.paimon.format.blob;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileAwareFormatWriter;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SharedBlobFileFormat}. */
public class SharedBlobFileFormatTest {

    @TempDir java.nio.file.Path tempPath;

    private FileIO fileIO;
    private Path file;
    private RowType rowType;

    @BeforeEach
    public void beforeEach() {
        fileIO = LocalFileIO.create();
        file = new Path(tempPath.resolve("data.shared-blob").toUri());
        rowType = RowType.of(DataTypes.BLOB());
    }

    @Test
    public void testRowsSharePhysicalBlobByExactDescriptor() throws IOException {
        Blob first = sourceBlob("first.mp4", "first-video");
        Blob second = sourceBlob("second.mp4", "second-video");

        write(first, first, second, first, null, BlobPlaceholder.INSTANCE);

        try (SeekableInputStream in = fileIO.newInputStream(file)) {
            SharedBlobFileMeta meta = new SharedBlobFileMeta(in, fileIO.getFileSize(file), null);
            assertThat(meta.recordNumber()).isEqualTo(6);
            assertThat(meta.physicalBlobNumber()).isEqualTo(2);
            assertThat(meta.blobOffset(0)).isEqualTo(meta.blobOffset(1));
            assertThat(meta.blobOffset(0)).isEqualTo(meta.blobOffset(3));
            assertThat(meta.blobLength(0)).isEqualTo(meta.blobLength(1));
            assertThat(meta.blobOffset(2)).isNotEqualTo(meta.blobOffset(0));
            assertThat(meta.isNull(4)).isTrue();
            assertThat(meta.isPlaceHolder(5)).isTrue();
        }

        List<InternalRow> rows = read(true, null);
        assertThat(rows).hasSize(6);
        BlobRef firstRow = (BlobRef) rows.get(0).getBlob(0);
        BlobRef secondRow = (BlobRef) rows.get(1).getBlob(0);
        BlobRef fourthRow = (BlobRef) rows.get(3).getBlob(0);
        assertThat(secondRow.toDescriptor()).isEqualTo(firstRow.toDescriptor());
        assertThat(fourthRow.toDescriptor()).isEqualTo(firstRow.toDescriptor());
        assertThat(firstRow.toData()).isEqualTo("first-video".getBytes());
        assertThat(rows.get(2).getBlob(0).toData()).isEqualTo("second-video".getBytes());
        assertThat(rows.get(4).isNullAt(0)).isTrue();
        assertThat(rows.get(5).getBlob(0)).isSameAs(BlobPlaceholder.INSTANCE);
    }

    @Test
    public void testSelectionKeepsLogicalRowPositions() throws IOException {
        Blob first = sourceBlob("first.mp4", "first-video");
        Blob second = sourceBlob("second.mp4", "second-video");
        write(first, first, second, first);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);
        selection.add(3);

        SharedBlobFileFormat format =
                new SharedBlobFileFormat(true, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection, null);
        try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
            org.apache.paimon.reader.FileRecordIterator<InternalRow> iterator = reader.readBatch();
            Blob firstSelected = iterator.next().getBlob(0);
            assertThat(iterator.returnedPosition()).isOne();
            Blob secondSelected = iterator.next().getBlob(0);
            assertThat(iterator.returnedPosition()).isEqualTo(3L);
            assertThat(secondSelected.toDescriptor()).isEqualTo(firstSelected.toDescriptor());
            assertThat(iterator.next()).isNull();
        }
    }

    @Test
    public void testRejectInlineBlobAndNestedBlobTypes() throws IOException {
        SharedBlobFileFormat format =
                new SharedBlobFileFormat(true, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        assertThatThrownBy(
                        () ->
                                format.validateDataFields(
                                        RowType.of(DataTypes.ARRAY(DataTypes.BLOB()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scalar BLOB");

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
            assertThatThrownBy(
                            () ->
                                    writer.addElement(
                                            GenericRow.of(new BlobData("inline".getBytes()))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("exact BlobRef");
            writer.close();
        }
    }

    @Test
    public void testIndependentFormatRegistrationAndClassification() {
        assertThat(FileFormat.fromIdentifier("shared-blob", new Options()))
                .isInstanceOf(SharedBlobFileFormat.class);
        assertThat(BlobFileFormat.isBlobFile("a.blob")).isTrue();
        assertThat(BlobFileFormat.isBlobFile("a.shared-blob")).isTrue();
        assertThat(BlobFileFormat.isBlobFile("a.parquet")).isFalse();
    }

    @Test
    public void testRejectCorruptRowReference() throws IOException {
        byte[] physicalIndex = DeltaVarintCompressor.compress(new long[0]);
        byte[] rowIndex = DeltaVarintCompressor.compress(new long[] {0});
        byte[] bytes =
                new byte[physicalIndex.length + rowIndex.length + Integer.BYTES * 3 + Byte.BYTES];
        int position = 0;
        System.arraycopy(physicalIndex, 0, bytes, position, physicalIndex.length);
        position += physicalIndex.length;
        System.arraycopy(rowIndex, 0, bytes, position, rowIndex.length);
        position += rowIndex.length;
        position = putInt(bytes, position, physicalIndex.length);
        position = putInt(bytes, position, rowIndex.length);
        position = putInt(bytes, position, SharedBlobFormatWriter.MAGIC_NUMBER);
        bytes[position] = SharedBlobFormatWriter.VERSION;
        Files.write(java.nio.file.Paths.get(file.toUri()), bytes);

        assertThatThrownBy(
                        () -> {
                            try (SeekableInputStream in = fileIO.newInputStream(file)) {
                                new SharedBlobFileMeta(in, fileIO.getFileSize(file), null);
                            }
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining(
                        "row 0 references physical blob 0, but physical blob count is 0");
    }

    private Blob sourceBlob(String name, String value) throws IOException {
        byte[] bytes = value.getBytes();
        java.nio.file.Path source = tempPath.resolve(name);
        Files.write(source, bytes);
        return Blob.fromFile(fileIO, new Path(source.toUri()).toString(), 0, bytes.length);
    }

    private void write(Object... blobs) throws IOException {
        SharedBlobFileFormat format =
                new SharedBlobFileFormat(true, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
            ((FileAwareFormatWriter) writer).setFile(file);
            for (Object blob : blobs) {
                writer.addElement(GenericRow.of(blob));
            }
            writer.close();
        }
    }

    private List<InternalRow> read(boolean blobAsDescriptor, RoaringBitmap32 selection)
            throws IOException {
        SharedBlobFileFormat format =
                new SharedBlobFileFormat(
                        blobAsDescriptor, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection, null);
        List<InternalRow> rows = new ArrayList<>();
        try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
            reader.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static int putInt(byte[] target, int position, int value) {
        byte[] bytes = intToLittleEndian(value);
        System.arraycopy(bytes, 0, target, position, bytes.length);
        return position + bytes.length;
    }
}
