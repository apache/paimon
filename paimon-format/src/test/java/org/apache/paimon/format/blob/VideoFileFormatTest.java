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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.VideoFrameDescriptor;
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
import org.apache.paimon.reader.FileRecordIterator;
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

/** Tests for {@link VideoFileFormat}. */
public class VideoFileFormatTest {

    @TempDir java.nio.file.Path tempPath;

    private FileIO fileIO;
    private Path file;
    private RowType rowType;

    @BeforeEach
    public void beforeEach() {
        fileIO = LocalFileIO.create();
        file = new Path(tempPath.resolve("data.video").toUri());
        rowType = RowType.of(DataTypes.BLOB());
    }

    @Test
    public void testPackRawVideosAndMapFrameRuns() throws IOException {
        byte[] firstBytes = "first-mp4".getBytes();
        byte[] secondBytes = "second-mp4".getBytes();
        Blob first0 = sourceFrame("first.mp4", firstBytes, 0);
        Blob first1 = sourceFrame("first.mp4", firstBytes, 1);
        Blob second7 = sourceFrame("second.mp4", secondBytes, 7);
        Blob first4 = sourceFrame("first.mp4", firstBytes, 4);

        write(first0, first1, second7, first4, null, BlobPlaceholder.INSTANCE);

        try (SeekableInputStream in = fileIO.newInputStream(file)) {
            VideoFileMeta meta = new VideoFileMeta(in, fileIO.getFileSize(file), null);
            assertThat(meta.recordNumber()).isEqualTo(6);
            assertThat(meta.physicalVideoNumber()).isEqualTo(2);
            assertThat(meta.runNumber()).isEqualTo(5);
            assertThat(meta.videoOffset(0)).isZero();
            assertThat(meta.videoLength(0)).isEqualTo(firstBytes.length);
            assertThat(meta.frameIndex(0)).isZero();
            assertThat(meta.frameIndex(1)).isOne();
            assertThat(meta.frameIndex(2)).isEqualTo(7);
            assertThat(meta.frameIndex(3)).isEqualTo(4);
            assertThat(meta.isNull(4)).isTrue();
            assertThat(meta.isPlaceHolder(5)).isTrue();
        }

        byte[] stored = Files.readAllBytes(java.nio.file.Paths.get(file.toUri()));
        assertThat(stored).startsWith(firstBytes);
        assertThat(stored).containsSubsequence(secondBytes);

        List<InternalRow> rows = read(null);
        assertThat(rows).hasSize(6);
        VideoFrameDescriptor frame0 = descriptor(rows.get(0));
        VideoFrameDescriptor frame1 = descriptor(rows.get(1));
        VideoFrameDescriptor frame2 = descriptor(rows.get(2));
        VideoFrameDescriptor frame3 = descriptor(rows.get(3));
        assertThat(frame0.frameIndex()).isZero();
        assertThat(frame1.frameIndex()).isOne();
        assertThat(frame0.payloadDescriptor()).isEqualTo(frame1.payloadDescriptor());
        assertThat(frame2.frameIndex()).isEqualTo(7);
        assertThat(frame2.payloadDescriptor()).isNotEqualTo(frame0.payloadDescriptor());
        assertThat(frame3.frameIndex()).isEqualTo(4);
        assertThat(frame3.payloadDescriptor()).isEqualTo(frame0.payloadDescriptor());
        assertThat(rows.get(4).isNullAt(0)).isTrue();
        assertThat(rows.get(5).getBlob(0)).isSameAs(BlobPlaceholder.INSTANCE);
    }

    @Test
    public void testSelectionKeepsLogicalRowPositions() throws IOException {
        byte[] bytes = "first-mp4".getBytes();
        write(
                sourceFrame("first.mp4", bytes, 0),
                sourceFrame("first.mp4", bytes, 1),
                sourceFrame("first.mp4", bytes, 2),
                sourceFrame("first.mp4", bytes, 3));

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);
        selection.add(3);

        VideoFileFormat format = new VideoFileFormat(BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection, null);
        try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
            FileRecordIterator<InternalRow> iterator = reader.readBatch();
            assertThat(descriptor(iterator.next()).frameIndex()).isOne();
            assertThat(iterator.returnedPosition()).isOne();
            assertThat(descriptor(iterator.next()).frameIndex()).isEqualTo(3);
            assertThat(iterator.returnedPosition()).isEqualTo(3L);
            assertThat(iterator.next()).isNull();
        }
    }

    @Test
    public void testRejectNonVideoFrameInputsAndNestedBlobTypes() throws IOException {
        VideoFileFormat format = new VideoFileFormat(BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
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
                    .hasMessageContaining("VideoFrameDescriptor");
            writer.close();
        }
    }

    @Test
    public void testIndependentFormatRegistrationAndClassification() {
        assertThat(FileFormat.fromIdentifier("video", new Options()))
                .isInstanceOf(VideoFileFormat.class);
        assertThat(BlobFileFormat.isBlobFile("a.blob")).isTrue();
        assertThat(BlobFileFormat.isBlobFile("a.video")).isTrue();
        assertThat(BlobFileFormat.isBlobFile("a.parquet")).isFalse();
    }

    @Test
    public void testRejectCorruptRunReference() throws IOException {
        byte[] physicalIndex = DeltaVarintCompressor.compress(new long[0]);
        byte[] runLengthIndex = DeltaVarintCompressor.compress(new long[] {1});
        byte[] runReferenceIndex = DeltaVarintCompressor.compress(new long[] {0});
        byte[] firstFrameIndex = DeltaVarintCompressor.compress(new long[] {0});
        byte[] bytes =
                new byte
                        [physicalIndex.length
                                + runLengthIndex.length
                                + runReferenceIndex.length
                                + firstFrameIndex.length
                                + VideoFormatWriter.FILE_FOOTER_LENGTH];
        int position = 0;
        position = put(bytes, position, physicalIndex);
        position = put(bytes, position, runLengthIndex);
        position = put(bytes, position, runReferenceIndex);
        position = put(bytes, position, firstFrameIndex);
        position = putInt(bytes, position, physicalIndex.length);
        position = putInt(bytes, position, runLengthIndex.length);
        position = putInt(bytes, position, runReferenceIndex.length);
        position = putInt(bytes, position, firstFrameIndex.length);
        position = putInt(bytes, position, VideoFormatWriter.MAGIC_NUMBER);
        bytes[position] = VideoFormatWriter.VERSION;
        Files.write(java.nio.file.Paths.get(file.toUri()), bytes);

        assertThatThrownBy(
                        () -> {
                            try (SeekableInputStream in = fileIO.newInputStream(file)) {
                                new VideoFileMeta(in, fileIO.getFileSize(file), null);
                            }
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining(
                        "run 0 references physical video 0, but physical video count is 0");
    }

    private Blob sourceFrame(String name, byte[] bytes, long frameIndex) throws IOException {
        java.nio.file.Path source = tempPath.resolve(name);
        if (!Files.exists(source)) {
            Files.write(source, bytes);
        }
        VideoFrameDescriptor descriptor =
                new VideoFrameDescriptor(
                        new Path(source.toUri()).toString(), 0, bytes.length, frameIndex);
        return Blob.fromDescriptor(org.apache.paimon.utils.UriReader.fromFile(fileIO), descriptor);
    }

    private void write(Object... frames) throws IOException {
        VideoFileFormat format = new VideoFileFormat(BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
            ((FileAwareFormatWriter) writer).setFile(file);
            for (Object frame : frames) {
                writer.addElement(GenericRow.of(frame));
            }
            writer.close();
        }
    }

    private List<InternalRow> read(RoaringBitmap32 selection) throws IOException {
        VideoFileFormat format = new VideoFileFormat(BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection, null);
        List<InternalRow> rows = new ArrayList<>();
        try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
            reader.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static VideoFrameDescriptor descriptor(InternalRow row) {
        return (VideoFrameDescriptor) row.getBlob(0).toDescriptor();
    }

    private static int put(byte[] target, int position, byte[] value) {
        System.arraycopy(value, 0, target, position, value.length);
        return position + value.length;
    }

    private static int putInt(byte[] target, int position, int value) {
        return put(target, position, intToLittleEndian(value));
    }
}
