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
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link VideoFileFormat}. */
public class VideoFileFormatTest {

    private static final String KEYFRAME_INDEX_HEX =
            "0149464b4f45444956010000000200000000000000000000000100000000000000"
                    + "789c6360c00e78a0f4821e08cd04e503001394013b";

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
    public void testCrossLanguageV1Fixture() throws IOException {
        byte[] fixture = fixture("video-v1.hex");
        byte[] video = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] mapping = fromHex(KEYFRAME_INDEX_HEX);
        java.nio.file.Path source = tempPath.resolve("indexed.mp4");
        byte[] sourceBytes = new byte[video.length + mapping.length];
        System.arraycopy(video, 0, sourceBytes, 0, video.length);
        System.arraycopy(mapping, 0, sourceBytes, video.length, mapping.length);
        Files.write(source, sourceBytes);
        VideoFrameDescriptor descriptor =
                new VideoFrameDescriptor(
                        new Path(source.toUri()).toString(),
                        0,
                        video.length,
                        2,
                        video.length,
                        mapping.length);
        Blob a2 =
                Blob.fromDescriptor(org.apache.paimon.utils.UriReader.fromFile(fileIO), descriptor);
        Blob a3 =
                Blob.fromDescriptor(
                        org.apache.paimon.utils.UriReader.fromFile(fileIO),
                        new VideoFrameDescriptor(
                                descriptor.uri(),
                                0,
                                video.length,
                                3,
                                video.length,
                                mapping.length));
        Blob b7 = sourceFrame("b.mp4", "WXYZ".getBytes(StandardCharsets.UTF_8), 7);
        Blob b8 = sourceFrame("b.mp4", "WXYZ".getBytes(StandardCharsets.UTF_8), 8);
        Blob a10 =
                Blob.fromDescriptor(
                        org.apache.paimon.utils.UriReader.fromFile(fileIO),
                        new VideoFrameDescriptor(
                                descriptor.uri(),
                                0,
                                video.length,
                                10,
                                video.length,
                                mapping.length));

        write(a2, a3, null, BlobPlaceholder.INSTANCE, b7, b8, a10);

        assertThat(Files.readAllBytes(java.nio.file.Paths.get(file.toUri()))).isEqualTo(fixture);
        try (SeekableInputStream in = fileIO.newInputStream(file)) {
            VideoFileMeta meta = new VideoFileMeta(in, fixture.length, null);
            assertThat(meta.recordNumber()).isEqualTo(7);
            assertThat(meta.physicalVideoNumber()).isEqualTo(2);
            assertThat(meta.videoLength(0)).isEqualTo(video.length);
            assertThat(meta.keyframeIndexOffset(0)).isEqualTo(7);
            assertThat(meta.keyframeIndexLength(0)).isEqualTo(mapping.length);
            assertThat(meta.keyframeIndexLength(4)).isZero();
        }
        VideoFrameDescriptor restored = descriptor(read(null).get(0));
        assertThat(
                        VideoFrameDescriptor.keyframeIndexBlob(
                                        Blob.fromDescriptor(
                                                org.apache.paimon.utils.UriReader.fromFile(fileIO),
                                                restored))
                                .toData())
                .isEqualTo(mapping);
    }

    @Test
    public void testRejectInvalidKeyframeIndex() throws IOException {
        byte[] video = "video".getBytes(StandardCharsets.UTF_8);
        byte[] mapping = "mapping".getBytes(StandardCharsets.UTF_8);
        java.nio.file.Path source = tempPath.resolve("invalid-index.mp4");
        byte[] sourceBytes = new byte[video.length + mapping.length];
        System.arraycopy(video, 0, sourceBytes, 0, video.length);
        System.arraycopy(mapping, 0, sourceBytes, video.length, mapping.length);
        Files.write(source, sourceBytes);
        Blob frame =
                Blob.fromDescriptor(
                        org.apache.paimon.utils.UriReader.fromFile(fileIO),
                        new VideoFrameDescriptor(
                                new Path(source.toUri()).toString(),
                                0,
                                video.length,
                                0,
                                video.length,
                                mapping.length));

        assertThatThrownBy(() -> write(frame))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid video keyframe index");
    }

    @Test
    public void testRejectTooManyKeyframes() {
        byte[] mapping =
                ByteBuffer.allocate(18)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .put((byte) 1)
                        .putLong(0x564944454F4B4649L)
                        .putInt(0)
                        .putInt((int) VideoKeyframeIndex.MAX_KEYFRAME_COUNT + 1)
                        .put((byte) 0)
                        .array();

        assertThatThrownBy(() -> VideoKeyframeIndex.validate(mapping, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyframe count exceeds limit");
    }

    @Test
    public void testRejectTooManyMetadataRanges() {
        byte[] mapping =
                ByteBuffer.allocate(18)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .put((byte) 1)
                        .putLong(0x564944454F4B4649L)
                        .putInt((int) VideoKeyframeIndex.MAX_METADATA_RANGE_COUNT + 1)
                        .putInt(1)
                        .put((byte) 0)
                        .array();

        assertThatThrownBy(() -> VideoKeyframeIndex.validate(mapping, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metadata range count exceeds limit");
    }

    @Test
    public void testRejectOversizedKeyframeIndexesBeforeFetch() throws IOException {
        String missing = new Path(tempPath.resolve("missing.mp4").toUri()).toString();
        Blob frame =
                Blob.fromDescriptor(
                        org.apache.paimon.utils.UriReader.fromFile(fileIO),
                        new VideoFrameDescriptor(
                                missing,
                                0,
                                1,
                                0,
                                1,
                                VideoFormatWriter.MAX_KEYFRAME_INDEX_BYTES + 1));

        assertThatThrownBy(() -> write(frame))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyframe index length")
                .hasMessageContaining("limit");
        assertThatThrownBy(
                        () ->
                                VideoFormatWriter.checkKeyframeIndexSize(
                                        1, VideoFormatWriter.MAX_TOTAL_KEYFRAME_INDEX_BYTES))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Buffered video keyframe indexes")
                .hasMessageContaining("limit");
    }

    @Test
    public void testRejectSeekOffsetsOutsideVideoPayload() throws IOException {
        byte[] video = "video".getBytes(StandardCharsets.UTF_8);
        byte[] mapping =
                fromHex(
                        "0149464b4f45444956010000000100000000000000000000000600000000000000"
                                + "789c6360c00e0000180001");
        java.nio.file.Path source = tempPath.resolve("out-of-range-index.mp4");
        byte[] sourceBytes = new byte[video.length + mapping.length];
        int offset = put(sourceBytes, 0, video);
        put(sourceBytes, offset, mapping);
        Files.write(source, sourceBytes);
        Blob frame =
                Blob.fromDescriptor(
                        org.apache.paimon.utils.UriReader.fromFile(fileIO),
                        new VideoFrameDescriptor(
                                new Path(source.toUri()).toString(),
                                0,
                                video.length,
                                0,
                                video.length,
                                mapping.length));

        assertThatThrownBy(() -> write(frame))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("video payload");
    }

    @Test
    public void testRejectInconsistentKeyframeIndexesForSamePayload() throws IOException {
        byte[] video = "video".getBytes(StandardCharsets.UTF_8);
        byte[] firstIndex = fromHex(KEYFRAME_INDEX_HEX);
        byte[] secondIndex =
                fromHex(
                        "0149464b4f454449560000000002000000789c6360c00ed8a074801b846682f201"
                                + "09ea009f");
        java.nio.file.Path source = tempPath.resolve("inconsistent-index.mp4");
        byte[] sourceBytes = new byte[video.length + firstIndex.length + secondIndex.length];
        int offset = put(sourceBytes, 0, video);
        offset = put(sourceBytes, offset, firstIndex);
        put(sourceBytes, offset, secondIndex);
        Files.write(source, sourceBytes);
        String uri = new Path(source.toUri()).toString();
        org.apache.paimon.utils.UriReader reader =
                org.apache.paimon.utils.UriReader.fromFile(fileIO);
        Blob unindexed =
                Blob.fromDescriptor(
                        reader, new VideoFrameDescriptor(uri, 0, video.length, 0, -1, 0));
        Blob first =
                Blob.fromDescriptor(
                        reader,
                        new VideoFrameDescriptor(
                                uri, 0, video.length, 1, video.length, firstIndex.length));
        Blob second =
                Blob.fromDescriptor(
                        reader,
                        new VideoFrameDescriptor(
                                uri,
                                0,
                                video.length,
                                2,
                                video.length + firstIndex.length,
                                secondIndex.length));

        assertThatThrownBy(() -> write(unindexed, first))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same payload");
        fileIO.delete(file, false);
        assertThatThrownBy(() -> write(first, unindexed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same payload");
        fileIO.delete(file, false);
        assertThatThrownBy(() -> write(first, second))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same payload");
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
    public void testRejectEmptyVideoPayload() throws IOException {
        Blob empty = sourceFrame("empty.mp4", new byte[0], 0);

        assertThatThrownBy(() -> write(empty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Encoded video payload must not be empty");
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
        byte[] keyframeLengthIndex = DeltaVarintCompressor.compress(new long[0]);
        byte[] runLengthIndex = DeltaVarintCompressor.compress(new long[] {1});
        byte[] runReferenceIndex = DeltaVarintCompressor.compress(new long[] {0});
        byte[] firstFrameIndex = DeltaVarintCompressor.compress(new long[] {0});
        byte[] bytes =
                new byte
                        [physicalIndex.length
                                + keyframeLengthIndex.length
                                + runLengthIndex.length
                                + runReferenceIndex.length
                                + firstFrameIndex.length
                                + VideoFormatWriter.FILE_FOOTER_LENGTH];
        int position = 0;
        position = put(bytes, position, physicalIndex);
        position = put(bytes, position, keyframeLengthIndex);
        position = put(bytes, position, runLengthIndex);
        position = put(bytes, position, runReferenceIndex);
        position = put(bytes, position, firstFrameIndex);
        position = putInt(bytes, position, physicalIndex.length);
        position = putInt(bytes, position, keyframeLengthIndex.length);
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

    @Test
    public void testReaderKeyframeIndexSizeLimits() throws IOException {
        long limit = VideoFormatWriter.MAX_KEYFRAME_INDEX_BYTES;
        long[][] cases = {
            {limit + 1}, {limit, limit, limit, limit, 1}, {limit, limit, limit, limit}, {0}
        };
        for (int i = 0; i < cases.length; i++) {
            long[] lengths = cases[i];
            long[] physicalLengths = new long[lengths.length];
            java.util.Arrays.fill(physicalLengths, 1);
            byte[][] indexes = {
                DeltaVarintCompressor.compress(physicalLengths),
                DeltaVarintCompressor.compress(lengths),
                DeltaVarintCompressor.compress(new long[0]),
                DeltaVarintCompressor.compress(new long[0]),
                DeltaVarintCompressor.compress(new long[0])
            };
            try (RandomAccessFile out =
                    new RandomAccessFile(java.nio.file.Paths.get(file.toUri()).toFile(), "rw")) {
                out.setLength(0);
                long indexStart = lengths.length;
                for (long length : lengths) {
                    indexStart += length;
                }
                out.seek(indexStart);
                for (byte[] index : indexes) {
                    out.write(index);
                }
                for (byte[] index : indexes) {
                    out.write(intToLittleEndian(index.length));
                }
                out.write(intToLittleEndian(VideoFormatWriter.MAGIC_NUMBER));
                out.write(VideoFormatWriter.VERSION);
            }
            try (SeekableInputStream in = fileIO.newInputStream(file)) {
                if (i < 2) {
                    String message = i == 0 ? "16 MiB" : "64 MiB";
                    assertThatThrownBy(() -> new VideoFileMeta(in, fileIO.getFileSize(file), null))
                            .isInstanceOf(IOException.class)
                            .hasMessageContaining(message);
                } else {
                    new VideoFileMeta(in, fileIO.getFileSize(file), null);
                }
            }
        }
    }

    @Test
    public void testIndexFetchNullPolicyBeforePayloadWrite() throws IOException {
        for (int status : new int[] {404, 416, 503, 0}) {
            for (boolean missing : new boolean[] {false, true}) {
                for (boolean failure : new boolean[] {false, true}) {
                    SeekableInputStream source =
                            new SeekableInputStream() {
                                private long position;

                                @Override
                                public void close() {}

                                @Override
                                public int read(byte[] bytes, int offset, int length)
                                        throws IOException {
                                    if (length == 0) {
                                        return 0;
                                    }
                                    int value = read();
                                    if (value < 0) {
                                        return -1;
                                    }
                                    bytes[offset] = (byte) value;
                                    return 1;
                                }

                                @Override
                                public void seek(long pos) throws IOException {
                                    if (pos == 5 && status != 0) {
                                        throw new IOException("HTTP error code: " + status);
                                    }
                                    position = pos;
                                }

                                @Override
                                public long getPos() {
                                    return position;
                                }

                                @Override
                                public int read() throws IOException {
                                    if (position >= 5 && status != 0) {
                                        throw new IOException("HTTP error code: " + status);
                                    }
                                    return position++ < 5 ? 1 : -1;
                                }
                            };
                    Blob frame =
                            Blob.fromDescriptor(
                                    uri -> source,
                                    new VideoFrameDescriptor(
                                            "http://example/video", 0, 5, 0, 5, 1));
                    int[] counts = new int[4];
                    org.apache.paimon.data.BlobFetchMetricReporter metrics =
                            new org.apache.paimon.data.BlobFetchMetricReporter() {
                                public void recordSuccess(long bytes) {
                                    counts[0]++;
                                }

                                public void recordMissingFileNullWritten(boolean http) {
                                    counts[1]++;
                                }

                                public void recordFetchFailureNullWritten(Throwable e) {
                                    counts[2]++;
                                }

                                public void recordFetchFailure(Throwable e) {
                                    counts[3]++;
                                }
                            };
                    try (PositionOutputStream out = fileIO.newOutputStream(file, true)) {
                        VideoFormatWriter writer =
                                new VideoFormatWriter(
                                        out,
                                        rowType,
                                        missing,
                                        failure,
                                        metrics,
                                        BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
                        writer.setFile(file);
                        boolean fallback = status == 404 ? missing : failure;
                        if (fallback) {
                            writer.addElement(GenericRow.of(frame));
                            assertThat(counts[status == 404 ? 1 : 2]).isEqualTo(1);
                        } else {
                            assertThatThrownBy(
                                            () -> writer.addElement(GenericRow.of(frame)),
                                            "status=%s missing=%s failure=%s",
                                            status,
                                            missing,
                                            failure)
                                    .isInstanceOfAny(IOException.class, RuntimeException.class);
                            assertThat(counts[3]).isEqualTo(1);
                        }
                        assertThat(out.getPos()).isZero();
                        writer.close();
                        if (fallback) {
                            try (SeekableInputStream in = fileIO.newInputStream(file)) {
                                assertThat(
                                                new VideoFileMeta(
                                                                in, fileIO.getFileSize(file), null)
                                                        .isNull(0))
                                        .isTrue();
                            }
                        }
                    }
                }
            }
        }
    }

    private Blob sourceFrame(String name, byte[] bytes, long frameIndex) throws IOException {
        java.nio.file.Path source = tempPath.resolve(name);
        if (!Files.exists(source)) {
            Files.write(source, bytes);
        }
        VideoFrameDescriptor descriptor =
                new VideoFrameDescriptor(
                        new Path(source.toUri()).toString(), 0, bytes.length, frameIndex, -1, 0);
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

    private static byte[] fromHex(String hex) {
        hex = hex.replaceAll("(?m)^#.*$", "").replaceAll("\\s", "");
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            int offset = i * 2;
            bytes[i] =
                    (byte)
                            ((Character.digit(hex.charAt(offset), 16) << 4)
                                    + Character.digit(hex.charAt(offset + 1), 16));
        }
        return bytes;
    }

    private static byte[] fixture(String name) throws IOException {
        return fromHex(
                new String(
                                IOUtils.readFully(
                                        VideoFileFormatTest.class
                                                .getClassLoader()
                                                .getResourceAsStream(
                                                        "org/apache/paimon/format/blob/" + name),
                                        true),
                                StandardCharsets.UTF_8)
                        .trim());
    }
}
