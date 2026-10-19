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

package org.apache.paimon.fileindex;

import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fileindex.empty.EmptyFileIndexReader;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.paimon.utils.RandomUtil.randomBytes;
import static org.apache.paimon.utils.RandomUtil.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/** Test for {@link FileIndexFormat}. */
public class FileIndexFormatFormatTest {

    private static final Random RANDOM = new Random();
    private static final RowType ROW_TYPE = RowType.of(DataTypes.INT());

    @Test
    public void testCreateReaderClosesStreamOnBadMagic() {
        byte[] notIndexFile = "this is definitely not a file index".getBytes();
        AtomicBoolean closed = new AtomicBoolean();
        SeekableInputStream counting =
                new ByteArraySeekableStream(notIndexFile) {
                    @Override
                    public void close() throws IOException {
                        closed.set(true);
                        super.close();
                    }
                };
        Throwable thrown =
                Assertions.catchThrowable(
                        () ->
                                FileIndexFormat.createReader(
                                        counting, RowType.builder().build(), notIndexFile.length));
        // A throwing constructor never assigns the caller's try-with-resources resource,
        // so closing the stream is the constructor's job.
        Assertions.assertThat(closed.get()).isTrue();
        Assertions.assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("This file is not file index file.");
    }

    @Test
    public void testWriteRead() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Map<String, Map<String, byte[]>> indexes = new HashMap<>();
        for (int j = 0; j < RANDOM.nextInt(1000); j++) {
            String type = randomString(RANDOM.nextInt(100));
            Map<String, byte[]> typeIndex = indexes.computeIfAbsent(type, t -> new HashMap<>());
            for (int i = 0; i < RANDOM.nextInt(1000); i++) {
                typeIndex.put(
                        randomString(RANDOM.nextInt(20)), randomBytes(RANDOM.nextInt(100000)));
            }
        }

        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos, 1)) {
            for (Map.Entry<String, Map<String, byte[]>> column : indexes.entrySet()) {
                for (Map.Entry<String, byte[]> index : column.getValue().entrySet()) {
                    writer.writeIndex(
                            column.getKey(),
                            index.getKey(),
                            output -> output.write(index.getValue()));
                }
            }
            writer.finish();
        }

        byte[] indexBytes = baos.toByteArray();

        FileIndexFormat.Reader reader =
                FileIndexFormat.createReader(
                        new ByteArraySeekableStream(indexBytes),
                        RowType.builder().build(),
                        indexBytes.length);

        for (Map.Entry<String, Map<String, byte[]>> entry : indexes.entrySet()) {
            String column = entry.getKey();
            for (String type : entry.getValue().keySet()) {
                ByteArrayOutputStream copied = new ByteArrayOutputStream();
                reader.copyPayload(column, type, copied);
                Assertions.assertThat(copied.toByteArray())
                        .containsExactly(indexes.get(column).get(type));
            }
        }
    }

    @Test
    public void testEmptyFileIndex() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos, 1)) {
            writer.writeIndex("a", "b", null);
            writer.writeIndex("a", "c", null);
            writer.finish();
        }

        byte[] indexBytes = baos.toByteArray();

        FileIndexFormat.Reader reader =
                FileIndexFormat.createReader(
                        new ByteArraySeekableStream(indexBytes),
                        RowType.builder()
                                .field("a", DataTypes.BYTES())
                                .field("b", DataTypes.STRING())
                                .build(),
                        indexBytes.length);

        Collection<FileIndexReader> fileIndexFormatList = reader.readColumnIndex("a");
        Assertions.assertThat(fileIndexFormatList.size()).isEqualTo(1);
        Assertions.assertThat(new ArrayList<>(fileIndexFormatList).get(0))
                .isEqualTo(EmptyFileIndexReader.INSTANCE);
    }

    @Test
    public void testIndexMetas() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos, 1)) {
            writer.writeIndex("user_id", "bitmap", output -> output.write(new byte[] {1, 2, 3}));
            writer.writeIndex("user_id", "bloom-filter", output -> output.write(new byte[] {4, 5}));
            writer.writeIndex("region", "bitmap", null);
            writer.finish();
        }

        List<FileIndexFormat.FileIndexMeta> metas;
        try (FileIndexFormat.Reader reader =
                FileIndexFormat.createMetadataReader(
                        new ByteArraySeekableStream(baos.toByteArray()), baos.size())) {
            metas = reader.indexMetas();
        }

        Assertions.assertThat(metas)
                .extracting(
                        FileIndexFormat.FileIndexMeta::columnName,
                        FileIndexFormat.FileIndexMeta::indexType,
                        FileIndexFormat.FileIndexMeta::sizeInBytesLong,
                        FileIndexFormat.FileIndexMeta::empty)
                .containsExactlyInAnyOrder(
                        tuple("user_id", "bitmap", 3L, false),
                        tuple("user_id", "bloom-filter", 2L, false),
                        tuple("region", "bitmap", 0L, true));
        Assertions.assertThatThrownBy(() -> metas.clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testV2TotalSizeOverTwoGiBAndLegacyBackendAtLargePosition() throws Exception {
        SparseOutput output = new SparseOutput();
        byte[] payload = new byte[1024 * 1024];
        BitmapFileIndex indexer = new BitmapFileIndex(DataTypes.INT(), new Options());
        FileIndexWriter bitmap = indexer.createWriter();
        bitmap.writeRecord(42);
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, 2)) {
            for (int i = 0; i < 2049; i++) {
                writer.writeIndex(
                        "large", String.format("part%04d", i), stream -> stream.write(payload));
            }
            writer.writeIndex("f0", "bitmap", bitmap::writeTo);
            writer.finish();
        }
        try (FileIndexFormat.Reader reader =
                FileIndexFormat.createReader(output.input(), ROW_TYPE, output.position)) {
            assertThat(output.position).isGreaterThan(Integer.MAX_VALUE);
            FileIndexReader bitmapReader = reader.readColumnIndex("f0").iterator().next();
            assertThat(
                            ((BitmapIndexResult)
                                            bitmapReader.visitEqual(
                                                    new FieldRef(0, "f0", DataTypes.INT()), 42))
                                    .get())
                    .isEqualTo(RoaringBitmap32.bitmapOf(0));
        }
    }

    @Test
    public void testV2StreamedPayloadOverInt32() throws Exception {
        SparseOutput output = new SparseOutput();
        byte[] block = new byte[1024 * 1024];
        BitmapFileIndex indexer = new BitmapFileIndex(DataTypes.INT(), new Options());
        FileIndexWriter bitmap = indexer.createWriter();
        bitmap.writeRecord(42);
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, 2)) {
            writer.writeIndex(
                    "large",
                    "bitmap",
                    stream -> {
                        for (int i = 0; i < 2049; i++) {
                            stream.write(block);
                        }
                    });
            writer.writeIndex("f0", "bitmap", bitmap::writeTo);
            writer.finish();
        }

        RowType rowType =
                RowType.builder()
                        .field("large", DataTypes.INT())
                        .field("f0", DataTypes.INT())
                        .build();
        try (FileIndexFormat.Reader reader =
                FileIndexFormat.createReader(output.input(), rowType, output.position)) {
            assertThat(reader.indexMetas())
                    .extracting(FileIndexFormat.FileIndexMeta::sizeInBytesLong)
                    .contains((long) block.length * 2049);
            FileIndexReader bitmapReader = reader.readColumnIndex("f0").iterator().next();
            assertThat(
                            ((BitmapIndexResult)
                                            bitmapReader.visitEqual(
                                                    new FieldRef(1, "f0", DataTypes.INT()), 42))
                                    .get())
                    .isEqualTo(RoaringBitmap32.bitmapOf(0));
        }
    }

    @Test
    public void testV2CopyPayloadOverInt32() throws Exception {
        SparseOutput source = new SparseOutput();
        byte[] block = new byte[1024 * 1024];
        long payloadLength = (long) block.length * 2049 + 1;
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(source, 2)) {
            writer.writeIndex(
                    "large",
                    "bitmap",
                    stream -> {
                        for (int i = 0; i < 2049; i++) {
                            stream.write(block);
                        }
                        stream.write(7);
                    });
            writer.finish();
        }

        try (FileIndexFormat.Reader reader =
                FileIndexFormat.createMetadataReader(source.input(), source.position)) {
            final long[] copied = {0};
            final int[] lastByte = {-1};
            reader.copyPayload(
                    "large",
                    "bitmap",
                    new OutputStream() {
                        @Override
                        public void write(int value) {
                            copied[0]++;
                            lastByte[0] = value;
                        }

                        @Override
                        public void write(byte[] bytes, int offset, int length) {
                            copied[0] += length;
                            lastByte[0] = bytes[offset + length - 1] & 0xff;
                        }
                    });
            assertThat(copied[0]).isEqualTo(payloadLength);
            assertThat(lastByte[0]).isEqualTo(7);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testIncrementalWriteAndCopyPayload(int version) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, version)) {
            writer.writeIndex("a", "bitmap", stream -> stream.write(new byte[] {1, 2, 3}));
            writer.writeIndex("a", "bsi", null);
            writer.writeIndex("b", "bitmap", stream -> {});
            writer.finish();
        }
        try (FileIndexFormat.Reader reader =
                FileIndexFormat.createMetadataReader(
                        new ByteArraySeekableStream(output.toByteArray()), output.size())) {
            ByteArrayOutputStream copied = new ByteArrayOutputStream();
            reader.copyPayload("a", "bitmap", copied);
            assertThat(copied.toByteArray()).containsExactly(1, 2, 3);
            assertThat(reader.indexMetas())
                    .extracting(FileIndexFormat.FileIndexMeta::empty)
                    .containsExactlyInAnyOrder(false, true, false);
        }
    }

    @Test
    public void testV2RejectInvalidTailMagic() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, 2)) {
            writer.writeIndex("f0", "bitmap", stream -> stream.write(new byte[] {1, 2, 3}));
            writer.finish();
        }
        byte[] bytes = output.toByteArray();
        bytes[bytes.length - 1] = 0;
        assertThatThrownBy(
                        () ->
                                FileIndexFormat.createReader(
                                        new ByteArraySeekableStream(bytes), ROW_TYPE, bytes.length))
                .rootCause()
                .hasMessageContaining("Invalid file index tail magic");
    }

    /**
     * Retains small metadata writes while large zero-filled payload writes only advance position.
     */
    private static class SparseOutput extends OutputStream {
        private final TreeMap<Long, byte[]> segments = new TreeMap<>();
        private long position;

        @Override
        public void write(int value) {
            write(new byte[] {(byte) value}, 0, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            if (length < 1024) {
                segments.put(position, Arrays.copyOfRange(bytes, offset, offset + length));
            }
            position += length;
        }

        private SeekableInputStream input() {
            return new SeekableInputStream() {
                private long cursor;

                @Override
                public void seek(long desired) {
                    cursor = desired;
                }

                @Override
                public long getPos() {
                    return cursor;
                }

                @Override
                public int read() {
                    if (cursor >= position) {
                        return -1;
                    }
                    Map.Entry<Long, byte[]> entry = segments.floorEntry(cursor);
                    int value =
                            entry != null && cursor - entry.getKey() < entry.getValue().length
                                    ? entry.getValue()[(int) (cursor - entry.getKey())] & 0xff
                                    : 0;
                    cursor++;
                    return value;
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    if (len == 0) {
                        return 0;
                    }
                    if (cursor >= position) {
                        return -1;
                    }
                    int n = (int) Math.min(len, position - cursor);
                    long end = cursor + n;
                    Arrays.fill(b, off, off + n, (byte) 0);
                    Map.Entry<Long, byte[]> entry = segments.floorEntry(cursor);
                    if (entry == null) {
                        entry = segments.ceilingEntry(cursor);
                    }
                    while (entry != null && entry.getKey() < end) {
                        long overlapStart = Math.max(cursor, entry.getKey());
                        long overlapEnd = Math.min(end, entry.getKey() + entry.getValue().length);
                        if (overlapStart < overlapEnd) {
                            System.arraycopy(
                                    entry.getValue(),
                                    (int) (overlapStart - entry.getKey()),
                                    b,
                                    off + (int) (overlapStart - cursor),
                                    (int) (overlapEnd - overlapStart));
                        }
                        entry = segments.higherEntry(entry.getKey());
                    }
                    cursor = end;
                    return n;
                }

                @Override
                public void close() {}
            };
        }
    }
}
