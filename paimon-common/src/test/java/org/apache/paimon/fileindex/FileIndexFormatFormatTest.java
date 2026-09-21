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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
        FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos, 1);

        Map<String, Map<String, byte[]>> indexes = new HashMap<>();
        for (int j = 0; j < RANDOM.nextInt(1000); j++) {
            String type = randomString(RANDOM.nextInt(100));
            Map<String, byte[]> typeIndex = indexes.computeIfAbsent(type, t -> new HashMap<>());
            for (int i = 0; i < RANDOM.nextInt(1000); i++) {
                typeIndex.put(
                        randomString(RANDOM.nextInt(20)), randomBytes(RANDOM.nextInt(100000)));
            }
        }

        writer.writeColumnIndexes(indexes);
        writer.close();

        byte[] indexBytes = baos.toByteArray();

        FileIndexFormat.Reader reader =
                FileIndexFormat.createReader(
                        new ByteArraySeekableStream(indexBytes),
                        RowType.builder().build(),
                        indexBytes.length);

        for (Map.Entry<String, Map<String, byte[]>> entry : indexes.entrySet()) {
            String column = entry.getKey();
            for (String type : entry.getValue().keySet()) {
                byte[] b =
                        reader.getBytesWithNameAndType(column, type)
                                .orElseThrow(RuntimeException::new);
                Assertions.assertThat(b).containsExactly(indexes.get(column).get(type));
            }
        }
    }

    @Test
    public void testEmptyFileIndex() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos, 1);

        Map<String, Map<String, byte[]>> indexes = new HashMap<>();

        indexes.computeIfAbsent("a", a -> new HashMap<>()).put("b", null);
        indexes.computeIfAbsent("a", a -> new HashMap<>()).put("c", null);

        writer.writeColumnIndexes(indexes);
        writer.close();

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
        Map<String, Map<String, byte[]>> indexes = new LinkedHashMap<>();
        indexes.computeIfAbsent("user_id", key -> new LinkedHashMap<>())
                .put("bitmap", new byte[] {1, 2, 3});
        indexes.computeIfAbsent("user_id", key -> new LinkedHashMap<>())
                .put("bloom-filter", new byte[] {4, 5});
        indexes.computeIfAbsent("region", key -> new LinkedHashMap<>()).put("bitmap", null);

        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos, 1)) {
            writer.writeColumnIndexes(indexes);
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
                        FileIndexFormat.FileIndexMeta::sizeInBytes,
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
        Map<String, byte[]> parts = new LinkedHashMap<>();
        for (int i = 0; i < 2049; i++) {
            parts.put(String.format("part%04d", i), payload);
        }
        BitmapFileIndex indexer = new BitmapFileIndex(DataTypes.INT(), new Options());
        FileIndexWriter bitmap = indexer.createWriter();
        bitmap.writeRecord(42);
        byte[] bitmapBytes = bitmap.serializedBytes();
        Map<String, Map<String, byte[]>> indexes = new LinkedHashMap<>();
        indexes.put("large", parts);
        indexes.put("f0", Collections.singletonMap("bitmap", bitmapBytes));
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, 2)) {
            writer.writeColumnIndexes(indexes);
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
    public void testV2RejectPayloadLengthOverInt32() throws Exception {
        byte[] bytes = writeSmallV2();
        // The only payload length is the last long in the footer, before the 12-byte trailer.
        ByteBuffer.wrap(bytes)
                .putLong(bytes.length - 12 - Long.BYTES, (long) Integer.MAX_VALUE + 1);
        try (FileIndexFormat.Reader reader =
                FileIndexFormat.createReader(
                        new ByteArraySeekableStream(bytes), ROW_TYPE, bytes.length)) {
            assertThat(reader.indexMetas().get(0).sizeInBytes())
                    .isEqualTo((long) Integer.MAX_VALUE + 1);
            assertThatThrownBy(reader::readAll)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("payload length exceeds int32");
            assertThatThrownBy(() -> reader.readColumnIndex("f0"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("payload length exceeds int32");
        }
    }

    @Test
    public void testV2RejectInvalidTailMagic() throws Exception {
        byte[] bytes = writeSmallV2();
        bytes[bytes.length - 1] = 0;
        assertThatThrownBy(
                        () ->
                                FileIndexFormat.createReader(
                                        new ByteArraySeekableStream(bytes), ROW_TYPE, bytes.length))
                .rootCause()
                .hasMessageContaining("Invalid file index tail magic");
    }

    private static byte[] writeSmallV2() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(bytes, 2)) {
            writer.writeColumnIndexes(
                    Collections.singletonMap(
                            "f0", Collections.singletonMap("bitmap", new byte[] {1, 2, 3})));
        }
        return bytes.toByteArray();
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
                    for (int i = 0; i < n; i++) {
                        b[off + i] = (byte) read();
                    }
                    return n;
                }

                @Override
                public void close() {}
            };
        }
    }
}
