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

import org.apache.paimon.fileindex.empty.EmptyFileIndexReader;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.paimon.utils.RandomUtil.randomBytes;
import static org.apache.paimon.utils.RandomUtil.randomString;

/** Test for {@link FileIndexFormat}. */
public class FileIndexFormatFormatTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testWriteRead() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos);

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
                        new ByteArraySeekableStream(indexBytes), RowType.builder().build());

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
        FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos);

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
                                .build());

        Collection<FileIndexReader> fileIndexFormatList = reader.readColumnIndex("a");
        Assertions.assertThat(fileIndexFormatList.size()).isEqualTo(1);
        Assertions.assertThat(new ArrayList<>(fileIndexFormatList).get(0))
                .isEqualTo(EmptyFileIndexReader.INSTANCE);
    }

    @Test
    public void testReaderClosesStreamOnBadMagic() throws IOException {
        byte[] indexBytes = validIndexBytes();
        // overwrite the magic
        ByteBuffer.wrap(indexBytes).putLong(0, 0L);
        CloseTrackingSeekableStream stream = new CloseTrackingSeekableStream(indexBytes);

        Assertions.assertThatThrownBy(() -> createReader(stream))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("This file is not file index file.");
        Assertions.assertThat(stream.closeCount()).isEqualTo(1);
    }

    @Test
    public void testReaderClosesStreamOnUnsupportedVersion() throws IOException {
        byte[] indexBytes = validIndexBytes();
        // overwrite the version, which follows the 8 bytes long magic
        ByteBuffer.wrap(indexBytes).putInt(8, 2);
        CloseTrackingSeekableStream stream = new CloseTrackingSeekableStream(indexBytes);

        Assertions.assertThatThrownBy(() -> createReader(stream))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("This index file is version of 2");
        Assertions.assertThat(stream.closeCount()).isEqualTo(1);
    }

    @Test
    public void testReaderClosesStreamOnCorruptedHeadLength() throws IOException {
        byte[] indexBytes = validIndexBytes();
        // overwrite the head length, which follows the magic and the version
        ByteBuffer.wrap(indexBytes).putInt(12, 0);
        CloseTrackingSeekableStream stream = new CloseTrackingSeekableStream(indexBytes);

        // a corrupted head length fails outside the IOException path
        Assertions.assertThatThrownBy(() -> createReader(stream))
                .isInstanceOf(NegativeArraySizeException.class);
        Assertions.assertThat(stream.closeCount()).isEqualTo(1);
    }

    @Test
    public void testReaderClosesStreamOnTruncatedHead() throws IOException {
        byte[] indexBytes = validIndexBytes();
        Assertions.assertThat(indexBytes.length).isGreaterThan(20);
        CloseTrackingSeekableStream stream =
                new CloseTrackingSeekableStream(Arrays.copyOf(indexBytes, 20));

        Assertions.assertThatThrownBy(() -> createReader(stream))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Exception happens while construct file index reader.")
                .hasCauseInstanceOf(EOFException.class);
        Assertions.assertThat(stream.closeCount()).isEqualTo(1);
    }

    @Test
    public void testReaderKeepsStreamOpenOnSuccess() throws IOException {
        CloseTrackingSeekableStream stream = new CloseTrackingSeekableStream(validIndexBytes());

        FileIndexFormat.Reader reader = createReader(stream);
        Assertions.assertThat(stream.closeCount()).isEqualTo(0);

        reader.close();
        Assertions.assertThat(stream.closeCount()).isEqualTo(1);
    }

    private static FileIndexFormat.Reader createReader(CloseTrackingSeekableStream stream) {
        return FileIndexFormat.createReader(stream, RowType.builder().build());
    }

    private static byte[] validIndexBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Map<String, Map<String, byte[]>> indexes = new HashMap<>();
        indexes.computeIfAbsent("a", a -> new HashMap<>()).put("bloom", randomBytes(64));
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos)) {
            writer.writeColumnIndexes(indexes);
        }
        return baos.toByteArray();
    }

    private static class CloseTrackingSeekableStream extends ByteArraySeekableStream {

        private int closeCount;

        private CloseTrackingSeekableStream(byte[] buf) {
            super(buf);
        }

        private int closeCount() {
            return closeCount;
        }

        @Override
        public void close() throws IOException {
            closeCount++;
            super.close();
        }
    }
}
