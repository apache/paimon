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
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
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
    public void testReaderClosesInputStreamOnBadMagic() {
        // first 8 bytes are not MAGIC, so the reader throws a RuntimeException from within the
        // constructor's try block before the resource is returned to the caller.
        CloseRecordingSeekableStream inputStream = new CloseRecordingSeekableStream(new byte[16]);

        Assertions.assertThatThrownBy(
                        () -> FileIndexFormat.createReader(inputStream, RowType.builder().build()))
                .isInstanceOf(RuntimeException.class);
        Assertions.assertThat(inputStream.isClosed()).isTrue();
    }

    @Test
    public void testReaderClosesInputStreamOnUnsupportedVersion() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos);
        writer.writeColumnIndexes(new HashMap<>());
        writer.close();

        // keep the valid MAGIC (bytes 0-7) but corrupt the version int (bytes 8-11) so the reader
        // throws a RuntimeException after the magic check but still inside the try block.
        byte[] indexBytes = baos.toByteArray();
        indexBytes[8] = 0x7f;
        indexBytes[9] = 0x7f;
        indexBytes[10] = 0x7f;
        indexBytes[11] = 0x7f;

        CloseRecordingSeekableStream inputStream = new CloseRecordingSeekableStream(indexBytes);

        Assertions.assertThatThrownBy(
                        () -> FileIndexFormat.createReader(inputStream, RowType.builder().build()))
                .isInstanceOf(RuntimeException.class);
        Assertions.assertThat(inputStream.isClosed()).isTrue();
    }

    /**
     * A {@link SeekableInputStream} that records whether {@link #close()} was called and delegates
     * reads to a backing {@link ByteArraySeekableStream} (whose own {@code close()} is a no-op).
     */
    private static class CloseRecordingSeekableStream extends SeekableInputStream {

        private final ByteArraySeekableStream delegate;
        private boolean closed = false;

        private CloseRecordingSeekableStream(byte[] bytes) {
            this.delegate = new ByteArraySeekableStream(bytes);
        }

        private boolean isClosed() {
            return closed;
        }

        @Override
        public void seek(long desired) throws IOException {
            delegate.seek(desired);
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            delegate.close();
        }
    }
}
