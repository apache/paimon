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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** File index file format. Put all column and offset in the header. */
public final class FileIndexFile {

    private static final long MAGIC = 1493475289347502L;

    enum Version {
        V_1(1);

        private final int version;

        Version(int version) {
            this.version = version;
        }

        public int version() {
            return version;
        }
    }

    public static Writer createWriter(OutputStream outputStream) {
        return new Writer(outputStream);
    }

    public static Reader createReader(SeekableInputStream inputStream, RowType fileRowType) {
        return new Reader(inputStream, fileRowType);
    }

    /** Writer for file index file. */
    public static class Writer implements Closeable {

        private final DataOutputStream dataOutputStream;

        // for version compatible
        private static final int REDUNDANT_LENGTH = 0;

        public Writer(OutputStream outputStream) {
            this.dataOutputStream = new DataOutputStream(outputStream);
        }

        public void writeColumnIndex(String indexType, Map<String, byte[]> bytesMap)
                throws IOException {

            Map<String, Pair<Integer, Integer>> bodyInfo = new HashMap<>();
            byte[] body = constructBody(bytesMap, bodyInfo);

            writeHead(indexType, bodyInfo);

            // writeBody
            dataOutputStream.write(body);
        }

        private byte[] constructBody(
                Map<String, byte[]> bytesMap, Map<String, Pair<Integer, Integer>> bodyInfo)
                throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            for (Map.Entry<String, byte[]> entry : bytesMap.entrySet()) {
                Integer startPosition = baos.size();
                baos.write(entry.getValue());
                bodyInfo.put(entry.getKey(), Pair.of(startPosition, baos.size() - 1));
            }

            return baos.toByteArray();
        }

        private void writeHead(String indexType, Map<String, Pair<Integer, Integer>> bodyInfo)
                throws IOException {

            int headLength = calculateHeadLength(indexType, bodyInfo);

            // writeMagic
            dataOutputStream.writeLong(MAGIC);
            // writeVersion
            dataOutputStream.writeInt(Version.V_1.version());
            // writeHeadLength
            dataOutputStream.writeInt(headLength);
            // writeIndexType
            dataOutputStream.writeUTF(indexType);
            // writeColumnSize
            dataOutputStream.writeInt(bodyInfo.size());
            // writeColumnInfo, offset = headLength
            for (Map.Entry<String, Pair<Integer, Integer>> entry : bodyInfo.entrySet()) {
                dataOutputStream.writeUTF(entry.getKey());
                dataOutputStream.writeInt(entry.getValue().getLeft() + headLength);
                dataOutputStream.writeInt(entry.getValue().getRight() + headLength);
            }
            // writeRedundantLength
            dataOutputStream.writeInt(REDUNDANT_LENGTH);
        }

        int calculateHeadLength(String indexType, Map<String, Pair<Integer, Integer>> bodyInfo)
                throws IOException {
            // magic 8 bytes, version 4 bytes, head length 4 bytes,
            // column size 4 bytes, body info start&end 8 bytes per
            // item, redundant length 4 bytes;
            int baseLength = 8 + 4 + 4 + 4 + bodyInfo.size() * 8 + 4;

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput dataOutput = new DataOutputStream(baos);
            dataOutput.writeUTF(indexType);
            for (String s : bodyInfo.keySet()) {
                dataOutput.writeUTF(s);
            }

            return baseLength + baos.size();
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(dataOutputStream);
        }
    }

    /** Reader for file index file. */
    public static class Reader implements Closeable {

        private final SeekableInputStream seekableInputStream;
        // get header and cache it.
        private final Map<String, Pair<Integer, Integer>> header = new HashMap<>();
        private final Map<String, DataField> fields = new HashMap<>();
        private final String type;

        public Map<String, Pair<Integer, Integer>> getHeader() {
            return header;
        }

        public Reader(SeekableInputStream seekableInputStream, RowType fileRowType) {
            this.seekableInputStream = seekableInputStream;
            DataInputStream dataInputStream = new DataInputStream(seekableInputStream);
            fileRowType.getFields().forEach(field -> this.fields.put(field.name(), field));
            try {
                long magic = dataInputStream.readLong();
                if (magic != MAGIC) {
                    throw new RuntimeException("This file is not file index file.");
                }

                int version = dataInputStream.readInt();
                if (version != Version.V_1.version()) {
                    throw new RuntimeException(
                            "This index file is version of "
                                    + version
                                    + ", not in supported version list ["
                                    + Version.V_1.version()
                                    + "]");
                }

                int headLength = dataInputStream.readInt();
                byte[] head = new byte[headLength - 8 - 4 - 4];
                dataInputStream.readFully(head);

                try (DataInputStream dataInput =
                        new DataInputStream(new ByteArrayInputStream(head))) {
                    this.type = dataInput.readUTF();
                    int columnSize = dataInput.readInt();
                    for (int i = 0; i < columnSize; i++) {
                        this.header.put(
                                dataInput.readUTF(),
                                Pair.of(dataInput.readInt(), dataInput.readInt()));
                    }
                }

            } catch (IOException e) {
                IOUtils.closeQuietly(seekableInputStream);
                throw new RuntimeException(
                        "Exception happens while construct file index reader.", e);
            }
        }

        public FileIndexReader readColumnIndex(String columnName) {

            return readColumnInputStream(columnName)
                    .map(
                            inputStream ->
                                    FileIndexer.create(type, fields.get(columnName).type())
                                            .createReader()
                                            .recoverFrom(inputStream))
                    .orElse(null);
        }

        @VisibleForTesting
        Optional<SeekableInputStream> readColumnInputStream(String columnName) {
            return Optional.ofNullable(header.getOrDefault(columnName, null))
                    .map(
                            startEnd ->
                                    new BytesInputWrapper(
                                            seekableInputStream,
                                            startEnd.getLeft(),
                                            startEnd.getRight()))
                    .map(
                            stream -> {
                                try {
                                    return stream.resetHead();
                                } catch (IOException e) {
                                    throw new RuntimeException(
                                            "Error happens while read column from index file.", e);
                                }
                            });
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(seekableInputStream);
        }
    }

    /** Wrap the origin stream with offset start and end. */
    static class BytesInputWrapper extends SeekableInputStream {

        private final SeekableInputStream originStream;
        // include
        private final int offsetStart;
        // include
        private final int offsetEnd;

        private int position;

        public BytesInputWrapper(SeekableInputStream originStream, int offsetStart, int offsetEnd) {
            this.originStream = originStream;
            this.offsetStart = offsetStart;
            this.position = offsetStart;
            this.offsetEnd = offsetEnd;
        }

        @Override
        public int read() throws IOException {
            if (remain() == 0) {
                throw new EOFException();
            }
            position++;
            return originStream.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remain() == 0) {
                throw new EOFException();
            }
            int read = originStream.read(b, off, Math.min(len, remain()));
            position += read;
            return read;
        }

        @Override
        public void seek(long desired) throws IOException {
            if (offsetStart + desired > offsetEnd) {
                throw new EOFException();
            }
            originStream.seek(desired + offsetStart);
            position = (int) desired + offsetStart;
        }

        @Override
        public long getPos() throws IOException {
            return position - offsetStart;
        }

        @Override
        public long skip(long n) throws IOException {
            if (n <= 0) {
                throw new IllegalArgumentException("skip bytes should greater than 0.");
            }
            if (remain() == 0) {
                throw new EOFException();
            }
            int skipped = (int) originStream.skip(Math.min(n, remain()));
            position += skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return remain();
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }

        public BytesInputWrapper resetHead() throws IOException {
            if (originStream.getPos() != offsetStart) {
                originStream.seek(offsetStart);
            }
            return this;
        }

        private int remain() {
            return offsetEnd - position + 1;
        }
    }
}
