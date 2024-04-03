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
import org.apache.paimon.options.Options;
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

/**
 * File index file format. Put all column and offset in the header.
 *
 * <pre>
 * _______________________________________    _____________________
 * ｜     magic    ｜version｜head length ｜
 * ｜-------------------------------------｜
 * ｜   index type        ｜body info size｜
 * ｜-------------------------------------｜
 * ｜ column name 1 ｜start pos ｜length  ｜
 * ｜-------------------------------------｜            HEAD
 * ｜ column name 2 ｜start pos ｜length  ｜
 * ｜-------------------------------------｜
 * ｜ column name 3 ｜start pos ｜length  ｜
 * ｜-------------------------------------｜
 * ｜                 ...                 ｜
 * ｜-------------------------------------｜
 * ｜                 ...                 ｜
 * ｜-------------------------------------｜
 * ｜  redundant length ｜redundant bytes ｜
 * ｜-------------------------------------｜    ---------------------
 * ｜                BODY                 ｜
 * ｜                BODY                 ｜
 * ｜                BODY                 ｜             BODY
 * ｜                BODY                 ｜
 * ｜_____________________________________｜    _____________________
 *
 * magic:                            8 bytes long
 * version:                          4 bytes int
 * head length:                      4 bytes int
 * index type:                       var bytes utf (length + bytes)
 * body info size:                   4 bytes int (how many column items below)
 * column name:                      var bytes utf
 * start pos:                        4 bytes int
 * length:                           4 bytes int
 * redundant length:                 4 bytes int (for compatibility with later versions, in this version, content is zero)
 * redundant bytes:                  var bytes (for compatibility with later version, in this version, is empty)
 * BODY:                             column bytes + column bytes + column bytes + .......
 *
 * </pre>
 */
public final class FileIndexFormat {

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

            // construct body
            ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
            for (Map.Entry<String, byte[]> entry : bytesMap.entrySet()) {
                int startPosition = baos.size();
                baos.write(entry.getValue());
                bodyInfo.put(entry.getKey(), Pair.of(startPosition, baos.size() - startPosition));
            }
            byte[] body = baos.toByteArray();

            writeHead(indexType, bodyInfo);

            // writeBody
            dataOutputStream.write(body);
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
                dataOutputStream.writeInt(entry.getValue().getRight());
            }
            // writeRedundantLength
            dataOutputStream.writeInt(REDUNDANT_LENGTH);
        }

        private int calculateHeadLength(
                String indexType, Map<String, Pair<Integer, Integer>> bodyInfo) throws IOException {
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
                            serializedBytes ->
                                    FileIndexer.create(
                                                    type,
                                                    fields.get(columnName).type(),
                                                    new Options())
                                            .createReader(serializedBytes))
                    .orElse(null);
        }

        @VisibleForTesting
        Optional<byte[]> readColumnInputStream(String columnName) {
            return Optional.ofNullable(header.getOrDefault(columnName, null))
                    .map(
                            startAndLength -> {
                                byte[] b = new byte[startAndLength.getRight()];
                                try {
                                    seekableInputStream.seek(startAndLength.getLeft());
                                    int n = 0;
                                    int len = b.length;
                                    // read fully until b is full else throw.
                                    while (n < len) {
                                        int count = seekableInputStream.read(b, n, len - n);
                                        if (count < 0) {
                                            throw new EOFException();
                                        }
                                        n += count;
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                return b;
                            });
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(seekableInputStream);
        }
    }
}
