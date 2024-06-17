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
import org.apache.paimon.fileindex.empty.EmptyFileIndexReader;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * File index file format. Put all column and offset in the header.
 *
 * <pre>
 *   _____________________________________    _____________________
 * ｜     magic    ｜version｜head length ｜
 * ｜-------------------------------------｜
 * ｜            column number            ｜
 * ｜-------------------------------------｜
 * ｜   column 1        ｜ index number   ｜
 * ｜-------------------------------------｜
 * ｜  index name 1 ｜start pos ｜length  ｜
 * ｜-------------------------------------｜
 * ｜  index name 2 ｜start pos ｜length  ｜
 * ｜-------------------------------------｜
 * ｜  index name 3 ｜start pos ｜length  ｜
 * ｜-------------------------------------｜            HEAD
 * ｜   column 2        ｜ index number   ｜
 * ｜-------------------------------------｜
 * ｜  index name 1 ｜start pos ｜length  ｜
 * ｜-------------------------------------｜
 * ｜  index name 2 ｜start pos ｜length  ｜
 * ｜-------------------------------------｜
 * ｜  index name 3 ｜start pos ｜length  ｜
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
 * column number:                    4 bytes int
 * column x:                         var bytes utf (length + bytes)
 * index number:                     4 bytes int (how many column items below)
 * index name x:                     var bytes utf
 * start pos:                        4 bytes int
 * length:                           4 bytes int
 * redundant length:                 4 bytes int (for compatibility with later versions, in this version, content is zero)
 * redundant bytes:                  var bytes (for compatibility with later version, in this version, is empty)
 * BODY:                             column index bytes + column index bytes + column index bytes + .......
 *
 * </pre>
 */
public final class FileIndexFormat {

    private static final long MAGIC = 1493475289347502L;
    private static final int EMPTY_INDEX_FLAG = -1;

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

        public void writeColumnIndexes(Map<String, Map<String, byte[]>> indexes)
                throws IOException {

            Map<String, Map<String, Pair<Integer, Integer>>> bodyInfo = new HashMap<>();

            // construct body
            ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
            for (Map.Entry<String, Map<String, byte[]>> columnMap : indexes.entrySet()) {
                Map<String, Pair<Integer, Integer>> innerMap =
                        bodyInfo.computeIfAbsent(columnMap.getKey(), k -> new HashMap<>());
                Map<String, byte[]> bytesMap = columnMap.getValue();
                for (Map.Entry<String, byte[]> entry : bytesMap.entrySet()) {
                    int startPosition = baos.size();
                    byte[] v = entry.getValue();
                    if (v == null) {
                        innerMap.put(entry.getKey(), Pair.of(EMPTY_INDEX_FLAG, 0));
                    } else {
                        baos.write(entry.getValue());
                        innerMap.put(
                                entry.getKey(),
                                Pair.of(startPosition, baos.size() - startPosition));
                    }
                }
            }
            byte[] body = baos.toByteArray();
            writeHead(bodyInfo);

            // writeBody
            dataOutputStream.write(body);
        }

        private void writeHead(Map<String, Map<String, Pair<Integer, Integer>>> bodyInfo)
                throws IOException {

            int headLength = calculateHeadLength(bodyInfo);

            // writeMagic
            dataOutputStream.writeLong(MAGIC);
            // writeVersion
            dataOutputStream.writeInt(Version.V_1.version());
            // writeHeadLength
            dataOutputStream.writeInt(headLength);
            // writeColumnSize
            dataOutputStream.writeInt(bodyInfo.size());
            for (Map.Entry<String, Map<String, Pair<Integer, Integer>>> entry :
                    bodyInfo.entrySet()) {
                // writeColumnName
                dataOutputStream.writeUTF(entry.getKey());
                // writeIndexTypeSize
                dataOutputStream.writeInt(entry.getValue().size());
                // writeColumnInfo, offset = headLength
                for (Map.Entry<String, Pair<Integer, Integer>> indexEntry :
                        entry.getValue().entrySet()) {
                    dataOutputStream.writeUTF(indexEntry.getKey());
                    int start = indexEntry.getValue().getLeft();
                    dataOutputStream.writeInt(
                            start == EMPTY_INDEX_FLAG ? EMPTY_INDEX_FLAG : start + headLength);
                    dataOutputStream.writeInt(indexEntry.getValue().getRight());
                }
            }
            // writeRedundantLength
            dataOutputStream.writeInt(REDUNDANT_LENGTH);
        }

        private int calculateHeadLength(Map<String, Map<String, Pair<Integer, Integer>>> bodyInfo)
                throws IOException {
            // magic 8 bytes, version 4 bytes, head length 4 bytes,
            // column size 4 bytes, body info start&end 8 bytes per
            // column-index, index type size 4 bytes per column, redundant length 4 bytes;
            int baseLength =
                    8
                            + 4
                            + 4
                            + 4
                            + bodyInfo.values().stream().mapToInt(Map::size).sum() * 8
                            + bodyInfo.size() * 4
                            + 4;

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput dataOutput = new DataOutputStream(baos);
            for (Map.Entry<String, Map<String, Pair<Integer, Integer>>> entry :
                    bodyInfo.entrySet()) {
                dataOutput.writeUTF(entry.getKey());
                for (String s : entry.getValue().keySet()) {
                    dataOutput.writeUTF(s);
                }
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
        private final Map<String, Map<String, Pair<Integer, Integer>>> header = new HashMap<>();
        private final Map<String, DataField> fields = new HashMap<>();

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
                    int columnSize = dataInput.readInt();
                    for (int i = 0; i < columnSize; i++) {
                        String columnName = dataInput.readUTF();
                        int indexSize = dataInput.readInt();
                        Map<String, Pair<Integer, Integer>> indexMap =
                                this.header.computeIfAbsent(columnName, n -> new HashMap<>());
                        for (int j = 0; j < indexSize; j++) {
                            indexMap.put(
                                    dataInput.readUTF(),
                                    Pair.of(dataInput.readInt(), dataInput.readInt()));
                        }
                    }
                }
            } catch (IOException e) {
                IOUtils.closeQuietly(seekableInputStream);
                throw new RuntimeException(
                        "Exception happens while construct file index reader.", e);
            }
        }

        public Set<FileIndexReader> readColumnIndex(String columnName) {
            return Optional.ofNullable(header.getOrDefault(columnName, null))
                    .map(
                            f ->
                                    f.entrySet().stream()
                                            .map(
                                                    entry ->
                                                            getFileIndexReader(
                                                                    columnName,
                                                                    entry.getKey(),
                                                                    entry.getValue()))
                                            .collect(Collectors.toSet()))
                    .orElse(Collections.emptySet());
        }

        private FileIndexReader getFileIndexReader(
                String columnName, String indexType, Pair<Integer, Integer> startAndLength) {
            if (startAndLength.getLeft() == EMPTY_INDEX_FLAG) {
                return EmptyFileIndexReader.INSTANCE;
            }
            return FileIndexer.create(
                            indexType,
                            FileIndexCommon.getFieldType(fields, columnName),
                            new Options())
                    .createReader(
                            seekableInputStream,
                            startAndLength.getLeft(),
                            startAndLength.getRight());
        }

        private byte[] getBytesWithStartAndLength(Pair<Integer, Integer> startAndLength) {
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
        }

        @VisibleForTesting
        // only for test yet
        Optional<byte[]> getBytesWithNameAndType(String columnName, String indexType) {
            return Optional.ofNullable(header.getOrDefault(columnName, null))
                    .map(i -> i.getOrDefault(indexType, null))
                    .map(this::getBytesWithStartAndLength);
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(seekableInputStream);
        }
    }
}
