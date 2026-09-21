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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.fileindex.FileIndexFormatUtils.EMPTY_INDEX_FLAG;
import static org.apache.paimon.fileindex.FileIndexFormatUtils.MAGIC;
import static org.apache.paimon.fileindex.FileIndexFormatUtils.VERSION_1;
import static org.apache.paimon.fileindex.FileIndexFormatUtils.VERSION_2;

/** Version-dispatching entry point and shared payload access for file index containers. */
public final class FileIndexFormat {

    enum Version {
        V_1(VERSION_1),
        V_2(VERSION_2);

        private final int version;

        Version(int version) {
            this.version = version;
        }

        public int version() {
            return version;
        }
    }

    public static Writer createWriter(OutputStream outputStream, int version) {
        return new Writer(outputStream, version);
    }

    public static Reader createReader(
            SeekableInputStream inputStream, RowType fileRowType, long length) {
        return new Reader(inputStream, fileRowType, length);
    }

    /** Creates a reader for accessing index metadata without reading index payloads. */
    public static Reader createMetadataReader(SeekableInputStream inputStream, long length) {
        return createReader(inputStream, RowType.builder().build(), length);
    }

    /** Metadata of one column index stored in a file index container. */
    public static class FileIndexMeta {

        private final String columnName;
        private final String indexType;
        private final long sizeInBytes;
        private final boolean empty;

        private FileIndexMeta(
                String columnName, String indexType, long sizeInBytes, boolean empty) {
            this.columnName = columnName;
            this.indexType = indexType;
            this.sizeInBytes = sizeInBytes;
            this.empty = empty;
        }

        public String columnName() {
            return columnName;
        }

        public String indexType() {
            return indexType;
        }

        public long sizeInBytes() {
            return sizeInBytes;
        }

        public boolean empty() {
            return empty;
        }
    }

    /** Writer for file index file. */
    public static class Writer implements Closeable {

        private final FileIndexFormatUtils.FormatWriter writer;

        private Writer(OutputStream outputStream, int version) {
            if (version == Version.V_1.version()) {
                this.writer = new FileIndexFormatV1.Writer(outputStream);
            } else if (version == Version.V_2.version()) {
                this.writer = new FileIndexFormatV2.Writer(outputStream);
            } else {
                throw new IllegalArgumentException("Unsupported file index version: " + version);
            }
        }

        public void writeColumnIndexes(Map<String, Map<String, byte[]>> indexes)
                throws IOException {
            writer.writeColumnIndexes(indexes);
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    /** Reader for file index file. */
    public static class Reader implements Closeable {

        private final SeekableInputStream seekableInputStream;
        // Cache the index entries.
        private final Map<String, Map<String, Pair<Long, Long>>> indexEntries = new HashMap<>();
        private final Map<String, DataField> fields = new HashMap<>();

        private Reader(SeekableInputStream seekableInputStream, RowType fileRowType, long length) {
            this.seekableInputStream = seekableInputStream;
            DataInputStream dataInputStream = new DataInputStream(seekableInputStream);
            fileRowType.getFields().forEach(field -> this.fields.put(field.name(), field));
            try {
                long magic = dataInputStream.readLong();
                if (magic != MAGIC) {
                    throw new RuntimeException("This file is not file index file.");
                }

                int version = dataInputStream.readInt();
                if (version == Version.V_2.version()) {
                    indexEntries.putAll(
                            FileIndexFormatV2.readIndexEntries(seekableInputStream, length));
                } else if (version == Version.V_1.version()) {
                    indexEntries.putAll(FileIndexFormatV1.readIndexEntries(dataInputStream));
                } else {
                    throw new RuntimeException(
                            "This index file is version of "
                                    + version
                                    + ", not in supported version list ["
                                    + Version.V_1.version()
                                    + ", "
                                    + Version.V_2.version()
                                    + "]");
                }
            } catch (IOException | RuntimeException e) {
                // Callers wrap the constructor in try-with-resources on the stream,
                // but a throwing constructor never assigns the resource, so both
                // checked and unchecked validation failures must close here.
                IOUtils.closeQuietly(seekableInputStream);
                throw new RuntimeException(
                        "Exception happens while construct file index reader.", e);
            }
        }

        public Set<FileIndexReader> readColumnIndex(String columnName) {
            return Optional.ofNullable(indexEntries.getOrDefault(columnName, null))
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

        /** Returns the parsed index metadata without reading index payloads. */
        public List<FileIndexMeta> indexMetas() {
            List<FileIndexMeta> metas = new ArrayList<>();
            for (Map.Entry<String, Map<String, Pair<Long, Long>>> columnEntry :
                    indexEntries.entrySet()) {
                for (Map.Entry<String, Pair<Long, Long>> indexEntry :
                        columnEntry.getValue().entrySet()) {
                    Pair<Long, Long> startAndLength = indexEntry.getValue();
                    metas.add(
                            new FileIndexMeta(
                                    columnEntry.getKey(),
                                    indexEntry.getKey(),
                                    startAndLength.getRight(),
                                    startAndLength.getLeft() == EMPTY_INDEX_FLAG));
                }
            }
            return Collections.unmodifiableList(metas);
        }

        private FileIndexReader getFileIndexReader(
                String columnName, String indexType, Pair<Long, Long> startAndLength) {
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
                            checkedPayloadLength(startAndLength.getRight()));
        }

        private byte[] getBytesWithStartAndLength(Pair<Long, Long> startAndLength) {
            byte[] b = new byte[checkedPayloadLength(startAndLength.getRight())];
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

        // TODO: support 64-bit payload length in version 2, and remove this method.
        private static int checkedPayloadLength(long length) {
            if (length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "File index payload length exceeds int32: " + length);
            }
            return (int) length;
        }

        public Map<String, Map<String, byte[]>> readAll() {
            Map<String, Map<String, byte[]>> result = new HashMap<>();
            for (Map.Entry<String, Map<String, Pair<Long, Long>>> entryOuter :
                    indexEntries.entrySet()) {
                for (Map.Entry<String, Pair<Long, Long>> entryInner :
                        entryOuter.getValue().entrySet()) {
                    result.computeIfAbsent(entryOuter.getKey(), key -> new HashMap<>())
                            .put(
                                    entryInner.getKey(),
                                    getBytesWithStartAndLength(entryInner.getValue()));
                }
            }
            return result;
        }

        @VisibleForTesting
        // only for test yet
        Optional<byte[]> getBytesWithNameAndType(String columnName, String indexType) {
            return Optional.ofNullable(indexEntries.getOrDefault(columnName, null))
                    .map(i -> i.getOrDefault(indexType, null))
                    .map(this::getBytesWithStartAndLength);
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(seekableInputStream);
        }
    }
}
