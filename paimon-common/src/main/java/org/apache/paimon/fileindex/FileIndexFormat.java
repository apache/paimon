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
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

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

    /** Writes a single payload to the container output. */
    @FunctionalInterface
    public interface Payload {
        void writeTo(OutputStream output) throws IOException;
    }

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

    public static Writer createWriter(OutputStream outputStream, int version) throws IOException {
        if (version == Version.V_1.version()) {
            return new FileIndexFormatV1.Writer(outputStream);
        } else if (version == Version.V_2.version()) {
            return new FileIndexFormatV2.Writer(outputStream);
        } else {
            throw new IllegalArgumentException("Unsupported file index version: " + version);
        }
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
    public abstract static class Writer implements Closeable {

        /**
         * @deprecated Use {@link #writeIndex(String, String, Payload)} and {@link #finish()} to
         *     stream payloads into the container.
         */
        @Deprecated
        public void writeColumnIndexes(Map<String, Map<String, byte[]>> indexes)
                throws IOException {
            for (Map.Entry<String, Map<String, byte[]>> column : indexes.entrySet()) {
                for (Map.Entry<String, byte[]> index : column.getValue().entrySet()) {
                    byte[] payload = index.getValue();
                    writeIndex(
                            column.getKey(),
                            index.getKey(),
                            payload == null ? null : output -> output.write(payload));
                }
            }
            finish();
        }

        /** Writes one payload to the container. A null payload is empty. */
        public abstract void writeIndex(
                String columnName, String indexType, @Nullable Payload payload) throws IOException;

        /** Completes the container after all payloads have been written. */
        public abstract void finish() throws IOException;
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
                            startAndLength.getRight());
        }

        /**
         * Copies a stored payload without loading it into a byte array or decoding its plugin
         * format.
         */
        public void copyPayload(String columnName, String indexType, OutputStream output)
                throws IOException {
            Pair<Long, Long> startAndLength = indexEntries.get(columnName).get(indexType);
            seekableInputStream.seek(startAndLength.getLeft());
            byte[] buffer = new byte[8192];
            long remaining = startAndLength.getRight();
            while (remaining > 0) {
                int count =
                        seekableInputStream.read(
                                buffer, 0, (int) Math.min(remaining, buffer.length));
                if (count < 0) {
                    throw new EOFException();
                }
                output.write(buffer, 0, count);
                remaining -= count;
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(seekableInputStream);
        }
    }
}
