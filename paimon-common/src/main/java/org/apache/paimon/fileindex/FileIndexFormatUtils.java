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

import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.Pair;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Utilities shared by file index container versions. */
final class FileIndexFormatUtils {

    static final long MAGIC = 1493475289347502L;
    static final int EMPTY_INDEX_FLAG = -1;
    static final int VERSION_1 = 1;
    static final int VERSION_2 = 2;

    private FileIndexFormatUtils() {}

    /** Internal contract implemented by each container version. */
    interface FormatWriter extends Closeable {

        void writeColumnIndexes(Map<String, Map<String, byte[]>> indexes) throws IOException;
    }

    interface IndexPayloadWriter {

        Pair<Long, Long> write(byte[] bytes) throws IOException;
    }

    static void writeMagicAndVersion(DataOutput output, int version) throws IOException {
        // writeMagic
        output.writeLong(MAGIC);
        // writeVersion
        output.writeInt(version);
    }

    static Map<String, Map<String, Pair<Long, Long>>> readIndexEntries(DataInput input, int version)
            throws IOException {
        Map<String, Map<String, Pair<Long, Long>>> indexEntries = new HashMap<>();
        int columnSize = input.readInt();
        for (int i = 0; i < columnSize; i++) {
            String columnName = input.readUTF();
            int indexSize = input.readInt();
            Map<String, Pair<Long, Long>> entries =
                    indexEntries.computeIfAbsent(columnName, ignored -> new HashMap<>());
            for (int j = 0; j < indexSize; j++) {
                String indexType = input.readUTF();
                Pair<Long, Long> startAndLength =
                        version == VERSION_1
                                ? Pair.of((long) input.readInt(), (long) input.readInt())
                                : Pair.of(input.readLong(), input.readLong());
                entries.put(indexType, startAndLength);
            }
        }
        return indexEntries;
    }

    static void writeIndexEntries(
            DataOutput output,
            Map<String, Map<String, Pair<Long, Long>>> indexEntries,
            int version,
            long startOffset)
            throws IOException {
        // writeColumnSize
        output.writeInt(indexEntries.size());
        for (Map.Entry<String, Map<String, Pair<Long, Long>>> column : indexEntries.entrySet()) {
            // writeColumnName
            output.writeUTF(column.getKey());
            // writeIndexTypeSize
            output.writeInt(column.getValue().size());
            for (Map.Entry<String, Pair<Long, Long>> index : column.getValue().entrySet()) {
                output.writeUTF(index.getKey());
                long start = index.getValue().getLeft();
                // Keep EMPTY_INDEX_FLAG unchanged when converting a relative start position.
                start = start == EMPTY_INDEX_FLAG ? EMPTY_INDEX_FLAG : start + startOffset;
                if (version == VERSION_1) {
                    output.writeInt((int) start);
                    output.writeInt(index.getValue().getRight().intValue());
                } else {
                    output.writeLong(start);
                    output.writeLong(index.getValue().getRight());
                }
            }
        }
    }

    static void writeIndexPayloads(
            Map<String, Map<String, byte[]>> indexes,
            Map<String, Map<String, Pair<Long, Long>>> indexEntries,
            IndexPayloadWriter payloadWriter)
            throws IOException {
        for (Map.Entry<String, Map<String, byte[]>> column : indexes.entrySet()) {
            Map<String, Pair<Long, Long>> entries =
                    indexEntries.computeIfAbsent(column.getKey(), ignored -> new LinkedHashMap<>());
            for (Map.Entry<String, byte[]> index : column.getValue().entrySet()) {
                // Empty indexes have no payload and use EMPTY_INDEX_FLAG as their start position.
                entries.put(
                        index.getKey(),
                        index.getValue() == null
                                ? Pair.of((long) EMPTY_INDEX_FLAG, 0L)
                                : payloadWriter.write(index.getValue()));
            }
        }
    }

    static class CountingPositionOutputStream extends PositionOutputStream {

        private final OutputStream output;
        private long position;

        CountingPositionOutputStream(OutputStream output) {
            this.output = output;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void write(int value) throws IOException {
            long next = Math.addExact(position, 1L);
            output.write(value);
            position = next;
        }

        @Override
        public void write(byte[] bytes) throws IOException {
            write(bytes, 0, bytes.length);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            long next = Math.addExact(position, length);
            output.write(bytes, offset, length);
            position = next;
        }

        @Override
        public void flush() throws IOException {
            output.flush();
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }
}
