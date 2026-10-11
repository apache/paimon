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

import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.fileindex.FileIndexFormatUtils.VERSION_1;

/**
 * V1 file index container: header followed by payloads.
 *
 * <pre>
 *  ______________________________________    _____________________
 * |     magic    | version| head length  |
 * |--------------------------------------|
 * |            column number             |
 * |--------------------------------------|
 * |   column 1        |  index number    |
 * |--------------------------------------|
 * |  index name 1 | start pos | length   |
 * |--------------------------------------|
 * |  index name 2 | start pos | length   |
 * |--------------------------------------|
 * |  index name 3 | start pos | length   |
 * |--------------------------------------|            HEAD
 * |   column 2        |  index number    |
 * |--------------------------------------|
 * |  index name 1 | start pos | length   |
 * |--------------------------------------|
 * |  index name 2 | start pos | length   |
 * |--------------------------------------|
 * |  index name 3 | start pos | length   |
 * |--------------------------------------|
 * |                 ...                  |
 * |--------------------------------------|
 * |                 ...                  |
 * |--------------------------------------|
 * |  redundant length | redundant bytes  |
 * |--------------------------------------|    ---------------------
 * |                BODY                  |
 * |                BODY                  |
 * |                BODY                  |             BODY
 * |                BODY                  |
 * |______________________________________|    _____________________
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
final class FileIndexFormatV1 {

    private FileIndexFormatV1() {}

    /** Reads the header after the common magic and version have been consumed. */
    static Map<String, Map<String, Pair<Long, Long>>> readIndexEntries(
            DataInputStream dataInputStream) throws IOException {
        int headLength = dataInputStream.readInt();
        byte[] head = new byte[headLength - 8 - 4 - 4];
        dataInputStream.readFully(head);
        try (DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(head))) {
            return FileIndexFormatUtils.readIndexEntries(dataInput, VERSION_1);
        }
    }

    /** V1 writer buffers payloads to compute the preceding header. */
    static final class Writer extends FileIndexFormat.Writer {

        private final DataOutputStream dataOutputStream;
        private final ByteArrayOutputStream body = new ByteArrayOutputStream(256);
        private final Map<String, Map<String, Pair<Long, Long>>> indexEntries =
                new LinkedHashMap<>();

        // for version compatible
        private static final int REDUNDANT_LENGTH = 0;

        Writer(OutputStream outputStream) {
            this.dataOutputStream = new DataOutputStream(outputStream);
        }

        @Override
        public void writeIndex(
                String columnName, String indexType, @Nullable FileIndexFormat.Payload payload)
                throws IOException {
            Map<String, Pair<Long, Long>> column =
                    indexEntries.computeIfAbsent(columnName, ignored -> new LinkedHashMap<>());
            if (payload == null) {
                // Empty indexes have no payload and use EMPTY_INDEX_FLAG as their start position.
                column.put(indexType, Pair.of((long) FileIndexFormatUtils.EMPTY_INDEX_FLAG, 0L));
            } else {
                int start = body.size();
                payload.writeTo(body);
                column.put(indexType, Pair.of((long) start, (long) body.size() - start));
            }
        }

        @Override
        public void finish() throws IOException {
            writeHead(indexEntries);

            // writeBody
            body.writeTo(dataOutputStream);
        }

        private void writeHead(Map<String, Map<String, Pair<Long, Long>>> indexEntries)
                throws IOException {

            int headLength = calculateHeadLength(indexEntries);

            FileIndexFormatUtils.writeMagicAndVersion(dataOutputStream, VERSION_1);
            // writeHeadLength
            dataOutputStream.writeInt(headLength);
            // writeColumnInfo, offset = headLength
            FileIndexFormatUtils.writeIndexEntries(
                    dataOutputStream, indexEntries, VERSION_1, headLength);
            // writeRedundantLength
            dataOutputStream.writeInt(REDUNDANT_LENGTH);
        }

        private int calculateHeadLength(Map<String, Map<String, Pair<Long, Long>>> indexEntries)
                throws IOException {
            // magic 8 bytes, version 4 bytes, head length 4 bytes,
            // column number 4 bytes, body info start&length 8 bytes per
            // column-index, index number size 4 bytes per column, redundant length 4 bytes;
            int baseLength =
                    8
                            + 4
                            + 4
                            + 4
                            + indexEntries.values().stream().mapToInt(Map::size).sum() * 8
                            + indexEntries.size() * 4
                            + 4;

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput dataOutput = new DataOutputStream(baos);
            for (Map.Entry<String, Map<String, Pair<Long, Long>>> entry : indexEntries.entrySet()) {
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
}
