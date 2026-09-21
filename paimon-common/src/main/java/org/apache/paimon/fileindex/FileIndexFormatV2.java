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

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.fileindex.FileIndexFormatUtils.VERSION_2;

/**
 * V2 file index container: prefix and body followed by footer and trailer.
 *
 * <pre>
 *  ______________________________________    _____________________
 * |     magic    |        version        |          PREFIX
 * |--------------------------------------|    ---------------------
 * |                BODY                  |
 * |                BODY                  |           BODY
 * |                BODY                  |
 * |--------------------------------------|    ---------------------
 * |            column number             |
 * |--------------------------------------|
 * |   column 1        |  index number    |
 * |--------------------------------------|
 * |  index name 1 | start pos | length   |
 * |--------------------------------------|
 * |  index name 2 | start pos | length   |
 * |--------------------------------------|          FOOTER
 * |   column 2        |  index number    |
 * |--------------------------------------|
 * |  index name 1 | start pos | length   |
 * |--------------------------------------|
 * |  index name 2 | start pos | length   |
 * |--------------------------------------|
 * |                 ...                  |
 * |--------------------------------------|
 * |                 ...                  |
 * |--------------------------------------|    ---------------------
 * | footer length |      tail magic      |          TRAILER
 * |______________________________________|    _____________________
 *
 * magic:                            8 bytes long
 * version:                          4 bytes int
 * BODY:                             column index bytes + column index bytes + .......
 * column number:                    4 bytes int
 * column x:                         var bytes utf (length + bytes)
 * index number:                     4 bytes int (how many column items below)
 * index name x:                     var bytes utf
 * start pos:                        8 bytes long
 * length:                           8 bytes long
 * footer length:                    4 bytes int
 * tail magic:                       8 bytes long
 *
 * </pre>
 */
final class FileIndexFormatV2 {

    // ASCII PAFIDX02.
    private static final long TAIL_MAGIC = 0x5041464944583032L;
    private static final int TRAILER_LENGTH = 12;

    private FileIndexFormatV2() {}

    static Map<String, Map<String, Pair<Long, Long>>> readIndexEntries(
            SeekableInputStream seekableInputStream, long length) throws IOException {
        seekableInputStream.seek(length - TRAILER_LENGTH);
        DataInputStream tail = new DataInputStream(seekableInputStream);
        int footerLength = tail.readInt();
        if (tail.readLong() != TAIL_MAGIC) {
            throw new IOException("Invalid file index tail magic");
        }
        long footerStart = length - TRAILER_LENGTH - footerLength;
        byte[] footer = new byte[footerLength];
        seekableInputStream.seek(footerStart);
        new DataInputStream(seekableInputStream).readFully(footer);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(footer))) {
            return FileIndexFormatUtils.readIndexEntries(in, VERSION_2);
        }
    }

    /** V2 writer writes byte-array payloads and records their ranges in the footer. */
    static final class Writer implements FileIndexFormatUtils.FormatWriter {

        private final DataOutputStream dataOutputStream;
        private final FileIndexFormatUtils.CountingPositionOutputStream positionOutputStream;

        Writer(OutputStream outputStream) {
            this.positionOutputStream =
                    new FileIndexFormatUtils.CountingPositionOutputStream(outputStream);
            this.dataOutputStream = new DataOutputStream(positionOutputStream);
        }

        @Override
        public void writeColumnIndexes(Map<String, Map<String, byte[]>> indexes)
                throws IOException {
            FileIndexFormatUtils.writeMagicAndVersion(dataOutputStream, VERSION_2);
            Map<String, Map<String, Pair<Long, Long>>> indexEntries = new LinkedHashMap<>();
            // writeBody
            FileIndexFormatUtils.writeIndexPayloads(
                    indexes,
                    indexEntries,
                    bytes -> {
                        long start = positionOutputStream.getPos();
                        dataOutputStream.write(bytes);
                        return Pair.of(start, (long) bytes.length);
                    });
            writeFooter(indexEntries);
        }

        private void writeFooter(Map<String, Map<String, Pair<Long, Long>>> indexEntries)
                throws IOException {
            long start = positionOutputStream.getPos();
            FileIndexFormatUtils.writeIndexEntries(dataOutputStream, indexEntries, VERSION_2, 0L);
            // writeFooterLength
            dataOutputStream.writeInt(Math.toIntExact(positionOutputStream.getPos() - start));
            // writeTailMagic
            dataOutputStream.writeLong(TAIL_MAGIC);
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(dataOutputStream);
        }
    }
}
