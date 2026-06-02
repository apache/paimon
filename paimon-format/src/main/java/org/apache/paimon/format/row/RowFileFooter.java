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

package org.apache.paimon.format.row;

import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;

/** Fixed 32-byte footer at the end of a row file. */
class RowFileFooter {

    static final int FOOTER_SIZE = 32;
    static final int MAGIC = 0x524F5753; // "ROWS"
    static final byte VERSION = 1;

    final long totalRowCount;
    final int blockCount;
    final long indexOffset;
    final int indexLength;

    RowFileFooter(long totalRowCount, int blockCount, long indexOffset, int indexLength) {
        this.totalRowCount = totalRowCount;
        this.blockCount = blockCount;
        this.indexOffset = indexOffset;
        this.indexLength = indexLength;
    }

    void writeTo(PositionOutputStream out) throws IOException {
        byte[] buf = new byte[FOOTER_SIZE];
        writeLongLE(buf, 0, totalRowCount);
        writeIntLE(buf, 8, blockCount);
        writeLongLE(buf, 12, indexOffset);
        writeIntLE(buf, 20, indexLength);
        buf[24] = VERSION;
        // bytes 25-27 reserved (zeros)
        writeIntLE(buf, 28, MAGIC);
        out.write(buf);
    }

    static RowFileFooter readFrom(SeekableInputStream in, long fileSize) throws IOException {
        in.seek(fileSize - FOOTER_SIZE);
        byte[] buf = new byte[FOOTER_SIZE];
        readFully(in, buf);
        return readFrom(buf, 0);
    }

    static RowFileFooter readFrom(byte[] buf, int offset) throws IOException {
        int magic = readIntLE(buf, offset + 28);
        if (magic != MAGIC) {
            throw new IOException(
                    String.format(
                            "Invalid row file magic: expected 0x%08X, got 0x%08X", MAGIC, magic));
        }

        byte version = buf[offset + 24];
        if (version != VERSION) {
            throw new IOException("Unsupported row file version: " + version);
        }

        long totalRowCount = readLongLE(buf, offset);
        int blockCount = readIntLE(buf, offset + 8);
        long indexOffset = readLongLE(buf, offset + 12);
        int indexLength = readIntLE(buf, offset + 20);

        return new RowFileFooter(totalRowCount, blockCount, indexOffset, indexLength);
    }

    private static void readFully(SeekableInputStream in, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int read = in.read(buf, off, buf.length - off);
            if (read < 0) {
                throw new IOException("Unexpected end of file");
            }
            off += read;
        }
    }

    static void writeIntLE(byte[] buf, int offset, int value) {
        buf[offset] = (byte) (value & 0xFF);
        buf[offset + 1] = (byte) ((value >>> 8) & 0xFF);
        buf[offset + 2] = (byte) ((value >>> 16) & 0xFF);
        buf[offset + 3] = (byte) ((value >>> 24) & 0xFF);
    }

    static int readIntLE(byte[] buf, int offset) {
        return (buf[offset] & 0xFF)
                | ((buf[offset + 1] & 0xFF) << 8)
                | ((buf[offset + 2] & 0xFF) << 16)
                | ((buf[offset + 3] & 0xFF) << 24);
    }

    static void writeLongLE(byte[] buf, int offset, long value) {
        buf[offset] = (byte) (value & 0xFF);
        buf[offset + 1] = (byte) ((value >>> 8) & 0xFF);
        buf[offset + 2] = (byte) ((value >>> 16) & 0xFF);
        buf[offset + 3] = (byte) ((value >>> 24) & 0xFF);
        buf[offset + 4] = (byte) ((value >>> 32) & 0xFF);
        buf[offset + 5] = (byte) ((value >>> 40) & 0xFF);
        buf[offset + 6] = (byte) ((value >>> 48) & 0xFF);
        buf[offset + 7] = (byte) ((value >>> 56) & 0xFF);
    }

    static long readLongLE(byte[] buf, int offset) {
        return (buf[offset] & 0xFFL)
                | ((buf[offset + 1] & 0xFFL) << 8)
                | ((buf[offset + 2] & 0xFFL) << 16)
                | ((buf[offset + 3] & 0xFFL) << 24)
                | ((buf[offset + 4] & 0xFFL) << 32)
                | ((buf[offset + 5] & 0xFFL) << 40)
                | ((buf[offset + 6] & 0xFFL) << 48)
                | ((buf[offset + 7] & 0xFFL) << 56);
    }
}
