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
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import java.io.IOException;

/** Block index that maps row numbers to block locations. */
class RowBlockIndex {

    private final long[] blockOffsets;
    private final long[] blockCompressedSizes;
    private final long[] blockUncompressedSizes;
    private final long[] blockRowStarts;

    RowBlockIndex(
            long[] blockCompressedSizes, long[] blockUncompressedSizes, long[] blockRowStarts) {
        this.blockCompressedSizes = blockCompressedSizes;
        this.blockUncompressedSizes = blockUncompressedSizes;
        this.blockRowStarts = blockRowStarts;
        this.blockOffsets = computeOffsets(blockCompressedSizes);
    }

    int blockCount() {
        return blockCompressedSizes.length;
    }

    long blockOffset(int blockIdx) {
        return blockOffsets[blockIdx];
    }

    long blockCompressedSize(int blockIdx) {
        return blockCompressedSizes[blockIdx];
    }

    long blockUncompressedSize(int blockIdx) {
        return blockUncompressedSizes[blockIdx];
    }

    long blockRowStart(int blockIdx) {
        return blockRowStarts[blockIdx];
    }

    void writeTo(PositionOutputStream out) throws IOException {
        writeArray(out, DeltaVarintCompressor.compress(blockCompressedSizes));
        writeArray(out, DeltaVarintCompressor.compress(blockUncompressedSizes));
        writeArray(out, DeltaVarintCompressor.compress(blockRowStarts));
    }

    static RowBlockIndex readFrom(SeekableInputStream in, long indexOffset, int indexLength)
            throws IOException {
        in.seek(indexOffset);
        byte[] indexData = new byte[indexLength];
        IOUtils.readFully(in, indexData);
        return readFrom(indexData);
    }

    static RowBlockIndex readFrom(byte[] indexData) {
        int pos = 0;
        int len1 = decodeVarInt(indexData, pos);
        pos += varIntSize(len1);
        long[] blockCompressedSizes =
                DeltaVarintCompressor.decompress(extractBytes(indexData, pos, len1));
        pos += len1;

        int len2 = decodeVarInt(indexData, pos);
        pos += varIntSize(len2);
        long[] blockUncompressedSizes =
                DeltaVarintCompressor.decompress(extractBytes(indexData, pos, len2));
        pos += len2;

        int len3 = decodeVarInt(indexData, pos);
        pos += varIntSize(len3);
        long[] blockRowStarts =
                DeltaVarintCompressor.decompress(extractBytes(indexData, pos, len3));

        return new RowBlockIndex(blockCompressedSizes, blockUncompressedSizes, blockRowStarts);
    }

    private static long[] computeOffsets(long[] compressedSizes) {
        long[] offsets = new long[compressedSizes.length];
        long offset = 0;
        for (int i = 0; i < compressedSizes.length; i++) {
            offsets[i] = offset;
            offset += compressedSizes[i];
        }
        return offsets;
    }

    private static void writeArray(PositionOutputStream out, byte[] encoded) throws IOException {
        byte[] lenBuf = new byte[VarLengthIntUtils.MAX_VAR_INT_SIZE];
        int lenBytes = VarLengthIntUtils.encodeInt(lenBuf, 0, encoded.length);
        out.write(lenBuf, 0, lenBytes);
        out.write(encoded);
    }

    private static int decodeVarInt(byte[] data, int offset) {
        int result = 0;
        int shift = 0;
        int pos = offset;
        while (true) {
            byte b = data[pos++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }

    private static int varIntSize(int value) {
        int size = 1;
        while ((value & ~0x7F) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }

    private static byte[] extractBytes(byte[] data, int offset, int length) {
        byte[] result = new byte[length];
        System.arraycopy(data, offset, result, 0, length);
        return result;
    }
}
