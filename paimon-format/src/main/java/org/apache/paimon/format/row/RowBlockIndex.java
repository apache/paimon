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

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Block index that maps row numbers to block locations. */
class RowBlockIndex {

    private final long[] blockOffsets;
    private final long[] blockCompressedSizes;
    private final long[] blockUncompressedSizes;
    private final long[] blockRowStarts;

    RowBlockIndex(
            long[] blockCompressedSizes, long[] blockUncompressedSizes, long[] blockRowStarts) {
        checkArgument(
                blockCompressedSizes.length == blockUncompressedSizes.length
                        && blockCompressedSizes.length == blockRowStarts.length,
                "Row file block index arrays disagree on the block count: %s compressed sizes, %s uncompressed sizes, %s row starts.",
                blockCompressedSizes.length,
                blockUncompressedSizes.length,
                blockRowStarts.length);
        this.blockCompressedSizes = blockCompressedSizes;
        this.blockUncompressedSizes = blockUncompressedSizes;
        this.blockRowStarts = blockRowStarts;
        this.blockOffsets = computeOffsets(blockCompressedSizes);
    }

    /**
     * Checks the index against the footer, which is the only place both are in hand. Blocks are
     * written contiguously from position 0 and the index follows the last one, so the compressed
     * sizes must sum to exactly {@code indexOffset} — see the row format spec. Row starts must
     * cover every row exactly once, because {@code RowFormatReader} turns consecutive starts into
     * the row range of a block and skips a block whose range a selection does not intersect: a
     * first start past 0, a repeated start, or a last start at the row count would drop rows
     * silently.
     */
    void validate(RowFileFooter footer) throws IOException {
        if (blockCount() != footer.blockCount) {
            throw new IOException(
                    String.format(
                            "Row file block index holds %d blocks, but the footer declares %d.",
                            blockCount(), footer.blockCount));
        }

        long blocksEnd = 0;
        for (int i = 0; i < blockCount(); i++) {
            if (blockCompressedSizes[i] < 0) {
                throw new IOException(
                        String.format(
                                "Row file block %d has a negative compressed size %d.",
                                i, blockCompressedSizes[i]));
            }
            blocksEnd += blockCompressedSizes[i];
        }
        if (blocksEnd != footer.indexOffset) {
            throw new IOException(
                    String.format(
                            "Row file blocks end at %d, but the footer puts the block index at %d.",
                            blocksEnd, footer.indexOffset));
        }

        for (int i = 0; i < blockCount(); i++) {
            // nothing in the footer bounds this one, and it sizes the decompression buffer
            if (blockUncompressedSizes[i] < 0) {
                throw new IOException(
                        String.format(
                                "Row file block %d has a negative uncompressed size %d.",
                                i, blockUncompressedSizes[i]));
            }
        }

        if (blockCount() == 0) {
            if (footer.totalRowCount != 0) {
                throw new IOException(
                        String.format(
                                "Row file block index is empty, but the footer declares %d rows.",
                                footer.totalRowCount));
            }
            return;
        }

        if (blockRowStarts[0] != 0) {
            throw new IOException(
                    String.format(
                            "Row file block 0 starts at row %d, so rows before it are unreachable.",
                            blockRowStarts[0]));
        }
        for (int i = 1; i < blockCount(); i++) {
            if (blockRowStarts[i] <= blockRowStarts[i - 1]) {
                throw new IOException(
                        String.format(
                                "Row file block %d starts at row %d, not after block %d at row %d.",
                                i, blockRowStarts[i], i - 1, blockRowStarts[i - 1]));
            }
        }
        if (blockRowStarts[blockCount() - 1] >= footer.totalRowCount) {
            throw new IOException(
                    String.format(
                            "Row file block %d starts at row %d, which the declared row count %d does not reach.",
                            blockCount() - 1,
                            blockRowStarts[blockCount() - 1],
                            footer.totalRowCount));
        }
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
