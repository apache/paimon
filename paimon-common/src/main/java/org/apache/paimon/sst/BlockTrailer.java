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

package org.apache.paimon.sst;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;

import static java.util.Objects.requireNonNull;

/** Trailer of a block. */
public class BlockTrailer {
    public static final int ENCODED_LENGTH = 6;

    private final BlockType blockType;
    private final int crc32c;
    private final boolean compressed;

    public BlockTrailer(BlockType blockType, int crc32c, boolean compressed) {
        requireNonNull(blockType, "blockType is null");

        this.blockType = blockType;
        this.crc32c = crc32c;
        this.compressed = compressed;
    }

    public BlockType getBlockType() {
        return blockType;
    }

    public int getCrc32c() {
        return crc32c;
    }

    public boolean isCompressed() {
        return compressed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockTrailer that = (BlockTrailer) o;
        return crc32c == that.crc32c
                && compressed == that.compressed
                && blockType == that.blockType;
    }

    @Override
    public int hashCode() {
        int result = blockType.hashCode();
        result = 31 * result + crc32c;
        result = 31 * result + (compressed ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BlockTrailer"
                + "{blockType="
                + blockType
                + ", crc32c=0x"
                + Integer.toHexString(crc32c)
                + ", compressed="
                + compressed
                + '}';
    }

    public static BlockTrailer readBlockTrailer(MemorySliceInput input) {
        BlockType blockType = BlockType.fromByte(input.readByte());
        int crc32c = input.readInt();
        boolean compressed = input.readByte() == 1;
        return new BlockTrailer(blockType, crc32c, compressed);
    }

    public static MemorySlice writeBlockTrailer(BlockTrailer blockTrailer) {
        MemorySliceOutput output = new MemorySliceOutput(ENCODED_LENGTH);
        writeBlockTrailer(blockTrailer, output);
        return output.toSlice();
    }

    public static void writeBlockTrailer(BlockTrailer blockTrailer, MemorySliceOutput sliceOutput) {
        sliceOutput.writeByte(blockTrailer.blockType.toByte());
        sliceOutput.writeInt(blockTrailer.getCrc32c());
        sliceOutput.writeByte(blockTrailer.isCompressed() ? 1 : 0);
    }
}
