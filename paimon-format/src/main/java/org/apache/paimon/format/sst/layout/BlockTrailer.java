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

package org.apache.paimon.format.sst.layout;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;

import java.util.Objects;

/** Trailer of a block. */
public class BlockTrailer {
    public static final int ENCODED_LENGTH = 4;

    private final int crc32c;

    public BlockTrailer(int crc32c) {
        this.crc32c = crc32c;
    }

    public int getCrc32c() {
        return crc32c;
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
        return crc32c == that.crc32c;
    }

    @Override
    public int hashCode() {
        return Objects.hash(crc32c);
    }

    @Override
    public String toString() {
        return "BlockTrailer" + "{crc32c=0x" + Integer.toHexString(crc32c) + '}';
    }

    public static BlockTrailer readBlockTrailer(MemorySliceInput input) {
        int crc32c = input.readInt();
        return new BlockTrailer(crc32c);
    }

    public static MemorySlice writeBlockTrailer(BlockTrailer blockTrailer) {
        MemorySliceOutput output = new MemorySliceOutput(ENCODED_LENGTH);
        writeBlockTrailer(blockTrailer, output);
        return output.toSlice();
    }

    public static void writeBlockTrailer(BlockTrailer blockTrailer, MemorySliceOutput sliceOutput) {
        sliceOutput.writeInt(blockTrailer.getCrc32c());
    }
}
