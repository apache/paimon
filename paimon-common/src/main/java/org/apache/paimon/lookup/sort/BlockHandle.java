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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;

/** Handle for a block. */
public class BlockHandle {
    public static final int MAX_ENCODED_LENGTH = 9 + 5;

    private final long offset;
    private final int size;

    BlockHandle(long offset, int size) {
        this.offset = offset;
        this.size = size;
    }

    public long offset() {
        return offset;
    }

    public int size() {
        return size;
    }

    public int getFullBlockSize() {
        return size + BlockTrailer.ENCODED_LENGTH;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockHandle that = (BlockHandle) o;

        if (size != that.size) {
            return false;
        }
        if (offset != that.offset) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + size;
        return result;
    }

    @Override
    public String toString() {
        return "BlockHandle" + "{offset=" + offset + ", size=" + size + '}';
    }

    public static BlockHandle readBlockHandle(MemorySliceInput sliceInput) {
        long offset = sliceInput.readVarLenLong();
        int size = sliceInput.readVarLenInt();
        return new BlockHandle(offset, size);
    }

    public static MemorySlice writeBlockHandle(BlockHandle blockHandle) {
        MemorySliceOutput sliceOutput = new MemorySliceOutput(MAX_ENCODED_LENGTH);
        writeBlockHandleTo(blockHandle, sliceOutput);
        return sliceOutput.toSlice();
    }

    public static void writeBlockHandleTo(BlockHandle blockHandle, MemorySliceOutput sliceOutput) {
        sliceOutput.writeVarLenLong(blockHandle.offset);
        sliceOutput.writeVarLenInt(blockHandle.size);
    }
}
