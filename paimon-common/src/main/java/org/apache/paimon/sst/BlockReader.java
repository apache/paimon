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

import java.util.Comparator;

import static org.apache.paimon.sst.BlockAlignedType.ALIGNED;

/** Reader for a block. */
public abstract class BlockReader {

    private final MemorySlice block;
    private final int recordCount;
    private final Comparator<MemorySlice> comparator;

    private BlockReader(MemorySlice block, int recordCount, Comparator<MemorySlice> comparator) {
        this.block = block;
        this.recordCount = recordCount;
        this.comparator = comparator;
    }

    public MemorySliceInput blockInput() {
        return block.toInput();
    }

    public int recordCount() {
        return recordCount;
    }

    public Comparator<MemorySlice> comparator() {
        return comparator;
    }

    public BlockIterator iterator() {
        return new BlockIterator(this);
    }

    /** Seek to slice position from record position. */
    public abstract int seekTo(int recordPosition);

    public static BlockReader create(MemorySlice block, Comparator<MemorySlice> comparator) {
        BlockAlignedType alignedType =
                BlockAlignedType.fromByte(block.readByte(block.length() - 1));
        int intValue = block.readInt(block.length() - 5);
        if (alignedType == ALIGNED) {
            return new AlignedBlockReader(block.slice(0, block.length() - 5), intValue, comparator);
        } else {
            int indexLength = intValue * 4;
            int indexOffset = block.length() - 5 - indexLength;
            MemorySlice data = block.slice(0, indexOffset);
            MemorySlice index = block.slice(indexOffset, indexLength);
            return new UnalignedBlockReader(data, index, comparator);
        }
    }

    private static class AlignedBlockReader extends BlockReader {

        private final int recordSize;

        public AlignedBlockReader(
                MemorySlice data, int recordSize, Comparator<MemorySlice> comparator) {
            super(data, data.length() / recordSize, comparator);
            this.recordSize = recordSize;
        }

        @Override
        public int seekTo(int recordPosition) {
            return recordPosition * recordSize;
        }
    }

    private static class UnalignedBlockReader extends BlockReader {

        private final MemorySlice index;

        public UnalignedBlockReader(
                MemorySlice data, MemorySlice index, Comparator<MemorySlice> comparator) {
            super(data, index.length() / 4, comparator);
            this.index = index;
        }

        @Override
        public int seekTo(int recordPosition) {
            return index.readInt(recordPosition * 4);
        }
    }
}
