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

import java.util.Comparator;

import static org.apache.paimon.lookup.sort.BlockAlignedType.ALIGNED;

/** Reader for a block. */
public class BlockReader {
    private final MemorySlice block;
    private final Comparator<MemorySlice> comparator;

    public BlockReader(MemorySlice block, Comparator<MemorySlice> comparator) {
        this.block = block;
        this.comparator = comparator;
    }

    public long size() {
        return block.length();
    }

    public BlockIterator iterator() {
        BlockAlignedType alignedType =
                BlockAlignedType.fromByte(block.readByte(block.length() - 1));
        int intValue = block.readInt(block.length() - 5);
        if (alignedType == ALIGNED) {
            return new AlignedIterator(block.slice(0, block.length() - 5), intValue, comparator);
        } else {
            int indexLength = intValue * 4;
            int indexOffset = block.length() - 5 - indexLength;
            MemorySlice data = block.slice(0, indexOffset);
            MemorySlice index = block.slice(indexOffset, indexLength);
            return new UnalignedIterator(data, index, comparator);
        }
    }

    private static class AlignedIterator extends BlockIterator {

        private final int recordSize;

        public AlignedIterator(
                MemorySlice data, int recordSize, Comparator<MemorySlice> comparator) {
            super(data.toInput(), data.length() / recordSize, comparator);
            this.recordSize = recordSize;
        }

        @Override
        public void seekTo(int record) {
            data.setPosition(record * recordSize);
        }
    }

    private static class UnalignedIterator extends BlockIterator {

        private final MemorySlice index;

        public UnalignedIterator(
                MemorySlice data, MemorySlice index, Comparator<MemorySlice> comparator) {
            super(data.toInput(), index.length() / 4, comparator);
            this.index = index;
        }

        @Override
        public void seekTo(int record) {
            data.setPosition(index.readInt(record * 4));
        }
    }
}
