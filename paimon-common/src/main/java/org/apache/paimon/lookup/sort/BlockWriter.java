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
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.utils.IntArrayList;

import java.io.IOException;

import static org.apache.paimon.lookup.sort.BlockAlignedType.ALIGNED;
import static org.apache.paimon.lookup.sort.BlockAlignedType.UNALIGNED;

/** Writer to build a Block. */
public class BlockWriter {

    private final IntArrayList positions;
    private final MemorySliceOutput block;

    private int alignedSize;
    private boolean aligned;

    public BlockWriter(int blockSize) {
        this.positions = new IntArrayList(32);
        this.block = new MemorySliceOutput(blockSize + 128);
        this.alignedSize = 0;
        this.aligned = true;
    }

    public void reset() {
        this.positions.clear();
        this.block.reset();
        this.alignedSize = 0;
        this.aligned = true;
    }

    public void add(byte[] key, byte[] value) {
        int startPosition = block.size();
        block.writeVarLenInt(key.length);
        block.writeBytes(key);
        block.writeVarLenInt(value.length);
        block.writeBytes(value);
        int endPosition = block.size();

        positions.add(startPosition);
        if (aligned) {
            int currentSize = endPosition - startPosition;
            if (alignedSize == 0) {
                alignedSize = currentSize;
            } else {
                aligned = alignedSize == currentSize;
            }
        }
    }

    public int size() {
        return positions.size();
    }

    public int memory() {
        int memory = block.size() + 5;
        if (!aligned) {
            memory += positions.size() * 4;
        }
        return memory;
    }

    public MemorySlice finish() throws IOException {
        if (positions.isEmpty()) {
            // Do not use alignment mode, as it is impossible to calculate how many records are
            // inside when reading
            aligned = false;
        }

        if (aligned) {
            block.writeInt(alignedSize);
        } else {
            for (int i = 0; i < positions.size(); i++) {
                block.writeInt(positions.get(i));
            }
            block.writeInt(positions.size());
        }
        block.writeByte(aligned ? ALIGNED.toByte() : UNALIGNED.toByte());
        return block.toSlice();
    }
}
