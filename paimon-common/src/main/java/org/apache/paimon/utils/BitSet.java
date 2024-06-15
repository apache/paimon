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

package org.apache.paimon.utils;

import org.apache.paimon.memory.MemorySegment;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** BitSet based on {@link MemorySegment}. */
public class BitSet {

    private static final int BYTE_INDEX_MASK = 0x00000007;

    private MemorySegment memorySegment;

    // MemorySegment byte array offset.
    private int offset;

    // The BitSet byte size.
    private final int byteLength;

    // The BitSet bit size.
    private final int bitLength;

    public BitSet(int byteSize) {
        checkArgument(byteSize > 0, "bits size should be greater than 0.");
        this.byteLength = byteSize;
        this.bitLength = byteSize << 3;
    }

    public void setMemorySegment(MemorySegment memorySegment, int offset) {
        checkArgument(memorySegment != null, "MemorySegment can not be null.");
        checkArgument(offset >= 0, "Offset should be positive integer.");
        checkArgument(
                offset + byteLength <= memorySegment.size(),
                "Could not set MemorySegment, the remain buffers is not enough.");
        this.memorySegment = memorySegment;
        this.offset = offset;
    }

    public void unsetMemorySegment() {
        this.memorySegment = null;
    }

    public MemorySegment getMemorySegment() {
        return this.memorySegment;
    }

    /**
     * Sets the bit at specified index.
     *
     * @param index - position
     */
    public void set(int index) {
        checkArgument(index < bitLength && index >= 0);

        int byteIndex = index >>> 3;
        byte current = memorySegment.get(offset + byteIndex);
        current |= (1 << (index & BYTE_INDEX_MASK));
        memorySegment.put(offset + byteIndex, current);
    }

    /**
     * Returns true if the bit is set in the specified index.
     *
     * @param index - position
     * @return - value at the bit position
     */
    public boolean get(int index) {
        checkArgument(index < bitLength && index >= 0);

        int byteIndex = index >>> 3;
        byte current = memorySegment.get(offset + byteIndex);
        return (current & (1 << (index & BYTE_INDEX_MASK))) != 0;
    }

    /** Number of bits. */
    public int bitSize() {
        return bitLength;
    }

    /** Clear the bit set. */
    public void clear() {
        int index = 0;
        while (index + 8 <= byteLength) {
            memorySegment.putLong(offset + index, 0L);
            index += 8;
        }
        while (index < byteLength) {
            memorySegment.put(offset + index, (byte) 0);
            index += 1;
        }
    }

    @Override
    public String toString() {
        return "BitSet:\n"
                + "\tMemorySegment:"
                + memorySegment.size()
                + "\n"
                + "\tOffset:"
                + offset
                + "\n"
                + "\tLength:"
                + byteLength
                + "\n";
    }
}
