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

package org.apache.paimon.memory;

/** Input for {@link MemorySlice}. */
public class MemorySliceInput {

    private final MemorySlice slice;

    private int position;

    public MemorySliceInput(MemorySlice slice) {
        this.slice = slice;
    }

    public int position() {
        return position;
    }

    public void setPosition(int position) {
        if (position < 0 || position > slice.length()) {
            throw new IndexOutOfBoundsException();
        }
        this.position = position;
    }

    public boolean isReadable() {
        return available() > 0;
    }

    public int available() {
        return slice.length() - position;
    }

    public byte readByte() {
        if (position == slice.length()) {
            throw new IndexOutOfBoundsException();
        }
        return slice.readByte(position++);
    }

    public int readUnsignedByte() {
        return (short) (readByte() & 0xFF);
    }

    public int readInt() {
        int v = slice.readInt(position);
        position += 4;
        return v;
    }

    public int readVarLenInt() {
        for (int offset = 0, result = 0; offset < 32; offset += 7) {
            int b = readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed integer.");
    }

    public long readLong() {
        long v = slice.readLong(position);
        position += 8;
        return v;
    }

    public long readVarLenLong() {
        long result = 0;
        for (int offset = 0; offset < 64; offset += 7) {
            long b = readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed long.");
    }

    public MemorySlice readSlice(int length) {
        MemorySlice newSlice = slice.slice(position, length);
        position += length;
        return newSlice;
    }
}
