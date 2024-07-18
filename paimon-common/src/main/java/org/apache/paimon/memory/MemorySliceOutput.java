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

/** Output for {@link MemorySegment}. */
public class MemorySliceOutput {

    private MemorySegment segment;
    private int size;

    public MemorySliceOutput(int estimatedSize) {
        this.segment = MemorySegment.wrap(new byte[estimatedSize]);
    }

    public int size() {
        return size;
    }

    public MemorySlice toSlice() {
        return new MemorySlice(segment, 0, size);
    }

    public void reset() {
        size = 0;
    }

    public void writeByte(int value) {
        ensureSize(size + 1);
        segment.put(size++, (byte) value);
    }

    public void writeInt(int value) {
        ensureSize(size + 4);
        segment.putInt(size, value);
        size += 4;
    }

    public void writeVarLenInt(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        while ((value & ~0x7F) != 0) {
            writeByte(((value & 0x7F) | 0x80));
            value >>>= 7;
        }

        writeByte((byte) value);
    }

    public void writeLong(long value) {
        ensureSize(size + 8);
        segment.putLong(size, value);
        size += 8;
    }

    public void writeVarLenLong(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        while ((value & ~0x7FL) != 0) {
            writeByte((((int) value & 0x7F) | 0x80));
            value >>>= 7;
        }
        writeByte((byte) value);
    }

    public void writeBytes(byte[] source) {
        writeBytes(source, 0, source.length);
    }

    public void writeBytes(byte[] source, int sourceIndex, int length) {
        ensureSize(size + length);
        segment.put(size, source, sourceIndex, length);
        size += length;
    }

    private void ensureSize(int minWritableBytes) {
        if (minWritableBytes <= segment.size()) {
            return;
        }

        int newCapacity = segment.size();
        int minNewCapacity = segment.size() + minWritableBytes;
        while (newCapacity < minNewCapacity) {
            newCapacity <<= 1;
        }

        MemorySegment newSegment = MemorySegment.wrap(new byte[newCapacity]);
        segment.copyTo(0, newSegment, 0, segment.size());
        segment = newSegment;
    }
}
