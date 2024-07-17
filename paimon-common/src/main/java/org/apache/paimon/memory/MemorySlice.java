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

import org.apache.paimon.utils.MurmurHashUtils;

/** Slice of a {@link MemorySegment}. */
public final class MemorySlice implements Comparable<MemorySlice> {

    private final MemorySegment segment;
    private final int offset;
    private final int length;

    public MemorySlice(MemorySegment segment, int offset, int length) {
        this.segment = segment;
        this.offset = offset;
        this.length = length;
    }

    public MemorySegment segment() {
        return segment;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    public MemorySlice slice(int index, int length) {
        if (index == 0 && length == this.length) {
            return this;
        }

        return new MemorySlice(segment, offset + index, length);
    }

    public byte readByte(int position) {
        return segment.get(offset + position);
    }

    public int readInt(int position) {
        return segment.getInt(offset + position);
    }

    public long readLong(int position) {
        return segment.getLong(offset + position);
    }

    public byte[] getHeapMemory() {
        return segment.getHeapMemory();
    }

    public byte[] copyBytes() {
        byte[] bytes = new byte[length];
        segment.get(offset, bytes, 0, length);
        return bytes;
    }

    public MemorySliceInput toInput() {
        return new MemorySliceInput(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MemorySlice slice = (MemorySlice) o;

        // do lengths match
        if (length != slice.length) {
            return false;
        }

        // if arrays have same base offset, some optimizations can be taken...
        if (offset == slice.offset && segment == slice.segment) {
            return true;
        }
        return segment.equalTo(slice.segment, offset, slice.offset, length);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, length);
    }

    public static MemorySlice wrap(byte[] bytes) {
        return new MemorySlice(MemorySegment.wrap(bytes), 0, bytes.length);
    }

    public static MemorySlice wrap(MemorySegment segment) {
        return new MemorySlice(segment, 0, segment.size());
    }

    @Override
    public int compareTo(MemorySlice other) {
        int len = Math.min(length, other.length);
        for (int i = 0; i < len; i++) {
            int res =
                    (segment.get(offset + i) & 0xFF) - (other.segment.get(other.offset + i) & 0xFF);
            if (res != 0) {
                return res;
            }
        }
        return length - other.length;
    }
}
