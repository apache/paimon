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

package dev.vortex.arrow;

import org.apache.arrow.memory.util.MemoryUtil;

/**
 * A top-level {@code Utf8View} or {@code BinaryView} column of a Vortex batch, read without
 * converting it to an Arrow 15 vector.
 *
 * <p>The first {@link #data()} or {@link #offsets()} call copies all values into one heap array.
 * Columns that are never read are never copied. The column reads native memory, so it is only
 * valid until its {@link ArrowBatch} is closed.
 */
public final class ViewColumn {

    private static final int VIEW_SIZE = 16;
    private static final int MAX_INLINE_LENGTH = 12;
    private static final long BYTE_ARRAY_BASE = MemoryUtil.UNSAFE.arrayBaseOffset(byte[].class);

    private final long validity;
    private final long views;
    private final long[] dataBuffers;
    private final long offset;
    private final int length;

    private volatile byte[] data;
    private int[] offsets;

    ViewColumn(long validity, long views, long[] dataBuffers, long offset, int length) {
        this.validity = validity;
        this.views = views;
        this.dataBuffers = dataBuffers;
        this.offset = offset;
        this.length = length;
    }

    public int length() {
        return length;
    }

    public boolean isNull(int index) {
        if (validity == 0) {
            return false;
        }
        long bit = offset + index;
        return ((MemoryUtil.UNSAFE.getByte(validity + (bit >> 3)) >> (bit & 7)) & 1) == 0;
    }

    /** All values, back to back. Value {@code i} spans {@code offsets()[i]} to {@code offsets()[i + 1]}. */
    public byte[] data() {
        materialize();
        return data;
    }

    public int[] offsets() {
        materialize();
        return offsets;
    }

    private void materialize() {
        if (data != null) {
            return;
        }

        synchronized (this) {
            if (data != null) {
                return;
            }

            long first = views + VIEW_SIZE * offset;
            long total = 0;
            for (int i = 0; i < length; i++) {
                if (!isNull(i)) {
                    total += MemoryUtil.UNSAFE.getInt(first + VIEW_SIZE * i);
                }
            }

            // Inline values are copied as one 8-byte and one 4-byte store, which can write up
            // to MAX_INLINE_LENGTH bytes past the value. The next value overwrites them, and
            // the slack keeps the last value inside the array.
            byte[] bytes = new byte[Math.toIntExact(total + MAX_INLINE_LENGTH)];
            int[] ends = new int[length + 1];
            long position = 0;
            for (int i = 0; i < length; i++) {
                if (!isNull(i)) {
                    long view = first + VIEW_SIZE * i;
                    int size = MemoryUtil.UNSAFE.getInt(view);
                    long target = BYTE_ARRAY_BASE + position;
                    if (size <= MAX_INLINE_LENGTH) {
                        MemoryUtil.UNSAFE.putLong(bytes, target, MemoryUtil.UNSAFE.getLong(view + 4));
                        MemoryUtil.UNSAFE.putInt(
                                bytes, target + 8, MemoryUtil.UNSAFE.getInt(view + 12));
                    } else {
                        long source =
                                dataBuffers[MemoryUtil.UNSAFE.getInt(view + 8)]
                                        + MemoryUtil.UNSAFE.getInt(view + 12);
                        MemoryUtil.UNSAFE.copyMemory(null, source, bytes, target, size);
                    }
                    position += size;
                }
                ends[i + 1] = (int) position;
            }

            offsets = ends;
            data = bytes;
        }
    }
}
