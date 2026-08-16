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

import java.util.Arrays;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Array-backed list whose elements are triples of primitive longs. */
public final class LongTripleArrayList {

    private static final int ELEMENT_WIDTH = 3;
    private static final int MIN_GROWN_LONGS = 16 * ELEMENT_WIDTH;

    private long[] longs;
    private int size;

    public LongTripleArrayList() {
        this(0);
    }

    public LongTripleArrayList(int expectedSize) {
        checkArgument(expectedSize >= 0, "Expected size cannot be negative.");
        checkArgument(
                expectedSize <= Integer.MAX_VALUE / ELEMENT_WIDTH, "Expected size is too large.");
        longs = new long[expectedSize * ELEMENT_WIDTH];
    }

    public void add(long first, long second, long third) {
        ensureCapacity(Math.addExact(size, 1));
        int offset = size * ELEMENT_WIDTH;
        longs[offset] = first;
        longs[offset + 1] = second;
        longs[offset + 2] = third;
        size++;
    }

    public long first(int index) {
        return longs[offset(index)];
    }

    public long second(int index) {
        return longs[offset(index) + 1];
    }

    public long third(int index) {
        return longs[offset(index) + 2];
    }

    public void swap(int left, int right) {
        int leftOffset = offset(left);
        int rightOffset = offset(right);
        if (left == right) {
            return;
        }
        for (int i = 0; i < ELEMENT_WIDTH; i++) {
            long value = longs[leftOffset + i];
            longs[leftOffset + i] = longs[rightOffset + i];
            longs[rightOffset + i] = value;
        }
    }

    public int size() {
        return size;
    }

    public int usedLongCount() {
        return size * ELEMENT_WIDTH;
    }

    public int retainedLongCount() {
        return longs.length;
    }

    public void clear() {
        size = 0;
    }

    public void release() {
        longs = new long[0];
        size = 0;
    }

    private int offset(int index) {
        checkArgument(index >= 0 && index < size, "Long triple index is out of bounds.");
        return index * ELEMENT_WIDTH;
    }

    private void ensureCapacity(int requiredSize) {
        long requiredLongs = (long) requiredSize * ELEMENT_WIDTH;
        checkState(
                requiredLongs <= Integer.MAX_VALUE,
                "Too many long triples to fit in a Java array.");
        if (requiredLongs <= longs.length) {
            return;
        }
        int newLength = Math.max(MIN_GROWN_LONGS, longs.length);
        while (newLength < requiredLongs) {
            int grown = newLength + (newLength >>> 1);
            if (grown <= newLength || grown > Integer.MAX_VALUE) {
                newLength = (int) requiredLongs;
                break;
            }
            newLength = grown;
        }
        longs = Arrays.copyOf(longs, newLength);
    }
}
