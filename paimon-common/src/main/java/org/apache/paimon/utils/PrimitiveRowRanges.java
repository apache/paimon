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

/**
 * Object-free mutable storage for inclusive row ranges.
 *
 * <p>Starts and ends are kept in separate primitive arrays. Ownership of both arrays can be
 * transferred without copying when the initial capacity matches the final range count.
 */
public final class PrimitiveRowRanges {

    private long[] starts;
    private long[] ends;
    private int size;
    private boolean sorted = true;
    private boolean normalized = true;

    public PrimitiveRowRanges(int expectedRanges) {
        checkArgument(expectedRanges >= 0, "Expected range count cannot be negative.");
        starts = new long[expectedRanges];
        ends = new long[expectedRanges];
    }

    public int size() {
        return size;
    }

    public int retainedWordCount() {
        return Math.addExact(starts.length, ends.length);
    }

    public long start(int index) {
        checkIndex(index);
        return starts[index];
    }

    public long end(int index) {
        checkIndex(index);
        return ends[index];
    }

    public void add(long start, long end) {
        checkArgument(start <= end, "Invalid row range [%s, %s].", start, end);
        ensureCapacity(Math.addExact(size, 1));
        if (size > 0 && compare(starts[size - 1], ends[size - 1], start, end) > 0) {
            sorted = false;
        }
        starts[size] = start;
        ends[size] = end;
        size++;
        normalized = false;
    }

    public void append(PrimitiveRowRanges other) {
        checkArgument(other != null, "Ranges to append cannot be null.");
        if (other.size == 0) {
            return;
        }
        int oldSize = size;
        int combinedSize = Math.addExact(size, other.size);
        ensureCapacity(combinedSize);
        if (oldSize > 0
                && compare(starts[oldSize - 1], ends[oldSize - 1], other.starts[0], other.ends[0])
                        > 0) {
            sorted = false;
        }
        sorted &= other.sorted;
        System.arraycopy(other.starts, 0, starts, oldSize, other.size);
        System.arraycopy(other.ends, 0, ends, oldSize, other.size);
        size = combinedSize;
        normalized = false;
    }

    public void normalizeOverlapping() {
        if (normalized) {
            return;
        }
        if (size <= 1) {
            sorted = true;
            normalized = true;
            return;
        }
        if (!sorted) {
            sort(0, size - 1);
            sorted = true;
        }
        int writeIndex = 0;
        for (int readIndex = 1; readIndex < size; readIndex++) {
            if (starts[readIndex] <= ends[writeIndex]) {
                ends[writeIndex] = Math.max(ends[writeIndex], ends[readIndex]);
            } else {
                writeIndex++;
                starts[writeIndex] = starts[readIndex];
                ends[writeIndex] = ends[readIndex];
            }
        }
        size = writeIndex + 1;
        normalized = true;
    }

    /**
     * Returns whether these ranges fully cover the inclusive range from {@code start} to {@code
     * end}.
     */
    public boolean covers(long start, long end) {
        checkArgument(start <= end, "Invalid row range [%s, %s].", start, end);
        normalizeOverlapping();
        long cursor = start;
        for (int i = 0; i < size; i++) {
            if (ends[i] < cursor) {
                continue;
            }
            if (starts[i] > cursor) {
                return false;
            }
            long segmentEnd = Math.min(ends[i], end);
            if (segmentEnd == end) {
                return true;
            }
            if (segmentEnd == Long.MAX_VALUE) {
                return false;
            }
            cursor = segmentEnd + 1L;
        }
        return false;
    }

    public Owned takeOwned() {
        long[] ownedStarts = starts.length == size ? starts : Arrays.copyOf(starts, size);
        long[] ownedEnds = ends.length == size ? ends : Arrays.copyOf(ends, size);
        starts = new long[0];
        ends = new long[0];
        size = 0;
        sorted = true;
        normalized = true;
        return new Owned(ownedStarts, ownedEnds);
    }

    private void ensureCapacity(int required) {
        if (required <= starts.length) {
            return;
        }
        int grown = Math.max(16, starts.length);
        while (grown < required) {
            int next = grown + (grown >>> 1);
            if (next <= grown || next < 0) {
                grown = required;
                break;
            }
            grown = next;
        }
        starts = Arrays.copyOf(starts, grown);
        ends = Arrays.copyOf(ends, grown);
    }

    private void sort(int left, int right) {
        while (left < right) {
            int middle = left + ((right - left) >>> 1);
            long pivotStart = starts[middle];
            long pivotEnd = ends[middle];
            int lower = left;
            int current = left;
            int upper = right;
            while (current <= upper) {
                int comparison = compare(starts[current], ends[current], pivotStart, pivotEnd);
                if (comparison < 0) {
                    swap(lower++, current++);
                } else if (comparison > 0) {
                    swap(current, upper--);
                } else {
                    current++;
                }
            }

            if (lower - left < right - upper) {
                if (left < lower - 1) {
                    sort(left, lower - 1);
                }
                left = upper + 1;
            } else {
                if (upper + 1 < right) {
                    sort(upper + 1, right);
                }
                right = lower - 1;
            }
        }
    }

    private void swap(int left, int right) {
        if (left == right) {
            return;
        }
        long start = starts[left];
        long end = ends[left];
        starts[left] = starts[right];
        ends[left] = ends[right];
        starts[right] = start;
        ends[right] = end;
    }

    private void checkIndex(int index) {
        checkArgument(index >= 0 && index < size, "Row range index is out of bounds.");
    }

    private static int compare(long leftStart, long leftEnd, long rightStart, long rightEnd) {
        int result = Long.compare(leftStart, rightStart);
        return result == 0 ? Long.compare(leftEnd, rightEnd) : result;
    }

    /** Owned primitive arrays transferred from {@link PrimitiveRowRanges}. */
    public static final class Owned {

        private final long[] starts;
        private final long[] ends;

        private Owned(long[] starts, long[] ends) {
            this.starts = starts;
            this.ends = ends;
        }

        public long[] starts() {
            return starts;
        }

        public long[] ends() {
            return ends;
        }
    }
}
