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

package org.apache.paimon.manifest;

import javax.annotation.Nullable;

import java.util.Arrays;

/** Primitive set used by RowID compaction to avoid rebuilding file identifiers. */
final class DeletedRowIdSet {

    private static final long EMPTY = Long.MIN_VALUE;

    private long[] table = emptyTable(16);
    private int size;
    private boolean containsMinValue;
    private @Nullable long[] sortedRowIds;

    void add(long value) {
        if (value == EMPTY) {
            if (!containsMinValue) {
                containsMinValue = true;
                size++;
                sortedRowIds = null;
            }
            return;
        }
        if ((size + 1) * 2 > table.length) {
            grow();
        }
        int slot = slot(value, table.length);
        while (table[slot] != EMPTY) {
            if (table[slot] == value) {
                return;
            }
            slot = (slot + 1) & (table.length - 1);
        }
        table[slot] = value;
        size++;
        sortedRowIds = null;
    }

    void addAll(DeletedRowIdSet other) {
        if (other.containsMinValue) {
            add(EMPTY);
        }
        for (long value : other.table) {
            if (value != EMPTY) {
                add(value);
            }
        }
    }

    boolean contains(long value) {
        if (value == EMPTY) {
            return containsMinValue;
        }
        int slot = slot(value, table.length);
        while (table[slot] != EMPTY) {
            if (table[slot] == value) {
                return true;
            }
            slot = (slot + 1) & (table.length - 1);
        }
        return false;
    }

    boolean intersects(long minInclusive, long maxInclusive) {
        if (minInclusive > maxInclusive) {
            return true;
        }
        long[] values = sortedRowIds();
        int position = Arrays.binarySearch(values, minInclusive);
        if (position < 0) {
            position = -position - 1;
        }
        return position < values.length && values[position] <= maxInclusive;
    }

    void prepareRangeIndex() {
        // Publish the immutable sorted snapshot before concurrent manifest planning starts.
        sortedRowIds();
    }

    void releaseRangeIndex() {
        sortedRowIds = null;
    }

    private long[] sortedRowIds() {
        if (sortedRowIds != null) {
            return sortedRowIds;
        }
        long[] values = new long[size];
        int position = 0;
        if (containsMinValue) {
            values[position++] = EMPTY;
        }
        for (long value : table) {
            if (value != EMPTY) {
                values[position++] = value;
            }
        }
        if (position != size) {
            throw new IllegalStateException("Failed to snapshot deleted RowID set.");
        }
        Arrays.sort(values);
        sortedRowIds = values;
        return values;
    }

    private void grow() {
        long[] previous = table;
        if (previous.length >= (1 << 30)) {
            throw new IllegalStateException("Too many deleted RowIDs in one manifest group.");
        }
        table = emptyTable(previous.length << 1);
        int previousSize = size;
        size = containsMinValue ? 1 : 0;
        for (long value : previous) {
            if (value != EMPTY) {
                add(value);
            }
        }
        if (size != previousSize) {
            throw new IllegalStateException("Failed to grow deleted RowID set.");
        }
    }

    private static int slot(long value, int length) {
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        return ((int) value) & (length - 1);
    }

    private static long[] emptyTable(int length) {
        long[] table = new long[length];
        Arrays.fill(table, EMPTY);
        return table;
    }
}
