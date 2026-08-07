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

package org.apache.paimon.sort;

import org.apache.paimon.data.BinaryRow;

import java.util.Arrays;

/** In-place MSD radix sort for binary sort-buffer normalized keys. */
final class NormalizedKeyRadixSort implements IndexedSorter {

    private static final int MIN_RADIX_SIZE = 1 << 10;
    private static final int RADIX_BUCKETS = 1 << 8;
    private static final QuickSort FALLBACK = new QuickSort();

    @Override
    public void sort(IndexedSortable sortable) {
        sort(sortable, 0, sortable.size());
    }

    @Override
    public void sort(IndexedSortable sortable, int from, int to) {
        int size = to - from;
        if (!(sortable instanceof BinaryIndexedSortable) || size < MIN_RADIX_SIZE) {
            FALLBACK.sort(sortable, from, to);
            return;
        }

        BinaryIndexedSortable binary = (BinaryIndexedSortable) sortable;
        int keyBytes = binary.normalizedKeyBytes();
        if (keyBytes == 0) {
            FALLBACK.sort(sortable, from, to);
            return;
        }

        int[][] counts = new int[keyBytes][RADIX_BUCKETS];
        int[][] starts = new int[keyBytes][RADIX_BUCKETS];
        radixSort(binary, from, to, 0, physicalKeyOffsets(keyBytes), counts, starts);
    }

    private static void radixSort(
            BinaryIndexedSortable sortable,
            int from,
            int to,
            int keyOffset,
            int[] physicalKeyOffsets,
            int[][] countsByDepth,
            int[][] startsByDepth) {
        if (to - from < MIN_RADIX_SIZE) {
            FALLBACK.sort(sortable, from, to);
            return;
        }
        if (keyOffset == sortable.normalizedKeyBytes()) {
            // A partial normalized key only narrows the final comparator work to equal prefixes.
            if (!sortable.normalizedKeyFullyDetermines()) {
                FALLBACK.sort(sortable, from, to);
            }
            return;
        }

        int[] counts = countsByDepth[keyOffset];
        int[] starts = startsByDepth[keyOffset];
        Arrays.fill(counts, 0);
        for (int position = from; position < to; position++) {
            counts[sortable.normalizedKeyByte(position, physicalKeyOffsets[keyOffset])]++;
        }

        int position = from;
        for (int bucket = 0; bucket < RADIX_BUCKETS; bucket++) {
            int bucketSize = counts[bucket];
            starts[bucket] = position;
            position += bucketSize;
            counts[bucket] = position;
        }

        for (int bucket = 0; bucket < RADIX_BUCKETS; bucket++) {
            int bucketEnd = counts[bucket];
            int current = starts[bucket];
            while (current < bucketEnd) {
                int targetBucket =
                        sortable.normalizedKeyByte(current, physicalKeyOffsets[keyOffset]);
                if (targetBucket == bucket) {
                    current++;
                } else {
                    sortable.swap(current, starts[targetBucket]++);
                }
            }
        }

        position = from;
        for (int bucket = 0; bucket < RADIX_BUCKETS; bucket++) {
            int bucketEnd = counts[bucket];
            int bucketSize = bucketEnd - position;
            if (bucketSize > 1) {
                radixSort(
                        sortable,
                        position,
                        bucketEnd,
                        keyOffset + 1,
                        physicalKeyOffsets,
                        countsByDepth,
                        startsByDepth);
            }
            position = bucketEnd;
        }
    }

    private static int[] physicalKeyOffsets(int keyBytes) {
        int[] offsets = new int[keyBytes];
        int chunkStart = 0;
        int remaining = keyBytes;
        while (remaining > 0) {
            // Generated normalized keys use native-endian chunks of 8, 4, 2, or 1 byte.
            int chunkSize = remaining >= 8 ? 8 : remaining >= 4 ? 4 : remaining >= 2 ? 2 : 1;
            for (int i = 0; i < chunkSize; i++) {
                offsets[chunkStart + i] =
                        BinaryRow.LITTLE_ENDIAN ? chunkStart + chunkSize - i - 1 : chunkStart + i;
            }
            chunkStart += chunkSize;
            remaining -= chunkSize;
        }
        return offsets;
    }
}
