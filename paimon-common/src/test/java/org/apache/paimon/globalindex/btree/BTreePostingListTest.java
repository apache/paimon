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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.utils.LongArrayList;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BTreePostingList}. */
public class BTreePostingListTest {

    @Test
    public void testChoosesSingleton() throws Exception {
        assertEncodingAndRoundTrip(rowIds(123), BTreePostingList.SINGLE);
    }

    @Test
    public void testChoosesRoaringForContiguousRows() throws Exception {
        assertEncodingAndRoundTrip(contiguous(0, 128), BTreePostingList.ROARING);
    }

    @Test
    public void testChoosesDeltaListForSparseRows() throws Exception {
        LongArrayList rowIds = new LongArrayList(256);
        for (int i = 0; i < 256; i++) {
            rowIds.add(i * 100L);
        }

        assertEncodingAndRoundTrip(rowIds, BTreePostingList.DELTA_LIST);
    }

    @Test
    public void testChoosesRoaringForLargeContiguousRows() throws Exception {
        assertEncodingAndRoundTrip(contiguous(1L << 33, 128), BTreePostingList.ROARING);
    }

    @Test
    public void testChoosesRoaringForRowsEndingAtMaxValue() throws Exception {
        assertEncodingAndRoundTrip(contiguous(Long.MAX_VALUE - 127, 128), BTreePostingList.ROARING);
    }

    @Test
    public void testChoosesSmallestSerializedEncoding() throws Exception {
        assertSmallest(contiguous(0, 10_000));

        LongArrayList sparse = new LongArrayList(10_000);
        for (int i = 0; i < 10_000; i++) {
            sparse.add(i * 100L);
        }
        assertSmallest(sparse);

        LongArrayList multipleHighBuckets = new LongArrayList(4);
        multipleHighBuckets.add(0);
        multipleHighBuckets.add(1);
        multipleHighBuckets.add(1L << 32);
        multipleHighBuckets.add((1L << 32) + 1);
        assertSmallest(multipleHighBuckets);
    }

    @Test
    public void testAllAdaptiveEncodingsHonorRowLimit() throws Exception {
        assertThat(first(contiguous(0, 1), 0)).isEmpty();
        assertThat(first(contiguous(0, 10), 2)).containsExactly(0L, 1L);

        LongArrayList sparse = new LongArrayList(10);
        for (int i = 0; i < 10; i++) {
            sparse.add(i * 100L);
        }
        assertThat(first(sparse, 2)).containsExactly(0L, 100L);
        assertThat(first(contiguous(1L << 33, 128), 2)).containsExactly(1L << 33, (1L << 33) + 1);
    }

    @Test
    public void testRejectsUnsortedRows() {
        assertThatThrownBy(() -> BTreePostingList.serialize(rowIds(1, 3, 2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly increasing");

        assertThatThrownBy(() -> BTreePostingList.serialize(rowIds(1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly increasing");
    }

    @Test
    public void testRejectsUnknownEncoding() {
        assertThatThrownBy(
                        () ->
                                BTreePostingList.deserialize(
                                        MemorySlice.wrap(new byte[] {99}), Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unknown BTree posting list type");
    }

    @Test
    public void testRejectsInvalidDeltaList() {
        MemorySliceOutput invalidCount = new MemorySliceOutput(3);
        invalidCount.writeByte(BTreePostingList.DELTA_LIST);
        invalidCount.writeVarLenInt(1);
        invalidCount.writeVarLenLong(0);
        assertThatThrownBy(
                        () ->
                                BTreePostingList.deserialize(
                                        invalidCount.toSlice(), Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid delta BTree posting list length");

        MemorySliceOutput zeroDelta = new MemorySliceOutput(4);
        zeroDelta.writeByte(BTreePostingList.DELTA_LIST);
        zeroDelta.writeVarLenInt(2);
        zeroDelta.writeVarLenLong(1);
        zeroDelta.writeVarLenLong(0);
        assertThatThrownBy(
                        () -> BTreePostingList.deserialize(zeroDelta.toSlice(), Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid non-positive BTree row id delta");
    }

    @Test
    public void testRejectsEmptyRoaring() throws Exception {
        byte[] emptyBitmap = new RoaringNavigableMap64().serialize();
        MemorySliceOutput output = new MemorySliceOutput(emptyBitmap.length + 1);
        output.writeByte(BTreePostingList.ROARING);
        output.writeBytes(emptyBitmap);

        assertThatThrownBy(() -> BTreePostingList.deserialize(output.toSlice(), Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid empty Roaring BTree posting list");
    }

    private static void assertEncodingAndRoundTrip(LongArrayList rowIds, int expectedEncoding)
            throws Exception {
        byte[] serialized = BTreePostingList.serialize(rowIds);
        assertThat(serialized[0]).isEqualTo((byte) expectedEncoding);
        MemorySlice paddedSlice = paddedSlice(serialized);
        assertThat(BTreePostingList.deserialize(paddedSlice, Integer.MAX_VALUE))
                .containsExactly(rowIds.toArray());

        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        BTreePostingList.addTo(paddedSlice, bitmap);
        List<Long> actual = new ArrayList<>();
        bitmap.iterator().forEachRemaining(actual::add);
        assertThat(actual).containsExactlyElementsOf(asList(rowIds));
    }

    private static MemorySlice paddedSlice(byte[] serialized) {
        byte[] framed = new byte[serialized.length + 4];
        System.arraycopy(serialized, 0, framed, 2, serialized.length);
        return MemorySlice.wrap(framed).slice(2, serialized.length);
    }

    private static void assertSmallest(LongArrayList rowIds) throws Exception {
        byte[] adaptive = BTreePostingList.serialize(rowIds);
        assertThat(adaptive.length)
                .isEqualTo(Math.min(deltaSerializedSize(rowIds), roaringSerializedSize(rowIds)));
    }

    private static int deltaSerializedSize(LongArrayList rowIds) {
        MemorySliceOutput output = new MemorySliceOutput(rowIds.size() + 10);
        output.writeByte(BTreePostingList.DELTA_LIST);
        output.writeVarLenInt(rowIds.size());
        long previous = rowIds.get(0);
        output.writeVarLenLong(previous);
        for (int i = 1; i < rowIds.size(); i++) {
            long current = rowIds.get(i);
            output.writeVarLenLong(current - previous);
            previous = current;
        }
        return output.size();
    }

    private static int roaringSerializedSize(LongArrayList rowIds) throws Exception {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        for (int i = 0; i < rowIds.size(); i++) {
            bitmap.add(rowIds.get(i));
        }
        return 1 + bitmap.serialize().length;
    }

    private static long[] first(LongArrayList rowIds, int maxRowIds) throws Exception {
        return BTreePostingList.deserialize(
                MemorySlice.wrap(BTreePostingList.serialize(rowIds)), maxRowIds);
    }

    private static LongArrayList contiguous(long first, int count) {
        LongArrayList rowIds = new LongArrayList(count);
        for (int i = 0; i < count; i++) {
            rowIds.add(first + i);
        }
        return rowIds;
    }

    private static LongArrayList rowIds(long... values) {
        LongArrayList rowIds = new LongArrayList(values.length);
        for (long value : values) {
            rowIds.add(value);
        }
        return rowIds;
    }

    private static List<Long> asList(LongArrayList rowIds) {
        List<Long> result = new ArrayList<>(rowIds.size());
        for (int i = 0; i < rowIds.size(); i++) {
            result.add(rowIds.get(i));
        }
        return result;
    }
}
