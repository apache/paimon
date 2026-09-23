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

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeltaVarintCompressorTest {

    // Test case for normal compression and decompression
    @Test
    void testNormalCase1() {
        long[] original = {80L, 50L, 90L, 80L, 70L};
        byte[] compressed = DeltaVarintCompressor.compress(original);
        long[] decompressed = DeltaVarintCompressor.decompress(compressed);

        assertArrayEquals(original, decompressed); // Verify data integrity
        assertEquals(6, compressed.length); // Optimized size for small deltas
    }

    @Test
    void testNormalCase2() {
        long[] original = {100L, 50L, 150L, 100L, 200L};
        byte[] compressed = DeltaVarintCompressor.compress(original);
        long[] decompressed = DeltaVarintCompressor.decompress(compressed);

        assertArrayEquals(original, decompressed); // Verify data integrity
        assertEquals(8, compressed.length); // Optimized size for small deltas
    }

    @RepeatedTest(10000)
    void testRandom() {
        long[] original = new long[100];
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < original.length; i++) {
            original[i] = rnd.nextLong();
        }
        byte[] compressed = DeltaVarintCompressor.compress(original);
        long[] decompressed = DeltaVarintCompressor.decompress(compressed);

        assertArrayEquals(original, decompressed); // Verify data integrity
    }

    // Test case for empty array
    @Test
    void testEmptyArray() {
        long[] original = {};
        byte[] compressed = DeltaVarintCompressor.compress(original);
        long[] decompressed = DeltaVarintCompressor.decompress(compressed);

        assertArrayEquals(original, decompressed);
        assertEquals(0, compressed.length);
    }

    // Test case for single-element array
    @Test
    void testSingleElement() {
        long[] original = {42L};
        byte[] compressed = DeltaVarintCompressor.compress(original);
        long[] decompressed = DeltaVarintCompressor.decompress(compressed);

        assertArrayEquals(original, decompressed);
        // Calculate expected size: Varint encoding for 42 (0x2A -> 1 byte)
        assertEquals(1, compressed.length);
    }

    // Test case for extreme values (Long.MIN_VALUE and MAX_VALUE)
    @Test
    void testExtremeValues() {
        long[] original = {Long.MIN_VALUE, Long.MAX_VALUE};
        byte[] compressed = DeltaVarintCompressor.compress(original);
        long[] decompressed = DeltaVarintCompressor.decompress(compressed);

        assertArrayEquals(original, decompressed);
        // Expected size: 10 bytes (MIN_VALUE) + 9 bytes (delta) = 19 bytes
        assertEquals(11, compressed.length);
    }

    // Test case for negative deltas with ZigZag optimization
    @Test
    void testNegativeDeltas() {
        long[] original = {100L, -50L, -150L, -100L}; // Negative values
        byte[] compressed = DeltaVarintCompressor.compress(original);
        long[] decompressed = DeltaVarintCompressor.decompress(compressed);

        assertArrayEquals(original, decompressed);
        // Verify ZigZag optimization: -1 → 1 (1 byte)
        // Delta sequence: [100, -150, -100, 50] → ZigZag →
        // Each encoded in 1-2 bytes
        assertTrue(compressed.length <= 8); // Optimized size
    }

    // Test case for unsorted data (worse compression ratio)
    @Test
    void testUnsortedData() {
        long[] original = {1000L, 5L, 9999L, 12345L, 6789L};
        byte[] compressed = DeltaVarintCompressor.compress(original);
        long[] decompressed = DeltaVarintCompressor.decompress(compressed);

        assertArrayEquals(original, decompressed);
        // Larger deltas → more bytes (e.g., 9994 → 3 bytes)
        assertTrue(compressed.length > 5); // Worse than sorted case
    }

    // Test case for corrupted input (invalid Varint)
    @Test
    void testCorruptedInput() {
        // Invalid Varint (all continuation flags)
        byte[] corrupted = new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80};
        assertThrows(
                RuntimeException.class,
                () -> {
                    DeltaVarintCompressor.decompress(corrupted);
                });
    }

    @Test
    void testCompressLongArrayListNullAndEmpty() {
        assertArrayEquals(new byte[0], DeltaVarintCompressor.compressLongArrayList(null));
        assertArrayEquals(new byte[0], DeltaVarintCompressor.compressLongArrayList(toList()));
    }

    @Test
    void testCompressLongArrayListMatchesArray() {
        assertByteEquivalence(new long[] {42L});
        assertByteEquivalence(new long[] {80L, 50L, 90L, 80L, 70L});
        assertByteEquivalence(new long[] {7L, 7L, 7L, 7L});
        assertByteEquivalence(new long[] {1L, 2L, 3L, 4L, 5L});
        assertByteEquivalence(new long[] {100L, 90L, 80L, 70L, 60L});
        assertByteEquivalence(new long[] {-1L, -2L});
        assertByteEquivalence(new long[] {-1L, 5L, -2L, 8L});
        assertByteEquivalence(new long[] {Long.MIN_VALUE, Long.MAX_VALUE});
        assertByteEquivalence(new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
        assertByteEquivalence(new long[] {Long.MIN_VALUE, 0L, Long.MAX_VALUE});
        assertByteEquivalence(new long[] {-3L, Long.MAX_VALUE, Long.MIN_VALUE, 3L});
    }

    @Test
    void testCompressLongArrayListMatchesArrayRandom() {
        long[] original = new long[100];
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < original.length; i++) {
            original[i] = rnd.nextLong();
        }
        assertByteEquivalence(original);
    }

    @Test
    void testCompressGoldenBytes() {
        // ZigZag(42) = 84 -> single varint byte.
        assertArrayEquals(new byte[] {0x54}, DeltaVarintCompressor.compress(new long[] {42L}));
        assertArrayEquals(
                new byte[] {0x54}, DeltaVarintCompressor.compressLongArrayList(toList(42L)));

        // {80, 50, 90, 80, 70}: deltas are 80, -30, 40, -10, -10.
        byte[] expected = {(byte) 0xA0, 0x01, 0x3B, 0x50, 0x13, 0x13};
        assertArrayEquals(
                expected, DeltaVarintCompressor.compress(new long[] {80L, 50L, 90L, 80L, 70L}));
        assertArrayEquals(
                expected,
                DeltaVarintCompressor.compressLongArrayList(toList(80L, 50L, 90L, 80L, 70L)));
    }

    @Test
    void testCompressGoldenBytesVarintWidths() {
        // 2-byte varint boundary: ZigZag(64) = 128, ZigZag(-64) = 127.
        assertGolden(new long[] {64L}, new byte[] {(byte) 0x80, 0x01});
        assertGolden(new long[] {-64L}, new byte[] {0x7F});

        // 3-byte varint boundary: ZigZag(8192) = 16384, ZigZag(-8192) = 16383.
        assertGolden(new long[] {8192L}, new byte[] {(byte) 0x80, (byte) 0x80, 0x01});
        assertGolden(new long[] {-8192L}, new byte[] {(byte) 0xFF, 0x7F});

        // Full-width 10-byte varints.
        assertGolden(
                new long[] {Long.MIN_VALUE},
                new byte[] {
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    0x01
                });
        assertGolden(
                new long[] {Long.MAX_VALUE},
                new byte[] {
                    (byte) 0xFE,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    0x01
                });

        // Adjacent values whose delta overflows long.
        assertGolden(
                new long[] {Long.MAX_VALUE, Long.MIN_VALUE},
                new byte[] {
                    (byte) 0xFE,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    0x01,
                    0x02
                });
        assertGolden(
                new long[] {Long.MIN_VALUE, Long.MAX_VALUE},
                new byte[] {
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    0x01,
                    0x01
                });
    }

    private static void assertGolden(long[] values, byte[] expected) {
        assertArrayEquals(expected, DeltaVarintCompressor.compress(values));
        assertArrayEquals(expected, DeltaVarintCompressor.compressLongArrayList(toList(values)));
    }

    private static void assertByteEquivalence(long[] values) {
        assertArrayEquals(
                DeltaVarintCompressor.compress(values),
                DeltaVarintCompressor.compressLongArrayList(toList(values)));
    }

    private static LongArrayList toList(long... values) {
        LongArrayList list = new LongArrayList(values.length);
        for (long value : values) {
            list.add(value);
        }
        return list;
    }
}
