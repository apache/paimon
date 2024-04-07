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

import org.apache.paimon.memory.MemorySegment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.utils.CommonTestUtils.generateRandomInts;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link org.apache.paimon.utils.BloomFilter}. */
public class BloomFilterTest {

    @Test
    public void testOneSegmentBuilder() {
        BloomFilter.Builder builder = BloomFilter.builder(100, 0.01);
        int[] inputs = generateRandomInts(100);
        for (int input : inputs) {
            builder.addHash(Integer.hashCode(input));
        }

        for (int input : inputs) {
            Assertions.assertThat(builder.testHash(Integer.hashCode(input))).isTrue();
        }
    }

    @Test
    public void testEstimatedHashFunctions() {
        Assertions.assertThat(BloomFilter.builder(1000, 0.01).getFilter().numHashFunctions())
                .isEqualTo(7);
        Assertions.assertThat(BloomFilter.builder(10_000, 0.01).getFilter().numHashFunctions())
                .isEqualTo(7);
        Assertions.assertThat(BloomFilter.builder(100_000, 0.01).getFilter().numHashFunctions())
                .isEqualTo(7);
        Assertions.assertThat(BloomFilter.builder(100_000, 0.01).getFilter().numHashFunctions())
                .isEqualTo(7);
        Assertions.assertThat(BloomFilter.builder(100_000, 0.05).getFilter().numHashFunctions())
                .isEqualTo(4);
        Assertions.assertThat(BloomFilter.builder(1_000_000, 0.01).getFilter().numHashFunctions())
                .isEqualTo(7);
        Assertions.assertThat(BloomFilter.builder(1_000_000, 0.05).getFilter().numHashFunctions())
                .isEqualTo(4);
    }

    @Test
    public void testBloomNumBits() {
        Assertions.assertThat(BloomFilter.optimalNumOfBits(0, 0)).isEqualTo(0);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(0, 1)).isEqualTo(0);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(1, 1)).isEqualTo(0);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(1, 0.03)).isEqualTo(7);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(10, 0.03)).isEqualTo(72);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(100, 0.03)).isEqualTo(729);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(1000, 0.03)).isEqualTo(7298);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(10000, 0.03)).isEqualTo(72984);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(100000, 0.03)).isEqualTo(729844);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(1000000, 0.03)).isEqualTo(7298440);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(1000000, 0.05)).isEqualTo(6235224);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(300000000, 0.05)).isEqualTo(1870567268);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(300000000, 0.1)).isEqualTo(1437758756);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(300000000, 0.5)).isEqualTo(432808512);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(3000000000L, 0.8)).isEqualTo(1393332198);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(3000000000L, 0.9)).isEqualTo(657882327);
        Assertions.assertThat(BloomFilter.optimalNumOfBits(3000000000L, 1)).isEqualTo(0);
    }

    @Test
    public void testBloomNumHashFunctions() {
        assertEquals(1, BloomFilter.optimalNumOfHashFunctions(-1, -1));
        assertEquals(1, BloomFilter.optimalNumOfHashFunctions(0, 0));
        assertEquals(1, BloomFilter.optimalNumOfHashFunctions(10, 0));
        assertEquals(1, BloomFilter.optimalNumOfHashFunctions(10, 10));
        assertEquals(7, BloomFilter.optimalNumOfHashFunctions(10, 100));
        assertEquals(1, BloomFilter.optimalNumOfHashFunctions(100, 100));
        assertEquals(1, BloomFilter.optimalNumOfHashFunctions(1000, 100));
        assertEquals(1, BloomFilter.optimalNumOfHashFunctions(10000, 100));
        assertEquals(1, BloomFilter.optimalNumOfHashFunctions(100000, 100));
        assertEquals(1, BloomFilter.optimalNumOfHashFunctions(1000000, 100));
        assertEquals(3634, BloomFilter.optimalNumOfHashFunctions(100, 64 * 1024 * 8));
        assertEquals(363, BloomFilter.optimalNumOfHashFunctions(1000, 64 * 1024 * 8));
        assertEquals(36, BloomFilter.optimalNumOfHashFunctions(10000, 64 * 1024 * 8));
        assertEquals(4, BloomFilter.optimalNumOfHashFunctions(100000, 64 * 1024 * 8));
        assertEquals(1, BloomFilter.optimalNumOfHashFunctions(1000000, 64 * 1024 * 8));
    }

    @Test
    public void testBloomFilter() {
        BloomFilter filter = new BloomFilter(100, 1024);

        // segment1
        MemorySegment segment1 = MemorySegment.wrap(new byte[1024]);
        filter.setMemorySegment(segment1, 0);
        int[] inputs1 = generateRandomInts(100);
        Arrays.stream(inputs1).forEach(i -> filter.addHash(Integer.hashCode(i)));
        Arrays.stream(inputs1)
                .forEach(i -> Assertions.assertThat(filter.testHash(Integer.hashCode(i))).isTrue());

        // segments 2
        MemorySegment segment2 = MemorySegment.wrap(new byte[1024]);
        filter.setMemorySegment(segment2, 0);
        int[] inputs2 = generateRandomInts(100);
        Arrays.stream(inputs2).forEach(i -> filter.addHash(Integer.hashCode(i)));
        Arrays.stream(inputs2)
                .forEach(i -> Assertions.assertThat(filter.testHash(Integer.hashCode(i))).isTrue());

        // switch to segment1
        filter.setMemorySegment(segment1, 0);
        Arrays.stream(inputs1)
                .forEach(i -> Assertions.assertThat(filter.testHash(Integer.hashCode(i))).isTrue());

        // clear segment1
        filter.reset();
        Arrays.stream(inputs1)
                .forEach(
                        i -> Assertions.assertThat(filter.testHash(Integer.hashCode(i))).isFalse());
        // clear segment2
        filter.setMemorySegment(segment2, 0);
        filter.reset();
        Arrays.stream(inputs2)
                .forEach(
                        i -> Assertions.assertThat(filter.testHash(Integer.hashCode(i))).isFalse());
    }
}
