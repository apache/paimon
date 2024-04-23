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

import java.util.stream.IntStream;

/** Test for {@link org.apache.paimon.utils.BitSet}. */
public class BitSetTest {

    @Test
    public void testBitSet() {
        BitSet bitSet = new BitSet(100);
        Assertions.assertThatThrownBy(
                        () -> bitSet.setMemorySegment(MemorySegment.wrap(new byte[99]), 0))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(
                        () -> bitSet.setMemorySegment(MemorySegment.wrap(new byte[100]), 1))
                .isInstanceOf(IllegalArgumentException.class);
        bitSet.setMemorySegment(MemorySegment.wrap(new byte[100]), 0);
        IntStream.range(0, 100).forEach(i -> Assertions.assertThat(bitSet.get(i)).isFalse());
        IntStream.range(0, 50).forEach(bitSet::set);
        IntStream.range(0, 50).forEach(i -> Assertions.assertThat(bitSet.get(i)).isTrue());
        IntStream.range(50, 100).forEach(i -> Assertions.assertThat(bitSet.get(i)).isFalse());
        bitSet.clear();
        IntStream.range(0, 100).forEach(i -> Assertions.assertThat(bitSet.get(i)).isFalse());
    }
}
