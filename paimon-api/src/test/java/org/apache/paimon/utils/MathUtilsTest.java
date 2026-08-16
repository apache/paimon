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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MathUtils}. */
class MathUtilsTest {

    @Test
    void testRoundDownToPowerOf2() {
        assertThat(MathUtils.roundDownToPowerOf2(0)).isZero();
        assertThat(MathUtils.roundDownToPowerOf2(1)).isEqualTo(1);
        assertThat(MathUtils.roundDownToPowerOf2(2)).isEqualTo(2);
        assertThat(MathUtils.roundDownToPowerOf2(3)).isEqualTo(2);
        assertThat(MathUtils.roundDownToPowerOf2(7)).isEqualTo(4);
        assertThat(MathUtils.roundDownToPowerOf2(8)).isEqualTo(8);
        assertThat(MathUtils.roundDownToPowerOf2(9)).isEqualTo(8);
        assertThat(MathUtils.roundDownToPowerOf2(65535)).isEqualTo(32768);
        assertThat(MathUtils.roundDownToPowerOf2(65536)).isEqualTo(65536);
        assertThat(MathUtils.roundDownToPowerOf2(Integer.MAX_VALUE)).isEqualTo(1 << 30);
    }

    @Test
    void testRoundUpToPowerOf2() {
        assertThat(MathUtils.roundUpToPowerOf2(0)).isEqualTo(2);
        assertThat(MathUtils.roundUpToPowerOf2(1)).isEqualTo(2);
        assertThat(MathUtils.roundUpToPowerOf2(2)).isEqualTo(2);
        assertThat(MathUtils.roundUpToPowerOf2(3)).isEqualTo(4);
        assertThat(MathUtils.roundUpToPowerOf2(7)).isEqualTo(8);
        assertThat(MathUtils.roundUpToPowerOf2(8)).isEqualTo(8);
        assertThat(MathUtils.roundUpToPowerOf2(9)).isEqualTo(16);
        assertThat(MathUtils.roundUpToPowerOf2(65535)).isEqualTo(65536);
        assertThat(MathUtils.roundUpToPowerOf2(65536)).isEqualTo(65536);
        assertThat(MathUtils.roundUpToPowerOf2(1 << 30)).isEqualTo(1 << 30);
        assertThat(MathUtils.roundUpToPowerOf2((1 << 30) - 1)).isEqualTo(1 << 30);
    }

    @Test
    void testIsPowerOf2() {
        assertThat(MathUtils.isPowerOf2(1L)).isTrue();
        assertThat(MathUtils.isPowerOf2(2L)).isTrue();
        assertThat(MathUtils.isPowerOf2(4L)).isTrue();
        assertThat(MathUtils.isPowerOf2(1024L)).isTrue();
        assertThat(MathUtils.isPowerOf2(1L << 30)).isTrue();
        assertThat(MathUtils.isPowerOf2(1L << 62)).isTrue();

        // The current implementation considers 0 a power of 2, which is unconventional.
        assertThat(MathUtils.isPowerOf2(0L)).isTrue();

        assertThat(MathUtils.isPowerOf2(3L)).isFalse();
        assertThat(MathUtils.isPowerOf2(5L)).isFalse();
        assertThat(MathUtils.isPowerOf2(100L)).isFalse();
        assertThat(MathUtils.isPowerOf2(-1L)).isFalse();
        assertThat(MathUtils.isPowerOf2(-2L)).isFalse();
        assertThat(MathUtils.isPowerOf2(-4L)).isFalse();

        // Long.MIN_VALUE is -2^63, which is a power of 2 in two's complement.
        assertThat(MathUtils.isPowerOf2(Long.MIN_VALUE)).isTrue();
        assertThat(MathUtils.isPowerOf2(Long.MAX_VALUE)).isFalse();
    }

    @Test
    void testLog2strict() {
        assertThat(MathUtils.log2strict(1)).isZero();
        assertThat(MathUtils.log2strict(2)).isEqualTo(1);
        assertThat(MathUtils.log2strict(8)).isEqualTo(3);
        assertThat(MathUtils.log2strict(1024)).isEqualTo(10);
        assertThat(MathUtils.log2strict(1 << 30)).isEqualTo(30);

        assertThatThrownBy(() -> MathUtils.log2strict(0))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Logarithm of zero is undefined.");

        assertThatThrownBy(() -> MathUtils.log2strict(3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The given value 3 is not a power of two.");

        assertThatThrownBy(() -> MathUtils.log2strict(5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The given value 5 is not a power of two.");

        assertThatThrownBy(() -> MathUtils.log2strict(-2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The given value -2 is not a power of two.");
    }

    @Test
    void testMax() {
        assertThat(MathUtils.max(1, 5)).isEqualTo(5);
        assertThat(MathUtils.max(5, 1)).isEqualTo(5);
        assertThat(MathUtils.max(5, 5)).isEqualTo(5);
        assertThat(MathUtils.max(-1, -5)).isEqualTo(-1);

        assertThat(MathUtils.max(null, 5)).isEqualTo(5);
        assertThat(MathUtils.max(5, null)).isEqualTo(5);
    }

    @Test
    void testMin() {
        assertThat(MathUtils.min(1, 5)).isEqualTo(1);
        assertThat(MathUtils.min(5, 1)).isEqualTo(1);
        assertThat(MathUtils.min(5, 5)).isEqualTo(5);
        assertThat(MathUtils.min(-1, -5)).isEqualTo(-5);

        assertThat(MathUtils.min(null, 5)).isEqualTo(5);
        assertThat(MathUtils.min(5, null)).isEqualTo(5);
    }

    @Test
    void testIncrementSafely() {
        assertThat(MathUtils.incrementSafely(0)).isEqualTo(1);
        assertThat(MathUtils.incrementSafely(10)).isEqualTo(11);
        assertThat(MathUtils.incrementSafely(-1)).isZero();
        assertThat(MathUtils.incrementSafely(Integer.MAX_VALUE - 1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.incrementSafely(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testAddSafely() {
        assertThat(MathUtils.addSafely(1, 2)).isEqualTo(3);
        assertThat(MathUtils.addSafely(-1, -2)).isEqualTo(-3);
        assertThat(MathUtils.addSafely(Integer.MAX_VALUE, 0)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.addSafely(Integer.MIN_VALUE, 0)).isEqualTo(Integer.MIN_VALUE);

        // Overflow
        assertThat(MathUtils.addSafely(Integer.MAX_VALUE, 1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.addSafely(Integer.MAX_VALUE - 1, 2)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.addSafely(1, Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);

        // Underflow
        assertThat(MathUtils.addSafely(Integer.MIN_VALUE, -1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.addSafely(Integer.MIN_VALUE + 1, -2)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.addSafely(-1, Integer.MIN_VALUE)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testMultiplySafely() {
        assertThat(MathUtils.multiplySafely(2, 3)).isEqualTo(6);
        assertThat(MathUtils.multiplySafely(-2, 3)).isEqualTo(-6);
        assertThat(MathUtils.multiplySafely(-2, -3)).isEqualTo(6);
        assertThat(MathUtils.multiplySafely(Integer.MAX_VALUE, 1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.multiplySafely(Integer.MIN_VALUE, 1)).isEqualTo(Integer.MIN_VALUE);
        assertThat(MathUtils.multiplySafely(100, 0)).isZero();

        // Overflow
        assertThat(MathUtils.multiplySafely(Integer.MAX_VALUE, 2)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.multiplySafely(Integer.MAX_VALUE / 2 + 1, 2))
                .isEqualTo(Integer.MAX_VALUE);

        // Underflow
        assertThat(MathUtils.multiplySafely(Integer.MIN_VALUE, 2)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.multiplySafely(Integer.MIN_VALUE / 2 - 1, 2))
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.multiplySafely(Integer.MAX_VALUE, -2)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testAddSafelyUnderflowReturnsMaxValue() {
        // The current implementation of addSafely returns Integer.MAX_VALUE on underflow,
        // which might be unexpected. This test verifies that behavior.
        assertThat(MathUtils.addSafely(Integer.MIN_VALUE, -1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.addSafely(-1, Integer.MIN_VALUE)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testMultiplySafelyUnderflowReturnsMaxValue() {
        // The current implementation of multiplySafely returns Integer.MAX_VALUE on underflow,
        // which might be unexpected. This test verifies that behavior.
        assertThat(MathUtils.multiplySafely(Integer.MIN_VALUE, 2)).isEqualTo(Integer.MAX_VALUE);
        assertThat(MathUtils.multiplySafely(Integer.MAX_VALUE, -2)).isEqualTo(Integer.MAX_VALUE);
    }
}
