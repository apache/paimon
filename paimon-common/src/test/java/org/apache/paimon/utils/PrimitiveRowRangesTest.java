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

/** Tests for {@link PrimitiveRowRanges}. */
class PrimitiveRowRangesTest {

    @Test
    void testAddAndTakeOwnership() {
        PrimitiveRowRanges ranges = new PrimitiveRowRanges(2);
        ranges.add(2L, 3L);
        ranges.add(5L, 8L);

        assertThat(ranges.size()).isEqualTo(2);
        assertThat(ranges.retainedWordCount()).isEqualTo(4);
        assertThat(ranges.start(0)).isEqualTo(2L);
        assertThat(ranges.end(1)).isEqualTo(8L);

        PrimitiveRowRanges.Owned owned = ranges.takeOwned();
        assertThat(owned.starts()).containsExactly(2L, 5L);
        assertThat(owned.ends()).containsExactly(3L, 8L);
        assertThat(ranges.size()).isZero();
        assertThat(ranges.retainedWordCount()).isZero();
    }

    @Test
    void testAppendSortAndNormalizeOverlapping() {
        PrimitiveRowRanges ranges = new PrimitiveRowRanges(2);
        ranges.add(10L, 15L);
        ranges.add(30L, 35L);
        PrimitiveRowRanges appended = new PrimitiveRowRanges(3);
        appended.add(12L, 20L);
        appended.add(5L, 8L);
        appended.add(20L, 25L);

        ranges.append(appended);
        ranges.normalizeOverlapping();

        assertThat(ranges.size()).isEqualTo(3);
        assertThat(ranges.start(0)).isEqualTo(5L);
        assertThat(ranges.end(0)).isEqualTo(8L);
        assertThat(ranges.start(1)).isEqualTo(10L);
        assertThat(ranges.end(1)).isEqualTo(25L);
        assertThat(ranges.start(2)).isEqualTo(30L);
        assertThat(ranges.end(2)).isEqualTo(35L);
        assertThat(appended.size()).isEqualTo(3);
    }

    @Test
    void testDoesNotMergeAdjacentRanges() {
        PrimitiveRowRanges ranges = new PrimitiveRowRanges(2);
        ranges.add(0L, 0L);
        ranges.add(1L, 1L);

        ranges.normalizeOverlapping();

        assertThat(ranges.size()).isEqualTo(2);
    }

    @Test
    void testCovers() {
        PrimitiveRowRanges ranges = new PrimitiveRowRanges(4);
        ranges.add(5L, 8L);
        ranges.add(0L, 2L);
        ranges.add(2L, 6L);
        ranges.add(9L, 10L);

        assertThat(ranges.covers(0L, 10L)).isTrue();
        assertThat(ranges.covers(1L, 9L)).isTrue();
        assertThat(ranges.covers(-1L, 10L)).isFalse();
        assertThat(ranges.covers(0L, 11L)).isFalse();
        assertThat(ranges.covers(10L, 10L)).isTrue();
    }

    @Test
    void testCoversLongMaxValue() {
        PrimitiveRowRanges ranges = new PrimitiveRowRanges(2);
        ranges.add(Long.MAX_VALUE - 1L, Long.MAX_VALUE - 1L);
        ranges.add(Long.MAX_VALUE, Long.MAX_VALUE);

        assertThat(ranges.covers(Long.MAX_VALUE - 1L, Long.MAX_VALUE)).isTrue();
    }

    @Test
    void testRejectsInvalidInput() {
        assertThatThrownBy(() -> new PrimitiveRowRanges(-1))
                .isInstanceOf(IllegalArgumentException.class);

        PrimitiveRowRanges ranges = new PrimitiveRowRanges(0);
        assertThatThrownBy(() -> ranges.add(2L, 1L)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ranges.start(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ranges.append(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ranges.covers(2L, 1L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
