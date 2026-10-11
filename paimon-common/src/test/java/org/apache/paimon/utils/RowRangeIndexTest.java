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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowRangeIndex}. */
class RowRangeIndexTest {

    @Test
    void testFromBitmap() {
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(2, 3, 4, 8, 9, 10);
        RowRangeIndex index = RowRangeIndex.fromBitmap(bitmap);

        assertThat(index.ranges()).containsExactly(new Range(2, 4), new Range(8, 10));
        assertThat(index.intersectedRanges(3, 9)).containsExactly(new Range(3, 4), new Range(8, 9));

        bitmap.add(20);
        assertThat(index.intersects(20, 20)).isFalse();

        assertThat(
                        RowRangeIndex.fromBitmap(
                                        RoaringNavigableMap64.bitmapOf(
                                                Long.MAX_VALUE, Long.MIN_VALUE))
                                .ranges())
                .containsExactly(
                        new Range(Long.MIN_VALUE, Long.MIN_VALUE),
                        new Range(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    @Test
    void testContains() {
        RowRangeIndex index =
                RowRangeIndex.create(
                        Arrays.asList(new Range(0, 99), new Range(100, 149), new Range(200, 299)));

        assertThat(index.contains(new Range(0, 149))).isTrue();
        assertThat(index.contains(new Range(50, 120))).isTrue();
        assertThat(index.contains(new Range(150, 199))).isFalse();
        assertThat(index.contains(new Range(100, 200))).isFalse();
    }
}
