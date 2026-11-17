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
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RangeHelper}. */
public class RangeHelperTest {

    private final RangeHelper<Range> rangeHelper = new RangeHelper<>(range -> range);

    @Test
    public void testMergeOverlappingRanges() {
        // [50, 60] does not overlap [10, 20], but both are covered by [0, 100]
        List<Range> ranges =
                Arrays.asList(
                        new Range(200, 300),
                        new Range(50, 60),
                        new Range(0, 100),
                        new Range(10, 20));

        // Groups are ordered by range start, and each group keeps the input order
        assertThat(rangeHelper.mergeOverlappingRanges(ranges))
                .containsExactly(
                        Arrays.asList(new Range(50, 60), new Range(0, 100), new Range(10, 20)),
                        Collections.singletonList(new Range(200, 300)));
    }

    @Test
    public void testMergeInclusiveBoundaries() {
        // [0, 9] and [9, 19] share row 9, while [20, 29] is only adjacent to [9, 19]
        List<Range> ranges = Arrays.asList(new Range(0, 9), new Range(9, 19), new Range(20, 29));

        assertThat(rangeHelper.mergeOverlappingRanges(ranges))
                .containsExactly(
                        Arrays.asList(new Range(0, 9), new Range(9, 19)),
                        Collections.singletonList(new Range(20, 29)));
    }

    @Test
    public void testMergeEmptyRanges() {
        assertThat(rangeHelper.mergeOverlappingRanges(Collections.emptyList())).isEmpty();
    }

    @Test
    public void testAreAllRangesSame() {
        assertThat(rangeHelper.areAllRangesSame(Collections.emptyList())).isTrue();
        assertThat(rangeHelper.areAllRangesSame(Collections.singletonList(new Range(0, 9))))
                .isTrue();
        assertThat(
                        rangeHelper.areAllRangesSame(
                                Arrays.asList(new Range(0, 9), new Range(0, 9), new Range(0, 9))))
                .isTrue();

        assertThat(
                        rangeHelper.areAllRangesSame(
                                Arrays.asList(new Range(0, 9), new Range(0, 9), new Range(0, 10))))
                .isFalse();
        assertThat(rangeHelper.areAllRangesSame(Arrays.asList(new Range(0, 9), new Range(1, 9))))
                .isFalse();
    }
}
