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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.utils.PrimitiveRowRanges;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CurrentRowIdEntries}. */
class CurrentRowIdEntriesTest {

    @Test
    void testHotStateUsesThreePrimitiveWords() {
        CurrentRowIdEntries entries = new CurrentRowIdEntries(10_000);

        for (int i = 0; i < 10_000; i++) {
            entries.add(i % 7, (i & 1) == 0, i * 10L, 3L);
        }

        assertThat(entries.size()).isEqualTo(10_000);
        assertThat(entries.usedWordCount()).isEqualTo(30_000);
        assertThat(entries.retainedWordCount()).isEqualTo(30_000);
    }

    @Test
    void testContiguousEntriesDoNotMaterializePerEntryRanges() {
        CurrentRowIdEntries entries = new CurrentRowIdEntries(10_000);
        for (int i = 0; i < 10_000; i++) {
            entries.add(0, false, i, 1L);
        }

        PrimitiveRowRanges ranges = entries.selectedRangesForTesting();

        assertThat(ranges).isNull();
        assertThat(entries.usedWordCount()).isEqualTo(30_000);
        assertThat(entries.retainedWordCount()).isEqualTo(30_000);
    }

    @Test
    void testFragmentedEntriesUsePrimitiveLogicalRanges() {
        CurrentRowIdEntries entries = new CurrentRowIdEntries(10_000);
        for (int i = 0; i < 10_000; i++) {
            entries.add(0, false, i * 2L, 1L);
        }

        PrimitiveRowRanges ranges = entries.selectedRangesForTesting();

        assertThat(ranges).isNotNull();
        assertThat(ranges.size()).isEqualTo(10_000);
        assertThat(ranges.retainedWordCount()).isEqualTo(20_000);
        assertThat(ranges.start(0)).isZero();
        assertThat(ranges.end(0)).isZero();
        assertThat(ranges.start(9_999)).isEqualTo(19_998L);
        assertThat(ranges.end(9_999)).isEqualTo(19_998L);
        assertThat(entries.usedWordCount()).isEqualTo(30_000);
        assertThat(entries.retainedWordCount()).isEqualTo(30_000);
    }
}
