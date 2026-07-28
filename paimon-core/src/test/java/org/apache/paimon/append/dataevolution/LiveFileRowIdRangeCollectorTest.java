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

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.append.dataevolution.LiveFileRowIdRangeCollector.FileRole.DEDICATED;
import static org.apache.paimon.append.dataevolution.LiveFileRowIdRangeCollector.FileRole.NORMAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LiveFileRowIdRangeCollector}. */
class LiveFileRowIdRangeCollectorTest {

    @Test
    void testHotStateUsesThreePrimitiveWords() {
        LiveFileRowIdRangeCollector ranges = new LiveFileRowIdRangeCollector(10_000);

        for (int i = 0; i < 10_000; i++) {
            ranges.add(i % 7, (i & 1) == 0 ? NORMAL : DEDICATED, i * 10L, 3L);
        }

        assertThat(ranges.fileCount()).isEqualTo(10_000);
        assertThat(ranges.usedWordCount()).isEqualTo(30_000);
        assertThat(ranges.retainedWordCount()).isEqualTo(30_000);
    }

    @Test
    void testEmptyCollectorCanFinishOnlyOnce() {
        LiveFileRowIdRangeCollector ranges = new LiveFileRowIdRangeCollector();

        assertThat(finish(ranges)).isEmpty();

        assertThatThrownBy(() -> ranges.add(0, NORMAL, 0L, 1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("after the collector is finished");
        assertThatThrownBy(() -> finish(ranges))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already finished");
    }

    @Test
    void testContiguousRangesDoNotSelectPartitionAndReleaseStorage() {
        LiveFileRowIdRangeCollector ranges = new LiveFileRowIdRangeCollector(10_000);
        for (int i = 0; i < 10_000; i++) {
            ranges.add(0, NORMAL, i, 1L);
        }

        Map<Integer, PrimitiveRowRanges> selections = finish(ranges);

        assertThat(selections).isEmpty();
        assertThat(ranges.usedWordCount()).isZero();
        assertThat(ranges.retainedWordCount()).isZero();
    }

    @Test
    void testFragmentedRangesSelectPartitionAndUsePrimitiveStorage() {
        LiveFileRowIdRangeCollector ranges = new LiveFileRowIdRangeCollector(10_000);
        for (int i = 0; i < 10_000; i++) {
            ranges.add(7, NORMAL, i * 2L, 1L);
        }

        Map<Integer, PrimitiveRowRanges> selections = finish(ranges);
        PrimitiveRowRanges selected = selections.get(7);

        assertThat(selections).containsOnlyKeys(7);
        assertThat(selected.size()).isEqualTo(10_000);
        assertThat(selected.retainedWordCount()).isEqualTo(20_000);
        assertThat(selected.start(0)).isZero();
        assertThat(selected.end(0)).isZero();
        assertThat(selected.start(9_999)).isEqualTo(19_998L);
        assertThat(selected.end(9_999)).isEqualTo(19_998L);
        assertThat(ranges.usedWordCount()).isZero();
        assertThat(ranges.retainedWordCount()).isZero();
    }

    @Test
    void testNormalFilesDefineLogicalRanges() {
        LiveFileRowIdRangeCollector ranges = new LiveFileRowIdRangeCollector();
        ranges.add(3, DEDICATED, 2L, 3L);
        ranges.add(3, NORMAL, 0L, 10L);
        ranges.add(3, DEDICATED, 22L, 4L);
        ranges.add(3, NORMAL, 20L, 10L);

        PrimitiveRowRanges selected = finish(ranges).get(3);

        assertRanges(selected, 0L, 9L, 20L, 29L);
    }

    @Test
    void testDedicatedFilesDefineSpanningRangeWithoutNormalFile() {
        LiveFileRowIdRangeCollector ranges = new LiveFileRowIdRangeCollector();
        ranges.add(5, DEDICATED, 0L, 5L);
        ranges.add(5, DEDICATED, 3L, 7L);
        ranges.add(5, DEDICATED, 20L, 5L);

        PrimitiveRowRanges selected = finish(ranges).get(5);

        assertRanges(selected, 0L, 9L, 20L, 24L);
    }

    @Test
    void testOverlappingNormalFilesMustHaveIdenticalRanges() {
        LiveFileRowIdRangeCollector ranges = new LiveFileRowIdRangeCollector();
        ranges.add(0, NORMAL, 0L, 10L);
        ranges.add(0, NORMAL, 5L, 10L);

        assertThatThrownBy(() -> finish(ranges))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Normal files in one overlapping row-id group must have the same row-id range");
        assertThat(ranges.retainedWordCount()).isZero();
    }

    @Test
    void testDedicatedFileMustBeInsideLogicalRange() {
        LiveFileRowIdRangeCollector ranges = new LiveFileRowIdRangeCollector();
        ranges.add(0, DEDICATED, 0L, 7L);
        ranges.add(0, NORMAL, 5L, 6L);

        assertThatThrownBy(() -> finish(ranges))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("File row-id range is outside its logical row-id range");
        assertThat(ranges.retainedWordCount()).isZero();
    }

    private static Map<Integer, PrimitiveRowRanges> finish(LiveFileRowIdRangeCollector collector) {
        Map<Integer, PrimitiveRowRanges> selections = new LinkedHashMap<>();
        collector.finish(
                (partitionId, logicalRanges) -> {
                    PrimitiveRowRanges previous = selections.put(partitionId, logicalRanges);
                    assertThat(previous).isNull();
                });
        return selections;
    }

    private static void assertRanges(PrimitiveRowRanges ranges, long... boundaries) {
        assertThat(ranges).isNotNull();
        assertThat(boundaries.length % 2).isZero();
        assertThat(ranges.size()).isEqualTo(boundaries.length / 2);
        for (int i = 0; i < ranges.size(); i++) {
            assertThat(ranges.start(i)).isEqualTo(boundaries[i * 2]);
            assertThat(ranges.end(i)).isEqualTo(boundaries[i * 2 + 1]);
        }
    }
}
