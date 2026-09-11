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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.append.dataevolution.CompactCandidateRangeCollector.IGNORED_DEDICATED_FILE;
import static org.apache.paimon.append.dataevolution.CompactCandidateRangeCollector.NORMAL_FILE;
import static org.apache.paimon.append.dataevolution.CompactCandidateRangeCollector.VECTOR_FILE;
import static org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator.largeFileThreshold;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompactCandidateRangeCollector}. */
class CompactCandidateRangeCollectorTest {

    @Test
    void testSelectsOnlyNormalFileBinsWhichCanCompact() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 200L);
        collector.add(0, NORMAL_FILE, 20L, 10L, 60L);
        collector.add(0, NORMAL_FILE, 30L, 10L, 60L);
        collector.add(0, NORMAL_FILE, 50L, 10L, 200L);

        assertThat(collector.usedWordCount()).isEqualTo(4 * 4);
        assertThat(finish(collector)).containsExactly("20-39:2");
        assertThat(collector.usedWordCount()).isZero();
        assertThat(collector.retainedWordCount()).isZero();
    }

    @Test
    void testSplitLargeFilesUsesPhysicalSizeAndIncludesColumnUpdates() {
        for (boolean enabled : new boolean[] {false, true}) {
            CompactCandidateRangeCollector collector =
                    new CompactCandidateRangeCollector(
                            16, 100L, 100L, 1000L, 10L, enabled ? 200L : Long.MAX_VALUE);
            collector.add(0, NORMAL_FILE, 0L, 10L, 199L);
            collector.add(0, NORMAL_FILE, 10L, 10L, 200L);
            collector.add(0, NORMAL_FILE, 20L, 10L, 201L);
            collector.add(0, NORMAL_FILE, 20L, 10L, 10L);
            // Dedicated files never trigger normal-file splitting.
            collector.add(0, 3, 0L, 10L, 1000L);
            if (enabled) {
                assertThat(finish(collector)).containsExactly("20-29:2");
            } else {
                assertThat(finish(collector)).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"1.0,100", "1.15,115", "1.5,150", "3.0,300"})
    void testCustomLargeFileRatioUsesIndividualPhysicalFileSize(double ratio, long threshold) {
        CompactCandidateRangeCollector collector =
                new CompactCandidateRangeCollector(
                        16, 100L, 100L, 1000L, 10L, largeFileThreshold(100L, ratio));
        collector.add(0, NORMAL_FILE, 0L, 10L, threshold - 1);
        collector.add(0, NORMAL_FILE, 10L, 10L, threshold);
        collector.add(0, NORMAL_FILE, 20L, 10L, 10L);
        collector.add(0, NORMAL_FILE, 20L, 10L, threshold + 1);
        // Neither the sum of versions nor dedicated-file sizes bypass the minimum file count.
        collector.add(0, NORMAL_FILE, 30L, 10L, threshold * 3 / 4);
        collector.add(0, NORMAL_FILE, 30L, 10L, threshold * 3 / 4);
        collector.add(0, IGNORED_DEDICATED_FILE, 0L, 10L, 1000L);

        assertThat(finish(collector)).containsExactly("20-29:2");
    }

    @Test
    void testSplitThresholdDoesNotOverflow() {
        CompactCandidateRangeCollector collector =
                new CompactCandidateRangeCollector(
                        16, Long.MAX_VALUE, 100L, 1L, 2L, largeFileThreshold(Long.MAX_VALUE, 2.0d));
        collector.add(0, NORMAL_FILE, 0L, 10L, Long.MAX_VALUE);
        assertThat(finish(collector)).isEmpty();
    }

    @Test
    void testSelectsUpdatedFilesEvenWhenOneLogicalRangeExceedsTarget() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 200L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 200L);

        assertThat(finish(collector)).containsExactly("0-9:2");
    }

    @Test
    void testDoesNotCompactNormalFilesAcrossPartitionsOrRowIdGaps() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 10L);
        collector.add(0, NORMAL_FILE, 20L, 10L, 10L);
        collector.add(1, NORMAL_FILE, 10L, 10L, 10L);

        assertThat(finish(collector)).isEmpty();
    }

    @Test
    void testSelectsContiguousSmallBlobFilesForSameField() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 200L);
        collector.add(0, 3, 0L, 5L, 40L);
        collector.add(0, 3, 5L, 5L, 40L);

        assertThat(finish(collector)).containsExactly("0-9:3");
    }

    @Test
    void testSkipsBlobFilesFromDifferentFieldsOrAtTargetSize() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, NORMAL_FILE, 0L, 20L, 200L);
        collector.add(0, 3, 0L, 10L, 40L);
        collector.add(0, 4, 10L, 10L, 40L);
        collector.add(0, 5, 0L, 10L, 100L);
        collector.add(0, 5, 10L, 10L, 100L);

        assertThat(finish(collector)).isEmpty();
    }

    @Test
    void testSelectsOverlappingBlobVersionsForSameField() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 200L);
        collector.add(0, 3, 0L, 10L, 200L);
        collector.add(0, 3, 0L, 10L, 200L);

        assertThat(finish(collector)).containsExactly("0-9:3");
    }

    @Test
    void testVectorCandidatesAreScopedToOneNormalFileRange() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 200L);
        collector.add(0, VECTOR_FILE, 0L, 10L, 200L);
        collector.add(0, VECTOR_FILE, 0L, 10L, 200L);
        collector.add(0, NORMAL_FILE, 20L, 10L, 200L);
        collector.add(0, VECTOR_FILE, 20L, 10L, 10L);

        assertThat(finish(collector)).containsExactly("0-9:3");
    }

    @Test
    void testSelectsUpdatedBlobFilesSpanningAdjacentNormalRanges() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 200L);
        collector.add(0, NORMAL_FILE, 10L, 10L, 200L);
        collector.add(0, 3, 5L, 10L, 40L);
        collector.add(0, 3, 5L, 10L, 40L);

        assertThat(finish(collector)).containsExactly("0-9:3");
    }

    @Test
    void testAssociatesDedicatedFilesBeforeNormalFileAtSameStart() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, 3, 0L, 5L, 40L);
        collector.add(0, 3, 0L, 5L, 40L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 200L);

        assertThat(finish(collector)).containsExactly("0-9:3");
    }

    @Test
    void testSelectsVectorFilesSpanningAdjacentNormalRanges() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 200L);
        collector.add(0, NORMAL_FILE, 10L, 10L, 200L);
        collector.add(0, VECTOR_FILE, 5L, 10L, 40L);
        collector.add(0, VECTOR_FILE, 5L, 10L, 40L);

        assertThat(finish(collector)).containsExactly("0-9:3");
    }

    @Test
    void testIgnoredDedicatedFileCanSpanAdjacentNormalRanges() {
        CompactCandidateRangeCollector collector = collector(100L, 100L, 1L, 2L);
        collector.add(0, NORMAL_FILE, 0L, 10L, 40L);
        collector.add(0, NORMAL_FILE, 10L, 10L, 40L);
        collector.add(0, IGNORED_DEDICATED_FILE, 5L, 10L, 40L);

        assertThat(finish(collector)).containsExactly("0-19:3");
    }

    private CompactCandidateRangeCollector collector(
            long targetFileSize,
            long blobTargetFileSize,
            long openFileCost,
            long compactMinFileNum) {
        return new CompactCandidateRangeCollector(
                16,
                targetFileSize,
                blobTargetFileSize,
                openFileCost,
                compactMinFileNum,
                Long.MAX_VALUE);
    }

    private List<String> finish(CompactCandidateRangeCollector collector) {
        List<String> candidates = new ArrayList<>();
        collector.finish(
                (start, end, fileCount) -> candidates.add(start + "-" + end + ":" + fileCount));
        return candidates;
    }
}
