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

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.append.dataevolution.CompactCandidateRangeCollector.NORMAL_FILE;
import static org.apache.paimon.append.dataevolution.CompactCandidateRangeCollector.VECTOR_FILE;
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

    private CompactCandidateRangeCollector collector(
            long targetFileSize,
            long blobTargetFileSize,
            long openFileCost,
            long compactMinFileNum) {
        return new CompactCandidateRangeCollector(
                16, targetFileSize, blobTargetFileSize, openFileCost, compactMinFileNum);
    }

    private List<String> finish(CompactCandidateRangeCollector collector) {
        List<String> candidates = new ArrayList<>();
        collector.finish(
                (start, end, fileCount) -> candidates.add(start + "-" + end + ":" + fileCount));
        return candidates;
    }
}
