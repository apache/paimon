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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Segments;
import org.apache.paimon.data.SingleSegments;
import org.apache.paimon.manifest.ManifestEntrySegments.RichSegments;
import org.apache.paimon.memory.MemorySegment;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.data.BinaryRow.singleColumn;
import static org.assertj.core.api.Assertions.assertThat;

class ManifestEntrySegmentsTest {

    @Test
    public void testManifestEntrySegments() {
        // Create test partitions
        BinaryRow partition1 = singleColumn(1);
        BinaryRow partition2 = singleColumn(2);

        // Create test segments
        Segments segments1 = newSegments(100);
        Segments segments2 = newSegments(200);
        Segments segments3 = newSegments(300);

        // Create RichSegments
        RichSegments richSegments1 = new RichSegments(partition1, 0, 1, segments1);
        RichSegments richSegments2 = new RichSegments(partition1, 1, 1, segments2);
        RichSegments richSegments3 = new RichSegments(partition2, 0, 1, segments3);

        // Create list of RichSegments
        List<RichSegments> richSegmentsList = new ArrayList<>();
        richSegmentsList.add(richSegments1);
        richSegmentsList.add(richSegments2);
        richSegmentsList.add(richSegments3);

        // Create ManifestEntrySegments
        ManifestEntrySegments manifestEntrySegments = new ManifestEntrySegments(richSegmentsList);

        // Test segments() method
        assertThat(manifestEntrySegments.segments()).containsExactlyElementsOf(richSegmentsList);

        // Test totalMemorySize() method
        long expectedTotalMemorySize = 100 + 200 + 300;
        assertThat(manifestEntrySegments.totalMemorySize()).isEqualTo(expectedTotalMemorySize);

        // Test indexedSegments() method
        Map<BinaryRow, Map<Integer, List<RichSegments>>> indexedSegments =
                manifestEntrySegments.indexedSegments();

        // Check that we have 2 partitions
        assertThat(indexedSegments).hasSize(2);

        // Check partition1
        Map<Integer, List<RichSegments>> partition1Map = indexedSegments.get(partition1);
        assertThat(partition1Map).hasSize(2);
        assertThat(partition1Map.get(0)).containsExactly(richSegments1);
        assertThat(partition1Map.get(1)).containsExactly(richSegments2);

        // Check partition2
        Map<Integer, List<RichSegments>> partition2Map = indexedSegments.get(partition2);
        assertThat(partition2Map).hasSize(1);
        assertThat(partition2Map.get(0)).containsExactly(richSegments3);
    }

    private Segments newSegments(long memorySize) {
        return new SingleSegments(
                MemorySegment.allocateHeapMemory((int) memorySize), (int) memorySize);
    }
}
