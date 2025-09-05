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

package org.apache.paimon.data;

import org.apache.paimon.memory.MemorySegment;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the factory method in {@link Segments}. */
class SegmentsTest {

    @Test
    void testCreateSingleSegmentNoResize() {
        // A single segment that is already a power of 2, no resize needed.
        int segmentSize = 1024;
        int limitInLastSegment = 1024;
        ArrayList<MemorySegment> segments =
                new ArrayList<>(
                        Collections.singletonList(MemorySegment.allocateHeapMemory(segmentSize)));

        Segments result = Segments.create(segments, limitInLastSegment);

        assertThat(result).isInstanceOf(SingleSegments.class);
        assertThat(result.totalMemorySize()).isEqualTo(segmentSize);
    }

    @Test
    void testCreateSingleSegmentWithResize() {
        // A single segment that is larger than needed, should be resized down to the next power of
        // 2.
        int segmentSize = 4096;
        int limitInLastSegment = 1500;
        int expectedRoundedSize = 2048; // MathUtils.roundUpToPowerOf2(1500)

        MemorySegment originalSegment = MemorySegment.allocateHeapMemory(segmentSize);
        // Put some data to check if it's copied
        originalSegment.put(0, (byte) 1);
        originalSegment.put(limitInLastSegment - 1, (byte) 2);

        ArrayList<MemorySegment> segments =
                new ArrayList<>(Collections.singletonList(originalSegment));

        Segments result = Segments.create(segments, limitInLastSegment);

        assertThat(result).isInstanceOf(SingleSegments.class);
        assertThat(result.totalMemorySize()).isEqualTo(expectedRoundedSize);
    }

    @Test
    void testCreateSingleSegmentNoResizeWhenRoundUpIsLarger() {
        // Edge case: rounded-up size is larger than segment size, so no resize should happen.
        int segmentSize = 1024;
        int limitInLastSegment = 800; // MathUtils.roundUpToPowerOf2(800) is 1024
        ArrayList<MemorySegment> segments =
                new ArrayList<>(
                        Collections.singletonList(MemorySegment.allocateHeapMemory(segmentSize)));

        Segments result = Segments.create(segments, limitInLastSegment);

        assertThat(result).isInstanceOf(SingleSegments.class);
        assertThat(result.totalMemorySize()).isEqualTo(segmentSize);
    }

    @Test
    void testCreateMultiSegments() {
        // With multiple segments, it should create a MultiSegments instance.
        int segmentSize = 2048;
        int limitInLastSegment = 500;

        ArrayList<MemorySegment> segmentList = new ArrayList<>();
        segmentList.add(MemorySegment.allocateHeapMemory(segmentSize));
        segmentList.add(MemorySegment.allocateHeapMemory(segmentSize));

        Segments result = Segments.create(segmentList, limitInLastSegment);

        assertThat(result).isInstanceOf(MultiSegments.class);
        assertThat(result.totalMemorySize()).isEqualTo((long) segmentSize * 2);
    }

    @Test
    void testCreateWithEmptySegmentsList() {
        Segments segments = Segments.create(new ArrayList<>(), 100);
        assertThat(segments.totalMemorySize()).isEqualTo(0);
    }
}
