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
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RowIdSequenceBuilderTest {

    @Test
    public void testEmptyRanges() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new RowIdSequenceBuilder(Collections.emptyList()));
    }

    @Test
    public void testSingleRange() {
        Collection<Range> ranges = Collections.singletonList(Range.of(10, 20));
        RowIdSequenceBuilder builder = new RowIdSequenceBuilder(ranges);
        // Test building the whole sequence
        for (int i = 0; i < 10; i++) {
            builder.more();
        }
        assertEquals(new RowIdSequence(10, 20), builder.build());
    }

    @Test
    public void testMultipleRanges() {
        Collection<Range> ranges = Arrays.asList(Range.of(1, 3), Range.of(5, 8), Range.of(10, 11));
        RowIdSequenceBuilder builder = new RowIdSequenceBuilder(ranges);

        // Build the whole sequence
        for (int i = 0; i < 2; i++) {
            builder.more();
        }
        for (int i = 0; i < 3; i++) {
            builder.more();
        }
        for (int i = 0; i < 1; i++) {
            builder.more();
        }
        assertEquals(new RowIdSequence(1, 3, 5, 8, 10, 11), builder.build());
    }

    @Test
    public void testMultipleRangesSplit() {
        Collection<Range> ranges = Arrays.asList(Range.of(1, 3), Range.of(5, 8), Range.of(10, 15));
        RowIdSequenceBuilder builder = new RowIdSequenceBuilder(ranges);

        // Build the whole sequence
        for (int i = 0; i < 1; i++) {
            builder.more();
        }
        assertEquals(new RowIdSequence(1, 2), builder.build());
        for (int i = 0; i < 5; i++) {
            builder.more();
        }
        assertEquals(new RowIdSequence(2, 3, 5, 8, 10, 11), builder.build());
        for (int i = 0; i < 4; i++) {
            builder.more();
        }
        assertEquals(new RowIdSequence(11, 15), builder.build());
    }

    @Test
    public void testNoMore() {
        Collection<Range> ranges = Collections.singletonList(Range.of(0, 5));
        RowIdSequenceBuilder builder = new RowIdSequenceBuilder(ranges);
        for (int i = 0; i < 5; i++) {
            builder.more();
        }
        builder.build();
        assertThrows(IllegalStateException.class, builder::more);
    }

    @Test
    public void testOverlappingRanges() {
        Collection<Range> ranges = Arrays.asList(Range.of(1, 5), Range.of(3, 7));
        assertThrows(IllegalArgumentException.class, () -> new RowIdSequenceBuilder(ranges));
    }

    @Test
    public void testAdjacentRanges() {
        Collection<Range> ranges = Arrays.asList(Range.of(1, 5), Range.of(5, 10));
        RowIdSequenceBuilder builder = new RowIdSequenceBuilder(ranges);

        for (int i = 0; i < 9; i++) {
            builder.more();
        }
        assertEquals(new RowIdSequence(1, 10), builder.build());
    }

    @Test
    public void testUnsortedRanges() {
        Collection<Range> ranges = Arrays.asList(Range.of(5, 10), Range.of(1, 5));
        RowIdSequenceBuilder builder = new RowIdSequenceBuilder(ranges);

        for (int i = 0; i < 9; i++) {
            builder.more();
        }
        assertEquals(new RowIdSequence(1, 10), builder.build());
    }
}
