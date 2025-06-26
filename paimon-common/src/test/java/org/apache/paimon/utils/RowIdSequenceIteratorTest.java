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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RowIdSequenceIteratorTest {

    @Test
    public void testAllNull() {
        RowIdSequenceIterator reader = new RowIdSequenceIterator(null);
        assertNull(reader.next());
    }

    @Test
    public void testSingleRange() {
        RowIdSequence sequence = new RowIdSequence(Arrays.asList(Range.of(10, 15)));
        RowIdSequenceIterator reader = new RowIdSequenceIterator(sequence);

        assertEquals(10L, reader.next());
        assertEquals(11L, reader.next());
        assertEquals(12L, reader.next());
        assertEquals(13L, reader.next());
        assertEquals(14L, reader.next());
        assertNull(reader.next());
    }

    @Test
    public void testMultipleRanges() {
        RowIdSequence sequence =
                new RowIdSequence(Arrays.asList(Range.of(1, 3), Range.of(5, 8), Range.of(10, 11)));
        RowIdSequenceIterator reader = new RowIdSequenceIterator(sequence);

        assertEquals(1L, reader.next());
        assertEquals(2L, reader.next());
        assertEquals(5L, reader.next());
        assertEquals(6L, reader.next());
        assertEquals(7L, reader.next());
        assertEquals(10L, reader.next());
        assertNull(reader.next());
    }

    @Test
    public void testLargeRanges() {
        RowIdSequence sequence =
                new RowIdSequence(Arrays.asList(Range.of(0, 100), Range.of(200, 300)));
        RowIdSequenceIterator reader = new RowIdSequenceIterator(sequence);

        for (long i = 0; i < 100; i++) {
            assertEquals(i, reader.next());
        }
        for (long i = 200; i < 300; i++) {
            assertEquals(i, reader.next());
        }
        assertNull(reader.next());
    }

    @Test
    public void testSingleElementRanges() {
        RowIdSequence sequence =
                new RowIdSequence(
                        Arrays.asList(Range.of(5, 6), Range.of(10, 11), Range.of(15, 16)));
        RowIdSequenceIterator reader = new RowIdSequenceIterator(sequence);

        assertEquals(5L, reader.next());
        assertEquals(10L, reader.next());
        assertEquals(15L, reader.next());
        assertNull(reader.next());
    }
}
