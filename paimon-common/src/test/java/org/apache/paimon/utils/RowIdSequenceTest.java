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

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowIdSequence}. */
public class RowIdSequenceTest {

    @Test
    void testSingleRangeConstructor() {
        RowIdSequence sequence = new RowIdSequence(0, 10);
        assertThat(sequence.size()).isEqualTo(1);
        assertThat(sequence.get(0)).isEqualTo(Range.of(0, 10));
        assertThat(sequence.toRowNumber(5)).isEqualTo(5);
        assertThat(sequence.toRowNumber(0)).isEqualTo(0);
        assertThat(sequence.toRowNumber(9)).isEqualTo(9);
        assertThat(sequence.toRowNumber(10)).isEqualTo(-1);
        assertThat(sequence.toRowNumber(-1)).isEqualTo(-1);
        assertThat(sequence.toString()).isEqualTo("RowIdSequence{[0, 10],}");
    }

    @Test
    void testSingleRangeConstructorWithInvalidEnd() {
        assertThatThrownBy(() -> new RowIdSequence(10, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("End must be greater than start.");
        assertThatThrownBy(() -> new RowIdSequence(10, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("End must be greater than start.");
    }

    @Test
    void testMultiRangeConstructor() {
        RowIdSequence sequence = new RowIdSequence(0, 5, 10, 15, 20, 25);
        assertThat(sequence.size()).isEqualTo(3);
        assertThat(sequence.get(0)).isEqualTo(Range.of(0, 5));
        assertThat(sequence.get(1)).isEqualTo(Range.of(10, 15));
        assertThat(sequence.get(2)).isEqualTo(Range.of(20, 25));

        // Test toRowNumber
        assertThat(sequence.toRowNumber(0)).isEqualTo(0);
        assertThat(sequence.toRowNumber(4)).isEqualTo(4);
        assertThat(sequence.toRowNumber(10)).isEqualTo(5); // 0-4 (5 elements) + 0 for next range
        assertThat(sequence.toRowNumber(14))
                .isEqualTo(9); // 0-4 (5 elements) + 0-4 for next range (5 elements) = 9
        assertThat(sequence.toRowNumber(20)).isEqualTo(10); // 0-4 (5) + 0-4 (5) + 0 for next range
        assertThat(sequence.toRowNumber(24)).isEqualTo(14); // 0-4 (5) + 0-4 (5) + 0-4 (5) = 14

        assertThat(sequence.toRowNumber(5)).isEqualTo(-1); // Not in any range
        assertThat(sequence.toRowNumber(7)).isEqualTo(-1); // Not in any range
        assertThat(sequence.toRowNumber(15)).isEqualTo(-1); // Not in any range
        assertThat(sequence.toRowNumber(25)).isEqualTo(-1); // Not in any range
        assertThat(sequence.toString()).isEqualTo("RowIdSequence{[0, 5],[10, 15],[20, 25],}");
    }

    @Test
    void testMultiRangeConstructorInvalidArguments() {
        // Odd number of elements
        assertThatThrownBy(() -> new RowIdSequence(0, 5, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RowIdSequence must have even number of elements.");

        // End not greater than start within a range
        assertThatThrownBy(() -> new RowIdSequence(0, 5, 10, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("End must be greater than start for range: [10, 10]");
        assertThatThrownBy(() -> new RowIdSequence(0, 5, 10, 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("End must be greater than start for range: [10, 8]");

        // Overlapping or non-increasing ranges
        assertThatThrownBy(() -> new RowIdSequence(0, 5, 4, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Start of the current range must be greater than end of the previous range: [5, 4]");
        assertThatThrownBy(() -> new RowIdSequence(0, 5, 5, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Start of the current range must be greater than end of the previous range: [5, 5]");
    }

    @Test
    void testEmptyRangesInConstructor() {
        assertThatThrownBy(() -> new RowIdSequence(1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RowIdSequence must have even number of elements.");
    }

    @Test
    void testGetMethodInvalidIndex() {
        RowIdSequence sequence = new RowIdSequence(0, 10);
        assertThatThrownBy(() -> sequence.get(1))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("Index out of bounds.");
        assertThatThrownBy(() -> sequence.get(-1))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("Index out of bounds.");

        RowIdSequence multiSequence = new RowIdSequence(0, 5, 10, 15);
        assertThatThrownBy(() -> multiSequence.get(2))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("Index out of bounds.");
    }

    @Test
    void testIterator() {
        RowIdSequence sequence = new RowIdSequence(0, 5, 10, 15);
        Iterator<Range> iterator = sequence.iterator();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(Range.of(0, 5));
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(Range.of(10, 15));
        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testEqualsAndHashCode() {
        RowIdSequence seq1 = new RowIdSequence(0, 10);
        RowIdSequence seq2 = new RowIdSequence(0, 10);
        RowIdSequence seq3 = new RowIdSequence(0, 5, 10, 15);
        RowIdSequence seq4 = new RowIdSequence(0, 5, 10, 15);
        RowIdSequence seq5 = new RowIdSequence(1, 10); // Different start
        RowIdSequence seq6 = new RowIdSequence(0, 11); // Different end

        // Test equality
        assertThat(seq1).isEqualTo(seq2);
        assertThat(seq3).isEqualTo(seq4);
        assertThat(seq1).isNotEqualTo(seq3);
        assertThat(seq1).isNotEqualTo(seq5);
        assertThat(seq1).isNotEqualTo(seq6);
        assertThat(seq1).isNotEqualTo(null);
        assertThat(seq1).isNotEqualTo("some string");

        // Test hashCode
        assertThat(seq1.hashCode()).isEqualTo(seq2.hashCode());
        assertThat(seq3.hashCode()).isEqualTo(seq4.hashCode());
        assertThat(seq1.hashCode()).isNotEqualTo(seq3.hashCode());
        assertThat(seq1.hashCode()).isNotEqualTo(seq5.hashCode());
        assertThat(seq1.hashCode()).isNotEqualTo(seq6.hashCode());
    }

    @Test
    void testLargeRowNumbers() {
        RowIdSequence sequence = new RowIdSequence(Long.MAX_VALUE - 10, Long.MAX_VALUE - 5);
        assertThat(sequence.size()).isEqualTo(1);
        assertThat(sequence.get(0)).isEqualTo(Range.of(Long.MAX_VALUE - 10, Long.MAX_VALUE - 5));
        assertThat(sequence.toRowNumber(Long.MAX_VALUE - 8)).isEqualTo(2);

        // Multi-range with large numbers
        RowIdSequence multiSeq = new RowIdSequence(0, 1, Long.MAX_VALUE - 10, Long.MAX_VALUE - 5);
        assertThat(multiSeq.size()).isEqualTo(2);
        assertThat(multiSeq.toRowNumber(0)).isEqualTo(0);
        assertThat(multiSeq.toRowNumber(Long.MAX_VALUE - 10))
                .isEqualTo(1); // The second range starts at global offset 1
        assertThat(multiSeq.toRowNumber(Long.MAX_VALUE - 6))
                .isEqualTo(5); // 1 + (MAX-6 - (MAX-10)) = 1 + 4 = 5
    }
}
